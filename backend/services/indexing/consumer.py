"""Kafka consumer with batch buffer, backpressure, graceful shutdown, and degradation."""

import json
import logging
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError

from core.config import settings
from services.indexing.embedder import Embedder
from services.indexing.milvus_store import MilvusStore
from services.indexing.es_store import ESStore
from common.constants import (
    INDEX_BATCH_SIZE_DEFAULT,
    INDEX_BATCH_TIMEOUT_DEFAULT,
    INDEX_BUFFER_MAX_SIZE,
    INDEX_DLQ_MAX_RETRY,
    INDEX_MAX_POLL_RECORDS,
    INDEX_SUB_BATCH_SIZE,
    INDEX_SINGLE_MAX_RETRIES,
    INDEX_BATCH_BACKOFF_BASE,
    INDEX_SINGLE_BACKOFF_BASE,
)
from common.metrics import (
    kafka_consumer_messages_consumed,
    indexing_batch_size_avg,
    indexing_embedding_duration,
    indexing_milvus_write_duration,
    indexing_es_write_duration,
    indexing_batch_success,
    indexing_batch_failure,
    indexing_dlq_messages,
)

logger = logging.getLogger(__name__)


class IndexConsumer:
    def __init__(self):
        self.buffer: list[dict[str, Any]] = []
        self._shutdown_flag = False
        self._last_flush_time = time.time()
        self._consumer: KafkaConsumer | None = None
        self._embedder: Embedder | None = None
        self._milvus: MilvusStore | None = None
        self._es: ESStore | None = None
        self._raptor_thread: threading.Thread | None = None

    # -- Entry Point --------------------------------------------------

    def run(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._embedder = Embedder()
        self._milvus = MilvusStore()
        self._es = ESStore()

        self._milvus.ensure_collection()
        self._es.ensure_index()

        # Start RAPTOR mini-consumer thread for summary ingestion
        self._start_raptor_mini_consumer()

        self._consumer = KafkaConsumer(
            settings.kafka_topic_chunks,
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            group_id=settings.index_kafka_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=INDEX_MAX_POLL_RECORDS,
            fetch_max_wait_ms=500,
            fetch_min_bytes=1024,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
            max_poll_interval_ms=300000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        # Drain buffer before losing partition ownership during rebalance
        self._consumer.subscribe(
            [settings.kafka_topic_chunks],
            on_revoke=self._on_partitions_revoked,
        )

        try:
            self._main_loop()
        finally:
            self._graceful_shutdown()

    # -- Signal -------------------------------------------------------

    def _handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown_flag = True

    # -- Rebalance ----------------------------------------------------

    def _on_partitions_revoked(self, consumer, partitions):
        """Drain buffer and commit offsets before losing partition ownership."""
        logger.warning(f"Partitions revoked: {partitions}, draining buffer...")
        while self.buffer:
            batch = self.buffer[:settings.index_batch_size]
            self.buffer = self.buffer[settings.index_batch_size:]
            try:
                self._degraded_process(batch)
            except Exception as e:
                logger.error(f"Failed to flush on revoke: {e}")
        consumer.commit()

    # -- Main Loop ----------------------------------------------------

    def _main_loop(self):
        while not self._shutdown_flag:
            # === Backpressure ===
            if len(self.buffer) >= INDEX_BUFFER_MAX_SIZE:
                extra = len(self.buffer) // INDEX_BATCH_SIZE_DEFAULT
                sleep_sec = 1.5 ** (extra - 1) if extra > 0 else 0
                logger.warning(
                    f"Buffer overflow ({len(self.buffer)}/{INDEX_BUFFER_MAX_SIZE}), "
                    f"backing off {sleep_sec:.1f}s"
                )
                time.sleep(sleep_sec)
                self._try_flush()
                continue

            # === Poll Kafka ===
            records = self._consumer.poll(timeout_ms=1000, max_records=INDEX_MAX_POLL_RECORDS)
            for tp, messages in records.items():
                for msg in messages:
                    value = msg.value
                    if value.get("granularity") != "SMALL":
                        continue
                    self.buffer.append({
                        "chunk_id":     value["chunk_id"],
                        "content":      value["content"],
                        "doc_id":       value["doc_id"],
                        "department":   value.get("metadata", {}).get("department", ""),
                        "file_type":    value.get("metadata", {}).get("file_type", ""),
                        "granularity":  value["granularity"],
                        "page_start":   value.get("page_start"),
                        "page_end":     value.get("page_end"),
                        "token_count":  value.get("token_count", 0),
                        "trace_id":     value.get("trace_id", ""),
                        "heading_path": value.get("heading_path", []),
                        "created_at":   value.get("created_at", ""),
                        "offset":       msg.offset,
                        "partition":    msg.partition,
                    })
                    kafka_consumer_messages_consumed.labels(topic=settings.kafka_topic_chunks).inc()

            # === Flush check ===
            if len(self.buffer) >= settings.index_batch_size or (
                self.buffer and (time.time() - self._last_flush_time) >= settings.index_batch_timeout
            ):
                self._try_flush()

    # -- Flush --------------------------------------------------------

    def _try_flush(self):
        batch = self.buffer[:settings.index_batch_size]
        self.buffer = self.buffer[settings.index_batch_size:]

        try:
            self._process_batch(batch)
            self._commit_offsets(batch)
            self._last_flush_time = time.time()
            indexing_batch_success.inc()
        except Exception as e:
            logger.error(f"Batch flush failed: {e}")
            indexing_batch_failure.inc()
            self._degraded_process(batch)

    def _process_batch(self, batch):
        indexing_batch_size_avg.observe(len(batch))

        # Embed
        t0 = time.time()
        texts = [item["content"] for item in batch]
        vectors = self._embedder.encode(texts)
        indexing_embedding_duration.observe(time.time() - t0)

        # Build Milvus rows + ES docs
        milvus_rows = []
        es_docs = []
        for item, vec in zip(batch, vectors):
            milvus_row = {
                "chunk_id":     item["chunk_id"],
                "embedding":    vec.tolist(),
                "doc_id":       item["doc_id"],
                "department":   item["department"],
                "file_type":    item["file_type"],
                "granularity":  item["granularity"],
                "page_start":   item["page_start"],
                "page_end":     item["page_end"],
                "token_count":  item["token_count"],
                "trace_id":     item["trace_id"],
                "created_at":   item["created_at"],
            }
            milvus_rows.append(milvus_row)

            es_docs.append({
                "_id": item["chunk_id"],
                "_source": {k: v for k, v in item.items() if k not in ("offset", "partition")},
            })

        # Parallel writes
        import concurrent.futures
        t1 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            milvus_future = pool.submit(self._milvus.upsert, milvus_rows)
            es_future = pool.submit(self._es.bulk_index, es_docs)

            milvus_future.result()
            t_milvus = time.time() - t1
            indexing_milvus_write_duration.observe(t_milvus)

            es_future.result()
            t_es = time.time() - t1 - t_milvus
            indexing_es_write_duration.observe(t_es)

    # -- Offset Commit ------------------------------------------------

    def _commit_offsets(self, batch):
        max_offsets: dict[tuple[str, int], int] = {}
        for item in batch:
            key = (settings.kafka_topic_chunks, item["partition"])
            max_offsets[key] = max(max_offsets.get(key, 0), item["offset"] + 1)
        for (topic, partition), offset in max_offsets.items():
            self._consumer.commit(offsets={
                TopicPartition(topic, partition): OffsetAndMetadata(offset, None)
            })

    # -- Degradation --------------------------------------------------

    def _degraded_process(self, batch):
        # Retry full batch 3 times with exponential backoff
        for attempt in range(3):
            try:
                self._process_batch(batch)
                self._commit_offsets(batch)
                return
            except Exception:
                wait = INDEX_BATCH_BACKOFF_BASE * (2 ** attempt)
                logger.warning(f"Batch retry {attempt+1}/3, waiting {wait}s")
                time.sleep(wait)

        # Split to sub-batches (10 records each)
        if len(batch) > INDEX_SUB_BATCH_SIZE:
            logger.info(f"Splitting batch of {len(batch)} into sub-batches of {INDEX_SUB_BATCH_SIZE}")
            for i in range(0, len(batch), INDEX_SUB_BATCH_SIZE):
                self._degraded_process(batch[i : i + INDEX_SUB_BATCH_SIZE])
            return

        # Single-record processing
        for item in batch:
            for attempt in range(INDEX_SINGLE_MAX_RETRIES):
                try:
                    self._process_batch([item])
                    break
                except Exception as e:
                    if attempt == INDEX_SINGLE_MAX_RETRIES - 1:
                        self._write_dlq(item, e)
                    else:
                        time.sleep(INDEX_SINGLE_BACKOFF_BASE * (2 ** attempt))

        self._commit_offsets(batch)

    def _write_dlq(self, item: dict, error: Exception):
        import traceback
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from models.dead_letter_index import DeadLetterIndex

        engine = create_engine(settings.database_url_sync)
        Session = sessionmaker(bind=engine)
        with Session() as db:
            record = DeadLetterIndex(
                chunk_id=item["chunk_id"],
                topic=settings.kafka_topic_chunks,
                partition=item.get("partition", 0),
                kafka_offset=item.get("offset", 0),
                payload=item,
                error_type=type(error).__name__,
                error_message=str(error)[:2000],
                retry_count=0,
            )
            db.add(record)
            db.commit()

        indexing_dlq_messages.labels(error_type=type(error).__name__).inc()
        logger.error(f"DLQ: chunk {item['chunk_id']} failed after {INDEX_SINGLE_MAX_RETRIES} retries: {error}")

    # -- RAPTOR Mini-Consumer -----------------------------------------

    def _start_raptor_mini_consumer(self):
        """Background thread: consumes knowledge.raptor.summaries, writes to Milvus."""
        def _run():
            from kafka import KafkaConsumer
            import json as _json

            consumer = KafkaConsumer(
                "knowledge.raptor.summaries",
                bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
                group_id=f"{settings.index_kafka_group_id}-raptor",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: _json.loads(m.decode("utf-8")),
            )
            for msg in consumer:
                if self._shutdown_flag:
                    break
                try:
                    value = msg.value
                    action = value.get("action", "updated")
                    chunk_id = value["chunk_id"]

                    if action == "deleted":
                        # Remove from Milvus
                        try:
                            from pymilvus import Collection
                            coll = Collection("chunks")
                            coll.delete(expr=f'chunk_id == "{chunk_id}"')
                        except Exception as e:
                            logger.warning(f"RAPTOR mini-consumer: failed to delete {chunk_id}: {e}")
                        continue

                    # action == "updated": embed and upsert
                    vector = self._embedder.encode([value["content"]])[0]
                    row = {
                        "chunk_id": chunk_id,
                        "embedding": vector.tolist(),
                        "doc_id": value.get("doc_id", ""),
                        "department": value.get("metadata", {}).get("department", ""),
                        "file_type": value.get("metadata", {}).get("file_type", ""),
                        "granularity": "SUMMARY",
                        "page_start": None,
                        "page_end": None,
                        "token_count": value.get("token_count", 0),
                        "trace_id": value.get("trace_id", ""),
                        "created_at": value.get("created_at", int(datetime.now(timezone.utc).timestamp())),
                    }
                    self._milvus.upsert([row])
                except Exception as e:
                    logger.error(f"RAPTOR mini-consumer error: {e}")

            consumer.close()

        self._raptor_thread = threading.Thread(target=_run, daemon=True)
        self._raptor_thread.start()
        logger.info("RAPTOR mini-consumer thread started")

    # -- Graceful Shutdown --------------------------------------------

    def _graceful_shutdown(self):
        logger.info(f"Graceful shutdown: draining buffer ({len(self.buffer)} items)...")
        while self.buffer:
            batch = self.buffer[:settings.index_batch_size]
            self.buffer = self.buffer[settings.index_batch_size:]
            try:
                self._degraded_process(batch)
            except ConnectionError as e:
                logger.critical(f"Connection lost during drain, aborting: {e}")
                break  # Downstream is gone, avoid infinite retries leading to SIGKILL
            except Exception as e:
                logger.error(f"Failed to flush during shutdown: {e}")

        if self._consumer:
            try:
                self._consumer.commit()
            except Exception:
                pass
            self._consumer.close()
            logger.info("Kafka consumer closed")

        if self._milvus:
            self._milvus.close()
            logger.info("Milvus connection closed")

        if self._es:
            self._es.close()
            logger.info("Elasticsearch connection closed")

        logger.info("Graceful shutdown complete")
