"""RAPTOR Kafka consumer -- Redis-buffers LARGE chunks per doc_id, triggers tree build on count match."""

import json
import logging
import signal
import time

from kafka import KafkaConsumer

from core.config import settings
from core.redis import get_sync_redis
from services.raptor.raptor_store import RaptorStore
from common.constants import RAPTOR_REDIS_TTL_SECONDS

logger = logging.getLogger(__name__)


class RaptorConsumer:
    def __init__(self):
        self._redis = get_sync_redis()
        self._store = RaptorStore()
        self._shutdown_flag = False
        self._consumer: KafkaConsumer | None = None

    def run(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._consumer = KafkaConsumer(
            settings.kafka_topic_chunks,
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            group_id="raptor-v1",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=500,
            fetch_max_wait_ms=500,
        )

        try:
            self._main_loop()
        finally:
            self._graceful_shutdown()

    def _handle_signal(self, signum, frame):
        logger.info(f"RAPTOR consumer received signal {signum}")
        self._shutdown_flag = True

    def _main_loop(self):
        while not self._shutdown_flag:
            records = self._consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        value = msg.value
                        if value.get("granularity") != "LARGE":
                            continue

                        doc_id = value["doc_id"]
                        chunk_json = json.dumps(value, ensure_ascii=False)

                        # Buffer in Redis per doc_id
                        pipe = self._redis.pipeline()
                        pipe.rpush(f"raptor:buffer:{doc_id}", chunk_json)
                        pipe.incr(f"raptor:count:{doc_id}")
                        pipe.expire(f"raptor:buffer:{doc_id}", RAPTOR_REDIS_TTL_SECONDS)
                        pipe.expire(f"raptor:count:{doc_id}", RAPTOR_REDIS_TTL_SECONDS)
                        pipe.execute()

                        # Check if all chunks arrived
                        count = int(self._redis.get(f"raptor:count:{doc_id}") or 0)
                        total = value.get("metadata", {}).get("total_chunk_count", 0)

                        if total > 0 and count >= total:
                            self._build_doc(doc_id, value.get("trace_id", ""))
                    except Exception:
                        logger.exception("Error processing RAPTOR message")

            try:
                self._consumer.commit()
            except Exception:
                logger.exception("Error committing RAPTOR offsets")

    def _build_doc(self, doc_id: str, trace_id: str):
        """Fetch all buffered chunks from Redis, build RAPTOR tree, clean up."""
        # Atomically get and delete buffer
        pipe = self._redis.pipeline()
        pipe.lrange(f"raptor:buffer:{doc_id}", 0, -1)
        pipe.delete(f"raptor:buffer:{doc_id}", f"raptor:count:{doc_id}")
        results = pipe.execute()
        raw_chunks = results[0]

        if not raw_chunks:
            return

        chunks = [json.loads(c) for c in raw_chunks]
        logger.info(f"Building RAPTOR tree for doc {doc_id} ({len(chunks)} LARGE chunks)")

        try:
            leaf_nodes = self._store.create_leaf_nodes(chunks, doc_id)
            self._store.build_tree(doc_id, leaf_nodes, trace_id)
        except Exception:
            logger.exception(f"Failed to build RAPTOR tree for doc {doc_id}")

    def _graceful_shutdown(self):
        if self._consumer:
            try:
                self._consumer.commit()
            except Exception:
                pass
            self._consumer.close()
        logger.info("RAPTOR consumer shut down")
