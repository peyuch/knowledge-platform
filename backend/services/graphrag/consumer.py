"""GraphRAG Kafka consumer — real-time extraction + Redis count + emit Celery postprocess."""

import json
import logging
import signal
from typing import Any

from kafka import KafkaConsumer

from core.config import settings
from core.redis import get_sync_redis
from services.graphrag.extractor import Extractor
from services.graphrag.neo4j_store import Neo4jStore
from services.indexing.embedder import Embedder
from common.constants import GRAPHRAG_REDIS_TTL_SECONDS

logger = logging.getLogger(__name__)


class GraphRagConsumer:
    def __init__(self):
        self._redis = get_sync_redis()
        self._neo4j = Neo4jStore()
        self._extractor = Extractor()
        self._embedder = Embedder()
        self._shutdown_flag = False
        self._consumer: KafkaConsumer | None = None

    def run(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._neo4j.ensure_schema()

        self._consumer = KafkaConsumer(
            settings.kafka_topic_chunks,
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            group_id="graphrag-v1",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=100,
        )

        try:
            self._main_loop()
        finally:
            self._graceful_shutdown()

    def _handle_signal(self, signum, frame):
        self._shutdown_flag = True

    def _main_loop(self):
        import asyncio
        loop = asyncio.new_event_loop()

        while not self._shutdown_flag:
            records = self._consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    value = msg.value
                    text = value.get("content", "")
                    if not text.strip():
                        continue

                    # LLM extraction (async)
                    try:
                        triples = loop.run_until_complete(self._extractor.extract(text))
                    except Exception as e:
                        logger.error(f"Extraction failed for chunk {value.get('chunk_id')}: {e}")
                        self._write_dlq(value, e)
                        continue

                    if not triples:
                        continue

                    # Collect unique entity names -> batch embed
                    entity_names = set()
                    for t in triples:
                        entity_names.add(t["entity1"]["name"])
                        entity_names.add(t["entity2"]["name"])
                    embeddings = self._embedder.encode(list(entity_names))
                    name_to_emb = dict(zip(entity_names, embeddings.tolist()))

                    # Build Neo4j entities/relations
                    import uuid
                    entities = []
                    relations = []
                    seen_entities = set()

                    for t in triples:
                        for side in ("entity1", "entity2"):
                            ent = t[side]
                            if ent["name"] not in seen_entities:
                                seen_entities.add(ent["name"])
                                entities.append({
                                    "entity_id": str(uuid.uuid4()),
                                    "name": ent["name"],
                                    "type": ent.get("type", "Unknown"),
                                    "embedding": name_to_emb.get(ent["name"], [0.0] * 384),
                                    "aliases": ent.get("aliases", []),
                                    "confidence": t["relation"].get("confidence", 0.5),
                                    "access_level": "internal",
                                    "department": value.get("metadata", {}).get("department", ""),
                                    "doc_id": value["doc_id"],
                                })

                        rel = t["relation"]
                        relations.append({
                            "relation_id": str(uuid.uuid4()),
                            "type": rel["type"],
                            "source_entity_id": next(e["entity_id"] for e in entities if e["name"] == t["entity1"]["name"]),
                            "target_entity_id": next(e["entity_id"] for e in entities if e["name"] == t["entity2"]["name"]),
                            "confidence": rel.get("confidence", 0.5),
                            "start_date": rel.get("start_date"),
                            "end_date": rel.get("end_date"),
                            "chunk_id": value["chunk_id"],
                            "doc_id": value["doc_id"],
                            "page_number": value.get("page_start"),
                            "sentence": text[:200],
                        })

                    # Write to Neo4j
                    self._neo4j.upsert_entities_and_relations(entities, relations)

                    # Redis count
                    doc_id = value["doc_id"]
                    pipe = self._redis.pipeline()
                    pipe.incr(f"graphrag:count:{doc_id}")
                    pipe.expire(f"graphrag:count:{doc_id}", GRAPHRAG_REDIS_TTL_SECONDS)
                    pipe.execute()

                    count = int(self._redis.get(f"graphrag:count:{doc_id}") or 0)
                    total = value.get("metadata", {}).get("total_chunk_count", 0)
                    if count >= total:
                        from workers.graph_postprocessor import postprocess_doc
                        postprocess_doc.delay(doc_id=doc_id)
                        self._redis.delete(f"graphrag:count:{doc_id}")

            self._consumer.commit()

    def _write_dlq(self, chunk_value: dict, error: Exception):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from models.dead_letter_graphrag import DeadLetterGraphrag

        engine = create_engine(settings.database_url_sync)
        Session = sessionmaker(bind=engine)
        with Session() as db:
            db.add(DeadLetterGraphrag(
                chunk_id=chunk_value.get("chunk_id", ""),
                topic=settings.kafka_topic_chunks,
                partition=0,
                kafka_offset=0,
                payload=chunk_value,
                error_type=type(error).__name__,
                error_message=str(error)[:2000],
                retry_count=0,
            ))
            db.commit()

    def _graceful_shutdown(self):
        if self._consumer:
            self._consumer.commit()
            self._consumer.close()
        self._neo4j.close()
        logger.info("GraphRAG consumer shut down")
