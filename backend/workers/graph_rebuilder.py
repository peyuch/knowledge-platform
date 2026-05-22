"""Celery Beat: nightly full rebuild of GraphRAG knowledge graphs for recently updated docs."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from services.graphrag.extractor import Extractor
from services.graphrag.neo4j_store import Neo4jStore
from models.document import Document
from models.chunk import Chunk
from common.constants import (
    GRAPHRAG_REBUILD_LOOKBACK_HOURS,
    GRAPHRAG_MAX_TOKENS_PER_BATCH,
)

logger = logging.getLogger(__name__)


@app.task(
    name="workers.graph_rebuilder.rebuild_recent_docs",
    time_limit=7200,
    soft_time_limit=6000,
)
def rebuild_recent_docs():
    import asyncio
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=GRAPHRAG_REBUILD_LOOKBACK_HOURS)

    with Session() as db:
        docs = db.query(Document).filter(Document.updated_at >= cutoff).all()

    store = Neo4jStore()
    extractor = Extractor()
    loop = asyncio.new_event_loop()

    for doc in docs:
        try:
            # Get all chunks, ordered by sequence
            with Session() as db:
                chunks = db.query(Chunk).filter(
                    Chunk.doc_id == doc.id
                ).order_by(Chunk.sequence).all()

            full_text = "\n\n".join(c.content for c in chunks)
            total_tokens = sum(c.token_count or 0 for c in chunks)

            if total_tokens > GRAPHRAG_MAX_TOKENS_PER_BATCH:
                # Split into batches
                triples = []
                for i in range(0, len(chunks), 50):
                    batch_text = "\n\n".join(c.content for c in chunks[i:i+50])
                    batch_triples = loop.run_until_complete(extractor.extract(batch_text))
                    triples.extend(batch_triples)
            else:
                triples = loop.run_until_complete(extractor.extract(full_text))

            # Build entities/relations from triples (same logic as consumer)
            import uuid
            entities = []
            relations = []
            seen = set()
            for t in triples:
                for side in ("entity1", "entity2"):
                    ent = t[side]
                    if ent["name"] not in seen:
                        seen.add(ent["name"])
                        entities.append({
                            "entity_id": str(uuid.uuid4()),
                            "name": ent["name"],
                            "type": ent.get("type", "Unknown"),
                            "embedding": [],  # Re-embed below
                            "aliases": ent.get("aliases", []),
                            "confidence": 1.0,
                            "access_level": "internal",
                            "department": "",
                            "doc_id": str(doc.id),
                        })
                rel = t["relation"]
                relations.append({
                    "relation_id": str(uuid.uuid4()),
                    "type": rel["type"],
                    "source_entity_id": next(e["entity_id"] for e in entities if e["name"] == t["entity1"]["name"]),
                    "target_entity_id": next(e["entity_id"] for e in entities if e["name"] == t["entity2"]["name"]),
                    "confidence": rel.get("confidence", 1.0),
                    "doc_id": str(doc.id),
                })

            # Shadow rebuild
            store.replace_doc_graph(str(doc.id), entities, relations)
            logger.info(f"Rebuilt graph for doc {doc.id}: {len(entities)} entities, {len(relations)} relations")

        except Exception as e:
            logger.error(f"Rebuild failed for doc {doc.id}: {e}")

    store.close()
    loop.close()
