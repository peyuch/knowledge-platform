"""Celery task: postprocess a document's GraphRAG entities after all chunks extracted."""

import logging

from celery import Task
from core.celery import app
from core.config import settings
from services.graphrag.neo4j_store import Neo4jStore
from services.graphrag.entity_normalizer import EntityNormalizer

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="workers.graph_postprocessor.postprocess_doc",
    max_retries=3,
    default_retry_delay=60,
)
def postprocess_doc(self: Task, doc_id: str):
    store = Neo4jStore()
    normalizer = EntityNormalizer()

    try:
        nodes = store.get_pending_nodes(doc_id)
        if not nodes:
            logger.info(f"No pending nodes for doc {doc_id}, skipping postprocess")
            return

        # Normalize entities
        normalized = normalizer.normalize(nodes)

        # Find merged entities (vector similarity > 0.85)
        merged_ids = [n["entity_id"] for n in normalized if len(n.get("aliases", [])) > 1]
        if merged_ids:
            store.merge_entities(merged_ids)

        # Remove :PendingResolution
        store.remove_pending_label(doc_id)
        logger.info(f"Postprocessed doc {doc_id}: {len(nodes)} entities resolved")

    except Exception as exc:
        logger.error(f"Postprocess failed for doc {doc_id}: {exc}")
        raise self.retry(exc=exc)
    finally:
        store.close()
