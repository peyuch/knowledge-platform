"""Celery Beat: scans Redis for RAPTOR docs past timeout, forces build."""

import json
import logging

from core.celery import app
from core.redis import get_sync_redis
from services.raptor.raptor_store import RaptorStore
from common.constants import RAPTOR_BUILD_TIMEOUT_MINUTES, RAPTOR_REDIS_TTL_SECONDS

logger = logging.getLogger(__name__)


@app.task(name="workers.raptor_timeout_scanner.scan_timeouts")
def scan_timeouts():
    redis_client = get_sync_redis()
    store = RaptorStore()

    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor, match="raptor:count:*", count=100)
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode()
            doc_id = key_str.split(":")[-1]

            # Check TTL -- if near expiry, force build
            ttl = redis_client.ttl(key_str)
            timeout_seconds = RAPTOR_BUILD_TIMEOUT_MINUTES * 60
            buffer_max_age = RAPTOR_REDIS_TTL_SECONDS - timeout_seconds

            if ttl < 0:
                continue  # already expired
            if ttl > buffer_max_age:
                continue  # too fresh, not timed out yet

            count = int(redis_client.get(key_str) or 0)
            if count == 0:
                continue

            logger.info(f"Timeout force-build for doc {doc_id} ({count} chunks)")
            raw_chunks = redis_client.lrange(f"raptor:buffer:{doc_id}", 0, -1)
            # Clean up Redis keys
            redis_client.delete(f"raptor:buffer:{doc_id}", key_str)

            if not raw_chunks:
                continue

            try:
                chunks = [json.loads(c) for c in raw_chunks]
                leaf_nodes = store.create_leaf_nodes(chunks, doc_id)
                store.build_tree(doc_id, leaf_nodes, trace_id="timeout-scanner")
            except Exception:
                logger.exception(f"Timeout force-build failed for doc {doc_id}")

        if cursor == 0:
            break
