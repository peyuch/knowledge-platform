"""Elasticsearch store for BM25 full-text indexing with idempotent _id."""

import logging
from typing import Any

from elasticsearch import Elasticsearch, helpers

from core.config import settings

logger = logging.getLogger(__name__)

INDEX_NAME = "chunks"

ES_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "30s",
        "analysis": {
            "analyzer": {
                "zh_analyzer": {
                    "type": "ik_max_word",
                    "use_smart": True,
                }
            }
        },
    },
    "mappings": {
        "dynamic": False,
        "properties": {
            "chunk_id":     {"type": "keyword"},
            "doc_id":       {"type": "keyword"},
            "heading_path": {"type": "keyword"},
            "content":      {"type": "text", "analyzer": "zh_analyzer", "term_vector": "with_positions_offsets"},
            "department":   {"type": "keyword"},
            "file_type":    {"type": "keyword"},
            "granularity":  {"type": "keyword"},
            "page_start":   {"type": "integer"},
            "page_end":     {"type": "integer"},
            "token_count":  {"type": "integer"},
            "trace_id":     {"type": "keyword"},
            "created_at":   {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        },
    },
}


class ESStore:
    def __init__(self, es_url: str = settings.es_url):
        self._client = Elasticsearch(es_url)

    def ensure_index(self) -> None:
        if not self._client.indices.exists(index=INDEX_NAME):
            self._client.indices.create(index=INDEX_NAME, body=ES_MAPPING)
            logger.info(f"Created ES index '{INDEX_NAME}'")

    def bulk_index(self, docs: list[dict[str, Any]]) -> tuple[int, list[dict]]:
        """Bulk-index using streaming_bulk for per-document error tracking. Returns (ok_count, errors)."""
        actions = []
        for doc in docs:
            source = doc.get("_source", doc)
            source.pop("_source", None)
            source.pop("offset", None)
            source.pop("partition", None)
            actions.append({
                "_index": INDEX_NAME,
                "_id": doc.get("_id", source.get("chunk_id")),
                "_source": source,
            })

        ok_count = 0
        errors = []
        for ok, result in helpers.streaming_bulk(
            self._client,
            actions,
            raise_on_error=False,
            raise_on_exception=False,
        ):
            if ok:
                ok_count += 1
            else:
                errors.append(result)
                logger.warning(f"ES indexing error for {result.get('index', {}).get('_id', '?')}: {result.get('index', {}).get('error', {})}")

        if errors:
            logger.warning(f"ES bulk had {len(errors)} errors out of {len(actions)} docs")
        return ok_count, errors

    def close(self) -> None:
        self._client.close()
