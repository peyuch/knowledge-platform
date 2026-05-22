"""Milvus Lite store for vector upsert with idempotent primary key."""

import logging
from typing import Any

from pymilvus import (
    Collection,
    FieldSchema,
    CollectionSchema,
    DataType,
    connections,
    utility,
)

from core.config import settings
from common.constants import INDEX_EMBEDDING_DIM

logger = logging.getLogger(__name__)

COLLECTION_NAME = "chunks"


class MilvusStore:
    def __init__(self, db_path: str = settings.milvus_db_path):
        self.db_path = db_path
        connections.connect("default", uri=db_path)

    def ensure_collection(self) -> None:
        if utility.has_collection(COLLECTION_NAME):
            return

        fields = [
            FieldSchema(name="chunk_id",     dtype=DataType.VARCHAR, is_primary=True, max_length=36),
            FieldSchema(name="embedding",    dtype=DataType.FLOAT_VECTOR, dim=INDEX_EMBEDDING_DIM),
            FieldSchema(name="doc_id",       dtype=DataType.VARCHAR, max_length=36),
            FieldSchema(name="department",   dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="file_type",    dtype=DataType.VARCHAR, max_length=16),
            FieldSchema(name="granularity",  dtype=DataType.VARCHAR, max_length=16),
            FieldSchema(name="page_start",   dtype=DataType.INT32),
            FieldSchema(name="page_end",     dtype=DataType.INT32),
            FieldSchema(name="token_count",  dtype=DataType.INT32),
            FieldSchema(name="trace_id",     dtype=DataType.VARCHAR, max_length=36),
            FieldSchema(name="created_at",   dtype=DataType.INT64),
        ]

        schema = CollectionSchema(
            fields,
            description="SMALL chunk vectors for semantic retrieval",
            enable_dynamic_field=False,
        )

        collection = Collection(COLLECTION_NAME, schema)

        index_params = {
            "index_type": "IVF_FLAT",
            "metric_type": "IP",
            "params": {"nlist": 128},
        }
        collection.create_index("embedding", index_params)

        collection.create_index("doc_id",     index_name="idx_doc_id")
        collection.create_index("department", index_name="idx_department")

        collection.load(consistency_level="BoundedConsistency")
        logger.info(f"Created Milvus collection '{COLLECTION_NAME}'")

    def upsert(self, rows: list[dict[str, Any]]) -> None:
        """Upsert rows by chunk_id (primary key). Duplicate IDs overwrite."""
        collection = Collection(COLLECTION_NAME)
        collection.load()
        collection.upsert(rows)
        collection.flush()

    def close(self) -> None:
        connections.disconnect("default")
