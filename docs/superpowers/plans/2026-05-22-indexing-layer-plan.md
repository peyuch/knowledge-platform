# 索引与存储层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an independent Kafka consumer process that reads SMALL chunks, embeds them via sentence-transformer, and dual-writes to Milvus Lite + Elasticsearch with idempotent upsert, backpressure, graceful shutdown, and DLQ recovery.

**Architecture:** Standalone Python process (not Celery) polls Kafka with `enable_auto_commit=False`, buffers 200 records / 3 seconds, batch-encodes via `all-MiniLM-L6-v2` (384-dim IP), parallel-upserts to Milvus and ES using `chunk_id` as primary key, commits offset manually after both succeed. On failure: exponential backoff (3x) → split to 10-record sub-batches → split to singles → DLQ after 5 failures. Backpressure pauses polling when buffer exceeds 2000 records. SIGTERM drains buffer before exiting.

**Tech Stack:** Python 3.11, kafka-python, sentence-transformers, pymilvus (Milvus Lite), elasticsearch-py, structlog, prometheus-client

---

### Task 1: Update constants and config for indexing

**Files:**
- Modify: `d:/knowledge-platform/backend/common/constants.py` — add INDEX_* constants
- Modify: `d:/knowledge-platform/backend/core/config.py` — add ES/Milvus/indexing settings

- [ ] **Step 1: Read current files**

Read `d:/knowledge-platform/backend/common/constants.py` and `d:/knowledge-platform/backend/core/config.py`.

- [ ] **Step 2: Add constants to common/constants.py**

```python
# --- Indexing ---
INDEX_BATCH_SIZE_DEFAULT = 200
INDEX_BATCH_TIMEOUT_DEFAULT = 3.0
INDEX_BUFFER_MAX_SIZE = 2000
INDEX_DLQ_MAX_RETRY = 10
INDEX_MAX_POLL_RECORDS = 500
INDEX_EMBEDDING_DIM = 384
INDEX_EMBEDDING_MODEL = "all-MiniLM-L6-v2"
INDEX_METRICS_PORT_DEFAULT = 9090
INDEX_SUB_BATCH_SIZE = 10
INDEX_SINGLE_MAX_RETRIES = 5
INDEX_BATCH_BACKOFF_BASE = 5  # seconds: 5, 10, 20
INDEX_SINGLE_BACKOFF_BASE = 2  # seconds: 2, 4, 8, 16, 32
INDEX_HEALTH_PORT = 9090
```

- [ ] **Step 3: Add config to core/config.py**

```python
# Elasticsearch
es_url: str = "http://localhost:9200"

# Milvus
milvus_db_path: str = "data/milvus.db"

# Index Consumer
index_batch_size: int = 200
index_batch_timeout: float = 3.0
index_buffer_max_size: int = 2000
index_kafka_group_id: str = "indexing-v1"
index_metrics_port: int = 9090
```

- [ ] **Step 4: Verify imports**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from common.constants import INDEX_BATCH_SIZE_DEFAULT; from core.config import settings; print(settings.es_url); print('OK')"`
Expected: `http://localhost:9200` then `OK`

- [ ] **Step 5: Commit**

```bash
cd d:/knowledge-platform && git add backend/common/constants.py backend/core/config.py && git commit -m "feat: add indexing constants and ES/Milvus config"
```

---

### Task 2: DLQ ORM model and schema DTO

**Files:**
- Create: `d:/knowledge-platform/backend/models/dead_letter_index.py`
- Create: `d:/knowledge-platform/backend/schemas/index_dlq.py`
- Modify: `d:/knowledge-platform/backend/models/__init__.py`

- [ ] **Step 1: Write the failing test**

Create `d:/knowledge-platform/backend/tests/unit/models/test_dead_letter_index.py`:

```python
"""Tests for DeadLetterIndex ORM model."""

import uuid
from datetime import datetime, timezone
from models.dead_letter_index import DeadLetterIndex


def test_dead_letter_index_creation():
    dlq = DeadLetterIndex(
        id=uuid.uuid4(),
        chunk_id="chunk-abc-123",
        topic="knowledge.ingestion.chunks",
        partition=0,
        kafka_offset=42,
        payload={"chunk_id": "chunk-abc-123", "content": "test"},
        error_type="ValueError",
        error_message="Embedding failed",
        retry_count=0,
        created_at=datetime.now(timezone.utc),
    )
    assert dlq.chunk_id == "chunk-abc-123"
    assert dlq.kafka_offset == 42
    assert dlq.retry_count == 0


def test_dead_letter_index_defaults():
    dlq = DeadLetterIndex(
        chunk_id="c1",
        topic="t",
        partition=0,
        kafka_offset=1,
        payload={},
    )
    assert dlq.retry_count == 0
    assert dlq.created_at is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/models/test_dead_letter_index.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create models/dead_letter_index.py**

```python
"""Dead letter queue for indexing failures."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Integer, BigInteger, SmallInteger, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB

from models import Base


class DeadLetterIndex(Base):
    __tablename__ = "dead_letter_index"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    chunk_id: Mapped[str] = mapped_column(String(36), nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(256), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(SmallInteger, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
```

- [ ] **Step 4: Update models/__init__.py**

Add the import:
```python
from models.dead_letter_index import DeadLetterIndex
```
And add `"DeadLetterIndex"` to `__all__`.

- [ ] **Step 5: Create schemas/index_dlq.py**

```python
"""Pydantic schemas for indexing DLQ management API."""

from datetime import datetime
from pydantic import BaseModel


class DlqItem(BaseModel):
    id: str
    chunk_id: str
    topic: str
    partition: int
    kafka_offset: int
    error_type: str | None = None
    error_message: str | None = None
    retry_count: int
    created_at: datetime

    model_config = {"from_attributes": True}


class DlqListResponse(BaseModel):
    items: list[DlqItem]
    total: int
    page: int
    page_size: int


class DlqRetryResponse(BaseModel):
    id: str
    status: str


class DlqDeleteResponse(BaseModel):
    id: str
    status: str
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/models/test_dead_letter_index.py -v`
Expected: 2 passed

- [ ] **Step 7: Commit**

```bash
cd d:/knowledge-platform && git add backend/models/dead_letter_index.py backend/models/__init__.py backend/schemas/index_dlq.py backend/tests/unit/models/ && git commit -m "feat: add DeadLetterIndex ORM model and DLQ DTO schemas"
```

---

### Task 3: Prometheus metrics and structlog setup

**Files:**
- Create: `d:/knowledge-platform/backend/common/metrics.py`

- [ ] **Step 1: Create common/metrics.py**

```python
"""Prometheus metrics for the indexing consumer."""

from prometheus_client import Counter, Gauge, Histogram

kafka_consumer_messages_consumed = Counter(
    "kafka_consumer_messages_consumed_total",
    "Total Kafka messages consumed by the indexer",
    ["topic"],
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Consumer lag per partition",
    ["topic", "partition"],
)

indexing_batch_size_avg = Histogram(
    "indexing_batch_size",
    "Batch size distribution",
    buckets=[10, 25, 50, 100, 200, 500],
)

indexing_embedding_duration = Histogram(
    "indexing_embedding_duration_seconds",
    "Time spent on embedding per batch",
)

indexing_milvus_write_duration = Histogram(
    "indexing_milvus_write_duration_seconds",
    "Time spent writing to Milvus per batch",
)

indexing_es_write_duration = Histogram(
    "indexing_es_write_duration_seconds",
    "Time spent writing to ES per batch",
)

indexing_batch_success = Counter(
    "indexing_batch_success_total",
    "Total successfully processed batches",
)

indexing_batch_failure = Counter(
    "indexing_batch_failure_total",
    "Total failed batches",
)

indexing_dlq_messages = Counter(
    "indexing_dlq_messages_total",
    "Total messages routed to DLQ",
    ["error_type"],
)
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from common.metrics import kafka_consumer_messages_consumed; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/common/metrics.py && git commit -m "feat: add Prometheus metrics definitions for indexing"
```

---

### Task 4: Embedder — sentence-transformer wrapper

**Files:**
- Create: `d:/knowledge-platform/backend/services/indexing/__init__.py`
- Create: `d:/knowledge-platform/backend/services/indexing/embedder.py`

- [ ] **Step 1: Write the failing test**

Create `d:/knowledge-platform/backend/tests/unit/services/indexing/__init__.py` (empty)

Create `d:/knowledge-platform/backend/tests/unit/services/indexing/test_embedder.py`:

```python
"""Tests for the sentence-transformer embedder."""

import pytest
import numpy as np
from services.indexing.embedder import Embedder


@pytest.fixture(scope="module")
def embedder():
    return Embedder()


def test_embedder_batch_encode_returns_correct_dim(embedder):
    texts = ["hello world", "another sentence"]
    vectors = embedder.encode(texts)
    assert vectors.shape == (2, 384)


def test_embedder_vectors_are_normalized(embedder):
    texts = ["test normalization"]
    vectors = embedder.encode(texts)
    norms = np.linalg.norm(vectors, axis=1)
    assert np.allclose(norms, 1.0, atol=1e-4)


def test_embedder_empty_list_returns_empty(embedder):
    vectors = embedder.encode([])
    assert vectors.shape == (0, 384)


def test_embedder_single_text(embedder):
    vectors = embedder.encode(["single"])
    assert vectors.shape == (1, 384)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/services/indexing/test_embedder.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create services/indexing/embedder.py**

```python
"""Sentence-transformer wrapper for batch text embedding."""

import logging
import numpy as np
from sentence_transformers import SentenceTransformer

from common.constants import INDEX_EMBEDDING_MODEL
from core.config import settings

logger = logging.getLogger(__name__)


class Embedder:
    def __init__(self, model_name: str = INDEX_EMBEDDING_MODEL):
        self.model = SentenceTransformer(model_name)
        # Warm-up to avoid cold-start latency on first batch
        self.model.encode(["warm up text"])
        logger.info(f"Embedder initialized with model {model_name}")

    def encode(self, texts: list[str]) -> np.ndarray:
        """Batch-encode texts to normalized float32 vectors. Returns (N, 384).

        Vectors are L2-normalized so IP metric in Milvus ≡ COSINE.
        """
        if not texts:
            return np.empty((0, 384), dtype=np.float32)
        vectors = self.model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=settings.index_embed_batch_size,
        )
        return vectors.astype(np.float32)
```

- [ ] **Step 4: Create services/indexing/__init__.py**

```python
"""Indexing service layer — Kafka consumer, embedder, Milvus/ES stores."""
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/services/indexing/test_embedder.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/indexing/ backend/tests/unit/services/indexing/ && git commit -m "feat: add sentence-transformer embedder with model warmup"
```

---

### Task 5: Milvus store — collection management + upsert

**Files:**
- Create: `d:/knowledge-platform/backend/services/indexing/milvus_store.py`

- [ ] **Step 1: Create services/indexing/milvus_store.py**

```python
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
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.indexing.milvus_store import MilvusStore; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/indexing/milvus_store.py && git commit -m "feat: add Milvus Lite store with idempotent upsert"
```

---

### Task 6: Elasticsearch store — index management + bulk

**Files:**
- Create: `d:/knowledge-platform/backend/services/indexing/es_store.py`

- [ ] **Step 1: Create services/indexing/es_store.py**

```python
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
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.indexing.es_store import ESStore; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/indexing/es_store.py && git commit -m "feat: add Elasticsearch store with IK analyzer and bulk indexing"
```

---

### Task 7: Kafka consumer core — buffer, flush, degrade, backpressure, graceful shutdown

**Files:**
- Create: `d:/knowledge-platform/backend/services/indexing/consumer.py`

- [ ] **Step 1: Create services/indexing/consumer.py**

```python
"""Kafka consumer with batch buffer, backpressure, graceful shutdown, and degradation."""

import json
import logging
import signal
import time
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

    # ── Entry Point ──────────────────────────────────────

    def run(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._embedder = Embedder()
        self._milvus = MilvusStore()
        self._es = ESStore()

        self._milvus.ensure_collection()
        self._es.ensure_index()

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
        # 再平衡时先排空缓冲区, 避免提交已失去所有权的分区 offset
        self._consumer.subscribe(
            [settings.kafka_topic_chunks],
            on_revoke=self._on_partitions_revoked,
        )

        try:
            self._main_loop()
        finally:
            self._graceful_shutdown()

    # ── Signal ───────────────────────────────────────────

    def _handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown_flag = True

    # ── Rebalance ────────────────────────────────────────

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

    # ── Main Loop ────────────────────────────────────────

    def _main_loop(self):
        while not self._shutdown_flag:
            # ═══ Backpressure ═══
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

            # ═══ Poll Kafka ═══
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

            # ═══ Flush check ═══
            if len(self.buffer) >= settings.index_batch_size or (
                self.buffer and (time.time() - self._last_flush_time) >= settings.index_batch_timeout
            ):
                self._try_flush()

    # ── Flush ────────────────────────────────────────────

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

    # ── Offset Commit ────────────────────────────────────

    def _commit_offsets(self, batch):
        max_offsets: dict[tuple[str, int], int] = {}
        for item in batch:
            key = (settings.kafka_topic_chunks, item["partition"])
            max_offsets[key] = max(max_offsets.get(key, 0), item["offset"] + 1)
        for (topic, partition), offset in max_offsets.items():
            self._consumer.commit(offsets={
                TopicPartition(topic, partition): OffsetAndMetadata(offset, None)
            })

    # ── Degradation ──────────────────────────────────────

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

    # ── Graceful Shutdown ────────────────────────────────

    def _graceful_shutdown(self):
        logger.info(f"Graceful shutdown: draining buffer ({len(self.buffer)} items)...")
        while self.buffer:
            batch = self.buffer[:settings.index_batch_size]
            self.buffer = self.buffer[settings.index_batch_size:]
            try:
                self._degraded_process(batch)
            except ConnectionError as e:
                logger.critical(f"Connection lost during drain, aborting: {e}")
                break   # 下游已挂, 避免无限重试导致 SIGKILL
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
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.indexing.consumer import IndexConsumer; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/indexing/consumer.py && git commit -m "feat: add Kafka index consumer with backpressure, degradation, and graceful shutdown"
```

---

### Task 8: Consumer process entry point with health HTTP server

**Files:**
- Create: `d:/knowledge-platform/backend/workers/tasks/embed.py`

- [ ] **Step 1: Create workers/tasks/embed.py**

```python
"""Index consumer process entry point.

Starts the Kafka consumer loop + HTTP health/metrics server.

Usage:
    python -m workers.tasks.embed
    python -m workers.tasks.embed --batch-size 200 --batch-timeout 3
"""

import argparse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from core.config import settings
from services.indexing.consumer import IndexConsumer


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self._respond(200, "OK")
        elif self.path == "/ready":
            self._respond(200, "OK")  # Stub; in prod: check ES/Milvus connections
        elif self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._respond(404, "Not Found")

    def _respond(self, status: int, body: str):
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, format, *args):
        pass  # Suppress HTTP access logs


def main():
    parser = argparse.ArgumentParser(description="Index Consumer")
    parser.add_argument("--batch-size", type=int, help="Batch size for flushing")
    parser.add_argument("--batch-timeout", type=float, help="Batch timeout (seconds)")
    parser.add_argument("--buffer-max", type=int, help="Buffer max size for backpressure")
    parser.add_argument("--kafka-group", type=str, help="Kafka consumer group ID")
    args = parser.parse_args()

    if args.batch_size:
        settings.index_batch_size = args.batch_size
    if args.batch_timeout:
        settings.index_batch_timeout = args.batch_timeout
    if args.buffer_max:
        settings.index_buffer_max_size = args.buffer_max
    if args.kafka_group:
        settings.index_kafka_group_id = args.kafka_group

    # Start HTTP server for health checks + metrics
    port = settings.index_metrics_port
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    http_thread = threading.Thread(target=server.serve_forever, daemon=True)
    http_thread.start()
    print(f"Health server listening on :{port} (/health /ready /metrics)")

    # Start Kafka consumer loop (runs until SIGTERM)
    consumer = IndexConsumer()
    consumer.run()

    server.shutdown()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from workers.tasks.embed import main; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/workers/tasks/embed.py && git commit -m "feat: add index consumer entry point with health/metrics HTTP server"
```

---

### Task 9: DLQ management API

**Files:**
- Create: `d:/knowledge-platform/backend/api/routers/index_dlq.py`

- [ ] **Step 1: Create api/routers/index_dlq.py**

```python
"""DLQ management endpoints — query, retry, delete dead-lettered index records."""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update

from core.database import get_db
from models.dead_letter_index import DeadLetterIndex
from schemas.index_dlq import DlqListResponse, DlqItem, DlqRetryResponse, DlqDeleteResponse
from api.dependencies import get_current_user

router = APIRouter(prefix="/api/v1/indexing/dlq", tags=["indexing-dlq"])


@router.get("/", response_model=DlqListResponse)
async def list_dlq(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List dead-lettered index records, most recent first."""
    offset = (page - 1) * page_size

    total_result = await db.execute(select(func.count(DeadLetterIndex.id)))
    total = total_result.scalar() or 0

    items_result = await db.execute(
        select(DeadLetterIndex)
        .order_by(DeadLetterIndex.created_at.desc())
        .offset(offset)
        .limit(page_size)
    )
    items = [DlqItem.model_validate(row) for row in items_result.scalars().all()]

    return DlqListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("/{dlq_id}/retry", response_model=DlqRetryResponse)
async def retry_dlq(
    dlq_id: str,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually retry a dead-lettered message by republishing to Kafka."""
    from core.kafka import get_kafka

    result = await db.execute(select(DeadLetterIndex).where(DeadLetterIndex.id == uuid.UUID(dlq_id)))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="DLQ record not found")

    kafka = get_kafka()
    kafka.send_single(record.payload)
    record.retry_count += 1
    record.updated_at = datetime.now(timezone.utc)
    await db.commit()

    return DlqRetryResponse(id=dlq_id, status="republished")


@router.delete("/{dlq_id}", response_model=DlqDeleteResponse)
async def delete_dlq(
    dlq_id: str,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a dead-lettered record (acknowledge as handled)."""
    result = await db.execute(select(DeadLetterIndex).where(DeadLetterIndex.id == uuid.UUID(dlq_id)))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="DLQ record not found")

    await db.delete(record)
    await db.commit()

    return DlqDeleteResponse(id=dlq_id, status="deleted")
```

- [ ] **Step 2: Register router in api/main.py**

Edit `d:/knowledge-platform/backend/api/main.py`, add:
```python
from api.routers import index_dlq
app.include_router(index_dlq.router)
```

- [ ] **Step 3: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from api.routers.index_dlq import router; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
cd d:/knowledge-platform && git add backend/api/routers/index_dlq.py backend/api/main.py && git commit -m "feat: add DLQ management API for indexing dead letters"
```

---

### Task 10: DLQ retry scheduler (Celery Beat)

**Files:**
- Create: `d:/knowledge-platform/backend/workers/dlq_retry.py`
- Modify: `d:/knowledge-platform/backend/core/celery.py` — add beat schedule entry

- [ ] **Step 1: Create workers/dlq_retry.py**

```python
"""Celery Beat: retries dead-lettered index records with retry_count < 10."""

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from common.constants import INDEX_DLQ_MAX_RETRY
from core.kafka import get_kafka

logger = logging.getLogger(__name__)


@app.task(name="workers.dlq_retry.retry_dlq_records")
def retry_dlq_records():
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        from models.dead_letter_index import DeadLetterIndex

        records = db.query(DeadLetterIndex).filter(
            DeadLetterIndex.retry_count < INDEX_DLQ_MAX_RETRY
        ).limit(100).all()

        kafka = get_kafka()
        for record in records:
            try:
                kafka.send_single(record.payload)
                record.retry_count += 1
                record.updated_at = datetime.now(timezone.utc)
            except Exception as e:
                logger.warning(f"DLQ retry failed for {record.chunk_id}: {e}")

        db.commit()
        if records:
            logger.info(f"DLQ retry: republished {len(records)} records to Kafka")
```

- [ ] **Step 2: Add beat schedule entry in core/celery.py**

```python
"retry-dlq": {
    "task": "workers.dlq_retry.retry_dlq_records",
    "schedule": crontab(hour=2, minute=0),  # Daily at 2am
},
```

Also add `"workers.dlq_retry"` to the `include` list.

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/workers/dlq_retry.py backend/core/celery.py && git commit -m "feat: add DLQ retry scheduler via Celery Beat"
```

---

### Task 11: Update docker-compose with Elasticsearch and run integration tests

**Files:**
- Modify: `d:/knowledge-platform/docker/docker-compose.yml` — add ES service

- [ ] **Step 1: Add ES to docker-compose.yml**

```yaml
  elasticsearch:
    image: elasticsearch:8.15.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports: ["9200:9200"]
    volumes: [esdata:/usr/share/elasticsearch/data]
    healthcheck:
      test: ["CMD-SHELL", "curl -s http://localhost:9200/_cluster/health | grep -q 'green\\|yellow'"]
      interval: 10s
      timeout: 10s
      retries: 30
```

And add to volumes: `esdata:`

- [ ] **Step 2: Install Elasticsearch IK plugin**

After ES starts: `docker compose -f docker/docker-compose.yml exec elasticsearch bin/elasticsearch-plugin install analysis-ik`

- [ ] **Step 3: Run existing test suite to verify no regressions**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/ -v`
Expected: All tests pass (22 passed, 1 skipped for PG)

- [ ] **Step 4: Commit**

```bash
cd d:/knowledge-platform && git add docker/docker-compose.yml && git commit -m "feat: add Elasticsearch to docker-compose"
```

---

### Task 12: Alembic migration for dead_letter_index table

**Files:**
- Create: `d:/knowledge-platform/backend/migrations/versions/<auto>_add_dead_letter_index.py`

- [ ] **Step 1: Generate migration**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m alembic revision --autogenerate -m "add_dead_letter_index"`
Expected: New migration file created.

Note: If PostgreSQL is not running, manually create the migration with:

```python
def upgrade():
    op.create_table(
        "dead_letter_index",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("chunk_id", sa.String(36), nullable=False),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("partition", sa.Integer, nullable=False),
        sa.Column("kafka_offset", sa.BigInteger, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("error_type", sa.String(256)),
        sa.Column("error_message", sa.Text),
        sa.Column("retry_count", sa.SmallInteger, default=0),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_dlq_chunk_id", "dead_letter_index", ["chunk_id"])
    op.create_index("idx_dlq_retry", "dead_letter_index", ["retry_count", "created_at"],
                    postgresql_where=sa.text("retry_count < 10"))
```

- [ ] **Step 2: Commit**

```bash
cd d:/knowledge-platform && git add backend/migrations/ && git commit -m "feat: add dead_letter_index table migration"
```

---

## Remaining Tasks (Phase 2 expansion)

The following tasks extend the indexing layer with additional quality-of-life features:

| Task | Files | What |
|------|-------|------|
| 13 | `tests/unit/services/indexing/test_stores.py` | TDD for ES bulk_index + Milvus upsert |
| 14 | `tests/unit/services/indexing/test_consumer.py` | TDD for consumer buffer/flush/degrade logic |
| 15 | `backend/services/indexing/` — structlog wiring | Add structured logging to consumer, embedder, stores |
| 16 | `backend/services/indexing/` — Kafka lag metric | Periodically update `kafka_consumer_lag` gauge |
| 17 | `docker/` — ES IK plugin auto-install | Dockerfile or init script for IK analyzer |
