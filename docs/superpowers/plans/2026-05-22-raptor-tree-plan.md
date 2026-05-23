# RAPTOR 递归摘要树 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a RAPTOR recursive summary tree from LARGE chunks — Redis count-based trigger, UMAP+GMM soft clustering, LLM summarization, stored to PostgreSQL raptor_nodes and pushed to Kafka for #2's mini-consumer to write into Milvus.

**Architecture:** Independent Kafka consumer process buffers LARGE chunks in Redis per doc_id. When all chunks arrive (count == total_chunk_count) or a 30-minute timeout fires, the doc's tree is built in one shot: UMAP reduces 384-dim embeddings to 5, GMM soft-clusters (probability > 0.2), LLM generates per-cluster summaries, recursing up to 3 levels with token-count early termination. Result nodes are written to PostgreSQL raptor_nodes and published to Kafka `knowledge.raptor.summaries` with `action` ("updated"/"deleted"). #2's mini-consumer handles Milvus upsert/delete.

**Tech Stack:** Python 3.11, Redis, Kafka, PostgreSQL (pgvector), scikit-learn (GMM), umap-learn, httpx (LLM API), Celery Beat (timeout scanner)

---

### Task 1: RAPTOR constants and config

**Files:**
- Modify: `d:/knowledge-platform/backend/common/constants.py` — add RAPTOR_* constants
- Modify: `d:/knowledge-platform/backend/core/config.py` — add raptor_* settings

- [ ] **Step 1: Read current files**

Read `d:/knowledge-platform/backend/common/constants.py` and `d:/knowledge-platform/backend/core/config.py`.

- [ ] **Step 2: Add constants to common/constants.py**

```python
# --- RAPTOR ---
RAPTOR_MAX_DEPTH = 3
RAPTOR_STOP_CLUSTERING_TOKENS = 1000
RAPTOR_BUILD_TIMEOUT_MINUTES = 30
RAPTOR_REDIS_TTL_SECONDS = 3600
RAPTOR_GMM_PROB_THRESHOLD = 0.2
RAPTOR_UMAP_N_COMPONENTS = 5
RAPTOR_TARGET_TOKENS_PER_CLUSTER = 1500
RAPTOR_LLM_MAX_TOKENS = 600
RAPTOR_LLM_BACKUP_TIMEOUT = 30
RAPTOR_KAFKA_TOPIC = "knowledge.raptor.summaries"
```

- [ ] **Step 3: Add config to core/config.py**

```python
# RAPTOR
raptor_llm_api_url: str = ""
raptor_llm_api_key: str = ""
raptor_llm_model: str = "deepseek-chat"
raptor_backup_llm_api_url: str = ""
raptor_backup_llm_api_key: str = ""
raptor_backup_llm_model: str = "qwen-turbo"
```

- [ ] **Step 4: Verify imports**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from common.constants import RAPTOR_MAX_DEPTH; from core.config import settings; print(settings.raptor_llm_model); print('OK')"`
Expected: `deepseek-chat` then `OK`

- [ ] **Step 5: Commit**

```bash
cd d:/knowledge-platform && git add backend/common/constants.py backend/core/config.py && git commit -m "feat: add RAPTOR constants and config"
```

---

### Task 2: raptor_node ORM model

**Files:**
- Create: `d:/knowledge-platform/backend/models/raptor_node.py`
- Modify: `d:/knowledge-platform/backend/models/__init__.py`

- [ ] **Step 1: Write the failing test**

Create `d:/knowledge-platform/backend/tests/unit/models/test_raptor_node.py`:

```python
"""Tests for RaptorNode ORM model."""

import uuid
from datetime import datetime, timezone
from models.raptor_node import RaptorNode


def test_raptor_node_creation():
    node = RaptorNode(
        id=uuid.uuid4(),
        doc_id=uuid.uuid4(),
        level=0,
        node_type="LEAF",
        content="原文内容",
        token_count=500,
        heading_path=["第一章", "1.1 概述"],
        created_at=datetime.now(timezone.utc),
    )
    assert node.level == 0
    assert node.node_type == "LEAF"
    assert node.token_count == 500


def test_raptor_node_summary_type():
    node = RaptorNode(
        id=uuid.uuid4(),
        doc_id=uuid.uuid4(),
        parent_id=uuid.uuid4(),
        level=2,
        node_type="SUMMARY",
        cluster_label=3,
        content="摘要内容",
        token_count=300,
        source_chunk_ids=[uuid.uuid4(), uuid.uuid4()],
    )
    assert node.node_type == "SUMMARY"
    assert node.cluster_label == 3
    assert len(node.source_chunk_ids) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/models/test_raptor_node.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create models/raptor_node.py**

```python
"""RAPTOR tree node — one node per LARGE chunk leaf or summary cluster."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, SmallInteger, Integer, Text, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, ARRAY

from models import Base


class RaptorNode(Base):
    __tablename__ = "raptor_nodes"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    doc_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("raptor_nodes.id", ondelete="CASCADE"), nullable=True
    )
    chunk_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )

    level: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    node_type: Mapped[str] = mapped_column(String(16), nullable=False)     # LEAF / SUMMARY / ROOT
    cluster_label: Mapped[int | None] = mapped_column(Integer, nullable=True)

    content: Mapped[str] = mapped_column(Text, nullable=False)
    token_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    heading_path: Mapped[list[str] | None] = mapped_column(ARRAY(Text), default=list)
    source_chunk_ids: Mapped[list[uuid.UUID] | None] = mapped_column(
        ARRAY(UUID(as_uuid=True)), default=list
    )

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

Add: `from models.raptor_node import RaptorNode` and `"RaptorNode"` to `__all__`.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/models/test_raptor_node.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
cd d:/knowledge-platform && git add backend/models/raptor_node.py backend/models/__init__.py backend/tests/unit/models/ && git commit -m "feat: add RaptorNode ORM model"
```

---

### Task 3: RAPTOR clusterer — UMAP + GMM soft clustering

**Files:**
- Create: `d:/knowledge-platform/backend/services/raptor/__init__.py`
- Create: `d:/knowledge-platform/backend/services/raptor/clusterer.py`

- [ ] **Step 1: Write the failing test**

Create `d:/knowledge-platform/backend/tests/unit/services/raptor/__init__.py` (empty)

Create `d:/knowledge-platform/backend/tests/unit/services/raptor/test_clusterer.py`:

```python
"""Tests for UMAP+GMM soft clustering."""

import numpy as np
from services.raptor.clusterer import soft_cluster, should_stop_clustering

def test_soft_cluster_returns_probabilities():
    rng = np.random.RandomState(42)
    embeddings = rng.randn(20, 384).astype(np.float32)
    probs = soft_cluster(embeddings, n_components=3)
    assert probs.shape == (20, 3)
    assert np.allclose(probs.sum(axis=1), 1.0, atol=1e-2)

def test_soft_cluster_handles_fewer_points_than_components():
    rng = np.random.RandomState(42)
    embeddings = rng.randn(3, 384).astype(np.float32)
    probs = soft_cluster(embeddings, n_components=5)
    assert probs.shape[0] == 3

def test_should_stop_by_depth():
    nodes = [{"token_count": 2000}, {"token_count": 2000}]
    assert should_stop_clustering(3, nodes) is True
    assert should_stop_clustering(2, nodes) is False

def test_should_stop_by_token_count():
    nodes = [{"token_count": 300}, {"token_count": 400}]
    assert should_stop_clustering(0, nodes) is True

def test_should_stop_by_node_count():
    nodes = [{"token_count": 2000}]
    assert should_stop_clustering(0, nodes) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/services/raptor/test_clusterer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create services/raptor/__init__.py**

```python
"""RAPTOR recursive summary tree — clustering, summarization, tree storage."""
```

- [ ] **Step 4: Create services/raptor/clusterer.py**

```python
"""UMAP dimensionality reduction + GMM soft clustering for RAPTOR."""

import numpy as np
from umap import UMAP
from sklearn.mixture import GaussianMixture

from common.constants import (
    RAPTOR_MAX_DEPTH,
    RAPTOR_STOP_CLUSTERING_TOKENS,
    RAPTOR_UMAP_N_COMPONENTS,
    RAPTOR_GMM_PROB_THRESHOLD,
    RAPTOR_TARGET_TOKENS_PER_CLUSTER,
)


def soft_cluster(embeddings: np.ndarray, n_components: int) -> np.ndarray:
    """UMAP reduce → GMM fit → soft probability matrix (N, K).

    Returns P[i][k] = probability node i belongs to cluster k.
    """
    n = len(embeddings)
    if n < 2:
        return np.ones((n, 1), dtype=np.float32)

    k = max(2, min(n_components, n))

    reducer = UMAP(n_components=min(RAPTOR_UMAP_N_COMPONENTS, n - 1), random_state=42)
    reduced = reducer.fit_transform(embeddings)

    gmm = GaussianMixture(n_components=k, random_state=42)
    gmm.fit(reduced)
    return gmm.predict_proba(reduced)


def assign_clusters(probs: np.ndarray, threshold: float = RAPTOR_GMM_PROB_THRESHOLD) -> list[list[int]]:
    """Convert probability matrix to cluster assignments (soft — one node can belong to multiple clusters)."""
    k = probs.shape[1]
    clusters = [[] for _ in range(k)]
    for i in range(probs.shape[0]):
        for j in range(k):
            if probs[i][j] >= threshold:
                clusters[j].append(i)
    return clusters


def calculate_optimal_k(nodes: list[dict]) -> int:
    """K = total_tokens / 1500, clamped to [2, 10]."""
    total = sum(n["token_count"] for n in nodes)
    return max(2, min(10, int(total / RAPTOR_TARGET_TOKENS_PER_CLUSTER)))


def should_stop_clustering(current_level: int, nodes: list[dict]) -> bool:
    """Check stop conditions: max depth, too few nodes, or total tokens below threshold."""
    if current_level >= RAPTOR_MAX_DEPTH:
        return True
    if len(nodes) < 2:
        return True
    if sum(n["token_count"] for n in nodes) < RAPTOR_STOP_CLUSTERING_TOKENS:
        return True
    return False
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/services/raptor/test_clusterer.py -v`
Expected: 5 passed

- [ ] **Step 6: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/raptor/ backend/tests/unit/services/raptor/ && git commit -m "feat: add UMAP+GMM soft clustering for RAPTOR"
```

---

### Task 4: RAPTOR summarizer — LLM API client

**Files:**
- Create: `d:/knowledge-platform/backend/services/raptor/summarizer.py`

- [ ] **Step 1: Create services/raptor/summarizer.py**

```python
"""LLM API client for generating RAPTOR cluster summaries with primary/backup fallback."""

import logging
import httpx

from core.config import settings
from common.constants import RAPTOR_LLM_MAX_TOKENS

logger = logging.getLogger(__name__)

SUMMARY_PROMPT = """你是一个企业文档摘要助手。以下是一个文档章节的多个段落，请生成一个200-500字的摘要，保留关键事实、数字、制度和流程名称。用中文输出。

原文:
{text}

摘要:"""


class LLMClient:
    def __init__(self, api_url: str, api_key: str, model: str):
        self.api_url = api_url
        self.api_key = api_key
        self.model = model

    async def complete(self, prompt: str, max_tokens: int = RAPTOR_LLM_MAX_TOKENS) -> str:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{self.api_url}/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                    "temperature": 0.3,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]


class Summarizer:
    def __init__(self):
        self._primary = LLMClient(
            api_url=settings.raptor_llm_api_url,
            api_key=settings.raptor_llm_api_key,
            model=settings.raptor_llm_model,
        )
        self._backup = LLMClient(
            api_url=settings.raptor_backup_llm_api_url,
            api_key=settings.raptor_backup_llm_api_key,
            model=settings.raptor_backup_llm_model,
        )

    async def summarize(self, texts: list[str]) -> str:
        """Generate a summary for a cluster of text chunks."""
        combined = "\n\n---\n\n".join(texts)
        prompt = SUMMARY_PROMPT.format(text=combined[:3000])

        try:
            return await self._primary.complete(prompt)
        except Exception as primary_error:
            logger.warning(f"Primary LLM failed, trying backup: {primary_error}")
            try:
                return await self._backup.complete(prompt)
            except Exception as backup_error:
                logger.error(f"Both LLMs failed. Primary: {primary_error}, Backup: {backup_error}")
                raise


_summarizer: Summarizer | None = None


def get_summarizer() -> Summarizer:
    global _summarizer
    if _summarizer is None:
        _summarizer = Summarizer()
    return _summarizer
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.raptor.summarizer import Summarizer; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/raptor/summarizer.py && git commit -m "feat: add RAPTOR summarizer with LLM primary/backup fallback"
```

---

### Task 5: RAPTOR store — raptor_nodes CRUD + tree build orchestrator

**Files:**
- Create: `d:/knowledge-platform/backend/services/raptor/raptor_store.py`

- [ ] **Step 1: Create services/raptor/raptor_store.py**

```python
"""RaptorNode CRUD + tree build orchestrator."""

import uuid
import json
import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from core.config import settings
from core.kafka import get_kafka
from services.raptor.clusterer import (
    soft_cluster,
    assign_clusters,
    calculate_optimal_k,
    should_stop_clustering,
)
from services.raptor.summarizer import get_summarizer
from models.raptor_node import RaptorNode
from common.constants import RAPTOR_KAFKA_TOPIC

logger = logging.getLogger(__name__)


class RaptorStore:
    def __init__(self):
        engine = create_engine(settings.database_url_sync)
        self.Session = sessionmaker(bind=engine)

    # ── CRUD ────────────────────────────────────────────

    def create_leaf_nodes(self, chunks: list[dict], doc_id: str) -> list[dict]:
        """Create level=0 LEAF nodes from LARGE chunks. Returns node dicts."""
        nodes = []
        with self.Session() as db:
            for chunk in chunks:
                node = RaptorNode(
                    doc_id=uuid.UUID(doc_id),
                    chunk_id=uuid.UUID(chunk["chunk_id"]),
                    level=0,
                    node_type="LEAF",
                    content=chunk["content"],
                    token_count=chunk.get("token_count", 0),
                    heading_path=chunk.get("heading_path", []),
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
                db.add(node)
                db.commit()
                db.refresh(node)
                nodes.append({
                    "id": str(node.id),
                    "doc_id": doc_id,
                    "chunk_id": chunk["chunk_id"],
                    "level": 0,
                    "node_type": "LEAF",
                    "content": node.content,
                    "token_count": node.token_count,
                    "heading_path": node.heading_path,
                    "embedding": chunk.get("embedding"),  # from ES/Milvus or pgvector
                })
        return nodes

    def create_summary_node(
        self,
        doc_id: str,
        parent_ids: list[str],
        level: int,
        cluster_label: int,
        content: str,
        token_count: int,
        source_chunk_ids: list[str],
        heading_path: list[str],
    ) -> dict:
        """Create a SUMMARY or ROOT node."""
        with self.Session() as db:
            node = RaptorNode(
                doc_id=uuid.UUID(doc_id),
                level=level,
                node_type="ROOT" if level >= 3 else "SUMMARY",
                cluster_label=cluster_label,
                content=content,
                token_count=token_count,
                source_chunk_ids=[uuid.UUID(cid) for cid in source_chunk_ids],
                heading_path=heading_path,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            db.add(node)
            db.commit()
            db.refresh(node)
            return {
                "id": str(node.id),
                "doc_id": doc_id,
                "level": level,
                "node_type": node.node_type,
                "cluster_label": cluster_label,
                "content": content,
                "token_count": token_count,
                "source_chunk_ids": source_chunk_ids,
                "heading_path": heading_path,
            }

    # ── Tree Build ───────────────────────────────────────

    def build_tree(self, doc_id: str, leaf_nodes: list[dict], trace_id: str) -> None:
        """Build RAPTOR tree from leaf nodes. Pushes SUMMARY nodes to Kafka."""
        total_tokens = sum(n["token_count"] for n in leaf_nodes)
        if total_tokens < 1000:
            logger.info(f"Doc {doc_id}: {total_tokens} tokens — skipping clustering (short doc)")
            return

        self._build_level(leaf_nodes, level=0, doc_id=doc_id, trace_id=trace_id)

    def _build_level(
        self, nodes: list[dict], level: int, doc_id: str, trace_id: str
    ) -> None:
        if should_stop_clustering(level, nodes):
            return

        embeddings = np.array([n.get("embedding") or self._get_embedding(n["id"]) for n in nodes], dtype=np.float32)
        k = calculate_optimal_k(nodes)
        probs = soft_cluster(embeddings, n_components=k)
        clusters = assign_clusters(probs)

        summarizer = get_summarizer()
        kafka = get_kafka()
        summary_nodes = []

        for cluster_idx, member_indices in enumerate(clusters):
            if len(member_indices) < 2:
                continue

            cluster_nodes = [nodes[i] for i in member_indices]
            texts = [n["content"] for n in cluster_nodes]
            summary_text = summarizer.summarize(texts)

            import asyncio
            if asyncio.iscoroutine(summary_text):
                summary_text = asyncio.get_event_loop().run_until_complete(summary_text)
            # In production: run in async context

            token_count = len(summary_text)  # approximate
            heading = f"第{level + 1}层摘要 — 簇{cluster_idx + 1}"
            source_ids = list({n.get("chunk_id", n["id"]) for n in cluster_nodes})

            node = self.create_summary_node(
                doc_id=doc_id,
                parent_ids=[n["id"] for n in cluster_nodes],
                level=level + 1,
                cluster_label=cluster_idx,
                content=summary_text,
                token_count=token_count,
                source_chunk_ids=source_ids,
                heading_path=[heading],
            )
            summary_nodes.append(node)

            # Push to Kafka for #2 mini-consumer
            msg = {
                "action": "updated",
                "event": "raptor.node_updated",
                "chunk_id": node["id"],
                "doc_id": doc_id,
                "parent_id": cluster_nodes[0].get("chunk_id"),
                "heading_level": "SUMMARY",
                "granularity": "SUMMARY",
                "heading_path": node["heading_path"],
                "content": summary_text,
                "content_hash": hashlib.md5(summary_text.encode()).hexdigest(),
                "level": level + 1,
                "node_type": node["node_type"],
                "source_chunk_ids": source_ids,
                "token_count": token_count,
                "trace_id": trace_id,
                "metadata": {},
            }
            kafka.send_single(msg)

        if summary_nodes:
            self._build_level(summary_nodes, level + 1, doc_id, trace_id)

    def _get_embedding(self, node_id: str) -> list[float]:
        """Get embedding from pgvector. Stub — real impl uses pgvector query."""
        with self.Session() as db:
            result = db.execute(
                text("SELECT embedding FROM raptor_nodes WHERE id = :id"),
                {"id": node_id},
            )
            row = result.fetchone()
            return list(row[0]) if row and row[0] else [0.0] * 384

    def delete_doc_nodes(self, doc_id: str) -> None:
        """Delete all nodes for a document (except level=0 if specified)."""
        with self.Session() as db:
            db.query(RaptorNode).filter(RaptorNode.doc_id == uuid.UUID(doc_id)).delete()
            db.commit()
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.raptor.raptor_store import RaptorStore; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/raptor/raptor_store.py && git commit -m "feat: add RAPTOR store with tree build orchestrator"
```

---

### Task 6: RAPTOR consumer — Redis buffer + count trigger

**Files:**
- Create: `d:/knowledge-platform/backend/services/raptor/consumer.py`

- [ ] **Step 1: Create services/raptor/consumer.py**

```python
"""RAPTOR Kafka consumer — Redis-buffers LARGE chunks per doc_id, triggers tree build on count match."""

import json
import logging
import signal
import time
from typing import Any

from kafka import KafkaConsumer
from redis import Redis

from core.config import settings
from core.redis import get_redis
from services.raptor.raptor_store import RaptorStore
from common.constants import (
    RAPTOR_REDIS_TTL_SECONDS,
    RAPTOR_KAFKA_TOPIC,
)

logger = logging.getLogger(__name__)


class RaptorConsumer:
    def __init__(self):
        self._redis: Redis = get_redis()
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

                    if count >= total:
                        self._build_doc(doc_id, value.get("trace_id", ""))

            self._consumer.commit()

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

        leaf_nodes = self._store.create_leaf_nodes(chunks, doc_id)
        self._store.build_tree(doc_id, leaf_nodes, trace_id)

    def _graceful_shutdown(self):
        if self._consumer:
            self._consumer.commit()
            self._consumer.close()
        logger.info("RAPTOR consumer shut down")
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.raptor.consumer import RaptorConsumer; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/raptor/consumer.py && git commit -m "feat: add RAPTOR consumer with Redis buffer and count trigger"
```

---

### Task 7: RAPTOR timeout scanner + process entry point

**Files:**
- Create: `d:/knowledge-platform/backend/workers/raptor_timeout_scanner.py`
- Create: `d:/knowledge-platform/backend/workers/tasks/raptor.py`
- Modify: `d:/knowledge-platform/backend/core/celery.py` — add beat entry

- [ ] **Step 1: Create workers/raptor_timeout_scanner.py**

```python
"""Celery Beat: scans Redis for RAPTOR docs past timeout, forces build."""

import json
import logging
from datetime import datetime, timezone

from core.celery import app
from core.redis import get_redis
from services.raptor.raptor_store import RaptorStore
from common.constants import RAPTOR_BUILD_TIMEOUT_MINUTES

logger = logging.getLogger(__name__)


@app.task(name="workers.raptor_timeout_scanner.scan_timeouts")
def scan_timeouts():
    redis = get_redis()
    store = RaptorStore()

    cursor = 0
    while True:
        cursor, keys = redis.scan(cursor, match="raptor:count:*", count=100)
        for key in keys:
            doc_id = key.decode().split(":")[-1]
            # Check TTL — if near expiry, force build
            ttl = redis.ttl(key)
            if ttl < 0 or ttl > (3600 - RAPTOR_BUILD_TIMEOUT_MINUTES * 60):
                continue  # already expired or too fresh

            count = int(redis.get(key) or 0)
            if count == 0:
                continue

            logger.info(f"Timeout force-build for doc {doc_id} ({count} chunks)")
            raw_chunks = redis.lrange(f"raptor:buffer:{doc_id}", 0, -1)
            redis.delete(f"raptor:buffer:{doc_id}", key)

            chunks = [json.loads(c) for c in raw_chunks]
            leaf_nodes = store.create_leaf_nodes(chunks, doc_id)
            store.build_tree(doc_id, leaf_nodes, trace_id="timeout-scanner")

        if cursor == 0:
            break
```

- [ ] **Step 2: Create workers/tasks/raptor.py (process entry point)**

```python
"""RAPTOR Consumer process entry point.

Usage:
    python -m workers.tasks.raptor
"""

from services.raptor.consumer import RaptorConsumer


def main():
    consumer = RaptorConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Add Celery Beat entry in core/celery.py**

```python
"raptor-timeout-scan": {
    "task": "workers.raptor_timeout_scanner.scan_timeouts",
    "schedule": 600.0,  # every 10 minutes
},
```

Also add `"workers.raptor_timeout_scanner"` to the `include` list.

- [ ] **Step 4: Commit**

```bash
cd d:/knowledge-platform && git add backend/workers/raptor_timeout_scanner.py backend/workers/tasks/raptor.py backend/core/celery.py && git commit -m "feat: add RAPTOR timeout scanner, process entry point, and beat schedule"
```

---

### Task 8: #2 mini-consumer for RAPTOR summaries

**Files:**
- Modify: `d:/knowledge-platform/backend/services/indexing/consumer.py` — add mini-consumer thread

- [ ] **Step 1: Add mini-consumer thread to IndexConsumer**

Add to `IndexConsumer.__init__`:
```python
import threading
self._raptor_thread: threading.Thread | None = None
```

Add method:
```python
def _start_raptor_mini_consumer(self):
    """Background thread: consumes knowledge.raptor.summaries, writes to Milvus."""
    def _run():
        from kafka import KafkaConsumer
        import json
        consumer = KafkaConsumer(
            "knowledge.raptor.summaries",
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            group_id=f"{settings.index_kafka_group_id}-raptor",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        for msg in consumer:
            value = msg.value
            action = value.get("action", "updated")
            chunk_id = value["chunk_id"]

            if action == "deleted":
                # Remove from Milvus
                from pymilvus import Collection
                coll = Collection("chunks")
                coll.delete(expr=f'chunk_id == "{chunk_id}"')
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
                "created_at": int(datetime.now(timezone.utc).timestamp()),
            }
            self._milvus.upsert([row])

        consumer.close()

    self._raptor_thread = threading.Thread(target=_run, daemon=True)
    self._raptor_thread.start()
```

Add `self._start_raptor_mini_consumer()` at the end of `run()` after `self._es.ensure_index()`.

- [ ] **Step 2: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/indexing/consumer.py && git commit -m "feat: add RAPTOR mini-consumer thread to #2 IndexConsumer"
```

---

### Task 9: Alembic migration for raptor_nodes table

**Files:**
- Create: migration file

- [ ] **Step 1: Generate migration**

If PostgreSQL is running:
```bash
cd d:/knowledge-platform/backend && alembic revision --autogenerate -m "add_raptor_nodes"
```

Otherwise, manually create the migration with the `raptor_nodes` table DDL from the spec.

- [ ] **Step 2: Commit**

```bash
cd d:/knowledge-platform && git add backend/migrations/ && git commit -m "feat: add raptor_nodes table migration"
```

---

### Task 10: Install RAPTOR dependencies

**Files:**
- Modify: `d:/knowledge-platform/backend/pyproject.toml` — add umap-learn, scikit-learn

- [ ] **Step 1: Add dependencies**

```toml
"umap-learn>=0.5.6",
"scikit-learn>=1.5.0",
"httpx>=0.28.0",
```

- [ ] **Step 2: Install**

```bash
cd d:/knowledge-platform && uv pip install -e "backend[dev]" -i https://mirrors.aliyun.com/pypi/simple
```

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/pyproject.toml && git commit -m "feat: add umap-learn, scikit-learn, and httpx for RAPTOR"
```

---

## Remaining Tasks (Phase 2)

| Task | Files | What |
|------|-------|------|
| 11 | `tests/unit/services/raptor/test_summarizer.py` | TDD for summarizer with mock LLM |
| 12 | `tests/unit/services/raptor/test_consumer.py` | TDD for Redis buffer + count trigger |
| 13 | `tests/unit/services/raptor/test_raptor_store.py` | TDD for create_leaf_nodes + build_tree |
| 14 | #2 Milvus/ES schema update | Add SUMMARY to granularity enum, update ES mapping + Milvus schema |
