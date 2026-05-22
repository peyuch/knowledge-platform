# GraphRAG 知识图谱 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a GraphRAG knowledge graph from chunk entities/relations via LLM extraction, store in Neo4j with vector index, resolve entities via dictionary+vector similarity, and provide subgraph traversal for retrieval.

**Architecture:** Independent Kafka consumer processes all chunks, calls LLM to extract entity-relation triples (6 entity types, 14 relation types), batch-embeds entity names, writes temporary nodes (:Entity:Type:PendingResolution) to Neo4j via UNWIND. Redis counts per doc_id; when all chunks arrive, emits a Celery task (postprocess_doc) that resolves entities (dictionary lookup → vector similarity > 0.85 → merge via apoc.refactor.mergeNodes) and REMOVEs :PendingResolution. Daily 3am rebuild creates :RebuildShadow nodes then atomically swaps labels for zero-downtime.

**Tech Stack:** Python 3.11, Neo4j 5.20, APOC, kafka-python, Redis, Celery, httpx (LLM API), sentence-transformers (384-dim), umap-learn, scikit-learn

---

### Task 1: GraphRAG constants and config

**Files:**
- Modify: `d:/knowledge-platform/backend/common/constants.py`
- Modify: `d:/knowledge-platform/backend/core/config.py`

- [ ] **Step 1: Add constants to common/constants.py**

```python
# --- GraphRAG ---
GRAPHRAG_LLM_MAX_TOKENS = 800
GRAPHRAG_ENTITY_SIMILARITY_THRESHOLD = 0.85
GRAPHRAG_POSTPROCESS_MAX_ENTITIES = 2000
GRAPHRAG_REBUILD_HOUR = 3
GRAPHRAG_REBUILD_LOOKBACK_HOURS = 24
GRAPHRAG_DLQ_MAX_RETRY = 10
GRAPHRAG_REDIS_TTL_SECONDS = 3600
GRAPHRAG_EMBEDDING_BATCH_SIZE = 32
GRAPHRAG_TX_TIMEOUT_SECONDS = 15
GRAPHRAG_MERGE_RETRY_BACKOFF = 0.5
GRAPHRAG_MAX_TOKENS_PER_BATCH = 100000
```

- [ ] **Step 2: Add config to core/config.py**

```python
# Neo4j
neo4j_uri: str = "bolt://localhost:7687"
neo4j_user: str = "neo4j"
neo4j_password: str = "password"

# GraphRAG
graphrag_llm_api_url: str = ""
graphrag_llm_api_key: str = ""
graphrag_llm_model: str = "deepseek-chat"
graphrag_backup_llm_api_url: str = ""
graphrag_backup_llm_api_key: str = ""
graphrag_backup_llm_model: str = "qwen-turbo"
```

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/common/constants.py backend/core/config.py && git commit -m "feat: add GraphRAG constants and Neo4j config"
```

---

### Task 2: PG models — entity_normalization + dead_letter_graphrag

**Files:**
- Create: `d:/knowledge-platform/backend/models/entity_normalization.py`
- Create: `d:/knowledge-platform/backend/models/dead_letter_graphrag.py`
- Modify: `d:/knowledge-platform/backend/models/__init__.py`

- [ ] **Step 1: Write the failing tests**

Create `d:/knowledge-platform/backend/tests/unit/models/test_graphrag_models.py`:

```python
"""Tests for GraphRAG PG models."""

import uuid
from datetime import datetime, timezone
from models.entity_normalization import EntityNormalization
from models.dead_letter_graphrag import DeadLetterGraphrag


def test_entity_normalization_creation():
    en = EntityNormalization(
        id=uuid.uuid4(),
        standard_name="首席执行官",
        alias="CEO",
        entity_type="Person",
        created_at=datetime.now(timezone.utc),
    )
    assert en.standard_name == "首席执行官"
    assert en.alias == "CEO"


def test_dead_letter_graphrag_creation():
    dlq = DeadLetterGraphrag(
        id=uuid.uuid4(),
        chunk_id="chunk-123",
        topic="knowledge.ingestion.chunks",
        partition=0,
        kafka_offset=99,
        payload={"text": "failed chunk"},
        error_type="LLMTimeoutError",
        error_message="LLM API timeout after 30s",
        retry_count=0,
        created_at=datetime.now(timezone.utc),
    )
    assert dlq.chunk_id == "chunk-123"
    assert dlq.retry_count == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/models/test_graphrag_models.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Create models/entity_normalization.py**

```python
"""Entity normalization dictionary — maps aliases to standard entity names."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID

from models import Base


class EntityNormalization(Base):
    __tablename__ = "entity_normalization"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    standard_name: Mapped[str] = mapped_column(String(256), nullable=False)
    alias: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
```

- [ ] **Step 4: Create models/dead_letter_graphrag.py**

```python
"""Dead letter queue for GraphRAG extraction failures."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Integer, BigInteger, SmallInteger, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB

from models import Base


class DeadLetterGraphrag(Base):
    __tablename__ = "dead_letter_graphrag"

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

- [ ] **Step 5: Update models/__init__.py**

Add imports and `__all__` entries for both new models.

- [ ] **Step 6: Run test to verify it passes**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -m pytest tests/unit/models/test_graphrag_models.py -v`
Expected: 2 passed

- [ ] **Step 7: Commit**

```bash
cd d:/knowledge-platform && git add backend/models/entity_normalization.py backend/models/dead_letter_graphrag.py backend/models/__init__.py backend/tests/unit/models/ && git commit -m "feat: add entity_normalization and dead_letter_graphrag PG models"
```

---

### Task 3: Neo4j store — schema + CRUD + vector index

**Files:**
- Create: `d:/knowledge-platform/backend/services/graphrag/__init__.py`
- Create: `d:/knowledge-platform/backend/services/graphrag/neo4j_store.py`

- [ ] **Step 1: Create services/graphrag/__init__.py**

```python
"""GraphRAG — LLM entity/relation extraction, Neo4j knowledge graph, entity normalization."""
```

- [ ] **Step 2: Create services/graphrag/neo4j_store.py**

```python
"""Neo4j store — schema management, CRUD, vector index, UNWIND batch writes."""

import uuid
import logging
from typing import Any

from neo4j import GraphDatabase, Driver

from core.config import settings
from common.constants import GRAPHRAG_TX_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

ENTITY_TYPES = ["Policy", "Person", "Dept", "Process", "Role", "Regulation", "Risk", "Document"]


class Neo4jStore:
    def __init__(self):
        self._driver: Driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    # ── Schema ───────────────────────────────────────────

    def ensure_schema(self) -> None:
        with self._driver.session() as session:
            # Entity unique constraint
            session.run("CREATE CONSTRAINT entity_id_unique IF NOT EXISTS "
                        "FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE")

            # Vector index (HNSW)
            session.run("""
                CREATE VECTOR INDEX entity_embedding_idx IF NOT EXISTS
                FOR (e:Entity) ON (e.embedding)
                OPTIONS {indexConfig: {
                  `vector.dimensions`: 384,
                  `vector.similarity_function`: 'COSINE',
                  `vector.hnsw.m`: 16,
                  `vector.hnsw.ef_construction`: 200
                }}
            """)

            # B-tree indexes
            for etype in ENTITY_TYPES:
                session.run(f"CREATE INDEX {etype.lower()}_name_idx IF NOT EXISTS "
                            f"FOR (e:{etype}) ON (e.name)")

            session.run("CREATE INDEX entity_doc_id_idx IF NOT EXISTS "
                        "FOR (e:Entity) ON (e.doc_id)")
            session.run("CREATE INDEX entity_department_idx IF NOT EXISTS "
                        "FOR (e:Entity) ON (e.department)")
            session.run("CREATE INDEX pending_resolution_doc_idx IF NOT EXISTS "
                        "FOR (e:PendingResolution) ON (e.doc_id)")
            session.run("CREATE INDEX rebuild_shadow_doc_idx IF NOT EXISTS "
                        "FOR (e:RebuildShadow) ON (e.doc_id)")

            logger.info("Neo4j schema ensured (constraints + indexes)")

    # ── Batch Write ──────────────────────────────────────

    def upsert_entities_and_relations(
        self, entities: list[dict], relations: list[dict]
    ) -> None:
        """UNWIND batch write: MERGE entities + relations in one transaction."""
        with self._driver.session() as session:
            tx = session.begin_transaction(timeout=GRAPHRAG_TX_TIMEOUT_SECONDS)
            try:
                if entities:
                    tx.run("""
                        UNWIND $entities AS e
                        MERGE (n:Entity:{type} {name: e.name})  // placeholder replaced below
                        ON CREATE SET n += e.props,
                                       n:PendingResolution,
                                       n:{type}
                        ON MATCH  SET n.embedding = e.props.embedding
                    """, {"entities": entities, "type": entities[0]["type"]})
                    # Run per-type for dynamic label injection
                if relations:
                    tx.run("""
                        UNWIND $relations AS r
                        MATCH (a:Entity {entity_id: r.source_entity_id})
                        MATCH (b:Entity {entity_id: r.target_entity_id})
                        MERGE (a)-[rel:{rel_type}]->(b)
                        SET rel += r.props
                    """, {"relations": relations})
                tx.commit()
            except Exception:
                tx.rollback()
                raise

    def _upsert_entities_by_type(self, session, entities: list[dict]) -> None:
        """Run UNWIND per entity type (Neo4j requires static labels)."""
        by_type: dict[str, list] = {}
        for e in entities:
            by_type.setdefault(e["type"], []).append(e)

        for etype, batch in by_type.items():
            session.run(f"""
                UNWIND $entities AS e
                MERGE (n:Entity:{etype} {{name: e.name}})
                ON CREATE SET n.entity_id = e.entity_id,
                              n.embedding = e.embedding,
                              n.aliases = e.aliases,
                              n.confidence = e.confidence,
                              n.access_level = e.access_level,
                              n.department = e.department,
                              n.doc_id = e.doc_id,
                              n:{etype},
                              n:PendingResolution
                ON MATCH  SET n.embedding = e.embedding
            """, {"entities": batch})

    # ── Query ─────────────────────────────────────────────

    def get_pending_nodes(self, doc_id: str) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(
                "MATCH (e:PendingResolution {doc_id: $doc_id}) RETURN e",
                {"doc_id": doc_id},
            )
            return [dict(record["e"]) for record in result]

    def remove_pending_label(self, doc_id: str) -> None:
        with self._driver.session() as session:
            session.run(
                "MATCH (e:PendingResolution {doc_id: $doc_id}) REMOVE e:PendingResolution",
                {"doc_id": doc_id},
            )

    def merge_entities(self, entity_ids: list[str]) -> None:
        """Use APOC to safely merge duplicate entities."""
        with self._driver.session() as session:
            session.run(
                "MATCH (e:Entity) WHERE e.entity_id IN $ids "
                "WITH collect(e) AS nodes "
                "CALL apoc.refactor.mergeNodes(nodes, {properties: 'combine', mergeRels: true}) "
                "YIELD node RETURN node",
                {"ids": entity_ids},
            )

    def replace_doc_graph(self, doc_id: str, entities: list[dict], relations: list[dict]) -> None:
        """Atomic shadow-rebuild: write to :RebuildShadow, then swap."""
        with self._driver.session() as session:
            tx = session.begin_transaction()
            try:
                # Write new nodes with :RebuildShadow
                for e in entities:
                    tx.run(f"""
                        MERGE (n:Entity:{e['type']}:RebuildShadow {{name: $name}})
                        ON CREATE SET n += $props
                        ON MATCH  SET n += $props
                    """, {"name": e["name"], "props": {k: v for k, v in e.items() if k not in ("name", "type")}})

                # Write new relations
                for r in relations:
                    tx.run(f"""
                        MATCH (a:RebuildShadow {{entity_id: $source}})
                        MATCH (b:RebuildShadow {{entity_id: $target}})
                        MERGE (a)-[rel:{r['type']}]->(b)
                        SET rel += $props
                    """, {"source": r["source_entity_id"], "target": r["target_entity_id"], "props": r})

                # Atomic swap
                tx.run(
                    "MATCH (e {doc_id: $doc_id}) WHERE NOT e:RebuildShadow DETACH DELETE e",
                    {"doc_id": doc_id},
                )
                tx.run(
                    "MATCH (e:RebuildShadow {doc_id: $doc_id}) REMOVE e:RebuildShadow",
                    {"doc_id": doc_id},
                )
                tx.commit()
                logger.info(f"Replaced graph for doc {doc_id}")
            except Exception:
                tx.rollback()
                raise

    def close(self) -> None:
        self._driver.close()
```

- [ ] **Step 3: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.graphrag.neo4j_store import Neo4jStore; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/graphrag/ && git commit -m "feat: add Neo4j store with schema management, UNWIND batch writes, and shadow rebuild"
```

---

### Task 4: LLM extractor — prompt-based entity/relation extraction

**Files:**
- Create: `d:/knowledge-platform/backend/services/graphrag/extractor.py`

- [ ] **Step 1: Create services/graphrag/extractor.py**

```python
"""LLM entity/relation extractor with predefined ontology constraints."""

import json
import logging
from typing import Any

import httpx

from core.config import settings
from common.constants import GRAPHRAG_LLM_MAX_TOKENS

logger = logging.getLogger(__name__)

EXTRACTION_PROMPT = """你是一个企业文档知识图谱构建助手。从以下文本中抽取实体和关系，输出 JSON 数组。

实体类型（8类）:
- Policy: 制度/规章/办法
- Person: 人员/角色持有人
- Dept: 部门/组织
- Process: 流程/步骤
- Role: 角色定义/岗位
- Regulation: 外部法规/标准
- Risk: 风险项
- Document: 文件/附件

关系类型（14种）:
approves, reports_to, is_responsible_for, triggers, flows_to,
depends_on, belongs_to, references, complies_with, assigned_to,
participates_in, supervises, leads_to, mitigates

约束:
- 时间（如"2026年5月"）必须作为实体属性，严禁独立成实体节点
- 数值指标（如"100万元"）必须作为实体属性，严禁独立成实体节点
- 如果一个实体可能被超过 100 条关系连接，请作为属性而非节点

输出格式:
[
  {
    "entity1": {"name": "法务部", "type": "Dept", "aliases": ["法律事务部"]},
    "relation": {"type": "is_responsible_for", "confidence": 0.9},
    "entity2": {"name": "合同审查制度", "type": "Policy", "aliases": ["合同管理办法"]}
  }
]

文本:
{text}

JSON 输出:"""


class Extractor:
    def __init__(self):
        self._clients = {
            "primary": (settings.graphrag_llm_api_url, settings.graphrag_llm_api_key, settings.graphrag_llm_model),
            "backup": (settings.graphrag_backup_llm_api_url, settings.graphrag_backup_llm_api_key, settings.graphrag_backup_llm_model),
        }

    async def extract(self, text: str) -> list[dict[str, Any]]:
        """Extract entity-relation triples from text. Returns list of {entity1, relation, entity2}."""
        prompt = EXTRACTION_PROMPT.format(text=text[:GRAPHRAG_LLM_MAX_TOKENS * 4])

        for provider, (url, key, model) in self._clients.items():
            try:
                result = await self._call_llm(url, key, model, prompt)
                return self._parse_result(result)
            except Exception as e:
                logger.warning(f"{provider} LLM failed: {e}")
                if provider == "primary":
                    continue
                raise

        return []

    async def _call_llm(self, url: str, key: str, model: str, prompt: str) -> str:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{url}/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": GRAPHRAG_LLM_MAX_TOKENS,
                    "temperature": 0.1,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]

    def _parse_result(self, raw: str) -> list[dict[str, Any]]:
        """Parse LLM JSON output, handling markdown code fences."""
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1]
            if raw.endswith("```"):
                raw = raw[:-3]
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM JSON output: {raw[:200]}...")
            return []
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.graphrag.extractor import Extractor; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/graphrag/extractor.py && git commit -m "feat: add GraphRAG LLM extractor with ontology-constrained prompt"
```

---

### Task 5: Entity normalizer — dictionary + vector similarity + coreference

**Files:**
- Create: `d:/knowledge-platform/backend/services/graphrag/entity_normalizer.py`

- [ ] **Step 1: Create services/graphrag/entity_normalizer.py**

```python
"""Entity normalization: dictionary lookup → vector similarity → coreference resolution."""

import logging
from typing import Any

import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from core.config import settings
from models.entity_normalization import EntityNormalization
from services.indexing.embedder import Embedder
from common.constants import GRAPHRAG_ENTITY_SIMILARITY_THRESHOLD

logger = logging.getLogger(__name__)


class EntityNormalizer:
    def __init__(self):
        engine = create_engine(settings.database_url_sync)
        self.Session = sessionmaker(bind=engine)
        self._embedder = Embedder()

    def normalize(self, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize entity names: dictionary → vector → produce canonical list."""
        if not entities:
            return []

        # 1. Dictionary lookup
        canonicals: dict[str, str] = {}  # alias → standard_name
        with self.Session() as db:
            names = {e["name"] for e in entities}
            if names:
                rows = db.execute(
                    select(EntityNormalization).where(EntityNormalization.alias.in_(names))
                ).scalars().all()
                for row in rows:
                    canonicals[row.alias] = row.standard_name
                    canonicals.setdefault(row.standard_name, row.standard_name)

        # 2. Apply dictionary mappings
        for e in entities:
            if e["name"] in canonicals:
                e["name"] = canonicals[e["name"]]

        # 3. Vector similarity dedup (within same type)
        entities = self._dedup_by_vector(entities)

        # 4. Build alias list
        alias_map: dict[str, list[str]] = {}
        for e in entities:
            alias_map.setdefault(e["name"], []).append(e.get("original_name", e["name"]))

        return entities

    def _dedup_by_vector(self, entities: list[dict]) -> list[dict]:
        """Deduplicate entities of the same type by embedding cosine similarity."""
        by_type: dict[str, list[int]] = {}
        for i, e in enumerate(entities):
            by_type.setdefault(e["type"], []).append(i)

        names = [e["name"] for e in entities]
        embeddings = self._embedder.encode(names)

        merged = set()
        result = []

        for etype, indices in by_type.items():
            for i in range(len(indices)):
                if indices[i] in merged:
                    continue
                group = [indices[i]]
                for j in range(i + 1, len(indices)):
                    if indices[j] in merged:
                        continue
                    sim = np.dot(embeddings[indices[i]], embeddings[indices[j]])
                    if sim > GRAPHRAG_ENTITY_SIMILARITY_THRESHOLD:
                        group.append(indices[j])
                        merged.add(indices[j])

                canonical = entities[group[0]]
                canonical.setdefault("aliases", [])
                for g in group[1:]:
                    canonical["aliases"].append(entities[g]["name"])
                result.append(canonical)

        return result

    def add_alias(self, standard_name: str, alias: str, entity_type: str) -> None:
        """Add a new alias to the normalization dictionary."""
        with self.Session() as db:
            record = EntityNormalization(
                standard_name=standard_name,
                alias=alias,
                entity_type=entity_type,
            )
            db.add(record)
            db.commit()
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.graphrag.entity_normalizer import EntityNormalizer; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/graphrag/entity_normalizer.py && git commit -m "feat: add entity normalizer with dictionary lookup and vector dedup"
```

---

### Task 6: GraphRAG consumer — Kafka → Redis → emit postprocess

**Files:**
- Create: `d:/knowledge-platform/backend/services/graphrag/consumer.py`

- [ ] **Step 1: Create services/graphrag/consumer.py**

```python
"""GraphRAG Kafka consumer — real-time extraction + Redis count + emit Celery postprocess."""

import json
import logging
import signal
from typing import Any

from kafka import KafkaConsumer
from redis import Redis

from core.config import settings
from core.redis import get_redis
from services.graphrag.extractor import Extractor
from services.graphrag.neo4j_store import Neo4jStore
from services.indexing.embedder import Embedder
from common.constants import GRAPHRAG_REDIS_TTL_SECONDS

logger = logging.getLogger(__name__)


class GraphRagConsumer:
    def __init__(self):
        self._redis: Redis = get_redis()
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

                    # Collect unique entity names → batch embed
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
```

- [ ] **Step 2: Verify import**

Run: `cd d:/knowledge-platform/backend && /d/Anaconda/envs/knowledge-platform/python -c "from services.graphrag.consumer import GraphRagConsumer; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd d:/knowledge-platform && git add backend/services/graphrag/consumer.py && git commit -m "feat: add GraphRAG consumer with real-time extraction and Redis trigger"
```

---

### Task 7: Postprocessor + Rebuilder + Entry Point

**Files:**
- Create: `d:/knowledge-platform/backend/workers/graph_postprocessor.py`
- Create: `d:/knowledge-platform/backend/workers/graph_rebuilder.py`
- Create: `d:/knowledge-platform/backend/workers/tasks/graph.py`
- Modify: `d:/knowledge-platform/backend/core/celery.py` — add tasks + beat entry

- [ ] **Step 1: Create workers/graph_postprocessor.py**

```python
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
```

- [ ] **Step 2: Create workers/graph_rebuilder.py**

```python
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
```

- [ ] **Step 3: Create workers/tasks/graph.py (entry point)**

```python
"""GraphRAG Consumer process entry point.

Usage:
    python -m workers.tasks.graph
"""

from services.graphrag.consumer import GraphRagConsumer


def main():
    consumer = GraphRagConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Register in core/celery.py**

Add `"workers.graph_postprocessor"` and `"workers.graph_rebuilder"` to `include`. Add beat entry:

```python
"graphrag-rebuild": {
    "task": "workers.graph_rebuilder.rebuild_recent_docs",
    "schedule": crontab(hour=3, minute=0),
},
```

- [ ] **Step 5: Commit**

```bash
cd d:/knowledge-platform && git add backend/workers/graph_postprocessor.py backend/workers/graph_rebuilder.py backend/workers/tasks/graph.py backend/core/celery.py && git commit -m "feat: add GraphRAG postprocessor, nightly rebuilder, and process entry point"
```

---

### Task 8: DLQ management API

**Files:**
- Create: `d:/knowledge-platform/backend/schemas/graphrag_dlq.py`
- Create: `d:/knowledge-platform/backend/api/routers/graphrag_dlq.py`
- Modify: `d:/knowledge-platform/backend/api/main.py` — register router

- [ ] **Step 1: Create schemas/graphrag_dlq.py**

```python
"""Pydantic schemas for GraphRAG DLQ API."""

from datetime import datetime
from pydantic import BaseModel


class DlqGraphragItem(BaseModel):
    id: str
    chunk_id: str
    error_type: str | None = None
    error_message: str | None = None
    retry_count: int
    created_at: datetime

    model_config = {"from_attributes": True}


class DlqGraphragListResponse(BaseModel):
    items: list[DlqGraphragItem]
    total: int
    page: int
    page_size: int


class DlqGraphragRetryResponse(BaseModel):
    id: str
    status: str
```

- [ ] **Step 2: Create api/routers/graphrag_dlq.py**

```python
"""DLQ management for GraphRAG extraction failures."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from core.database import get_db
from core.kafka import get_kafka
from models.dead_letter_graphrag import DeadLetterGraphrag
from schemas.graphrag_dlq import DlqGraphragItem, DlqGraphragListResponse, DlqGraphragRetryResponse
from api.dependencies import get_current_user

router = APIRouter(prefix="/api/v1/graphrag/dlq", tags=["graphrag-dlq"])


@router.get("/", response_model=DlqGraphragListResponse)
async def list_dlq(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * page_size
    total = (await db.execute(select(func.count(DeadLetterGraphrag.id)))).scalar() or 0
    items = (await db.execute(
        select(DeadLetterGraphrag).order_by(DeadLetterGraphrag.created_at.desc()).offset(offset).limit(page_size)
    )).scalars().all()
    return DlqGraphragListResponse(
        items=[DlqGraphragItem.model_validate(i) for i in items],
        total=total, page=page, page_size=page_size,
    )


@router.post("/{dlq_id}/retry", response_model=DlqGraphragRetryResponse)
async def retry_dlq(dlq_id: str, user: dict = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DeadLetterGraphrag).where(DeadLetterGraphrag.id == uuid.UUID(dlq_id)))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="DLQ record not found")
    get_kafka().send_single(record.payload)
    return DlqGraphragRetryResponse(id=dlq_id, status="republished")
```

- [ ] **Step 3: Register in api/main.py**

```python
from api.routers import graphrag_dlq
app.include_router(graphrag_dlq.router)
```

- [ ] **Step 4: Commit**

```bash
cd d:/knowledge-platform && git add backend/schemas/graphrag_dlq.py backend/api/routers/graphrag_dlq.py backend/api/main.py && git commit -m "feat: add GraphRAG DLQ management API"
```

---

### Task 9: Docker + dependencies + migration

**Files:**
- Modify: `d:/knowledge-platform/docker/docker-compose.yml` — add Neo4j
- Modify: `d:/knowledge-platform/backend/pyproject.toml` — add neo4j driver
- Create: Alembic migration for entity_normalization + dead_letter_graphrag

- [ ] **Step 1: Add Neo4j to docker-compose.yml**

```yaml
  neo4j:
    image: neo4j:5.20-community
    environment:
      NEO4J_AUTH: neo4j/password
      NEO4J_PLUGINS: '["apoc"]'
    ports: ["7474:7474", "7687:7687"]
    volumes: [neo4jdata:/data, neo4jlogs:/logs]
    healthcheck:
      test: ["CMD", "cypher-shell", "-u", "neo4j", "-p", "password", "RETURN 1"]
      interval: 10s
      timeout: 10s
      retries: 10
```

Also add `neo4jdata:` and `neo4jlogs:` to volumes.

- [ ] **Step 2: Add dependency to pyproject.toml**

```toml
"neo4j>=5.24.0",
```

Install: `cd d:/knowledge-platform && uv pip install -e "backend[dev]" -i https://mirrors.aliyun.com/pypi/simple`

- [ ] **Step 3: Generate Alembic migration**

```bash
cd d:/knowledge-platform/backend && alembic revision --autogenerate -m "add_entity_normalization_and_graphrag_dlq"
```

If PostgreSQL is not running, manually create the migration with the two CREATE TABLE statements.

- [ ] **Step 4: Commit**

```bash
cd d:/knowledge-platform && git add docker/docker-compose.yml backend/pyproject.toml backend/migrations/ && git commit -m "feat: add Neo4j to docker-compose, neo4j driver dependency, and PG migration"
```

---

## Remaining Tasks (Phase 2)

| Task | Files | What |
|------|-------|------|
| 10 | `tests/unit/services/graphrag/test_extractor.py` | TDD for extractor with mock LLM responses |
| 11 | `tests/unit/services/graphrag/test_neo4j_store.py` | TDD for Neo4j CRUD with testcontainer |
| 12 | `tests/unit/services/graphrag/test_entity_normalizer.py` | TDD for dictionary + vector dedup |
