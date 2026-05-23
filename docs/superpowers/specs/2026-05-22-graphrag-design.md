# GraphRAG 知识图谱 — 设计文档

**项目**：企业多模态知识中台 — 子项目 #4  
**日期**：2026-05-22  
**状态**：设计中  

---

## 1. 概述

### 1.1 目标

从 Kafka `knowledge.ingestion.chunks` 消费所有 chunk（SMALL + LARGE），通过 LLM 抽取实体和关系构建企业知识图谱。为子项目 #5 提供子图检索 + 语义检索双路召回能力，处理制度、流程、关联问题。

### 1.2 范围边界

**本期包含：**
- 独立 Kafka Consumer 进程（非 Celery），消费所有 granularity 的 chunk
- LLM 实体关系抽取（主用 DeepSeek，备用通义千问），基于预定义本体约束
- Neo4j 5.x 统一存储：图结构 + 向量索引 + 属性 + 溯源。不引入额外 Milvus/PG 表
- 实时路径：逐 chunk 抽取 → Neo4j 写入临时节点（`:PendingResolution`）→ Redis 计数
- Redis 齐套触发 Celery 后处理任务：实体消歧 + 指代消解 → 原子替换为最终节点
- 每日凌晨 3 点全量重建（对 24h 更新文档用全局上下文重新抽取）
- DLQ + 指数退避重试 + 管理接口

**本期不包含：**
- 法律专属实体（Case, Lawyer, Client 等）
- 人工审核流程（is_verified / review_status）
- Neo4j Fabric 分片（v2）

---

## 2. 架构设计

### 2.1 架构总览

```
Kafka (knowledge.ingestion.chunks)
  │ 所有 granularity (SMALL + LARGE)
  ▼
┌──────────────────────────────────────────┐
│     GraphRAG Consumer (独立进程)          │
│                                           │
│  ┌────────────────────────────────────┐  │
│  │  实时路径 (逐 chunk):               │  │
│  │  chunk → LLM 实体关系抽取            │  │
│  │  → Neo4j upsert                     │  │
│  │    (:Entity:Policy                 │  │
│  │     :PendingResolution)            │  │
│  │  → Redis 计数 (INCR per doc_id)     │  │
│  └──────────────┬─────────────────────┘  │
│                 │ Redis 齐套              │
│                 ▼                        │
│  ┌────────────────────────────────────┐  │
│  │  后处理路径 (Celery 异步任务):      │  │
│  │  postprocess_doc(doc_id):          │  │
│  │  ├─ 查 entity_normalization 字典   │  │
│  │  ├─ 向量相似度消歧                 │  │
│  │  ├─ 指代消解                       │  │
│  │  └─ REMOVE :PendingResolution      │  │
│  └────────────────────────────────────┘  │
│                                           │
│  每日全量重建 (Celery Beat 凌晨3点):      │
│  24h更新文档 → 全文 LLM → 原子替换       │
└──────────────────────────────────────────┘
                    │
                    ▼
            ┌──────────────┐
            │    Neo4j     │  统一存储
            │  (5.x+)      │  图 + 向量 + 属性 + 溯源
            └──────┬───────┘
                   │
                   ▼
              #5 检索层
    Cypher 子图遍历 + 向量语义
           单库查询
```

### 2.2 关键设计决策

- **Neo4j 单库闭环**：Neo4j 5.x 原生支持向量索引和属性存储。砍掉独立 Milvus collection 和 PG 属性表，消除分布式事务不一致风险。
- **多标签（Multi-Label）设计**：每个节点 `:Entity`（向量索引）+ 具体语义标签（`:Policy`/`:Person`等）。具体标签上有独立 B-Tree 属性索引。
- **:PendingResolution 标签**：实时路径产出的节点用 `:PendingResolution` 标记；后处理完成后 REMOVE。用标签存灭代替布尔值过滤，避免低基数性能退化。
- **溯源在边上**：完整溯源（sentence, chunk_id, doc_id, page_number）存在关系属性上，节点仅存轻量元数据。
- **Redis 齐套触发**：与 RAPTOR 一致，逐 chunk 累加计数，到齐后 emit Celery async task。无定时轮询开销。
- **每日全量重建**：凌晨 3 点对 24h 更新文档用全局上下文重新抽取，消除增量累积误差。
- **预定义本体 + 归一化后处理**：LLM 抽取受限于预定义实体/关系类型；后处理阶段做实体消歧（字典表 + 向量相似度 + 指代消解）。

---

## 3. 数据流

### 3.1 实时路径（逐 chunk 抽取）

```
Kafka chunk 到达
  │
  ▼
1. 检查 chunk.granularity IN (SMALL, LARGE)
   (SMALL 提供细粒度实体, LARGE 提供全局上下文)
  │
  ▼
2. LLM 抽取 (extractor.py):
   prompt = 预定义本体 + chunk.content
   → 三元组列表 [(entity, relation, entity), ...]
  │
  ▼
3. 批量 Embedding + Neo4j upsert (neo4j_store.py):
   a. 收集所有去重实体名称 → 一次 batch_encode() → 384维矩阵
   b. 使用 UNWIND 单 Cypher 语句批量写入:
      UNWIND $entities AS e
      MERGE (n:Entity:{type} {name: e.name})
        ON CREATE SET n +={entity_id: e.entity_id, embedding: e.embedding, ...},
                       n:PendingResolution
        ON MATCH  SET n.embedding = e.embedding
   c. 批量写入关系:
      UNWIND $relations AS r
      MERGE (a:Entity {entity_id: r.source})-[rel:{type}]->(b:Entity {entity_id: r.target})
        SET rel +={relation_id: r.relation_id, confidence: r.confidence, sentence: r.sentence}
  │
  ▼
4. Redis 计数: INCR raptor:graph:count:{doc_id}
   EXPIRE 3600s
```

### 3.2 后处理路径（Redis 齐套 → Celery 异步）

```
Redis 计数 == total_chunk_count (从消息 metadata 获取)
  │
  ▼
Celery task: postprocess_doc(doc_id)
  1. 获取该 doc 所有 :PendingResolution 节点
  2. 实体消歧:
     a. 查 entity_normalization 字典 (PG)
        "CEO" → standard_name="首席执行官"
     b. 向量相似度: 同 type 实体间 COSINE 匹配
        相似度 > GRAPHRAG_ENTITY_SIMILARITY_THRESHOLD → 合并为同一实体
     c. 指代消解: "该制度", "前述规定" → 解析为具体实体
  3. 生成归一化后的最终实体列表
  4. 原子收尾 (避免物理 DELETE):
     a. 对需要合并的节点: CALL apoc.refactor.mergeNodes(nodes, {properties: 'combine'})
        (APOC 内置分段行锁, 消除跨文档死锁)
     b. MATCH (e:PendingResolution {doc_id: $doc_id})
        REMOVE e:PendingResolution
        SET e.confidence = 1.0
     c. 合并重复关系 (MERGE + SET)
  5. 更新 Redis (清理计数)
```

### 3.3 每日全量重建

```
Celery Beat 凌晨 3:00
  │
  ▼
1. 查询: 过去 24h 有更新的 doc_id (来自 ingestion_tasks)
2. 对每个 doc:
   a. 获取完整 chunk 列表 (SMALL + LARGE, 按 sequence 排序)
   b. 若总 token 数超过 LLM 上下文 → 按 100k token 分批抽取 → 合并三元组
   c. 使用全局上下文调用 LLM 抽取
   d. 生成全量最终图谱: 所有新节点加 :RebuildShadow 标签
   e. 影子重建完全闭环后, 毫秒级原子切换:
      MATCH (e {doc_id: $doc_id}) WHERE NOT e:RebuildShadow DETACH DELETE e;
      MATCH (e:RebuildShadow {doc_id: $doc_id}) REMOVE e:RebuildShadow;
      (检索层 #5 全程零感知, 查询绝不断流)
3. 记录重建日志
```

### 3.4 LLM 抽取 Prompt

```
你是一个企业文档知识图谱构建助手。
从以下文本中抽取实体（Entities）和关系（Relationships），
输出 JSON 数组。

实体类型（8类）:
- Policy: 制度/规章/办法
- Person: 人员/角色持有人
- Dept: 部门/组织
- Process: 流程/步骤
- Role: 角色定义/岗位
- Regulation: 外部法规/标准
- Risk: 风险项
- Document: 文件/附件

重要约束:
- 时间（如"2026年5月"、"本月"）必须作为事件或实体的属性（如 effective_date），严禁独立成节点
- 具体数值指标（如"100万元"、"30天"）必须作为流程或制度的属性（如 sla_hours），严禁独立成节点
- 如果一个实体会被超过 100 条关系连接，请优先将其作为属性而非节点

关系类型（14种）:
approves, reports_to, is_responsible_for, triggers, flows_to,
depends_on, belongs_to, references, complies_with, assigned_to,
participates_in, supervises, leads_to, mitigates

输出格式:
[
  {
    "entity1": {"name": "法务部", "type": "Dept", "aliases": ["法律事务部"]},
    "relation": {"type": "is_responsible_for", "confidence": 0.9},
    "entity2": {"name": "合同审查制度", "type": "Policy", "aliases": ["合同管理办法"]}
  }
]

文本:
{chunk.content}
```

### 3.5 错误处理与 DLQ

```
整批抽取失败
  │
  ├─ 3次指数退避重试 (5s, 10s, 20s)
  │
  ├─ 拆小批 (10条/批) → 逐批重试
  │
  ├─ 拆单条 → 5次重试 → 全败 → DLQ
  │
  ▼
DLQ 表 → Celery Beat 凌晨重试 → /api/v1/graphrag/dlq 管理接口
```

---

## 4. 数据模型

### 4.1 Neo4j 节点 Schema

```cypher
// 所有节点继承 :Entity。多标签共存: (:Entity:Policy), (:Entity:Person), ...

CREATE CONSTRAINT entity_id_unique FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE;

// 向量索引 (HNSW)
CREATE VECTOR INDEX entity_embedding_idx
FOR (e:Entity) ON (e.embedding)
OPTIONS {indexConfig: {
  `vector.dimensions`: 384,
  `vector.similarity_function`: 'COSINE',
  `vector.hnsw.m`: 16,
  `vector.hnsw.ef_construction`: 200
}};

// 属性索引 (每种具体标签单独建)
CREATE INDEX policy_name_idx FOR (e:Policy) ON (e.name);
CREATE INDEX person_name_idx FOR (e:Person) ON (e.name);
CREATE INDEX dept_name_idx   FOR (e:Dept) ON (e.name);
// ... 其他类型同理

CREATE INDEX entity_doc_id_idx FOR (e:Entity) ON (e.doc_id);
CREATE INDEX entity_department_idx FOR (e:Entity) ON (e.department);
CREATE INDEX pending_resolution_doc_idx FOR (e:PendingResolution) ON (e.doc_id);
CREATE INDEX rebuild_shadow_doc_idx FOR (e:RebuildShadow) ON (e.doc_id);
```

**节点属性（`:Entity` 基类）：**

| 属性 | 类型 | 说明 |
|------|------|------|
| `entity_id` | STRING | UUID, 全局唯一 |
| `name` | STRING | 实体显示名（归一化后的最终名） |
| `aliases` | LIST<STRING> | 同义别名 |
| `embedding` | LIST<FLOAT> | 384 维向量 |
| `confidence` | FLOAT | LLM 抽取置信度 0-1 |
| `access_level` | STRING | public / internal / confidential / secret |
| `department` | STRING | 所属部门（用于权限过滤） |
| `doc_id` | STRING | 首次出现文档 |

**各具体标签特有属性：**

```cypher
// :Policy
{version, effective_date, expire_date}

// :Person
{title}

// :Dept
// 无特有属性, 层级用 belongs_to 关系

// :Process
{steps, owner, sla_hours}

// :Role
{level, scope}

// :Regulation
{law_name, article_number}

// :Risk
{level, probability, impact}

// :Document
{file_type}
```

### 4.2 关系 Schema

```cypher
// 所有关系共用此 Schema
{
  relation_id:         STRING,       // UUID
  type:                STRING,       // 见关系类型表
  source_entity_id:    STRING,       // 源实体 entity_id (用于唯一性约束)
  target_entity_id:    STRING,       // 目标实体 entity_id
  confidence:          FLOAT,
  start_date:          STRING,       // 生效日期, null=未知
  end_date:            STRING,       // 失效日期, null=永久有效

  // 溯源 (完整版)
  chunk_id:            STRING,
  doc_id:              STRING,
  page_number:         INTEGER,
  sentence:            STRING        // 原始句子片段
}
```

### 4.3 关系类型表（14 种）

| 关系 | from → to | 语义 |
|------|-----------|------|
| `approves` | Person → Policy/Process | 审批 |
| `reports_to` | Person → Person/Dept | 汇报 |
| `is_responsible_for` | Person/Dept → Process/Policy | 负责 |
| `triggers` | Process → Process | 触发 |
| `flows_to` | Process → Process | 流转 |
| `depends_on` | Process/Policy → Policy/Process | 依赖 |
| `belongs_to` | Any → Dept | 归属 |
| `references` | Policy → Regulation/Policy | 引用 |
| `complies_with` | Policy/Process → Regulation | 合规 |
| `assigned_to` | Role → Person | 分配 |
| `participates_in` | Person → Process | 参与 |
| `supervises` | Person → Person/Process | 监督 |
| `leads_to` | Risk → Risk | 导致 |
| `mitigates` | Policy/Process → Risk | 缓解 |

### 4.4 实体归一化字典表（PostgreSQL）

```sql
CREATE TABLE entity_normalization (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    standard_name   VARCHAR(256) NOT NULL,
    alias           VARCHAR(256) NOT NULL UNIQUE,
    entity_type     VARCHAR(32) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_en_standard ON entity_normalization(standard_name);
CREATE INDEX idx_en_alias     ON entity_normalization(alias);
```

后处理时先查此表 — "CEO" → "首席执行官" — 再对未匹配的做向量相似度判断。

### 4.5 GraphRAG DLQ 表（PostgreSQL）

```sql
CREATE TABLE dead_letter_graphrag (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chunk_id        VARCHAR(36) NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    partition       INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    payload         JSONB NOT NULL,
    error_type      VARCHAR(256),
    error_message   TEXT,
    retry_count     SMALLINT DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 5. 配置

### constants.py 新增

```python
GRAPHRAG_LLM_MAX_TOKENS = 800
GRAPHRAG_ENTITY_SIMILARITY_THRESHOLD = 0.85
GRAPHRAG_POSTPROCESS_MAX_ENTITIES = 2000
GRAPHRAG_REBUILD_HOUR = 3
GRAPHRAG_REBUILD_LOOKBACK_HOURS = 24
GRAPHRAG_DLQ_MAX_RETRY = 10
GRAPHRAG_REDIS_TTL_SECONDS = 3600
GRAPHRAG_KAFKA_TOPIC = "knowledge.ingestion.chunks"
GRAPHRAG_EMBEDDING_BATCH_SIZE = 32
GRAPHRAG_TX_TIMEOUT_SECONDS = 15
GRAPHRAG_MERGE_RETRY_BACKOFF = 0.5
GRAPHRAG_MAX_TOKENS_PER_BATCH = 100000
```

### config.py 新增

```python
neo4j_uri: str = "bolt://localhost:7687"
neo4j_user: str = "neo4j"
neo4j_password: str = "password"
graphrag_llm_api_url: str = ""
graphrag_llm_api_key: str = ""
graphrag_llm_model: str = "deepseek-chat"
graphrag_backup_llm_api_url: str = ""
graphrag_backup_llm_api_key: str = ""
graphrag_backup_llm_model: str = "qwen-turbo"
```

---

## 6. 目录结构增量

```
backend/
├── workers/tasks/
│   └── graph.py                       # NEW: GraphRAG Consumer 进程入口
├── workers/
│   ├── graph_postprocessor.py         # NEW: Celery 异步: postprocess_doc(doc_id)
│   └── graph_rebuilder.py             # NEW: Celery Beat: 凌晨3点全量重建
├── services/graphrag/                 # NEW
│   ├── __init__.py
│   ├── consumer.py                    # Kafka consumer + Redis 计数 + emit task
│   ├── extractor.py                   # LLM 三元组抽取 + 预定义本体
│   ├── neo4j_store.py                 # Neo4j CRUD + schema + 向量索引
│   └── entity_normalizer.py           # 实体消歧 + 指代消解
├── models/
│   ├── entity_normalization.py        # NEW: PG 归一化字典表 ORM
│   └── dead_letter_graphrag.py        # NEW: GraphRAG DLQ ORM
├── schemas/
│   └── graphrag_dlq.py                # NEW: DLQ 管理 DTO
├── api/routers/
│   └── graphrag_dlq.py                # NEW: DLQ 管理接口
├── common/
│   └── constants.py                   # MODIFY: +GRAPHRAG_* 常量
├── core/
│   └── config.py                      # MODIFY: +neo4j_*, +graphrag_*
└── tests/unit/services/graphrag/
    ├── test_extractor.py
    ├── test_neo4j_store.py
    └── test_entity_normalizer.py
```

### docker-compose.yml 增量

```yaml
  neo4j:
    image: neo4j:5.20-community
    environment:
      NEO4J_AUTH: neo4j/password
      NEO4J_PLUGINS: '["apoc"]'
    ports: ["7474:7474", "7687:7687"]
    volumes: [neo4jdata:/data, neo4jlogs:/logs]
```

---

## 7. 自审清单

- [x] 无 TODO / TBD / 未完成段落
- [x] Prompt 严格剔除 Date/Metric 实体类型，加约束防止超级节点
- [x] 后处理用 REMOVE :PendingResolution + apoc.refactor.mergeNodes，避免 DELETE 跨文档死锁
- [x] 实时路径批量 Embedding + UNWIND 单事务写入
- [x] 全量重建用 :RebuildShadow 双缓冲标签，毫秒级原子切换，检索零感知
- [x] :PendingResolution + :RebuildShadow 建索引，后处理查询加速
- [x] 关系含 source_entity_id/target_entity_id，支持唯一性约束
- [x] 大文档按 100k token 分批抽取
