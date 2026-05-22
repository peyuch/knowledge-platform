# RAPTOR 递归摘要树 — 设计文档

**项目**：企业多模态知识中台 — 子项目 #3  
**日期**：2026-05-22  
**状态**：设计中  

---

## 1. 概述

### 1.1 目标

从 Kafka `knowledge.ingestion.chunks` 消费 LARGE chunk（H1/H2 级别），构建递归摘要树：叶子层原文 → k-means++ 聚类 → LLM 生成上层摘要 → 再聚类 → 再摘要，形成多粒度语义树。解决长文档"切分丢上下文"的问题，为子项目 #5 提供分层检索能力。

### 1.2 范围边界

**本期包含：**
- 独立 Kafka Consumer 进程（非 Celery），消费 `granularity=LARGE` 的 chunk
- 增量流式构建：来一个 LARGE chunk → 创建 level=0 节点 → 增量聚类到最相似簇 → 更新受影响的上层摘要
- 离线全量重构建：Celery Beat 每天凌晨 2 点，对过去 24 小时更新的文档全量重建 RAPTOR 树
- k-means++ 聚类（K = `total_tokens / 1500`，`random_state=42` 保证可复现）
- 固定最大深度 3 层 + `total_tokens < 1000` 自适应截断
- 远程 LLM API 摘要生成（主用 DeepSeek，备用通义千问）
- 存储：PostgreSQL `raptor_nodes` 表 + 通过 Kafka `knowledge.raptor.summaries` 推给 #2 落 Milvus（单一 `chunks` collection，`granularity=SUMMARY`）
- SMALL chunk 通过 `parent_id` 挂载到对应 LARGE 节点，检索时由 #5 动态组合

**本期不包含：**
- RAPTOR 直接写 Milvus/ES（由 #2 mini-consumer 统一落库，避免 Milvus Lite 多进程锁死）
- RAPTOR 专用 ES index（单一 `chunks` index，`granularity` 字段区分）
- #2 消费 RAPTOR 摘要事件（#5 检索层分别查询后组合）
- 历史文档全量重建（v2）

---

## 2. 架构设计

### 2.1 架构总览

```
                    Kafka (knowledge.ingestion.chunks)
                         │
              ┌──────────┴──────────┐
              │ granularity=LARGE    │ granularity=SMALL
              ▼                      ▼
┌──────────────────────┐   ┌──────────────────────────┐
│  #3 RAPTOR Consumer  │   │  #2 Index Consumer        │
│  (独立进程, 非 Celery)│   │                           │
│                      │   │  embed → Milvus + ES      │
│  ┌────────────────┐  │   │  (granularity=SMALL)       │
│  │ 增量路径         │  │   │                           │
│  │ 来一个 → 建节点  │  │   │  ┌────────────────────┐   │
│  │ → 聚类 → 摘要   │──┼───┼─→│ mini-consumer 线程   │   │
│  └────────────────┘  │   │  │ 监听 raptor.        │   │
│                      │   │  │ summaries topic     │   │
│  ┌────────────────┐  │   │  │ → Milvus (SUMMARY)  │   │
│  │ 离线路径         │  │   │  └────────────────────┘   │
│  │ Celery Beat     │  │   │                           │
│  │ 凌晨2点 全量重建 │  │   │                           │
│  └────────────────┘  │   │                           │
└─────────┬────────────┘   └──────────────────────────┘
          │                          │
          ▼                          ▼
   Kafka topic:              ┌─────────────────┐
   knowledge.raptor.         │    Milvus       │
   summaries                 │  "chunks"       │
          │                  │  granularity=   │
          └─────────────────→│  {SMALL, SUMMARY}│
                             ├─────────────────┤
                             │       ES        │
                             │  "chunks"       │
                             │  granularity=   │
                             │  {SMALL}        │
                             └─────────────────┘
                                      │
                                      ▼
                              #5 检索层
                              分别查询后组合重排
```

### 2.2 关键设计决策

- **RAPTOR 不直接写 Milvus**：避免 Milvus Lite 多进程锁死。摘要节点推入 Kafka `knowledge.raptor.summaries`，由 #2 的 mini-consumer 统一落 Milvus（单一 `chunks` collection，`granularity=SUMMARY`）。
- **增量流式 + 全量重构建**：实时来一个 LARGE chunk 就增量挂载并更新上游摘要；凌晨 2 点对 24h 内更新过的文档全量重建，消除增量累积误差。
- **k-means++ 固定随机种子**：K = `total_tokens / 1500`（工业界黄金标准），`random_state=42` 保证可复现。
- **单一索引 + 逻辑隔离**：ES 和 Milvus 各自只维护一个 index/collection，用 `granularity` 字段（SMALL / LARGE / SUMMARY）内部区分，检索端统一过滤。
- **RAPTOR 不管 ES**：摘要节点不做 BM25 全文索引，只存 Milvus 向量 + PG 树结构。全文检索完全由 SMALL chunk 覆盖。
- **#2 不消费 RAPTOR 事件**：#2 的 mini-consumer 把 SUMMARY 落 Milvus，但不落 ES。#5 检索层分别查 #2 的 SMALL（原文）和 #3 的 SUMMARY（摘要），再统一重排。

---

## 3. 数据流

### 3.1 增量流式构建（实时路径）

```
Kafka: granularity=LARGE chunk 到达
  │
  ▼
1. 创建 level=0 节点 (叶子, 内容=原文 LARGE chunk)
2. 存入 raptor_nodes (PG)
3. 推送 knowledge.raptor.summaries (granularity=LARGE, 供 #2 落 Milvus)
  │
  ▼
4. 增量聚类:
   a. 获取该文档当前层所有节点的 embedding
   b. 计算 k = max(2, min(10, total_tokens / 1500))
   c. k-means++ 聚类 (random_state=42)
  │
  ▼
5. 对每个簇:
   a. 如果簇有变化 → 重新调用 LLM 生成簇摘要
   b. 更新/创建 level+1 节点
   c. 存入 raptor_nodes + 推送 Kafka
  │
  ▼
6. 递归: 对新生成的 level+1 节点, 重复步骤 4-5
   直到 should_stop() 返回 True
```

### 3.2 离线全量重构建

```
Celery Beat 凌晨 2:00
  │
  ▼
1. 查询 PG: 过去 24h 有更新的 doc_id 列表
2. 删除这些 doc 的旧 RAPTOR 节点 (level ≥ 1, 保留 level=0 叶子)
3. 重新获取所有 level=0 节点的 embedding
4. 执行 k-means++ 聚类 → LLM 摘要 → 上层节点
5. 深度 ≤ 3, total_tokens < 1000 停止
6. 更新 raptor_nodes → 推送 Kafka 更新下游
```

### 3.3 聚类算法

```python
TARGET_TOKENS_PER_CLUSTER = 1500
MAX_RAPTOR_DEPTH = 3
STOP_CLUSTERING_TOKEN_THRESHOLD = 1000

def calculate_optimal_k(nodes: list[dict]) -> int:
    total = sum(n["token_count"] for n in nodes)
    return max(2, min(10, int(total / TARGET_TOKENS_PER_CLUSTER)))

def cluster_nodes(embeddings: list[list[float]], k: int) -> list[int]:
    from sklearn.cluster import KMeans
    return KMeans(
        n_clusters=k,
        init="k-means++",
        n_init=10,
        random_state=42,
        tol=1e-4,
    ).fit_predict(embeddings)

def should_stop_clustering(current_level: int, nodes: list[dict]) -> bool:
    if current_level >= MAX_RAPTOR_DEPTH:
        return True
    if len(nodes) < 2:
        return True
    if sum(n["token_count"] for n in nodes) < STOP_CLUSTERING_TOKEN_THRESHOLD:
        return True
    return False
```

### 3.4 LLM 摘要生成

```python
async def summarize_cluster(nodes: list[dict], llm_client) -> str:
    """合并一个簇内所有节点的内容, 调用 LLM 生成摘要."""
    combined = "\n\n---\n\n".join(n["content"] for n in nodes)
    prompt = f"""你是一个企业文档摘要助手。以下是一个文档章节的多个段落, 请生成一个 200-500 字的摘要,
保留关键事实、数字、制度和流程名称。用中文输出。

原文:
{combined[:3000]}

摘要:"""

    try:
        return await llm_client.complete(prompt, max_tokens=600)
    except Exception as primary_error:
        # fallback to backup LLM
        return await backup_llm.complete(prompt, max_tokens=600)
```

### 3.5 与 #2 Index Consumer 的协作

```
#3 RAPTOR Consumer → Kafka knowledge.raptor.summaries
                              │
                              ▼
                    #2 Index Consumer 内的 mini-consumer 线程
                      │
                      ├─ content → embedder.encode() → vector
                      ├─ milvus.upsert(vector, pk=chunk_id, granularity=SUMMARY)
                      └─ ES: 不写入 (RAPTOR 不需要 BM25)
```

---

## 4. 数据模型

### 4.1 raptor_nodes 表 (PostgreSQL)

```sql
CREATE TABLE raptor_nodes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id          UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    parent_id       UUID REFERENCES raptor_nodes(id) ON DELETE CASCADE,
    chunk_id        UUID,                       -- 关联原始 LARGE chunk (level=0 时)
    
    level           SMALLINT NOT NULL,          -- 0=原文, 1=第一层摘要, 2=第二层, 3=根
    node_type       VARCHAR(16) NOT NULL,       -- LEAF / SUMMARY / ROOT
    
    cluster_label   INT,                        -- 所属簇编号 (同层内)
    
    content         TEXT NOT NULL,               -- 原文或摘要文本
    token_count     INT,
    embedding       VECTOR(384),                 -- pgvector 存储向量, 与 Milvus 同步
    
    heading_path    TEXT[] DEFAULT '{}',        -- 继承原始 chunk 的 heading_path
    source_chunk_ids UUID[] DEFAULT '{}',       -- 摘要节点覆盖的原始 chunk ID 列表
    
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_raptor_doc_id   ON raptor_nodes(doc_id);
CREATE INDEX idx_raptor_level    ON raptor_nodes(doc_id, level);
CREATE INDEX idx_raptor_parent   ON raptor_nodes(parent_id);
CREATE INDEX idx_raptor_cluster  ON raptor_nodes(doc_id, level, cluster_label);
```

### 4.2 Kafka Topic

```
knowledge.raptor.summaries
  消息格式 (与 knowledge.ingestion.chunks 兼容):
  {
    "event": "raptor.node_updated",
    "chunk_id": "uuid",
    "doc_id": "uuid",
    "parent_id": "uuid | null",
    "heading_level": "SUMMARY",
    "granularity": "SUMMARY",           # #2 mini-consumer 通过此字段识别
    "heading_path": ["第1层摘要", "簇3"],
    "content": "该章节描述了...",
    "content_hash": "md5...",
    "level": 2,
    "node_type": "SUMMARY",
    "source_chunk_ids": ["uuid1", "uuid2"],
    "token_count": 350,
    "trace_id": "...",
    "metadata": {
      "file_type": "pdf",
      "original_filename": "制度文件v3.pdf",
      "department": "合规部"
    }
  }
```

### 4.3 Milvus / ES schema 扩展（#2 负责）

将 `granularity` 字段值扩展为 `{SMALL, LARGE, SUMMARY}`。RAPTOR 生成的 SUMMARY 节点通过 #2 的 mini-consumer 写入 Milvus（仅向量，不含 BM25）。

---

## 5. 配置

### constants.py 新增

```python
RAPTOR_TARGET_TOKENS_PER_CLUSTER = 1500
RAPTOR_MAX_DEPTH = 3
RAPTOR_STOP_CLUSTERING_TOKENS = 1000
RAPTOR_REBUILD_HOUR = 2             # 凌晨2点全量重建
RAPTOR_REBUILD_LOOKBACK_HOURS = 24
RAPTOR_LLM_MAX_TOKENS = 600
RAPTOR_LLM_BACKUP_TIMEOUT = 30
```

### config.py 新增

```python
raptor_llm_api_url: str = ""
raptor_llm_api_key: str = ""
raptor_llm_model: str = "deepseek-chat"
raptor_backup_llm_api_url: str = ""
raptor_backup_llm_api_key: str = ""
raptor_backup_llm_model: str = "qwen-turbo"
```

---

## 6. 目录结构增量

```
backend/
├── workers/tasks/
│   └── raptor.py                       # NEW: RAPTOR Consumer 进程入口
├── workers/
│   └── raptor_rebuilder.py             # NEW: Celery Beat 凌晨全量重建任务
├── services/raptor/                    # NEW
│   ├── __init__.py
│   ├── consumer.py                     # Kafka consumer + 增量构建调度
│   ├── clusterer.py                    # k-means++ 聚类
│   ├── summarizer.py                   # LLM API 调用 (主/备 fallback)
│   └── raptor_store.py                 # raptor_nodes CRUD
├── models/
│   └── raptor_node.py                  # NEW: RAPTOR 树节点 ORM
└── common/
    └── constants.py                    # MODIFY: +RAPTOR_* 常量
```

### #2 Index Consumer 修改

```
backend/services/indexing/consumer.py   # MODIFY: 新增 mini-consumer 线程,
                                        # 监听 knowledge.raptor.summaries,
                                        # 对 granularity=SUMMARY 的消息落 Milvus (不落 ES)
```

---

## 7. 自审清单

- [x] 无 TODO / TBD / 未完成段落
- [x] RAPTOR 不直接写 Milvus，通过 Kafka 由 #2 统一落库
- [x] ES/Milvus 各单一 index/collection，`granularity` 字段逻辑隔离
- [x] 增量流式 + 凌晨全量重构建，消除等待超时和累积误差
- [x] k-means++ 固定 `random_state=42`，结果可复现
- [x] 深度固定 ≤ 3 + token < 1000 自适应截断
- [x] LLM 主备 fallback（DeepSeek → 通义千问）
- [x] #2 不消费 RAPTOR 摘要语义；#5 检索层分别查询后组合
- [x] raptor_nodes 使用 pgvector 存储向量，与 Milvus 同步
