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
- **Redis 计数齐套触发 + 超时兜底**：等一个 doc 的所有 LARGE chunk 到齐后一次性构建 RAPTOR 树；超时未到齐则用已有 chunk 构建
- UMAP 降维（384→5 维）+ GMM 软聚类（概率隶属，一个 chunk 可同时属多个簇）
- 固定最大深度 3 层 + 自底向上 Token 规模拦截（level=0 时若所有节点 token 已 < 阈值，直接不聚类）
- 远程 LLM API 摘要生成（主用 DeepSeek，备用通义千问）
- 存储：PostgreSQL `raptor_nodes` 表 + 通过 Kafka `knowledge.raptor.summaries` 推给 #2 落 Milvus
- Kafka 消息含 `action` 字段（`updated` / `deleted`），#2 mini-consumer 按 action 执行 upsert 或 delete
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
- **Redis 齐套触发 + 超时兜底**：Consumer 将 LARGE chunk 缓存在 Redis 中，按 doc_id 计数。当计数等于 `chunk_count`（文档总 chunk 数）时触发一次性全量构建。超时（默认 30 分钟）后不论到齐与否都用已有 chunk 构建，避免丢失 chunk 导致永不等齐。
- **UMAP + GMM 软聚类**：UMAP 将 384 维向量降到 5 维（抵消 GMM 计算开销），GMM 输出概率隶属——一个 chunk 可同时以不同概率属于多个簇，嵌入多份 LLM 摘要中。解决企业文档"一节多意"问题。
- **自底向上 Token 拦截**：level=0 时若所有节点 token 总量 < 1000 → 直接不聚类，文档原文即为最终节点。避免短文档生成无意义的"复读机"摘要。
- **单一索引 + 逻辑隔离**：ES 和 Milvus 各自只维护一个 index/collection，用 `granularity` 字段（SMALL / LARGE / SUMMARY）内部区分，检索端统一过滤。
- **RAPTOR 不管 ES**：摘要节点不做 BM25 全文索引，只存 Milvus 向量 + PG 树结构。全文检索完全由 SMALL chunk 覆盖。
- **#2 不消费 RAPTOR 事件**：#2 的 mini-consumer 把 SUMMARY 落 Milvus，但不落 ES。#5 检索层分别查 #2 的 SMALL（原文）和 #3 的 SUMMARY（摘要），再统一重排。

---

## 3. 数据流

### 3.1 Redis 齐套触发构建（主路径）

```
Kafka: granularity=LARGE chunk 到达
  │
  ▼
1. 存入 Redis:
   RPUSH raptor:buffer:{doc_id} {chunk_json}
   INCR raptor:count:{doc_id}
   EXPIRE raptor:buffer:{doc_id} 3600       # 1h TTL
  │
  ▼
2. 检查是否齐套:
   if count == chunk_meta.total_chunk_count:  # 从消息 metadata 获取
      → 触发构建
  │
  ▼
3. 一次性全量构建:
   a. LRANGE raptor:buffer:{doc_id} 0 -1 → 获取全量 LARGE chunk
   b. DEL raptor:buffer:{doc_id} raptor:count:{doc_id}  # 清理 Redis
   c. 创建全部 level=0 节点 (每个 LARGE chunk 一个)
   d. embedding → UMAP(5维) → GMM 软聚类
   e. 对每个簇调用 LLM 生成 level=1 摘要
   f. 递归聚类 → 摘要 → level=2 → ... → 直到 should_stop()
   g. INSERT raptor_nodes (PG) + 推送 Kafka (action="updated")
```

### 3.2 超时兜底构建

```
Celery Beat 每 10 分钟扫描:
  │
  ▼
1. SCAN Redis: raptor:count:{doc_id} 中创建时间 > 30 分钟的
2. 不论 count 是否等于 total_chunk_count, 强制触发构建
3. 适用场景: 部分 LARGE chunk 丢失 (MinerU 解析失败等),
   防止文档永远无法构建 RAPTOR 树
```

### 3.3 聚类算法

```python
import numpy as np
from umap import UMAP
from sklearn.mixture import GaussianMixture

MAX_RAPTOR_DEPTH = 3
STOP_CLUSTERING_TOKEN_THRESHOLD = 1000
UMAP_N_COMPONENTS = 5                    # 降至5维, 抵消GMM计算开销

def build_raptor_tree(nodes: list[dict], doc_id: str) -> None:
    """自底向上构建 RAPTOR 树。"""
    # ═══ 自底向上 Token 拦截 ═══
    total_tokens = sum(n["token_count"] for n in nodes)
    if total_tokens < STOP_CLUSTERING_TOKEN_THRESHOLD:
        # 文档本身已足够短, 不再聚类, level=0 就是最终节点
        return

    _cluster_and_summarize(nodes, level=0, doc_id=doc_id)


def _cluster_and_summarize(nodes: list[dict], level: int, doc_id: str) -> list[dict]:
    if _should_stop(level, nodes):
        return nodes

    embeddings = np.array([n["embedding"] for n in nodes])

    # 1. UMAP 降维 (384 → 5)
    reducer = UMAP(n_components=UMAP_N_COMPONENTS, random_state=42)
    reduced = reducer.fit_transform(embeddings)

    # 2. GMM 软聚类 — 输出概率矩阵 P[i][k] = 节点i属于簇k的概率
    n_components = max(2, min(10, int(sum(n["token_count"] for n in nodes) / 1500)))
    gmm = GaussianMixture(n_components=n_components, random_state=42)
    gmm.fit(reduced)
    probs = gmm.predict_proba(reduced)   # shape: (N, K)

    # 3. 软分配: probability > 0.2 的节点即属于该簇 (一个节点可属多个簇)
    threshold = 0.2
    summary_nodes = []
    for k in range(n_components):
        cluster_nodes = [
            nodes[i] for i in range(len(nodes))
            if probs[i][k] >= threshold
        ]
        if len(cluster_nodes) < 2:
            continue  # 单节点簇不摘要

        summary_text = summarize_cluster(cluster_nodes)
        node = create_summary_node(cluster_nodes, summary_text, level + 1, k)
        summary_nodes.append(node)

    # 4. 递归上层
    if summary_nodes:
        return _cluster_and_summarize(summary_nodes, level + 1, doc_id)
    return nodes


def _should_stop(level: int, nodes: list[dict]) -> bool:
    if level >= MAX_RAPTOR_DEPTH:
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
                         │ 每条消息含 action 字段:
                         │   "updated" — 新增/覆盖
                         │   "deleted" — 删除
                         ▼
               #2 Index Consumer 内的 mini-consumer 线程
                 │
                 ├─ action="updated":
                 │    content → embed → milvus.upsert(pk=chunk_id, granularity=SUMMARY)
                 │    ES: 不写入 (RAPTOR 不需要 BM25)
                 │
                 └─ action="deleted":
                      milvus.delete(expr=f'chunk_id == "{chunk_id}"')
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
  消息格式:
  {
    "action": "updated",               // "updated" | "deleted"
    "event": "raptor.node_updated",
    "chunk_id": "uuid",
    "doc_id": "uuid",
    "parent_id": "uuid | null",
    "heading_level": "SUMMARY",
    "granularity": "SUMMARY",           // #2 mini-consumer 通过此字段识别
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
RAPTOR_MAX_DEPTH = 3
RAPTOR_STOP_CLUSTERING_TOKENS = 1000
RAPTOR_BUILD_TIMEOUT_MINUTES = 30      # 超时未到齐则强制构建
RAPTOR_REDIS_TTL_SECONDS = 3600        # Redis 缓冲区 TTL
RAPTOR_GMM_PROB_THRESHOLD = 0.2        # GMM 软聚类概率阈值
RAPTOR_UMAP_N_COMPONENTS = 5           # UMAP 降维目标维度
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
│   └── raptor_timeout_scanner.py       # NEW: Celery Beat 每10min扫Redis超时doc
├── services/raptor/                    # NEW
│   ├── __init__.py
│   ├── consumer.py                     # Kafka consumer + Redis缓冲 + 齐套触发
│   ├── clusterer.py                    # UMAP降维 + GMM软聚类
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
- [x] UMAP + GMM 软聚类：一个 chunk 可属多个簇，解决企业文档多主题重叠
- [x] Redis 齐套触发 + 超时 30min 兜底：不会因丢失 chunk 永不等齐，不会因增量导致 LLM 调用风暴
- [x] 自底向上 Token 拦截：短文档不生成冗余摘要（total_tokens < 1000 → 直接不聚类）
- [x] Kafka 消息带 `action` 字段（updated/deleted），#2 mini-consumer 正确处理删除语义
- [x] 深度固定 ≤ 3；LLM 主备 fallback；raptor_nodes 用 pgvector 存向量
