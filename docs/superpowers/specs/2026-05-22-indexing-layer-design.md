# 索引与存储层 — 设计文档

**项目**：企业多模态知识中台 — 子项目 #2  
**日期**：2026-05-22  
**状态**：设计中  

---

## 1. 概述

### 1.1 目标

从 Kafka `knowledge.ingestion.chunks` 消费 SMALL chunk，经 sentence-transformer 嵌入后双写 Milvus（向量）和 Elasticsearch（BM25 全文），为子项目 #5 混合检索提供双路存储。

### 1.2 范围边界

**本期包含：**
- 独立 Kafka Consumer 进程（非 Celery），内存缓冲区攒批（200 条 / 3 秒）
- sentence-transformer 嵌入（进程内加载，`all-MiniLM-L6-v2`，384 维，IP 度量）
- Milvus Lite（本地文件型向量库）+ Elasticsearch 双写
- `chunk_id` 幂等 upsert + 批量降级（3 次退避 → 拆小批 10 条 → 拆单条 → DLQ）
- 背压机制（缓冲区上限 2000 条）+ 优雅关闭（SIGTERM → 排空 → commit → close）
- Prometheus 指标 + structlog 结构化日志 + `/health` liveness/readiness
- DLQ 表 + Celery Beat 凌晨重试 + `GET/POST /api/v1/indexing/dlq` 管理接口

**本期不包含：**
- RAPTOR 递归摘要树（子项目 #3）
- LARGE chunk 索引入库（留给 #3 处理）
- 嵌入独立 gRPC 服务（v2）
- 数据版本/软删除（v2）
- Milvus Standalone/Cluster 升级（v2）

> ⚠️ **部署约束**：Milvus Lite 基于 SQLite 本地文件，不支持多进程并发写入。Index Consumer **只能部署 1 个副本**。多副本需升级到 Milvus Standalone（v2）。检索服务（子项目 #5）也不得直接读写同一 `milvus.db` 文件。

---

## 2. 架构设计

### 2.1 架构总览

```
           Kafka (knowledge.ingestion.chunks)
                         │
                         ▼
┌───────────────────────────────────────────┐
│          Index Consumer (独立进程)         │
│                                           │
│  ┌─────────────────────────────────────┐  │
│  │  KafkaConsumer                      │  │
│  │  enable_auto_commit=False           │  │
│  │  max_poll_records=500               │  │
│  └──────────┬──────────────────────────┘  │
│             │ filter: granularity=SMALL    │
│             ▼                              │
│  ┌─────────────────────────────────────┐  │
│  │  内存缓冲区 (按 Partition 分桶)      │  │
│  │  dict[TopicPartition, list[msg]]    │  │
│  │  每分区 200条 / 3s flush,           │  │
│  │  全局上限 2000 条, 超限触发背压      │  │
│  └──────────┬──────────────────────────┘  │
│             │                              │
│             ▼                              │
│  ┌─────────────────────────────────────┐  │
│  │  Embedder (sentence-transformer)    │  │
│  │  all-MiniLM-L6-v2, 384维, IP度量    │  │
│  └──────────┬──────────────────────────┘  │
│             │                              │
│       ┌─────┴─────┐                       │
│       ▼           ▼                       │
│  ┌─────────┐ ┌──────────┐                 │
│  │ Milvus  │ │    ES    │  并行 upsert     │
│  │  Lite   │ │(:9200)   │  (pk=chunk_id)   │
│  └────┬────┘ └─────┬────┘                 │
│       │            │                      │
│       └─────┬──────┘                      │
│             ▼                              │
│  手动 commit Kafka offset (最后一条 + 1)    │
│                                           │
│  失败降级: 3次退避 → 拆10条 → 拆1条 → DLQ  │
│                                           │
│  ┌─────────────────────────────────┐      │
│  │  HTTP Server (:9090)            │      │
│  │  /health /ready /metrics        │      │
│  └─────────────────────────────────┘      │
└───────────────────────────────────────────┘
```

### 2.2 关键设计决策

- **独立进程非 Celery**：索引 Consumer 有自己的生命周期（缓冲、背压、优雅关闭），不适合 Celery 的 task 模型。直接使用 `kafka-python` Consumer。
- **chunk_id 幂等**：每条 chunk 的 `chunk_id` 在 Milvus 作 primary key、ES 作 `_id`，双写都用 upsert。重复消费 → 覆盖写入 → 零重复数据。
- **并行双写**：Milvus 和 ES 之间无依赖关系，嵌入后同时发起 upsert，都成功后再 commit offset。
- **批量降级**：一批失败不阻塞整个消费流。逐级降级直到识别出单条坏数据 → 入 DLQ → offset 继续推进。
- **背压机制**：缓冲区超过 2000 条停止 poll，指数退避等待 flush 清空，防止 OOM。
- **向量归一化**：`all-MiniLM-L6-v2` 默认输出未归一化向量。embedder 中使用 `normalize_embeddings=True` 强制 L2 归一化，使 `metric_type="IP"`（内积）在数学上等价于 `COSINE`（余弦相似度），但计算更高效。
- **Kafka 消息为全量快照**：ES/Milvus 写入使用完整覆盖（upsert by `chunk_id`），要求 Kafka 消息必须包含 chunk 的全部字段，不支持增量字段更新。

---

## 3. 数据流

### 3.1 正常流程

```
Kafka poll (max 500 records)
  │ filter: granularity == "SMALL"
  ▼
按 partition 分桶: buffer[partition].append(msg)
                         │
                         ▼ 任一 partition >= batch_size OR 全局 timeout >= 3s
flush (针对单分区):
  1. embedder.encode(texts, normalize=True) → normed vectors[384]
  2. milvus.upsert(vectors, pk=chunk_id) ──┐
  3. es.streaming_bulk(docs, _id=chunk_id) ─┤ 并行, 逐条追踪错误
  4. wait both ✓                            │
  5. commit 该分区的 offset (max_offset + 1)
     (按分区独立桶化, 天然保证 offset 连续性)
```

### 3.2 失败降级

```
flush 失败 (单分区 Batch)
  │
  ├─ retry 1-3: 指数退避 (5s, 10s, 20s) → 全批重试
  │   └─ 成功 → commit offset
  │
  ├─ 拆小批 (10条/批) → 逐批串行处理 (同分区内顺序推进)
  │   └─ 每小批成功 → 立即 commit 该小批的 max_offset+1
  │
  ├─ 拆单条 → 5次退避重试 (2s, 4s, 8s, 16s, 32s)
  │   ├─ 成功 → commit 该单条 offset+1
  │   └─ 全败 → write_dlq(item, error) → commit 跳过该条 (offset 线推进)
  │
  │  (以上全部在同一分区内顺序推进, 不存在跨分区 offset 污染)
  ▼
DLQ 记录 → Celery Beat 每天凌晨扫 retry_count < 10 重新入队
         → /api/v1/indexing/dlq 管理接口
```

### 3.3 背压

```
buffer 全局总量 >= buffer_max_size (2000)
  │
  ├─ 暂停 poll
  ├─ 对各分区独立 flush
  ├─ sleep = 1.5 ^ (total_size / batch_size - 1) 秒
  └─ 总量 < 阈值 → resume poll
```

### 3.4 优雅关闭

```
SIGINT / SIGTERM
  │
  ├─ _shutdown_flag = True → 退出 while 循环
  ├─ while buffer: _degraded_process(batch)  # 排空残留
  │   └─ 若遇 ConnectionError → 立即中断排空, log critical, 退出
  │      (避免下游已挂时无限重试导致进程被 SIGKILL 强杀)
  ├─ consumer.commit()                        # 提交所有 offset
  └─ close (Kafka → Milvus → ES)              # 顺序关闭连接
```

---

## 4. 数据模型

### 4.1 ES Index Mapping

```json
PUT /chunks
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "30s",
    "analysis": {
      "analyzer": {
        "zh_analyzer": {
          "type": "ik_max_word",
          "use_smart": true
        }
      }
    }
  },
  "mappings": {
    "dynamic": false,
    "properties": {
      "chunk_id":     { "type": "keyword" },
      "doc_id":       { "type": "keyword" },
      "heading_path": { "type": "keyword" },
      "content":      { "type": "text", "analyzer": "zh_analyzer", "term_vector": "with_positions_offsets" },
      "department":   { "type": "keyword" },
      "file_type":    { "type": "keyword" },
      "granularity":  { "type": "keyword" },
      "page_start":   { "type": "integer" },
      "page_end":     { "type": "integer" },
      "token_count":  { "type": "integer" },
      "trace_id":     { "type": "keyword" },
      "created_at":   { "type": "date", "format": "strict_date_optional_time||epoch_millis" }
    }
  }
}
```

### 4.2 Milvus Collection Schema

```python
from pymilvus import Collection, FieldSchema, CollectionSchema, DataType

fields = [
    FieldSchema(name="chunk_id",     dtype=DataType.VARCHAR, is_primary=True, max_length=36),
    FieldSchema(name="embedding",    dtype=DataType.FLOAT_VECTOR, dim=384),
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

index_params = {
    "index_type": "IVF_FLAT",
    "metric_type": "IP",
    "params": {"nlist": 128},
}

# 注意: embedder 端已 normalize_embeddings=True,
# 归一化后 IP 等价于 COSINE 但计算更高效
# 注意: Milvus 调用 collection.load() 之前必须先建向量索引

if not collection.has_index():
    collection.create_index("embedding", index_params)

# 标量索引 (v1 数据量小时可省略, 减少 SQLite I/O 锁争用;
# 若需按 doc_id/department 过滤检索, 取消注释即可)
# if not collection.has_index():
#     collection.create_index("doc_id",     index_name="idx_doc_id")
#     collection.create_index("department", index_name="idx_department")

collection.load(consistency_level="BoundedConsistency")
```

### 4.3 DLQ 表

```sql
CREATE TABLE dead_letter_index (
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

CREATE INDEX idx_dlq_chunk_id ON dead_letter_index(chunk_id);
CREATE INDEX idx_dlq_retry     ON dead_letter_index(retry_count, created_at)
    WHERE retry_count < 10;
```

---

## 5. 配置

### constants.py 新增

```python
INDEX_BATCH_SIZE_DEFAULT = 200
INDEX_BATCH_TIMEOUT_DEFAULT = 3.0
INDEX_BUFFER_MAX_SIZE = 2000
INDEX_DLQ_MAX_RETRY = 10
INDEX_MAX_POLL_RECORDS = 500
INDEX_EMBEDDING_DIM = 384
INDEX_EMBEDDING_MODEL = "all-MiniLM-L6-v2"
INDEX_EMBED_BATCH_SIZE = 32             # sentence-transformer 内部 batch_size, 2核=16-32, 4核=32-64
INDEX_METRICS_PORT_DEFAULT = 9090
```

### config.py 新增

```python
es_url: str = "http://localhost:9200"
milvus_db_path: str = "data/milvus.db"
index_batch_size: int = 200
index_batch_timeout: float = 3.0
index_buffer_max_size: int = 2000
index_kafka_group_id: str = "indexing-v1"
index_metrics_port: int = 9090
index_embed_batch_size: int = 32
```

---

## 6. 可观测性

### 6.1 Prometheus 指标 (common/metrics.py)

```python
kafka_consumer_messages_consumed = Counter('kafka_consumer_messages_consumed_total', ..., ['topic'])
kafka_consumer_lag = Gauge('kafka_consumer_lag', ..., ['topic', 'partition'])
indexing_batch_size_avg = Histogram('indexing_batch_size', ...)
indexing_embedding_duration = Histogram('indexing_embedding_duration_seconds', ...)
indexing_milvus_write_duration = Histogram('indexing_milvus_write_duration_seconds', ...)
indexing_es_write_duration = Histogram('indexing_es_write_duration_seconds', ...)
indexing_batch_success = Counter('indexing_batch_success_total', ...)
indexing_batch_failure = Counter('indexing_batch_failure_total', ...)
indexing_dlq_messages = Counter('indexing_dlq_messages_total', ...)
```

### 6.2 structlog 上下文

每个日志绑定：`trace_id, batch_id, chunk_id, topic, partition, offset`

### 6.3 健康检查

独立 HTTP server（`:9090`）：
- `GET /health` — liveness：进程还活着即可返回 200
- `GET /ready` — readiness：Kafka/Milvus/ES 连接就绪，否则返回 503。**不检查缓冲区水位**（背压是正常自愈行为，不应触发 K8s 重启）。缓冲区积压通过 Prometheus 告警处理。
- `GET /metrics` — Prometheus text format

---

## 7. 目录结构增量

```
backend/
├── workers/tasks/
│   └── embed.py                       # NEW: Consumer 进程入口 + HTTP server
├── services/indexing/                 # NEW
│   ├── __init__.py
│   ├── consumer.py                    # Kafka consumer loop + 缓冲区 + 降级 + offset + 背压 + 优雅关闭
│   ├── embedder.py                    # sentence-transformer 封装 + 预热
│   ├── milvus_store.py                # collection 管理 + upsert (幂等)
│   └── es_store.py                    # index 管理 + helpers.streaming_bulk (逐条追踪, 幂等)
├── models/
│   └── dead_letter_index.py           # NEW: DLQ ORM
├── common/
│   ├── constants.py                   # MODIFY: +INDEX_* 常量
│   └── metrics.py                     # NEW: 9 个 Prometheus 指标
├── core/
│   └── config.py                      # MODIFY: +es_url, +milvus_db_path, +index_*
├── schemas/
│   └── index_dlq.py                   # NEW: DLQ DTO
├── api/routers/
│   └── index_dlq.py                   # NEW: DLQ 管理接口
└── tests/unit/services/indexing/
    ├── test_consumer.py
    ├── test_embedder.py
    └── test_stores.py
```

### docker-compose.yml 增量

```yaml
  elasticsearch:
    image: elasticsearch:8.15.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports: ["9200:9200"]
    volumes: [esdata:/usr/share/elasticsearch/data]
```

---

## 8. 自审清单

- [x] 无 TODO / TBD / 未完成段落
- [x] ES mapping 与 Milvus schema 标量字段对齐
- [x] chunk_id 幂等机制在双写两端一致（主键 / _id + upsert）
- [x] 失败降级路径完整：批 → 小批 → 单条 → DLQ
- [x] 背压 + 优雅关闭均已明确流程
- [x] 可观测性：9 个 Prometheus 指标 + structlog + /health
- [x] 向量归一化：`normalize_embeddings=True` 确保 IP 度量等价于 COSINE
- [x] Kafka offset 按分区独立提交，降级拆单条时不会跨分区跳 offset
- [x] ES 使用 `streaming_bulk` 逐条追踪错误，支持精确单条降级
- [x] Milvus Lite 单进程部署约束已明确警告
- [x] 优雅关闭遇 ConnectionError 立即退出，防止无限重试被 SIGKILL
- [x] 缓冲区按 Partition 分桶，跨分区 offset 污染不可能发生
- [x] Milvus 向量索引先于 `load()` 创建，标量索引 v1 可省略
- [x] Readiness 不检查缓冲区水位，背压通过 Prometheus 告警处理
