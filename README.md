# 企业多模态知识中台

面向全公司提供低幻觉、可溯源、高并发的多模态智能问答，支撑合规、审计、业务提效。

## 架构

```
React 前端 (:5173)
  │
  ▼
FastAPI (:8000) ── POST /api/v1/search ──→ 混合检索 ──→ BGE-Reranker
  │                                              │
  │                                    ┌─────────┼─────────┐
  │                                    ▼         ▼         ▼
  │                                  ES BM25   Milvus   Neo4j
  │                                    │         │         │
  │                                    └─────────┼─────────┘
  │                                              ▼
  │                                     Corrective RAG
  │                                      ├─ 相关性过滤
  │                                      ├─ 幻觉抑制
  │                                      └─ 答案 + 溯源
  │
  ├─ 摄入管线 ──→ MinerU / ASR ──→ 标题感知分块 ──→ Kafka ──→ PG + MinIO
  │                                                         │
  ├─ 索引层 ←── Kafka (SMALL chunk) ──→ Milvus + ES          │
  ├─ RAPTOR ←── Kafka (LARGE chunk) ──→ 递归摘要树 ─────────┤
  └─ GraphRAG ← Kafka (全chunk) ──→ Neo4j 知识图谱 ─────────┘
```

## 子项目总览

| # | 子系统 | 职责 |
|---|--------|------|
| 1 | 文档摄入管线 | PDF/Word/PPT/Excel/图片/音视频 → MinerU/ASR 解析 → 标题感知分块 → Kafka |
| 2 | 索引与存储层 | 消费 SMALL chunk → sentence-transformer 嵌入 → Milvus + ES 双写 |
| 3 | RAPTOR 递归摘要树 | 消费 LARGE chunk → UMAP+GMM 聚类 → LLM 摘要 → 分层索引 |
| 4 | GraphRAG 知识图谱 | 消费全量 chunk → LLM 实体关系抽取 → Neo4j 子图 |
| 5 | 混合检索 + 重排序 | 查询路由 → BM25/向量/子图三路召回 → BGE-Reranker 重排 |
| 6 | Corrective RAG | 相关性过滤 → 幻觉抑制 → 强制溯源 → 答案生成 |
| 7 | API 服务层 | 统一端点、JWT/RBAC、限流熔断、可观测性 |

## 快速开始

### 1. 环境准备

```bash
conda create -n knowledge-platform python=3.11 -y
conda activate knowledge-platform
cd backend && uv pip install -e ".[dev]" -i https://mirrors.aliyun.com/pypi/simple
cd frontend && npm install
```

### 2. 启动基础设施

```bash
docker compose -f docker/docker-compose.yml up -d
# PostgreSQL + MinIO + RabbitMQ + Redis + Kafka + Elasticsearch + Neo4j
```

### 3. 数据库迁移

```bash
cd backend && alembic upgrade head
```

### 4. 启动服务

```bash
# FastAPI
cd backend && uvicorn api.main:app --reload --port 8000

# Celery — GPU Worker (解析任务)
celery -A core.celery worker -Q gpu_queue --concurrency=2

# Celery — CPU Worker (分块 + 索引任务)
celery -A core.celery worker -Q cpu_queue --concurrency=4

# Celery Beat (定时调度: poller, outbox, heartbeat, DLQ retry, RAPTOR rebuild, GraphRAG rebuild)
celery -A core.celery beat

# 独立 Consumer 进程 (索引层、RAPTOR、GraphRAG)
python -m workers.tasks.embed      # 索引 Consumer (:9090 /health /metrics)
python -m workers.tasks.raptor     # RAPTOR Consumer
python -m workers.tasks.graph      # GraphRAG Consumer

# 前端
cd frontend && npm run dev
```

### 5. 验证

```bash
# 健康检查
curl http://localhost:8000/health

# 文档上传 (dev-token 绕过 JWT)
curl -X POST http://localhost:8000/api/v1/documents/upload \
  -H "Authorization: Bearer dev-token" \
  -F "file=@test.pdf"

# 智能检索
curl -X POST http://localhost:8000/api/v1/search \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"query": "请假流程需要谁审批", "top_k": 10}'

# 问答 (检索 + RAG 生成)
curl -X POST http://localhost:8000/api/v1/search/answer \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{"query": "合同审查流程是什么", "top_k": 10}'
```

## 测试

```bash
cd backend && python -m pytest tests/unit/ -v    # 37 tests
```

## 关键技术决策

- **文档与部门解耦**：`documents` 存内容，`department` 在 `ingestion_tasks.metadata`，同文件多部门不重复解析
- **MinerU 非阻塞轮询**：parse 提交即释放 Worker，Celery Beat poller 查状态
- **Transactional Outbox**：chunks + outbox 同一 PG 事务 → at-least-once Kafka
- **标题感知分块**：H1/H2 → LARGE (RAPTOR)，H3/H4 → SMALL (检索+实体抽取)
- **单库闭环**：Neo4j 统一存图+向量+属性，无分布式事务
- **UMAP+GMM 软聚类**：一个 chunk 可属多个簇，解决多主题重叠
- **智能路由 + 三路并行**：短查询走 BM25，长查询走向量，关联型走子图
- **强制溯源**：答案必须标注 `chunk_id + page_start`，无匹配拒绝编造

## 技术栈

| 组件 | 选型 | 用途 |
|------|------|------|
| 后端框架 | FastAPI (Python 3.11) | API 服务 |
| 任务队列 | Celery + RabbitMQ | 异步任务 |
| 消息队列 | Kafka | 子系统解耦 |
| 文档解析 | MinerU 3.1 | PDF/Word/PPT |
| 语音识别 | 阿里云 ASR (主) / 腾讯云 ASR (备) | 音视频 |
| 数据库 | PostgreSQL 15+ (pgvector) | 主存储 |
| 向量库 | Milvus Lite | 语义检索 |
| 搜索引擎 | Elasticsearch 8 + IK 分词 | BM25 全文检索 |
| 图数据库 | Neo4j 5.20 | 知识图谱 |
| 对象存储 | MinIO | 文件存储 |
| 缓存/锁 | Redis | 分布式锁 |
| 嵌入模型 | all-MiniLM-L6-v2 | 384维向量 |
| 重排序 | BGE-Reranker v2-m3 | 检索重排 |
| 摘要/抽取 | DeepSeek (主) / 通义千问 (备) | LLM |
| 认证 | JWT (RBAC 部门×角色) | 安全 |
| 前端 | React + TypeScript + Vite | UI |
| 可观测性 | Prometheus + OpenTelemetry | 监控 |

## 基础设施端口

| 服务 | 端口 |
|------|------|
| FastAPI | 8000 |
| 前端 | 5173 |
| PostgreSQL | 5432 |
| MinIO | 9000 / 9001 |
| RabbitMQ | 5672 / 15672 |
| Redis | 6379 |
| Kafka | 9092 |
| Elasticsearch | 9200 |
| Neo4j | 7474 / 7687 |
