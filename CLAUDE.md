# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

企业多模态知识中台 — 子项目 #1：文档摄入管线。接收 PDF/Word/PPT/Excel/图片/音视频，通过 MinerU（文档）/ 阿里云 ASR（音视频）解析，按标题结构分块，通过 Kafka 推送给下游。

## 常用命令

```bash
# 激活 conda 环境
source activate knowledge-platform
# Python 路径: /d/Anaconda/envs/knowledge-platform/python

# 安装依赖
cd backend && uv pip install -e ".[dev]" -i https://mirrors.aliyun.com/pypi/simple

# 启动基础设施 (Docker Desktop 需运行)
docker compose -f docker/docker-compose.yml up -d

# 数据库迁移
cd backend && alembic upgrade head
cd backend && alembic revision --autogenerate -m "description"

# 运行测试
cd backend && python -m pytest tests/unit/ -v              # 所有单元测试
cd backend && python -m pytest tests/unit/services/ingestion/test_chunker.py -v  # 单文件

# 启动服务
cd backend && uvicorn api.main:app --reload --port 8000     # FastAPI
cd backend && celery -A core.celery worker -Q gpu_queue --concurrency=2
cd backend && celery -A core.celery worker -Q cpu_queue --concurrency=4
cd backend && celery -A core.celery beat                    # 定时调度器

# 前端
cd frontend && npm install && npm run dev                   # Vite dev on :5173
cd frontend && npm run build                                # 生产构建

# 验证
curl http://localhost:8000/health
curl -H "Authorization: Bearer dev-token" http://localhost:8000/api/v1/tasks/
```

API 开发模式使用 `Authorization: Bearer dev-token` 绕过 JWT，获得 admin 权限。

## 架构

```
前端 (React+Vite :5173)
  → FastAPI (:8000) → Celery (RabbitMQ) → MinerU/ASR
                                          ↓
                    Celery Beat poller (每5s查状态)
                                          ↓
                    Celery chunk task → 事务写 chunks+outbox → PG
                                          ↓
                    Outbox Poller (每5s) → Kafka (knowledge.ingestion.chunks)
```

**关键设计决策：**

- **部门归属在 task 上，不在 document 上**：`documents` 表纯存内容元数据（`file_hash UNIQUE`），`department` 存在 `ingestion_tasks.task_metadata`（DB 列名 `metadata`）。同文件多部门上传 → 复用 document，各建独立 task，不重复解析/分块/Kafka 推送。
- **MinerU 非阻塞轮询**：parse task 提交 MinerU 后立即释放 Worker slot，由 Celery Beat `poller` 每 5s 查状态，完成后触发 chunk task。不会 100 个 Worker 全部阻塞等 MinerU。
- **Transactional Outbox**：chunk 任务将 `chunks` INSERT 和 `outbox` INSERT 放同一 PG 事务，`outbox_poller` 独立发 Kafka，保证 at-least-once。
- **标题感知分块**：H1/H2 生成 LARGE chunk（给 RAPTOR），H3/H4 生成 SMALL chunk（给检索/实体抽取），LARGE 超 2048 token 直接丢弃（不切分，让 RAPTOR 聚合）。

## 目录职责

```
backend/
├── api/                FastAPI 路由 + 中间件 (JWT 认证, trace_id 注入)
│   ├── routers/        documents(上传/导入/详情), tasks(状态/列表/取消/重试)
│   └── middleware/     auth.py, tracing.py
├── workers/            Celery 任务 (tasks/ 只做编排, 业务逻辑在 services/)
│   ├── tasks/          parse.py (提交 MinerU/ASR 后释放), chunk.py (分块+outbox)
│   ├── poller.py       Beat: 查 MinerU/ASR 状态, 触发 chunk
│   ├── outbox_poller.py Beat: 扫未发布 outbox → Kafka
│   ├── heartbeat_checker.py Beat: 30s 扫心跳超时(120s) → 自动取消
│   ├── orphan_checker.py Beat: 5min 确保 cancelled 任务的 MinerU/ASR 也取消
│   └── dead_letter.py  重试耗尽后标记 FAILED
├── core/               基础设施单例: config(Settings), celery(app), database(async engine),
│                       minio(get_minio), kafka(get_kafka), redis(get_redis, acquire_lock)
├── services/ingestion/ 业务逻辑层
│   ├── chunker.py         标题感知分块算法 (mistune AST → ChunkDraft, 11 个单元测试)
│   ├── mineru_client.py   MinerU 3.1 API + 熔断器 (5次连续失败 → 30s打开)
│   ├── asr_client.py      阿里云(主) → 腾讯云(备) fallback
│   ├── outbox_service.py  build_outbox_records + publish_pending_outbox
│   ├── ingestion_service.py check_duplicate + create_task (幂等+审计)
│   └── document_service.py CRUD + 部门权限过滤
├── models/             SQLAlchemy 2.0 ORM: Document, IngestionTask, Chunk, Outbox, AuditLog, Batch
├── schemas/            Pydantic 请求/响应 DTO
└── common/             enums (TaskStatus, ChunkGranularity, FileType), constants
```

## 关键约定

- **Celery 数据库访问必须用同步引擎**：`create_engine(settings.database_url_sync)` + `sessionmaker`。只有 FastAPI 层用 `async_session_factory`。
- **`metadata` 列名冲突**：`IngestionTask` 的 JSONB 列在 DB 叫 `metadata`，Python 属性名是 `task_metadata`（避开 SQLAlchemy `declarative_base` 的 `metadata` 保留字）。
- **Worker 队列隔离**：`gpu_queue`（parse 任务，绑 GPU 节点）和 `cpu_queue`（chunk 任务，绑 CPU 节点）不混用。
- **Kafka 消息格式**：`send_batch()` 参数类型是 `list[tuple[str|None, dict]]` — 每个元素是 `(key, payload)` 对。
- **JWT 开发模式**：`APP_ENV=development` + token=`dev-token` → 跳过真实 JWT 验证，获得 `{user_id: "dev-user", department: "技术部", role: "admin"}`。

## 基础设施依赖

| 服务 | 端口 | 用途 |
|------|------|------|
| PostgreSQL (pgvector) | 5432 | 主数据库 |
| MinIO | 9000/9001 | S3 对象存储 |
| RabbitMQ | 5672/15672 | Celery broker |
| Redis | 6379 | 分布式锁、缓存 |
| Kafka | 9092 | 下游消息队列 |
