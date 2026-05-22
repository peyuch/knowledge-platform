# 企业多模态知识中台

面向全公司提供低幻觉、可溯源、高并发的多模态智能问答，支撑合规、审计、业务提效。

**子项目 #1：文档摄入管线** — 接收 PDF/Word/PPT/Excel/图片/音视频，通过 MinerU / 阿里云 ASR 解析，按标题结构分块，通过 Kafka 推送给下游。

## 架构

```
前端 (React+Vite :5173)
  → FastAPI (:8000) → Celery (RabbitMQ) → MinerU / ASR
                                          ↓
                    Celery Beat poller (每5s查状态)
                                          ↓
                    Celery chunk task → 事务写 chunks + outbox → PG
                                          ↓
                    Outbox Poller (每5s) → Kafka (knowledge.ingestion.chunks)
```

## 快速开始

### 1. 环境准备

```bash
# conda 环境 (Python 3.11)
conda create -n knowledge-platform python=3.11 -y
conda activate knowledge-platform

# 安装后端依赖
cd backend && uv pip install -e ".[dev]" -i https://mirrors.aliyun.com/pypi/simple

# 安装前端依赖
cd frontend && npm install
```

### 2. 启动基础设施

```bash
docker compose -f docker/docker-compose.yml up -d
```

### 3. 数据库迁移

```bash
cd backend && alembic upgrade head
```

### 4. 启动服务

```bash
# 后端 API
cd backend && uvicorn api.main:app --reload --port 8000

# Celery Workers (GPU + CPU 隔离)
celery -A core.celery worker -Q gpu_queue --concurrency=2
celery -A core.celery worker -Q cpu_queue --concurrency=4

# 定时调度
celery -A core.celery beat

# 前端
cd frontend && npm run dev
```

### 5. 验证

```bash
curl http://localhost:8000/health
curl -H "Authorization: Bearer dev-token" http://localhost:8000/api/v1/tasks/
```

开发模式下使用 `Authorization: Bearer dev-token` 绕过 JWT，获得 admin 权限。

## 测试

```bash
cd backend && python -m pytest tests/unit/ -v
```

## 关键技术决策

- **部门归属在 task 上**：同文件多部门上传 → 复用 document，各建 task，不重复解析，`documents` 表纯存内容元数据
- **MinerU 非阻塞轮询**：parse 提交后立即释放 Worker，Celery Beat poller 查状态
- **Transactional Outbox**：chunks + outbox 同一 PG 事务 → outbox_poller → Kafka (at-least-once)
- **标题感知分块**：H1/H2 → LARGE chunk (RAPTOR)，H3/H4 → SMALL chunk (检索/实体抽取)

## 技术栈

| 组件 | 选型 |
|------|------|
| 后端框架 | FastAPI (Python 3.11) |
| 任务队列 | Celery + RabbitMQ |
| 消息队列 | Kafka |
| 文档解析 | MinerU 3.1 |
| 语音识别 | 阿里云 ASR (主) / 腾讯云 ASR (备) |
| 数据库 | PostgreSQL 15+ (pgvector) |
| 对象存储 | MinIO |
| 缓存/锁 | Redis |
| 认证 | JWT (RBAC 部门×角色) |
| 前端 | React + TypeScript + Vite |
| 可观测性 | OpenTelemetry + Prometheus |

## 基础设施端口

| 服务 | 端口 |
|------|------|
| FastAPI | 8000 |
| PostgreSQL | 5432 |
| MinIO | 9000 / 9001 |
| RabbitMQ | 5672 / 15672 |
| Redis | 6379 |
| Kafka | 9092 |
| 前端 | 5173 |
