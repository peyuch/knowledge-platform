# 文档摄入管线 — 设计文档

**项目**：企业多模态知识中台 — 子项目 #1  
**日期**：2026-05-21  
**状态**：待评审  

---

## 1. 概述

### 1.1 目标

构建文档摄入管线（Ingestion Pipeline），将多模态文档（PDF、Word、PPT、Excel、图片、音视频）统一解析、分块、存储，并通过 Kafka 推送给下游索引、RAPTOR、GraphRAG 等子系统。

### 1.2 范围边界

**本期包含：**
- 前端单文件上传 + API 批量导入
- MinerU 文档解析（PDF / DOCX / PPTX / XLSX / 图片）
- 商业 ASR 音视频字幕提取（主用阿里云，备选腾讯云）
- JWT 认证 + 部门级 RBAC 权限控制
- Markdown 标题感知分块（为 RAPTOR + GraphRAG 提供多粒度 chunk）
- Celery + RabbitMQ 异步任务编排（GPU/CPU Worker 隔离），Kafka 解耦下游（Transactional Outbox）
- 幂等性、分布式锁、指数退避重试、死信队列、任务取消 + 心跳检测
- 全链路 tracing、Prometheus 监控、AlertManager 告警

**本期不包含（属于后续子项目）：**
- 向量化入库（子项目 #2 索引与存储层）
- RAPTOR 递归摘要树构建（子项目 #3）
- GraphRAG 实体关系抽取（子项目 #4）
- 混合检索与重排序（子项目 #5）
- Corrective RAG（子项目 #6）

### 1.3 触发方式

| 方式 | 描述 |
|------|------|
| 前端上传 | `POST /api/v1/documents/upload`，`multipart/form-data`，单个文件 |
| API 批量导入 | `POST /api/v1/documents/import`，传入文件列表（含 MinIO 路径 + 元数据） |
| 无自动扫描 | 不监听文件夹，不自动触发 |

### 1.4 支持的文件格式

| 格式 | 解析方式 | 说明 |
|------|----------|------|
| PDF（含扫描件） | MinerU pipeline / VLM | 扫描件自动 OCR |
| DOCX / PPTX / XLSX | MinerU 原生解析 | 3.1 新增原生支持 |
| PNG / JPG | MinerU OCR | |
| TXT / MD | 直通，跳过 MinerU | 无需解析，直接分块 |
| MP4 / MP3 | 商业 ASR API | ffmpeg 抽音频 → 阿里云（主）/ 腾讯云（备）语音识别 |

文件限制：单个文件 ≤ 100MB，批量导入 ≤ 100 个/次。

### 1.5 SLA / SLO

| 指标 | 目标 |
|------|------|
| 系统可用性 | 99.5%（月度） |
| 单文件 P95 处理时长 | ≤ 2 分钟（50 页 PDF 为参考基准） |
| 批量 100 文件整体完成 | ≤ 30 分钟 |
| 失败任务人工响应 | ≤ 2 小时（工作时间） |
| 失败任务解决 | ≤ 8 小时（工作时间） |

### 1.6 项目里程碑

| 阶段 | 内容 | 交付物 |
|------|------|--------|
| Phase 1 | 单文件上传 + 同步解析（MVP） | 可用 API，单 Worker 验证 |
| Phase 2 | 批量导入 + 异步 Celery + 死信 + 取消 + 幂等 | 完整摄入管线，集成测试通过 |
| Phase 3 | 压测（100 并发上传）+ 灰度（1-2 试点部门） | 性能报告，试点反馈 |
| Phase 4 | 全量推广 + 监控面板 | 全公司可用 |

---

## 2. 架构设计

### 2.1 架构总览

```
                          ┌─────────────────────────────┐
                          │        前端 / API 调用        │
                          └─────────────┬───────────────┘
                                        │
                          ┌─────────────▼───────────────┐
                          │  FastAPI Server              │
                          │  ├─ JWT 认证中间件            │
                          │  ├─ slowapi 限流             │
                          │  ├─ SHA256(file)             │
                          │  ├─ Redis 分布式锁 (幂等)     │
                          │  └─ INSERT ingestion_tasks    │
                          └─────────────┬───────────────┘
                                        │ apply_async (指定 gpu_queue)
                          ┌─────────────▼───────────────┐
                          │    RabbitMQ (Celery Broker)  │
                          │    ├─ gpu_queue (parse)      │
                          │    └─ cpu_queue (chunk)      │
                          └─────────────┬───────────────┘
                                        │
          ┌─────────────────────────────┼──────────────────────────┐
          │                             │                          │
┌─────────▼────────┐          ┌─────────▼────────┐    ┌────────────────────┐
│  GPU Worker      │          │  CPU Worker      │    │  Celery Beat       │
│  (parse 任务)    │          │  (chunk 任务)    │    │  ├─ poller         │
│                  │          │                  │    │  │  每 5s 查状态   │
│  1. POST MinerU  │          │  1. 读 Markdown  │    │  ├─ heartbeat      │
│  2. 存 mineru_id │          │  2. 标题感知分块 │    │  │  check (30s)    │
│  3. 释放 Worker  │          │  3. 事务写 PG    │    │  └─ orphan_checker │
│                  │          │     (chunks +     │    │     (每 5min)     │
│                  │          │      outbox)      │    └────────────────────┘
│                  │          │  4. 更新状态      │
└──────────────────┘          └─────────┬────────┘
                                        │
                          ┌─────────────▼───────────────┐
                          │   PostgreSQL                 │
                          │   ├─ documents              │
                          │   ├─ ingestion_tasks         │
                          │   ├─ chunks                 │
                          │   ├─ outbox                 │◄── 与 chunks 同一事务写入
                          │   └─ audit_logs             │
                          └─────────────┬───────────────┘
                                        │
                          ┌─────────────▼───────────────┐
                          │   Outbox Poller              │
                          │   轮询 outbox, 批量发 Kafka   │
                          │   标记 published_at          │
                          └─────────────┬───────────────┘
                                        │
                          ┌─────────────▼───────────────┐
                          │   Kafka                      │
                          │   knowledge.ingestion.chunks │
                          └─────────────┬───────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
              索引 #2              RAPTOR #3           GraphRAG #4
```

### 2.2 关键设计决策

**MinerU 非阻塞轮询**：parse 任务提交 MinerU 后立即释放 Worker slot。Celery Beat poller 每 5 秒查询 MinerU 任务状态，完成后触发 chunk 任务。这样 100 并发上传不会占满 100 个 Worker 全部卡在等待。

**Transactional Outbox**：chunk 任务将 chunks INSERT 和 outbox INSERT 放在同一个 PG 事务中。独立 Outbox Poller 轮询 `published_at IS NULL` 的记录发 Kafka，标记 `published_at`。保证 at-least-once delivery。

**Worker 队列隔离**：`gpu_queue` 绑定 GPU 节点（parse 任务），`cpu_queue` 绑定 CPU 节点（chunk 任务），防止 chunk 抢占 parse 的 GPU 资源。

**文档与部门解耦**：`documents` 表仅存内容元数据（`file_hash UNIQUE`），`department` 归属于 `ingestion_tasks.metadata`。同物理文件被多部门上传时，只解析一次，各生成独立的 ingestion_task + 审计记录。不浪费 GPU、不重复存储、不丢审计。

### 2.3 模块职责

| 模块 | 职责 | 依赖 |
|------|------|------|
| `api/routers/documents.py` | 文件上传/导入/查询 HTTP 路由 | services |
| `api/routers/tasks.py` | 任务状态/列表/取消 HTTP 路由 | services |
| `api/middleware/auth.py` | JWT 认证中间件 | core.exceptions |
| `workers/tasks/parse.py` | 编排：提交 MinerU/ASR → 存 mineru_id → 释放 | mineru_client, asr_client |
| `workers/tasks/chunk.py` | 编排：读 Markdown → 调 chunker → 事务写 chunks+outbox | chunker |
| `workers/tasks/outbox_poller.py` | Celery Beat：每 5s 扫 outbox 未发布记录，批量发 Kafka | kafka |
| `workers/poller.py` | Celery Beat：每 5s 查 MinerU/ASR 状态，完成后触发 chunk | mineru_client |
| `workers/heartbeat_checker.py` | Celery Beat：每 30s 扫 running 任务心跳，超时自动取消 | models |
| `workers/base_task.py` | IngestionBaseTask 基类，约束 tasks/ 只做编排 | celery |
| `workers/dead_letter.py` | 死信队列管理 | celery |
| `workers/orphan_checker.py` | Celery Beat 每 5min 扫 cancelled 孤儿 | mineru_client |
| `services/ingestion/ingestion_service.py` | "一次摄入"完整生命周期（幂等、锁、调度、状态流转） | models, core |
| `services/ingestion/document_service.py` | Document CRUD + 按部门过滤（JOIN ingestion_tasks） | models, core |
| `services/ingestion/mineru_client.py` | MinerU API 客户端（提交/查询/取消），熔断 | core.config |
| `services/ingestion/asr_client.py` | ASR API 客户端（主阿里云、备腾讯云），熔断 + fallback | core.config |
| `services/ingestion/chunker.py` | 标题感知分块算法 | utils |
| `services/ingestion/outbox_service.py` | Outbox 写入 + 轮询发布 | models, kafka |

### 2.4 tasks/ 编排约定

所有 `workers/tasks/` 下的任务必须遵守：

- **只做编排**：调用 service + 写状态 + 更新进度
- **不写业务判断**：所有分支逻辑、错误处理策略下沉到 services/
- **必须继承 `IngestionBaseTask`**（定义在 `workers/base_task.py`）

### 2.5 Worker 队列隔离

```
gpu_queue (concurrency=GPU节点数 × 2, 绑 GPU 节点):
  - parse_document

cpu_queue (concurrency=CPU核心数, 绑 CPU 节点):
  - chunk_document

celery_beat (单实例):
  - poller              (每 5s)
  - outbox_poller       (每 5s)
  - heartbeat_checker   (每 30s)
  - orphan_checker      (每 5min)
```

### 2.6 分布式锁（幂等性）

批量导入场景下，多个请求可能并发上传同一文件。在 FastAPI 层基于 Redis 锁保护 `file_hash` 幂等检查：

```
1. Redis SETNX lock:ingestion:{file_hash}, TTL 60s
2. 获取锁成功 → 查 PG 判断是否重复 → 插入 ingestion_tasks → 释放锁
3. 获取锁失败 → 等待 100ms 重试 (最多 3 次) → 最终查到已插入的 task → 返回 duplicate=true
```

### 2.7 API 限流与熔断

| 层级 | 机制 | 配置 |
|------|------|------|
| FastAPI | slowapi 限流 | 单 IP 10 req/s，`/upload` 5 req/s |
| MinerU | tenacity 熔断器 | 连续 5 次失败 → 熔断 30s → half-open 探测 |
| ASR | tenacity 熔断器 | 连续 3 次失败 → fallback 到腾讯云 → 两次都失败则熔断 60s |

---

## 3. 数据流

### 3.1 正常流程

```
Client  FastAPI  RabbitMQ  GPU Worker   MinerU    Beat Poller  CPU Worker   PG        OutboxPoller  Kafka
  │       │        │         │            │           │            │          │            │           │
  │ POST  │        │         │            │           │            │          │            │           │
  │──────>│        │         │            │           │            │          │            │           │
  │       │ Redis 锁         │            │           │            │          │            │           │
  │       │ SHA256           │            │           │            │          │            │           │
  │       │ INSERT task      │            │           │            │          │            │           │
  │       │ (pending) ──────────────────────────────────────────────────────>│            │           │
  │       │ store raw ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌> MinIO                             │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │ apply_async(gpu_queue)──────>│            │           │            │          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │ 201 {task_id} │         │            │           │            │          │            │           │
  │<──────│        │         │            │           │            │          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │ consume │            │           │            │          │            │           │
  │       │        │────────>│            │           │            │          │            │           │
  │       │        │         │ status=parsing ────────────────────────────────>│            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │ POST /tasks│           │            │          │            │           │
  │       │        │         │───────────>│           │            │          │            │           │
  │       │        │         │ 返回 mineru_task_id      │            │          │            │           │
  │       │        │         │<───────────│           │            │          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │ INSERT mineru_task_id, update heartbeat ───────>│            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │ ☆ Worker 释放, 回到队列 │            │          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │ 每 5s 轮询 │            │          │            │           │
  │       │        │         │            │<──────────│            │          │            │           │
  │       │        │         │            │ GET /tasks/{id}        │          │            │           │
  │       │        │         │            │──────────>│            │          │            │           │
  │       │        │         │            │ done      │            │          │            │           │
  │       │        │         │            │<──────────│            │          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │ fetch Markdown+JSON    │          │            │           │
  │       │        │         │            │──────────>│            │          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │ store to MinIO         │          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │ update PG (markdown_url, stats) ──>│            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │ apply_async(cpu_queue) ──────────────────>│           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │           │ status=chunking ──────────>│           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │           │ 读 Markdown<──────────────│           │
  │       │        │         │            │           │ 标题感知分块 │          │            │           │
  │       │        │         │            │           │ 生成 chunk 树│          │            │           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │           │ BEGIN TXN ───────────────>│           │
  │       │        │         │            │           │ INSERT chunks ────────────>│           │
  │       │        │         │            │           │ INSERT outbox ────────────>│           │
  │       │        │         │            │           │ COMMIT ───────────────────>│           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │           │ status=storing ──────────>│           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │           │            │          │ 扫 outbox  │           │
  │       │        │         │            │           │            │          │<───────────│           │
  │       │        │         │            │           │            │          │ 批量发 Kafka         │
  │       │        │         │            │           │            │          │───────────────────────>│
  │       │        │         │            │           │            │          │ UPDATE published_at    │
  │       │        │         │            │           │            │          │───────────────────────>│
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │           │ status=done──────────────>│           │
  │       │        │         │            │           │            │          │            │           │
  │       │        │         │            │           │            │          │ 写 audit_log           │
```

### 3.2 典型耗时（50 页 PDF）

| 阶段 | 耗时 | 说明 |
|------|------|------|
| pending | < 1s | 文件上传 + Redis 锁 + SHA256 + 写 PG |
| parsing（提交） | < 2s | Worker 调 MinerU API，拿到 mineru_task_id 后释放 |
| parsing（矿工等） | 10-60s | MinerU 实际解析，Worker 已释放，poller 每 5s 查状态 |
| chunking | 1-5s | CPU Worker 读 Markdown → 分块 → 事务写 PG |
| storing + Kafka | < 3s | Outbox Poller 每 5s 扫一次，批量发布 |

### 3.3 MinerU 轮询机制

parse 任务使用 **提交即释放** 模式，不阻塞 Worker：

```
parse_document(task_id):
  1. 调用 mineru_client.submit(file) → 拿到 mineru_task_id
  2. UPDATE ingestion_tasks SET mineru_task_id=$1, last_heartbeat=NOW()
  3. return  (Worker 释放)

poller (Celery Beat, 每 5s):
  FOR each task WHERE status='parsing' AND mineru_task_id IS NOT NULL:
    status = mineru_client.query(mineru_task_id)
    IF status == 'done':
      fetch markdown → store MinIO → update PG
      apply_async(chunk_document, queue='cpu_queue')
    ELIF status == 'failed':
      handle retry / DLQ
    // ELSE: 继续轮询
```

### 3.4 Transactional Outbox

chunk 事务中同时写入 `outbox` 表，保证 chunk 数据与 Kafka 事件原子性：

```sql
BEGIN;
  INSERT INTO chunks (...);
  INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
    VALUES ('chunk', chunk_id, 'chunk.created', row_to_json(...));
COMMIT;
```

Outbox Poller（Celery Beat 每 5s）：

```sql
SELECT * FROM outbox
WHERE published_at IS NULL
ORDER BY created_at
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

- 批量发送到 Kafka
- `UPDATE outbox SET published_at = NOW() WHERE id IN (...)`
- 每条 Kafka 消息带 `outbox_id`，下游幂等消费

### 3.5 重试与死信

```
parse / chunk 失败
  │
  ├─ retry 1: 延迟 30s, count=1
  │   └─ 失败
  ├─ retry 2: 延迟 60s, count=2
  │   └─ 失败
  ├─ retry 3: 延迟 120s, count=3
  │   └─ 失败
  ▼
路由到 DLQ (ingestion.dlq)
  │
  ├─ PG status = failed, error_message = 最后一次错误
  │
  ▼
人工介入 → POST /api/v1/tasks/{task_id}/retry → 生成新 task (retry_of=旧ID, retry_count+1)
```

SLA：失败任务 2 小时内人工响应，8 小时内解决。

### 3.6 取消流程（含心跳检测）

```
POST /api/v1/tasks/{task_id}/cancel
  │
  ├─ 1. 读 PG 获取 mineru_task_id
  ├─ 2. 调 MinerU DELETE 接口取消（若 mineru_task_id 存在）
  ├─ 3. Celery revoke(task_id, signal='SIGTERM')
  ├─ 4. UPDATE status = cancelled
  │
  ▼
heartbeat_checker (Celery Beat, 每 30s):
  扫 status IN ('parsing','chunking') AND last_heartbeat < NOW() - INTERVAL '120s'
    → 自动取消, UPDATE status = cancelled, error_message = '心跳超时，自动取消'

orphan_checker (Celery Beat, 每 5min):
  扫 status = 'cancelled' AND mineru_task_id IS NOT NULL
    → 查 MinerU API 确认状态
    → 若仍 running → 再次调取消
```

### 3.7 幂等性

- 上传时计算文件 SHA256 → 加 Redis 锁 → 查 `documents.file_hash`
- **文档已存在**（`documents` 有记录）→ 复用该 `doc_id`，不做重复解析，创建新的 `ingestion_tasks`（带本部门的 metadata），`doc_id` 指向已有文档
- **文档不存在** + 无进行中 task → 新文件，正常流程（创建 documents + ingestion_tasks + 触发解析）
- 已有 `pending` / `parsing` / `chunking` / `storing`（同 hash）→ 返回已有 `task_id`，提示"处理中"
- 已有 `failed` / `cancelled`（同 hash + 同部门）→ 允许重新上传，生成新 `task_id`

**多部门上传同一文件示例：**
```
HR 上传《公司治理制度v3.pdf》→ SHA256=abc
  → documents 无记录 → 新建 document (id=D1) + task (id=T1, metadata={department:"HR"})
  → MinerU 解析 → chunk 入 PG + Kafka

合规部上传《公司治理制度v3.pdf》→ SHA256=abc
  → documents 已有 D1 → 复用 D1，不再解析
  → 新建 task (id=T2, doc_id=D1, metadata={department:"合规部"})
  → 状态直接 done（不触发解析、不分块、不推 Kafka）
  → 审计日志记录合规部上传操作
```

---

## 4. 标题感知分块算法

### 4.1 输入

MinerU 产出的 Markdown 文本（保留标题层级、段落、表格、公式）。

### 4.2 分块规则

| 规则 | 说明 |
|------|------|
| H1/H2 → LARGE chunk | H1/H2 下的所有内容聚合为一个 chunk，`granularity=LARGE`，最大 2048 token，供 RAPTOR 做递归摘要 |
| H3/H4/悬空文本 → SMALL chunk | H3/H4 下的内容、以及 H1/H2 下没有子标题包裹的悬空文本，独立成 SMALL chunk，最大 512 token，供语义检索和实体抽取 |
| LARGE 超限不切分 | 如果 H1/H2 内容超过 2048 token，不生成该 LARGE chunk，只生成其下的 SMALL chunk。RAPTOR 会自动聚合。切开的 LARGE chunk 失去语义完整性 |
| SMALL 超限自然断句 | 超过 512 token 时按句号/换行切分，保留前一个 chunk 最后 100 token 作为重叠 |
| 表格独立 | `<table>` 块不切分，整个表格作为一个独立 SMALL chunk |
| 代码/公式保持完整 | 不截断代码块和 LaTeX 公式块 |
| 悬空段落归属 | 无标题段落的 `parent_id` 指向最近的上级标题节点，`heading_level` 等于上级标题 level + 1 |
| content_hash | 每个 chunk 计算 MD5，同一文档下 `(doc_id, content_hash)` 唯一约束，重新解析时内容未变的 chunk 不重复向量化 |

### 4.3 示例

输入 Markdown：
```markdown
# 第一章 制度总则
## 1.1 适用范围
本制度适用于公司全体员工...
### 1.1.1 一般规定
员工需遵守...
```

产出 chunk 树：
```
# 第一章                    ← heading_level=H1, granularity=LARGE (≤2048t)
├─ ## 1.1 适用范围          ← heading_level=H2, granularity=LARGE (≤2048t)
│  ├─ 本制度适用于...        ← heading_level=H3, granularity=SMALL (leaf)
│  ├─ ### 1.1.1 一般规定    ← heading_level=H3, granularity=SMALL
│  │  ├─ 员工需遵守...       ← heading_level=H4, granularity=SMALL (leaf)
```

### 4.4 Kafka 消息格式

```json
{
  "event": "chunk.created",
  "outbox_id": "uuid",
  "chunk_id": "uuid",
  "doc_id": "uuid",
  "parent_id": "uuid | null",
  "heading_level": "H3",
  "granularity": "SMALL",
  "heading_path": ["第一章 制度总则", "1.1 适用范围", "1.1.1 一般规定"],
  "content": "员工需遵守...",
  "content_hash": "d41d8cd98f00b204e9800998ecf8427e",
  "page_start": 3,
  "page_end": 3,
  "trace_id": "abc123...",
  "metadata": {
    "file_type": "pdf",
    "original_filename": "公司管理制度v3.pdf",
    "department": "合规部"
  }
}
```

Topic: `knowledge.ingestion.chunks`  
批量推送：Outbox Poller 每 5s 扫一次，攒够最多 50 条一起 `send()`。  
下游按 `granularity` 过滤：RAPTOR 消费 `LARGE`，语义检索 + 实体抽取消费 `SMALL`。  
下游按 `outbox_id` 幂等消费（已处理过的 outbox_id 跳过）。  
Kafka 消息中 `metadata.department` 取自该次上传的 `ingestion_tasks.metadata`（首部门上传时即为该部门；复用文档时不会重复推送 chunk，故后续部门不会产生新 chunk 消息）。

---

## 5. API 设计

### 5.1 文档上传

```
POST /api/v1/documents/upload
Authorization: Bearer <JWT>
Content-Type: multipart/form-data

Form fields:
  file:        (binary, required)
  metadata:    (JSON string, optional)    -- 存入 ingestion_tasks.metadata
    department, category, tags, language  -- department 从 JWT 自动注入，也可手动覆盖

Response 201:
{
  "task_id": "uuid",
  "doc_id": "uuid | null",               -- null 表示新文档待解析；非 null 表示复用已有文档
  "file_hash": "sha256...",
  "status": "pending",                   -- 新文档；若复用已有文档则为 "done"
  "duplicate": false
}
```

文件限制：单文件 ≤ 100MB。支持格式：PDF, DOCX, PPTX, XLSX, TXT, MD, PNG, JPG, MP4, MP3。

### 5.2 批量导入

```
POST /api/v1/documents/import
Authorization: Bearer <JWT>
Content-Type: application/json

{
  "files": [
    { "url": "s3://bucket/path/to/doc.pdf", "filename": "制度文件v3.pdf", "metadata": {...} },
    ...
  ]
}

Response 201:
{
  "batch_id": "uuid",
  "tasks": [
    { "task_id": "uuid", "file_hash": "...", "filename": "制度文件v3.pdf" },
    ...
  ]
}
```

批量导入 ≤ 100 个/次。

### 5.3 任务状态

```
GET /api/v1/tasks/{task_id}
Authorization: Bearer <JWT>

Response 200:
{
  "task_id": "uuid",
  "batch_id": "uuid | null",
  "retry_of": "old-task-uuid | null",
  "original_filename": "制度文件v3.pdf",
  "file_type": "pdf",
  "file_hash": "sha256...",
  "status": "parsing",
  "progress": 45,
  "current_step": "MinerU 解析中, 12/34 页",
  "error_message": null,
  "retry_count": 0,
  "stats": {
    "chunk_count": null,
    "total_tokens": null,
    "parse_duration_ms": 23400,
    "chunk_duration_ms": null
  },
  "created_at": "...",
  "updated_at": "..."
}
```

状态枚举：`pending | parsing | chunking | storing | done | failed | cancelled`

### 5.4 任务列表

```
GET /api/v1/tasks?status=failed&page=1&page_size=20&sort=created_at:desc
Authorization: Bearer <JWT>

参数:
  status       可选，筛选状态
  page         默认 1, 最小 1
  page_size    默认 20, 最小 1, 最大 100
  sort         默认 created_at:desc, 支持 created_at / updated_at / filename

Response 200:
{
  "items": [...],
  "total": 47,
  "page": 1,
  "page_size": 20
}
```

### 5.5 任务取消

```
POST /api/v1/tasks/{task_id}/cancel
Authorization: Bearer <JWT>
Response 200
```

### 5.6 失败重试

```
POST /api/v1/tasks/{task_id}/retry
Authorization: Bearer <JWT>
Response 201:
{
  "task_id": "new-uuid",
  "status": "pending",
  "retry_of": "old-task-uuid"
}
```

### 5.7 文档详情

```
GET /api/v1/documents/{doc_id}?chunks_page=1&chunks_page_size=50
Authorization: Bearer <JWT>

Response 200:
{
  "doc_id": "uuid",
  "filename": "制度文件v3.pdf",
  "file_type": "pdf",
  "file_hash": "sha256...",
  "original_url": "https://minio/bucket/raw/abc.pdf",
  "markdown_url": "https://minio/bucket/parsed/abc.md",
  "departments": ["合规部", "HR"],          -- 所有上传过此文档的部门 (从 ingestion_tasks 聚合)
  "chunks": {
    "items": [ { "chunk_id": "...", "heading_level": "H2", ... } ],
    "page": 1,
    "page_size": 50,
    "total": 340
  },
  "created_at": "..."
}
```

RBAC 限制：用户只能查看其部门已上传过的文档（通过 `ingestion_tasks.metadata->>'department'` 过滤）。

### 5.8 汇总

| 端点 | 方法 | 状态码 | 认证 | 用途 |
|------|------|--------|------|------|
| `/api/v1/documents/upload` | POST | 201 | JWT | 前端单文件上传 |
| `/api/v1/documents/import` | POST | 201 | JWT | API 批量导入 |
| `/api/v1/documents/{doc_id}` | GET | 200 | JWT | 文档详情 + chunk 分页 |
| `/api/v1/tasks` | GET | 200 | JWT | 任务列表（分页+筛选） |
| `/api/v1/tasks/{task_id}` | GET | 200 | JWT | 任务状态/进度 |
| `/api/v1/tasks/{task_id}/cancel` | POST | 200 | JWT | 取消进行中的任务 |
| `/api/v1/tasks/{task_id}/retry` | POST | 201 | JWT | 死信任务重新入队 |

---

## 6. 数据模型

### 6.1 DDL

```sql
-- ============================================================
-- 枚举
-- ============================================================
CREATE TYPE task_status AS ENUM (
    'pending', 'parsing', 'chunking', 'storing', 'done', 'failed', 'cancelled'
);

CREATE TYPE chunk_granularity AS ENUM ('LARGE', 'SMALL');

-- ============================================================
-- 批次
-- ============================================================
CREATE TABLE batches (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(256),
    created_by  VARCHAR(128),                       -- JWT subject
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- 文档（纯内容层，不绑定部门或上传者 — 这些属于 ingestion_tasks）
-- ============================================================
CREATE TABLE documents (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename        VARCHAR(512) NOT NULL,
    file_type       VARCHAR(16) NOT NULL,
    file_hash       VARCHAR(64) NOT NULL UNIQUE,    -- 全局唯一：同物理文件只解析一次
    file_size_bytes BIGINT,
    language        VARCHAR(8) DEFAULT 'zh',
    raw_url         VARCHAR(1024),
    markdown_url    VARCHAR(1024),
    json_url        VARCHAR(1024),
    page_count      INT,
    chunk_count     INT,
    total_tokens    BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_documents_file_type   ON documents(file_type);
CREATE INDEX idx_documents_created_at  ON documents(created_at DESC);

-- ============================================================
-- 摄入任务
-- ============================================================
CREATE TABLE ingestion_tasks (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id          UUID REFERENCES batches(id) ON DELETE SET NULL,
    doc_id            UUID REFERENCES documents(id) ON DELETE SET NULL,
    retry_of          UUID REFERENCES ingestion_tasks(id) ON DELETE SET NULL,
    trace_id          VARCHAR(64) NOT NULL,         -- 全链路追踪 ID

    file_hash         VARCHAR(64) NOT NULL,
    original_filename VARCHAR(512) NOT NULL,
    file_type         VARCHAR(16) NOT NULL,
    file_size_bytes   BIGINT,

    status            task_status NOT NULL DEFAULT 'pending',
    progress          SMALLINT DEFAULT 0,
    current_step      VARCHAR(256),
    error_message     TEXT,
    retry_count       SMALLINT DEFAULT 0,

    mineru_task_id    VARCHAR(64),
    asr_task_id       VARCHAR(64),

    last_heartbeat    TIMESTAMPTZ,                  -- 任务心跳, 每 30s 更新

    parse_duration_ms   BIGINT,
    chunk_duration_ms   BIGINT,

    metadata          JSONB DEFAULT '{}',          -- department, category, tags 等上传时附带的业务元数据
    created_by        VARCHAR(128),                 -- JWT subject

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_done_has_doc CHECK (status != 'done' OR doc_id IS NOT NULL)
);

CREATE INDEX idx_it_file_hash    ON ingestion_tasks(file_hash);
CREATE INDEX idx_it_status_time  ON ingestion_tasks(status, created_at DESC);
CREATE INDEX idx_it_batch_id     ON ingestion_tasks(batch_id);
CREATE INDEX idx_it_doc_id       ON ingestion_tasks(doc_id);
CREATE INDEX idx_it_retry_of     ON ingestion_tasks(retry_of);
CREATE INDEX idx_it_trace_id     ON ingestion_tasks(trace_id);
CREATE INDEX idx_it_metadata     ON ingestion_tasks USING GIN (metadata);
CREATE INDEX idx_it_heartbeat    ON ingestion_tasks(status, last_heartbeat)
    WHERE status IN ('parsing', 'chunking');
CREATE INDEX idx_it_orphan       ON ingestion_tasks(status, mineru_task_id)
    WHERE status = 'cancelled' AND mineru_task_id IS NOT NULL;

-- ============================================================
-- 分块
-- ============================================================
CREATE TABLE chunks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id          UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    parent_id       UUID REFERENCES chunks(id) ON DELETE CASCADE,
    outbox_id       UUID NOT NULL,                  -- 关联 outbox 记录

    heading_level   VARCHAR(4) NOT NULL,
    granularity     chunk_granularity NOT NULL,
    heading_path    TEXT[] NOT NULL DEFAULT '{}',
    content         TEXT NOT NULL,
    content_hash    VARCHAR(32) NOT NULL,

    token_count     INT,
    page_start      INT,
    page_end        INT,
    has_table       BOOLEAN DEFAULT FALSE,
    has_formula     BOOLEAN DEFAULT FALSE,
    has_image       BOOLEAN DEFAULT FALSE,
    sequence        INT NOT NULL,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_heading_level CHECK (heading_level IN ('H1','H2','H3','H4','LEAF'))
);

CREATE UNIQUE INDEX idx_chunks_doc_hash       ON chunks(doc_id, content_hash);
CREATE INDEX        idx_chunks_doc_sequence   ON chunks(doc_id, sequence);
CREATE INDEX        idx_chunks_doc_granularity ON chunks(doc_id, granularity);
CREATE INDEX        idx_chunks_parent         ON chunks(parent_id);
CREATE INDEX        idx_chunks_heading_path   ON chunks USING GIN (heading_path);
CREATE INDEX        idx_chunks_outbox_id      ON chunks(outbox_id);

-- ============================================================
-- Transactional Outbox
-- ============================================================
CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(64) NOT NULL,           -- 'chunk'
    aggregate_id    UUID NOT NULL,                  -- chunk.id
    event_type      VARCHAR(128) NOT NULL,          -- 'chunk.created'
    payload         JSONB NOT NULL,
    trace_id        VARCHAR(64) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ                     -- NULL = 未发布
);

CREATE INDEX idx_outbox_pending ON outbox(published_at, created_at)
    WHERE published_at IS NULL;

-- ============================================================
-- 审计日志
-- ============================================================
CREATE TABLE audit_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         VARCHAR(128) NOT NULL,          -- JWT subject
    action          VARCHAR(64) NOT NULL,           -- upload / import / cancel / retry / delete / view
    resource_type   VARCHAR(64) NOT NULL,           -- document / task
    resource_id     UUID NOT NULL,
    details         JSONB DEFAULT '{}',
    ip_address      INET,
    trace_id        VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_user     ON audit_logs(user_id, created_at DESC);
CREATE INDEX idx_audit_resource ON audit_logs(resource_type, resource_id);
CREATE INDEX idx_audit_trace    ON audit_logs(trace_id);

-- ============================================================
-- updated_at 自动触发器
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_documents_updated_at
    BEFORE UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_ingestion_tasks_updated_at
    BEFORE UPDATE ON ingestion_tasks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

### 6.2 表关系图

```
┌──────────┐     ┌──────────────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ batches  │────<│ ingestion_tasks          │     │    documents     │     │    audit_logs    │
│ - id     │     │ - id (PK)               │     │ - id (PK)        │     │ - id             │
│ - name   │     │ - batch_id (FK)          │────>│ - file_hash (U)  │     │ - user_id        │
│ - ...    │     │ - doc_id (FK) ───────────│────>│ - filename       │     │ - action         │
└──────────┘     │ - retry_of (FK)──────────│┐    │ - raw_url        │     │ - resource_type  │
                 │ - trace_id               ││    │ - markdown_url   │     │ - resource_id    │
                 │ - metadata (department,  ││    │ - ...            │     │ - trace_id       │
                 │     category, tags)      ││    └────────┬─────────┘     └──────────────────┘
                 │ - mineru_task_id         ││             │
                 │ - asr_task_id            ││             │ (done 后关联；多个 task 可指向同一 doc)
                 │ - last_heartbeat         ││             │
                 │ - status                 ││             ▼
                 │ - created_by             ││    ┌──────────────────┐     ┌──────────────────┐
                 └──────────────────────────┘│    │     chunks       │     │     outbox       │
                          │                  │    │ - id (PK)        │     │ - id (PK)        │
                          └──────────────────┘    │ - doc_id (FK)    │     │ - aggregate_type │
                          (retry_of 自引用)        │ - parent_id (FK)─│─┐   │ - aggregate_id   │──> chunks.id
                                                  │ - outbox_id (FK)─│───> │ - event_type     │
                                                  │ - heading_level  │ │   │ - payload        │
                                                  │ - granularity    │─┘   │ - trace_id       │
                                                  │ - heading_path   │     │ - published_at   │
                                                  │ - content        │     └──────────────────┘
                                                  │ - content_hash   │
                                                  └──────────────────┘
```

### 6.3 幂等查询

```sql
-- Step 1: 查文档是否已存在
SELECT id, markdown_url, chunk_count
FROM documents
WHERE file_hash = $1;
-- 有记录 → 复用 doc_id, 跳过解析 + 分块, 直接创建新 ingestion_task (status=done)

-- Step 2: 查是否有进行中的同 hash 任务
SELECT t.id AS task_id, t.status, t.doc_id, t.metadata->>'department' AS department
FROM ingestion_tasks t
WHERE t.file_hash = $1
  AND t.metadata->>'department' = $2    -- 同部门才判重
ORDER BY t.created_at DESC
LIMIT 1;
-- 无记录 → 新上传，正常流程
-- done    → 返回已有 task_id, duplicate=true
-- failed / cancelled → 允许重新上传, 返回新 task_id
-- 其他 (pending/parsing/chunking/storing) → 返回已有 task_id, 提示"处理中"
```

---

## 7. 安全与合规

### 7.1 认证与授权

| 层级 | 机制 |
|------|------|
| API 认证 | JWT Bearer Token，过期时间 24h，支持 refresh |
| 权限模型 | RBAC：`user → role → department × action` |
| 角色定义 | `admin`（全部门全权限）、`manager`（本部门上传+查看+删除）、`viewer`（本部门只读） |
| 权限检查 | FastAPI dependency `require_role(role, department)` 注入路由 |
| 文档级权限 | 用户能查看的文档 = `documents` JOIN `ingestion_tasks` WHERE `metadata->>'department'` = 用户部门 |

### 7.2 数据加密

| 层级 | 方式 |
|------|------|
| MinIO 存储 | 服务端 SSE-S3 加密（AES-256） |
| Kafka 传输 | TLS 1.3 双向认证 |
| PostgreSQL | 传输层 TLS，敏感字段（不需要，文档内容不加密以支持全文检索） |

### 7.3 审计日志

所有敏感操作写入 `audit_logs` 表：

| 操作 | 记录内容 |
|------|----------|
| upload / import | user_id, filename, file_hash, department, ip_address |
| cancel / retry | user_id, task_id, old_status → new_status |
| view document | user_id, doc_id, ip_address |
| delete document | user_id, doc_id, filename |

不记录文件内容本身。

### 7.4 数据保留与删除

- 文档删除：软删除 30 天 → 硬删除（MinIO + PG chunks）
- 失败任务记录保留 90 天
- 审计日志保留 1 年，超过归档到冷存储
- 用户无权查看非本部门文档

---

## 8. 可观测性

### 8.1 全链路追踪

每条请求生成 `trace_id`（UUID v7），贯穿全链路：

```
HTTP Header: X-Trace-Id → FastAPI → Celery task (header) → MinerU request (header)
  → PG (trace_id column) → Kafka record (trace_id field) → 下游
```

- Python: OpenTelemetry SDK + `opentelemetry-instrumentation-fastapi` + `opentelemetry-instrumentation-celery`
- Exporter: OTLP → Jaeger / Grafana Tempo

### 8.2 监控指标（Prometheus）

| 指标 | 类型 | 说明 |
|------|------|------|
| `ingestion_upload_total` | Counter | 上传总数，按 status/file_type 分 label |
| `ingestion_task_duration_seconds` | Histogram | 任务全流程耗时 P50/P95/P99 |
| `ingestion_parse_duration_seconds` | Histogram | MinerU 解析耗时 |
| `ingestion_chunk_duration_seconds` | Histogram | 分块耗时 |
| `ingestion_task_status` | Gauge | 各状态任务数量 |
| `ingestion_dlq_size` | Gauge | 死信队列积压量 |
| `kafka_outbox_lag` | Gauge | 未发布的 outbox 记录数 |
| `mineru_circuit_breaker_state` | Gauge | 熔断器状态 (0=closed, 1=open, 2=half-open) |
| `worker_utilization` | Gauge | gpu_queue / cpu_queue 活跃 Worker 比例 |

### 8.3 告警规则

| 规则 | 条件 | 级别 |
|------|------|------|
| 任务失败率飙升 | `rate(ingestion_upload_total{status=failed}[5m]) / rate(ingestion_upload_total[5m]) > 0.05` | Critical |
| DLQ 积压 | `ingestion_dlq_size > 10` | Warning |
| Kafka lag 过大 | `kafka_outbox_lag > 100` | Warning |
| 熔断器打开 | `mineru_circuit_breaker_state == 1` | Critical |
| GPU Worker 全部阻塞 | `worker_utilization{gpu} > 0.95` | Warning |
| 心跳超时任务堆积 | `ingestion_task_status{status=parsing} WITH no heartbeat > 120s` 持续 5 min | Warning |

---

## 9. 测试策略

| 层级 | 覆盖内容 | 工具 | 门禁 |
|------|----------|------|------|
| 单元测试 | chunker 算法（分块规则、边界 case、悬空段落归属）；幂等检查逻辑；红锁逻辑 | pytest | PR 门禁，覆盖率 ≥ 80% |
| 集成测试 | 完整摄入流程（upload → parse → chunk → Kafka 消费验证）；API 契约（所有端点 200/201/4xx）；RBAC 权限拒绝 | pytest + testcontainers (PG+Kafka+MinIO) | PR 门禁 |
| 性能测试 | 100 并发上传；50 页 PDF × 50 并发解析；Kafka 批量推送吞吐 | locust | Phase 3 门禁 |
| 乱码测试 | MinerU 输出异常 Markdown（无标题、嵌套错误、空表格）→ chunker 不崩溃 | pytest | PR 门禁 |

---

## 10. 部署与运维

### 10.1 本地开发

```bash
docker compose -f docker/docker-compose.yml up -d   # PG + MinIO + RabbitMQ + Kafka + Redis
cd backend && uvicorn api.main:app --reload          # FastAPI
celery -A core.celery worker -Q gpu_queue --concurrency=2
celery -A core.celery worker -Q cpu_queue --concurrency=4
celery -A core.celery beat                            # poller + heartbeat + outbox + orphan
cd frontend && npm run dev                            # Vite dev server
```

### 10.2 生产部署

```
┌─────────────────────────────────────────────────┐
│ K8s Cluster                                      │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ FastAPI  │  │ FastAPI  │  │ FastAPI  │  HPA  │
│  │ Pod × 3  │  │ Pod × 3  │  │ Pod × 3  │       │
│  └──────────┘  └──────────┘  └──────────┘       │
│                                                  │
│  ┌──────────┐  ┌──────────┐                     │
│  │GPU Worker│  │GPU Worker│  (GPU 节点, 固定)    │
│  │ Pod × 2  │  │ Pod × 2  │                     │
│  └──────────┘  └──────────┘                     │
│                                                  │
│  ┌──────────┐  ┌──────────┐                     │
│  │CPU Worker│  │CPU Worker│  (CPU 节点, HPA)     │
│  │ Pod × 4  │  │ Pod × 4  │                     │
│  └──────────┘  └──────────┘                     │
│                                                  │
│  ┌──────────┐  ┌──────────┐                     │
│  │CeleryBeat│  │Frontend  │                     │
│  │ Pod × 1  │  │ Pod × 2  │                     │
│  └──────────┘  └──────────┘                     │
│                                                  │
│  ┌──────────────────────────────────────────┐   │
│  │ MinerU Router (多实例负载均衡)              │   │
│  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

- MinerU 通过 `mineru-router` 多 GPU 实例负载均衡
- CPU Worker HPA：基于队列深度自动扩缩
- GPU Worker 固定 2 实例（GPU 资源有限）

### 10.3 灰度发布

1. 部署新版本到 1 个 FastAPI Pod + 1 个 Worker
2. 将 1-2 个试点部门的流量路由到新版本（基于 department header）
3. 观察 24h，对比错误率、P95 耗时
4. 无异常 → 全量滚动更新

### 10.4 回滚

- K8s: `kubectl rollout undo deployment/ingestion-api`
- Docker Compose: `docker compose down && docker compose -f docker-compose.yml up -d`（使用旧镜像 tag）
- 数据库：Alembic `downgrade` 到上一版本
- 回滚期间 MinIO/Kafka 无数据丢失（已在旧版本处理完毕）

---

## 11. 项目目录结构

```
knowledge-platform/
├── backend/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── dependencies.py          # get_db, get_current_user, require_role
│   │   ├── middleware/
│   │   │   ├── __init__.py
│   │   │   ├── auth.py              # JWT 认证中间件
│   │   │   └── tracing.py           # trace_id 注入
│   │   └── routers/
│   │       ├── __init__.py
│   │       ├── documents.py
│   │       └── tasks.py
│   │
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── task_registry.py         # from .tasks import *
│   │   ├── base_task.py             # IngestionBaseTask 基类
│   │   ├── dead_letter.py
│   │   ├── poller.py                # Celery Beat: MinerU/ASR 状态轮询
│   │   ├── outbox_poller.py         # Celery Beat: outbox → Kafka
│   │   ├── heartbeat_checker.py     # Celery Beat: 心跳超时扫除
│   │   ├── orphan_checker.py        # Celery Beat: cancelled 孤儿清理
│   │   └── tasks/
│   │       ├── __init__.py           # __all__ 显式导出
│   │       ├── parse.py
│   │       └── chunk.py
│   │
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── celery.py                # Celery 实例, 队列定义
│   │   ├── database.py
│   │   ├── redis.py                  # Redis 连接 (分布式锁)
│   │   ├── minio.py
│   │   ├── kafka.py
│   │   └── exceptions.py
│   │
│   ├── common/
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   └── enums.py
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   └── ingestion/
│   │       ├── __init__.py           # 写明各模块边界
│   │       ├── ingestion_service.py  # 摄入生命周期 (幂等、锁、调度、状态流转)
│   │       ├── document_service.py   # Document CRUD
│   │       ├── mineru_client.py      # MinerU API 客户端 + 熔断
│   │       ├── asr_client.py         # ASR API 客户端 (主/备 fallback) + 熔断
│   │       ├── chunker.py            # 标题感知分块算法
│   │       └── outbox_service.py     # Outbox 写入 + 发布
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── batch.py
│   │   ├── document.py
│   │   ├── chunk.py
│   │   ├── ingestion_task.py
│   │   ├── outbox.py
│   │   └── audit_log.py
│   │
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── document.py
│   │   ├── task.py
│   │   └── common.py
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── hash.py
│   │   ├── text.py
│   │   └── datetime.py
│   │
│   ├── migrations/
│   │   ├── env.py
│   │   └── versions/
│   ├── alembic.ini
│   │
│   └── tests/
│       ├── conftest.py
│       ├── unit/
│       │   ├── services/ingestion/
│       │   │   └── test_chunker.py
│       │   └── workers/
│       │       └── test_ingestion_service.py
│       ├── integration/
│       │   ├── test_api_documents.py
│       │   ├── test_api_tasks.py
│       │   └── test_ingestion_flow.py
│       └── performance/
│           └── locustfile.py
│
├── frontend/
│   ├── src/
│   │   ├── pages/ingestion/
│   │   │   ├── UploadPage.tsx
│   │   │   └── TaskListPage.tsx
│   │   ├── components/
│   │   │   ├── FileUploader.tsx
│   │   │   ├── TaskProgress.tsx
│   │   │   └── TaskTable.tsx
│   │   ├── api/
│   │   │   └── ingestion.ts
│   │   ├── App.tsx
│   │   └── main.tsx
│   ├── package.json
│   └── vite.config.ts
│
├── docker/
│   ├── docker-compose.yml           # 本地开发 (PG, MinIO, RabbitMQ, Kafka, Redis)
│   ├── docker-compose.prod.yml      # 生产环境 (资源配置, 密码, TLS)
│   ├── Dockerfile.api
│   ├── Dockerfile.worker-gpu
│   ├── Dockerfile.worker-cpu
│   └── Dockerfile.frontend
│
├── docs/superpowers/specs/
├── .env.example
├── pyproject.toml
└── README.md
```

---

## 12. 技术栈

| 组件 | 选型 | 说明 |
|------|------|------|
| 后端框架 | FastAPI (Python 3.11) | 异步，MinerU API 兼容 |
| 任务队列 | Celery + RabbitMQ | gpu_queue / cpu_queue 隔离 |
| 消息队列 | Kafka | Transactional Outbox 保证原子性 |
| 文档解析 | MinerU 3.1 | 异步 API + Router 多 GPU 负载均衡 |
| 语音识别 | 阿里云 ASR（主）→ 腾讯云 ASR（备 fallback） | 熔断 + 自动切换 |
| 数据库 | PostgreSQL 15+ | JSONB、GIN、CHECK、分区键预留 |
| 缓存 / 锁 | Redis | 分布式锁（幂等性） |
| 对象存储 | MinIO | SSE-S3 加密，S3 兼容 |
| 认证 | JWT (python-jose) | RBAC 部门 × 角色 |
| 限流 | slowapi | 单 IP 10 req/s |
| 熔断 | tenacity | MinerU / ASR 调用保护 |
| 前端 | React + TypeScript + Vite | |
| 可观测性 | OpenTelemetry + Prometheus + Jaeger | 全链路 trace |
| 依赖管理 | uv + pyproject.toml | |
| 数据库迁移 | Alembic | |

---

## 13. 成本估算框架

> 以下为成本估算公式，不包含具体金额。部署前填入实际单价。

| 成本项 | 估算方式 |
|--------|----------|
| MinerU GPU | GPU 实例单价 × 实例数 × 运行时长 × 利用率系数 |
| ASR API | 阿里云 ASR 单价/小时 × 预估月音视频时长 + 腾讯云备用量（按 10% 计算） |
| MinIO 存储 | 原始文件 × 1.0 + Markdown × 0.3 + JSON × 0.5 + 提取图片 × 0.2 倍原始大小 → 按 SSD 热存储单价 |
| Kafka | 月消息量 = 文档数 × 平均 chunk 数 × 1KB → 对应 Kafka 实例规格 |
| ASR 降级策略 | 月度 ASR 费用超预算阈值 → 自动切换开源 Whisper 兜底（v2 规划） |
| MinIO 生命周期 | 原始文件：热存储 90 天 → 冷归档；Markdown/JSON：热存储永久 |

---

## 14. 术语表

| 术语 | 说明 |
|------|------|
| MinerU | 开源文档解析引擎，将 PDF/图片/DOCX/PPTX/XLSX 转为 Markdown/JSON |
| ASR | 自动语音识别 (Automatic Speech Recognition) |
| RAPTOR | 递归摘要树 (Recursive Abstractive Processing for Tree-Organized Retrieval) |
| GraphRAG | 基于知识图谱的检索增强生成 |
| LARGE chunk | 粗粒度 chunk（H1/H2 级别），供 RAPTOR 构建递归摘要 |
| SMALL chunk | 细粒度 chunk（H3/H4/LEAF 级别），供语义检索和实体抽取 |
| DLQ | 死信队列 (Dead Letter Queue)，存放重试耗尽后仍失败的任务 |
| Transactional Outbox | 在数据库事务中同时写入业务数据和待发送消息，再由独立进程投递到消息队列的模式 |
| Celery Beat | Celery 内置定时调度器 |
| RBAC | 基于角色的访问控制 (Role-Based Access Control) |
| SSE-S3 | MinIO 服务端加密 (Server-Side Encryption with S3-managed keys) |

---

## 15. v2 迭代规划

以下功能在当前设计文档中注明方案，但实现排到 v2：

| 功能 | 当前方案 | v2 升级方向 |
|------|----------|------------|
| 任务编排 | Celery Chain | 事件驱动（parse 完成 → Kafka 事件 → chunk 消费） |
| MinerU 降级 | 仅 MinerU | MinerU 集群故障时切换轻量解析器兜底 |
| ASR 降级 | 阿里云 → 腾讯云 fallback | 预算超限时切换开源 Whisper 兜底 |
| 数据库分区 | 单表 | ingestion_tasks 按月分区，chunks 按 doc_id 哈希分区 |
| 前端大文件 | 基础上传 | 断点续传 + 分片上传 |
| 批量可视化 | API 返回 batch_id | 批量导入进度面板（已完成 30/100，失败 2） |

---

## 16. 自审清单

- [x] 无 TODO / TBD / 未完成段落
- [x] 架构与 DDL 一致（outbox / audit_logs / trace_id / heartbeat 均已建模）
- [x] 范围聚焦：仅 ingestion，未越界到下游子项目
- [x] 命名一致：`heading_level` / `granularity` / `trace_id` / `outbox_id` 全文统一
- [x] 文档与部门解耦：`documents` 只存内容，`department` 在 `ingestion_tasks.metadata`，同文件多部门上传不重复解析
- [x] 幂等路径完整：同 hash + 同部门 → duplicate；同 hash + 不同部门 → 复用 doc + 新 task
- [x] Kafka 原子性：Transactional Outbox 保证 at-least-once
- [x] Worker 不阻塞：提交即释放 + Celery Beat poller
- [x] 安全覆盖：JWT 认证、RBAC 授权、传输/存储加密、审计日志
- [x] 可观测性：全链路 trace、9 个 Prometheus 指标、6 条告警规则
- [x] 成本模型：ASR/GPU/MinIO/Kafka 估算公式 + 降级策略
- [x] 部署方案：K8s + 灰度 + 回滚
- [x] 术语表完整
