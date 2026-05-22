# API 服务层 — 设计文档

**项目**：企业多模态知识中台 — 子项目 #7  
**日期**：2026-05-22  
**状态**：设计中  

---

## 1. 概述

### 1.1 目标

统一 #5 检索和 #6 Corrective RAG 的 API 入口，提供 `POST /api/v1/search` 和 `POST /api/v1/search/answer`，整合现有 #1 的文档/任务管理端点。

### 1.2 范围边界

**本期包含：**
- `POST /api/v1/search` — 混合检索
- `POST /api/v1/search/answer` — Corrective RAG 答案生成
- 统一 OpenAPI 文档（Swagger）
- rate limiting 调整

**本期不包含：**
- Chat/对话应用层（会话管理、多轮对话）
- WebSocket

**预留接口（v2）：**
- `POST /api/v1/search/answer/stream` — SSE 流式输出，`Content-Type: text/event-stream`

---

## 2. 统一错误格式

所有端点返回统一错误结构：

```json
{
  "code": 10001,
  "message": "参数错误",
  "details": "query字段不能为空",
  "request_id": "uuid"
}
```

| 错误码段 | 模块 |
|----------|------|
| 10000-10999 | 通用（参数校验、认证） |
| 11000-11999 | 检索（#5） |
| 12000-12999 | RAG（#6） |
| 13000-13999 | 文档管理（#1） |

---

## 3. 超时规范

| 环节 | 超时 |
|------|------|
| 检索总超时 | 10s |
| LLM 生成超时 | 30s |
| DB 查询超时 | 5s |
| 外部 API 调用超时 | 10s |

---

## 4. 认证与限流

- 所有端点强制 JWT Bearer Token（已有 #1 的 `dependencies.get_current_user`）
- RBAC：`admin` 可访问 DLQ/管理端点，`viewer` 仅可访问查询端点
- 限流：普通用户 10 QPS，管理端点 100 QPS，上传 5 QPS
- **LLM 并发控制**：`/search/answer` 单用户最大并发 2，全局最大并发 20。超限返回 429
- **LLM 首字超时 8s**：超时立即触发降级，不白等 30s
- LLM 熔断：连续 5 次失败 → 熔断 60s → half-open 探测
- LLM 不可用时 `/search/answer` 降级返回检索结果 + `is_fallback: true`
- AnswerResponse 增加 `is_fallback: bool` 字段，前端据此提示用户"AI生成超时，已切换为精准搜索"

---

## 5. 端点汇总

现有（#1）+ 新增（#5/#6）：

| 端点 | 方法 | 来源 |
|------|------|------|
| `/api/v1/documents/upload` | POST | #1 |
| `/api/v1/documents/import` | POST | #1 |
| `/api/v1/documents/{id}` | GET | #1 |
| `/api/v1/tasks` | GET | #1 |
| `/api/v1/tasks/{id}` | GET | #1 |
| `/api/v1/tasks/{id}/cancel` | POST | #1 |
| `/api/v1/tasks/{id}/retry` | POST | #1 |
| `/api/v1/indexing/dlq` | GET | #2 |
| `/api/v1/indexing/dlq/{id}/retry` | POST | #2 |
| `/api/v1/graphrag/dlq` | GET | #4 |
| `/api/v1/graphrag/dlq/{id}/retry` | POST | #4 |
| **`/api/v1/search`** | **POST** | **#5 新增** |
| **`/api/v1/search/answer`** | **POST** | **#6 新增** |

---

## 6. 可观测性

- **结构化日志**：`request_id`, `user_id`, `module`, `latency`, `error_code`
- **Prometheus 指标**：`api_requests_total`(端点/状态码/角色), `api_request_duration_seconds`, `llm_requests_total`(模型/状态), `llm_request_duration_seconds`, `search_recall_count`(来源)
- **分布式追踪**：OpenTelemetry 贯穿 API → 检索 → LLM 全链路

## 7. CORS 配置

已在 #1 `api/main.py` 配置（`allow_origins=["*"]` 开发模式，生产改为具体域名）。

---

## 8. 目录结构增量

```
```
依赖链（胶水层模式）:
  api/routers/search.py     ← 统一路由网关, 不写业务逻辑
    │
    ├─ POST /search → services/search/search_service.py (纯检索)
    │
    └─ POST /search/answer → services/search/search_service.py → services/corrective_rag/answer_generator.py
                              (#6 接收 #5 的干净检索结果, 不自调检索, 避免循环依赖)
```

```
backend/
├── api/main.py                       # MODIFY: 注册 search router
├── api/routers/
│   └── search.py                     # NEW/MODIFY: POST /search, /search/answer (胶水层)
└── schemas/
    └── search.py                     # NEW/MODIFY: 共享 DTO
```
```

---

## 4. 自审清单

- [x] 所有端点归属明确
- [x] Swagger 自动生成
