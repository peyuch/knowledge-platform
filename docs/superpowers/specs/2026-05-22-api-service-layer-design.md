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
- Chat/对话应用层（会话管理、SSE 流式、多轮对话）
- WebSocket

---

## 2. 端点汇总

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

## 3. 目录结构增量

```
backend/
├── api/main.py                       # MODIFY: 注册 search router
├── api/routers/
│   └── search.py                     # NEW/MODIFY: POST /search, /search/answer
└── schemas/
    └── search.py                     # NEW/MODIFY: SearchRequest, SearchResponse, AnswerRequest, AnswerResponse
```

---

## 4. 自审清单

- [x] 所有端点归属明确
- [x] Swagger 自动生成
