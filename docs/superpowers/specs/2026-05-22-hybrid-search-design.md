# 混合检索 + 重排序 — 设计文档

**项目**：企业多模态知识中台 — 子项目 #5  
**日期**：2026-05-22  
**状态**：设计中  

---

## 1. 概述

### 1.1 目标

实现智能查询路由 + BM25/向量/GraphRAG 三路并行召回 + 动态加权融合 + BGE-Reranker 重排序，提供统一检索接口 `POST /api/v1/search`。

### 1.2 范围边界

**本期包含：**
- 查询分析（短/长 query + 类型分类：事实型/概念型/关联型）
- 三路并行召回：ES BM25 + Milvus 向量 + Neo4j 子图
- 动态加权融合（不同查询类型不同权重）
- `BAAI/bge-reranker-v2-m3` 重排序（进程内加载）
- 返回 `{content, chunk_id, doc_id, score, source}`

**本期不包含：**
- 端到端问答 / RAG 对话（属于 Chat 应用层）
- Corrective RAG 评估过滤（子项目 #6）

---

## 2. 架构设计

```
POST /api/v1/search {query, top_k, filters}
  │
  ▼
┌──────────────────────────────────────┐
│  Query Analyzer                      │
│  ├─ query_type: fact/concept/rel     │
│  ├─ is_short: len < 8 chars          │
│  └─ → RouteConfig {weights, top_k}   │
└──────────────┬───────────────────────┘
               │
    ┌──────────┼──────────┐
    ▼          ▼          ▼
┌───────┐ ┌───────┐ ┌─────────┐
│ ES    │ │Milvus │ │ Neo4j   │  三路并行
│ BM25  │ │向量   │ │ 子图    │
│ k1*3  │ │ k2*3  │ │ k3*3    │  top_k × 3 过召回
└───┬───┘ └───┬───┘ └────┬────┘
    │         │          │
    └────┬────┴────┬─────┘
         ▼
┌──────────────────────────────────────┐
│  Fusion                              │
│  ├─ Min-Max 归一化 per source        │
│  ├─ weighted = Σ w_i × score_i      │
│  └─ 合并去重 (by chunk_id)           │
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│  BGE-Reranker (bge-reranker-v2-m3)   │
│  rerank(query, candidates)           │
│  → Top-K reranked                    │
└──────────────┬───────────────────────┘
               ▼
Response: [{content, chunk_id, doc_id, score, source}, ...]
```

### 2.2 查询路由规则

| 条件 | query_type | BM25权重 | Vector权重 | Graph权重 |
|------|-----------|----------|------------|-----------|
| 短 + 事实型 (如 "请假流程") | fact | 0.6 | 0.3 | 0.1 |
| 长 + 概念型 (如 "数据安全的合规要求") | concept | 0.1 | 0.6 | 0.3 |
| 含关系关键词 (如 "谁负责审批") | rel | 0.2 | 0.3 | 0.5 |
| 默认 | concept | 0.2 | 0.5 | 0.3 |

### 2.3 关键设计决策

- **查询分类用规则 + LLM 兜底**：先关键词匹配（"谁/负责/审批/流程"），无法判断时用轻量 prompt 调用 LLM 分类
- **三路并行**：`asyncio.gather()` 并发执行，总耗时 = max(各路耗时)
- **分数归一化**：每路用 Min-Max归一化，然后加权求和
- **Reranker 复用 sentence-transformer 实例**

---

## 3. 接口定义

```
POST /api/v1/search

Request:
{
  "query": "请假流程需要谁审批",
  "top_k": 10,
  "filters": {
    "department": "技术部",           // 可选
    "file_type": "pdf",               // 可选
    "date_range": ["2024-01-01", null] // 可选
  }
}

Response:
{
  "results": [
    {
      "content": "员工请假需经部门经理审批...",
      "chunk_id": "uuid",
      "doc_id": "uuid",
      "score": 0.92,
      "source": "vector"              // "bm25" | "vector" | "graph"
    }
  ],
  "query_analysis": {
    "query_type": "rel",
    "is_short": false,
    "rewritten_query": null
  },
  "took_ms": 245
}
```

---

## 4. 目录结构增量

```
backend/
├── services/search/                  # NEW
│   ├── __init__.py
│   ├── query_analyzer.py             # 查询分析 + 路由
│   ├── hybrid_searcher.py            # 三路并行召回 + 融合
│   ├── reranker.py                   # BGE-Reranker 封装
│   └── search_service.py             # 统一入口, 编排
├── api/routers/
│   └── search.py                     # NEW: POST /api/v1/search
└── schemas/
    └── search.py                     # NEW: 请求/响应 DTO
```

---

## 5. 测试

| 文件 | 测试内容 |
|------|----------|
| `test_query_analyzer.py` | 短/长 query 分类, 事实/概念/关联分类 |
| `test_hybrid_searcher.py` | 三路并行 mock, 融合分数计算 |
| `test_reranker.py` | Reranker 重排分数递减 |

---

## 6. 自审清单

- [x] 查询路由规则表明确
- [x] 三路并行 + Min-Max 归一化 + 加权融合
- [x] 分数归一化公式明确
- [x] 溯源字段最小化 (`chunk_id, doc_id, score, source`)
