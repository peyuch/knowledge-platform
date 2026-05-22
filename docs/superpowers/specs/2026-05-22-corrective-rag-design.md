# Corrective RAG — 设计文档

**项目**：企业多模态知识中台 — 子项目 #6  
**日期**：2026-05-22  
**状态**：设计中  

---

## 1. 概述

### 1.1 目标

对检索结果进行自动相关性评估，过滤低质片段，调用 LLM 生成答案并强制溯源（文档/页码/段落），满足企业合规要求。

### 1.2 范围边界

**本期包含：**
- 检索结果相关性评估（relevance scoring per chunk）
- 低质片段过滤（score < threshold → 丢弃）
- LLM 答案生成 + 溯源输出（答案必须引用 chunk_id, doc_id, page）
- 幻觉抑制（无匹配来源时不编造，返回"未找到相关信息"）

**本期不包含：**
- Chat/对话应用层（会话管理、多轮对话、流式 SSE）
- 人工审核工作流

---

## 2. 架构设计

```
#5 检索结果 [{content, chunk_id, doc_id, score, source}]
  │
  ▼
┌──────────────────────────────────────┐
│  Corrective RAG Service              │
│                                       │
│  1. Relevance Check:                  │
│     直接复用 #5 返回的 rerank_score  │
│     → filter: score < 0.3 → discard  │
│     (不重复调 Reranker, 避免算力翻倍) │
│                                       │
│  2. Context Assembly (Token驱动贪婪): │
│     按 score 降序, 保证源多样性:     │
│     - 至少 2 条 Text (bm25/vector)   │
│     - 至少 1 条 Graph                │
│     - 总量 ≤ 3K token, 不限条数      │
│                                       │
│  3. LLM Generation + 三重幻觉防护:    │
│     a. Few-Shot JSON 强制格式        │
│     b. 输出后验证 chunk_id 存在性    │
│     c. 验证引用与原文一致性           │
│                                       │
│  4. 分级拒答:                         │
│     - 完全无: "未找到相关信息"        │
│     - 部分: 回答+标注"未找到XX信息"   │
│     - 低置信度: 回答+标注             │
└──────────────┬───────────────────────┘
               ▼
Response: {answer, citations, confidence, no_answer}
```

### 2.2 关键设计决策

- **复用 #5 分数**：不重复调 Reranker。`score < 0.3` 直接丢弃
- **Token 驱动贪婪填充**：不限条数，保证源多样性（Text≥2, Graph≥1），总量 ≤ 3K token
- **Few-Shot JSON 输出**：Prompt 含少样本示例，LLM 输出 `{"answer": "...", "citations": [{"chunk_id": "...", "quote": "..."}]}`
- **三重幻觉防护**：格式解析 → chunk_id 存在性检查 → 引用与原文一致性验证
- **分级拒答**：完全无匹配 / 部分匹配 / 低置信度三种策略
- **LLM 重试 + fallback**：3次退避重试 → 备用 LLM → 降级返回检索结果
- **confidence = avg(引用chunk的rerank_score)**

---

## 3. 接口定义

```
POST /api/v1/search/answer

Request:
{
  "query": "请假流程需要谁审批",
  "top_k": 10,
  "filters": { "department": "技术部" }
}

Response:
{
  "answer": "根据《考勤管理制度》，员工请假需经部门经理审批（第3章第2节）。主管级以下员工请假3天内由直属经理审批[ref: chunk-a1]，超过3天需总监审批[ref: chunk-a2]。",
  "citations": [
    {
      "chunk_id": "chunk-a1",
      "doc_id": "doc-001",
      "page": 12,
      "quote": "员工请假3天内由直属经理审批"
    },
    {
      "chunk_id": "chunk-a2",
      "doc_id": "doc-001",
      "page": 12,
      "quote": "超过3天需经总监审批"
    }
  ],
  "confidence": 0.85,
  "no_answer": false
}
```

---

## 4. LLM Prompt

```
你是一个企业知识库问答助手。请根据以下检索到的文档片段回答用户问题。
每个断言必须标注引用来源 [ref: chunk_id]。

规则:
1. 只使用提供的文档片段, 不要编造任何信息
2. 无法找到答案时, 回复 "未找到相关信息, 请补充查询条件"
3. 答案末尾列出所有引用的 chunk_id

文档片段:
---
[ref: chunk-uuid-1] 来源: 考勤管理制度.pdf, 第12页
员工请假3天内由直属经理审批, 超过3天需经总监审批...

[ref: chunk-uuid-2] 来源: 人力资源手册.pdf, 第5页
...

问题: {query}

回答:
```

---

## 5. 目录结构增量

```
backend/
├── services/corrective_rag/          # NEW
│   ├── __init__.py
│   ├── relevance_checker.py          # Reranker 相关性过滤
│   ├── answer_generator.py           # LLM 答案生成 + 溯源
│   └── hallucination_guard.py        # 幻觉检测 + 拒答
├── api/routers/
│   └── search.py                     # MODIFY: + POST /api/v1/search/answer
└── schemas/
    └── search.py                     # MODIFY: + AnswerRequest/AnswerResponse
```

---

## 6. 自审清单

- [x] Relevance 阈值明确 (0.3)
- [x] 最多 5 条 context, 总量 ≤ 3K token
- [x] 强制溯源 prompt + [ref: chunk_id] 格式
- [x] 拒答机制防止幻觉
