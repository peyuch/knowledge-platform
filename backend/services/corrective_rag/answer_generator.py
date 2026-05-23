"""LLM answer generation with forced citation — Few-Shot JSON Schema prompt."""

import json
import re
import logging
import httpx

from core.config import settings

logger = logging.getLogger(__name__)

ANSWER_SYSTEM_PROMPT = """你是一个企业知识库问答助手。请根据以下检索到的文档片段回答用户问题。

你必须以 JSON 格式输出，格式如下:
```json
{
  "answer": "你的回答内容",
  "citations": [{"chunk_id": "引用来源ID", "quote": "引用的原文内容"}],
  "confidence": 0.85,
  "no_answer": false
}
```

规则:
1. 只使用提供的文档片段, 不要编造任何信息。每个断言必须标注引用来源。
2. 无法找到答案时, 设 no_answer 为 true, answer 为 "未找到相关信息, 请补充查询条件"。
3. confidence 取值范围 0.0-1.0, 表示你对答案的置信度。
4. 回答要简洁、专业, 以要点形式呈现。
5. 优先使用最新、最权威的来源。

示例 1 (有答案):
问题: 谁负责审批采购申请?
文档: [ref: chunk-001] 来源: 采购管理办法, 第12页
采购申请金额在10万元以下的, 由部门负责人审批; 超过10万元的, 由财务总监审批。
输出:
```json
{
  "answer": "审批权限分两级:\\n- 10万元以下: 部门负责人审批\\n- 10万元以上: 财务总监审批",
  "citations": [{"chunk_id": "chunk-001", "quote": "采购申请金额在10万元以下的, 由部门负责人审批; 超过10万元的, 由财务总监审批"}],
  "confidence": 0.9,
  "no_answer": false
}
```

示例 2 (无答案):
问题: 公司年假政策是什么?
文档: [ref: chunk-002] 来源: 考勤管理办法, 第5页
员工每天需打卡两次, 迟到超过30分钟记为旷工半天。
输出:
```json
{
  "answer": "未找到相关信息, 请补充查询条件",
  "citations": [],
  "confidence": 0.0,
  "no_answer": true
}
```
"""

ANSWER_USER_TEMPLATE = """文档片段:
---
{contexts}
---

问题: {query}

请以 JSON 格式输出回答。"""


class AnswerGenerator:
    async def generate(
        self, query: str, relevant_chunks: list[dict]
    ) -> dict:
        """Generate an answer with citations from relevant chunks.

        Returns a dict compatible with schemas.search.AnswerResponse.
        """
        if not relevant_chunks:
            return {
                "answer": "未找到相关信息, 请补充查询条件",
                "citations": [],
                "confidence": 0.0,
                "no_answer": True,
                "is_fallback": True,
            }

        contexts = "\n\n".join(
            self._format_chunk(i + 1, c) for i, c in enumerate(relevant_chunks)
        )

        try:
            answer_json = await self._call_llm(contexts[:6000], query)
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return {
                "answer": "服务暂时不可用, 请稍后重试",
                "citations": [],
                "confidence": 0.0,
                "no_answer": True,
                "is_fallback": True,
            }

        citations = self._extract_citations(answer_json, relevant_chunks)
        no_answer = answer_json.get("no_answer", False)
        is_fallback = no_answer or not citations

        return {
            "answer": answer_json.get("answer", ""),
            "citations": citations,
            "confidence": answer_json.get("confidence", 0.0),
            "no_answer": no_answer,
            "is_fallback": is_fallback,
        }

    @staticmethod
    def _format_chunk(index: int, chunk: dict) -> str:
        chunk_id = chunk.get("chunk_id", chunk.get("entity_id", f"unknown-{index}"))
        doc_id = chunk.get("doc_id", "unknown")
        page = chunk.get("page_start", "?")
        return f"[ref: {chunk_id}] 来源: {doc_id}, 第{page}页\n{chunk['content']}"

    async def _call_llm(self, contexts: str, query: str) -> dict:
        """Call the LLM API and parse the JSON response."""
        user_prompt = ANSWER_USER_TEMPLATE.format(contexts=contexts, query=query)

        async with httpx.AsyncClient(timeout=60, trust_env=False) as client:
            resp = await client.post(
                f"{settings.graphrag_llm_api_url}/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.graphrag_llm_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": settings.graphrag_llm_model,
                    "messages": [
                        {"role": "system", "content": ANSWER_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "max_tokens": 1200,
                    "temperature": 0.3,
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]

        # Extract JSON from possible markdown code fences
        json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", content, re.DOTALL)
        if json_match:
            content = json_match.group(1).strip()

        return json.loads(content)

    @staticmethod
    def _extract_citations(
        answer_json: dict, chunks: list[dict]
    ) -> list[dict]:
        """Build citation objects from the LLM response and source chunks."""
        if "citations" not in answer_json or not answer_json["citations"]:
            # Fallback: try to extract refs from the answer text
            answer_text = answer_json.get("answer", "")
            refs = set(re.findall(r"\[ref:\s*([^\]]+)\]", answer_text))
            chunk_map = {c.get("chunk_id", ""): c for c in chunks}
            return [
                {
                    "chunk_id": rid,
                    "doc_id": chunk_map.get(rid, {}).get("doc_id", ""),
                    "page": chunk_map.get(rid, {}).get("page_start"),
                    "quote": chunk_map.get(rid, {}).get("content", "")[:200],
                }
                for rid in refs
                if rid in chunk_map
            ]

        chunk_map = {c.get("chunk_id", ""): c for c in chunks}
        citations = []
        for cit in answer_json["citations"]:
            chunk_id = cit.get("chunk_id", "")
            src = chunk_map.get(chunk_id, {})
            citations.append(
                {
                    "chunk_id": chunk_id,
                    "doc_id": src.get("doc_id", ""),
                    "page": src.get("page_start"),
                    "quote": cit.get("quote", src.get("content", "")[:200]),
                }
            )
        return citations
