"""LLM entity/relation extractor with predefined ontology constraints."""

import json
import logging
from typing import Any

import httpx

from core.config import settings
from common.constants import GRAPHRAG_LLM_MAX_TOKENS

logger = logging.getLogger(__name__)

EXTRACTION_PROMPT = """你是一个企业文档知识图谱构建助手。从以下文本中抽取实体和关系，输出 JSON 数组。

实体类型（8类）:
- Policy: 制度/规章/办法
- Person: 人员/角色持有人
- Dept: 部门/组织
- Process: 流程/步骤
- Role: 角色定义/岗位
- Regulation: 外部法规/标准
- Risk: 风险项
- Document: 文件/附件

关系类型（14种）:
approves, reports_to, is_responsible_for, triggers, flows_to,
depends_on, belongs_to, references, complies_with, assigned_to,
participates_in, supervises, leads_to, mitigates

约束:
- 时间（如"2026年5月"）必须作为实体属性，严禁独立成实体节点
- 数值指标（如"100万元"）必须作为实体属性，严禁独立成实体节点
- 如果一个实体可能被超过 100 条关系连接，请作为属性而非节点

输出格式:
[
  {
    "entity1": {"name": "法务部", "type": "Dept", "aliases": ["法律事务部"]},
    "relation": {"type": "is_responsible_for", "confidence": 0.9},
    "entity2": {"name": "合同审查制度", "type": "Policy", "aliases": ["合同管理办法"]}
  }
]

文本:
{text}

JSON 输出:"""


class Extractor:
    def __init__(self):
        self._clients = {
            "primary": (settings.graphrag_llm_api_url, settings.graphrag_llm_api_key, settings.graphrag_llm_model),
            "backup": (settings.graphrag_backup_llm_api_url, settings.graphrag_backup_llm_api_key, settings.graphrag_backup_llm_model),
        }

    async def extract(self, text: str) -> list[dict[str, Any]]:
        """Extract entity-relation triples from text. Returns list of {entity1, relation, entity2}."""
        prompt = EXTRACTION_PROMPT.format(text=text[:GRAPHRAG_LLM_MAX_TOKENS * 4])

        for provider, (url, key, model) in self._clients.items():
            try:
                result = await self._call_llm(url, key, model, prompt)
                return self._parse_result(result)
            except Exception as e:
                logger.warning(f"{provider} LLM failed: {e}")
                if provider == "primary":
                    continue
                raise

        return []

    async def _call_llm(self, url: str, key: str, model: str, prompt: str) -> str:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{url}/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": GRAPHRAG_LLM_MAX_TOKENS,
                    "temperature": 0.1,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]

    def _parse_result(self, raw: str) -> list[dict[str, Any]]:
        """Parse LLM JSON output, handling markdown code fences."""
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1]
            if raw.endswith("```"):
                raw = raw[:-3]
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM JSON output: {raw[:200]}...")
            return []
