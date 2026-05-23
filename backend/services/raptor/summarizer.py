"""LLM API client for generating RAPTOR cluster summaries with primary/backup fallback."""

import logging
import httpx

from core.config import settings
from common.constants import RAPTOR_LLM_MAX_TOKENS

logger = logging.getLogger(__name__)

SUMMARY_PROMPT = """你是一个企业文档摘要助手。以下是一个文档章节的多个段落，请生成一个200-500字的摘要，保留关键事实、数字、制度和流程名称。用中文输出。

原文:
{text}

摘要:"""


class LLMClient:
    def __init__(self, api_url: str, api_key: str, model: str):
        self.api_url = api_url
        self.api_key = api_key
        self.model = model

    async def complete(self, prompt: str, max_tokens: int = RAPTOR_LLM_MAX_TOKENS) -> str:
        async with httpx.AsyncClient(timeout=60, trust_env=False) as client:
            resp = await client.post(
                f"{self.api_url}/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                    "temperature": 0.3,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]


class Summarizer:
    def __init__(self):
        self._primary = LLMClient(
            api_url=settings.raptor_llm_api_url,
            api_key=settings.raptor_llm_api_key,
            model=settings.raptor_llm_model,
        )
        self._backup = LLMClient(
            api_url=settings.raptor_backup_llm_api_url,
            api_key=settings.raptor_backup_llm_api_key,
            model=settings.raptor_backup_llm_model,
        )

    async def summarize(self, texts: list[str]) -> str:
        """Generate a summary for a cluster of text chunks."""
        combined = "\n\n---\n\n".join(texts)
        prompt = SUMMARY_PROMPT.format(text=combined[:3000])

        try:
            return await self._primary.complete(prompt)
        except Exception as primary_error:
            logger.warning(f"Primary LLM failed, trying backup: {primary_error}")
            try:
                return await self._backup.complete(prompt)
            except Exception as backup_error:
                logger.error(f"Both LLMs failed. Primary: {primary_error}, Backup: {backup_error}")
                raise


_summarizer: Summarizer | None = None


def get_summarizer() -> Summarizer:
    global _summarizer
    if _summarizer is None:
        _summarizer = Summarizer()
    return _summarizer
