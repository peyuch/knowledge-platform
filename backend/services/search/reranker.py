"""BGE-Reranker v2-m3 wrapper for result reranking."""

import logging
from sentence_transformers import CrossEncoder

from core.config import settings
from common.constants import RERANKER_MODEL

logger = logging.getLogger(__name__)


class Reranker:
    def __init__(self, model_name: str = RERANKER_MODEL):
        self.model = CrossEncoder(model_name)
        # Warm-up to avoid cold-start latency on first call
        self.model.predict([("warm up query", "warm up doc")])
        logger.info(f"Reranker initialized with {model_name}")

    def rerank(self, query: str, candidates: list[dict]) -> list[dict]:
        """Rerank candidates by relevance to query. Higher score = more relevant."""
        if not candidates:
            return []

        pairs = [(query, c["content"]) for c in candidates]
        scores = self.model.predict(pairs)

        for c, s in zip(candidates, scores):
            c["score"] = float(s)

        candidates.sort(key=lambda r: r.get("score", 0), reverse=True)
        return candidates


_reranker: Reranker | None = None


def get_reranker() -> Reranker:
    global _reranker
    if _reranker is None:
        _reranker = Reranker()
    return _reranker
