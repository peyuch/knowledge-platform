"""Relevance filter — rerank and discard low-scoring chunks.

Token-driven greedy fill: Text >= 2, Graph >= 1, up to 3K tokens.
Defense: if a source has fewer than the minimum, drop the hard constraint.
"""

import logging
from services.search.reranker import get_reranker
from common.constants import SEARCH_RERANKER_THRESHOLD

logger = logging.getLogger(__name__)

MIN_TEXT_CHUNKS = 2
MIN_GRAPH_CHUNKS = 1
MAX_CONTEXT_TOKENS = 3000


class RelevanceChecker:
    def __init__(self):
        self._reranker = get_reranker()

    def filter(self, query: str, candidates: list[dict]) -> list[dict]:
        """Rerank candidates, filter by threshold, then greedy token-budget fill."""
        if not candidates:
            return []

        reranked = self._reranker.rerank(query, candidates)
        relevant = [
            r for r in reranked if r.get("score", 0) >= SEARCH_RERANKER_THRESHOLD
        ]

        # Greedy fill: prioritize variety of sources, up to token budget
        text_chunks = [r for r in relevant if r.get("source") != "graph"]
        graph_chunks = [r for r in relevant if r.get("source") == "graph"]

        # Defense: if a source has fewer than min required, drop the constraint
        actual_min_text = min(MIN_TEXT_CHUNKS, len(text_chunks))
        actual_min_graph = min(MIN_GRAPH_CHUNKS, len(graph_chunks))

        kept = []
        token_count = 0

        # Fill text chunks (highest score first)
        for r in text_chunks:
            if len([x for x in kept if x.get("source") != "graph"]) >= 5:
                break
            tokens = self._estimate_tokens(r["content"])
            if token_count + tokens > MAX_CONTEXT_TOKENS:
                break
            kept.append(r)
            token_count += tokens

        # Fill graph chunks (highest score first)
        for r in graph_chunks:
            if len([x for x in kept if x.get("source") == "graph"]) >= 3:
                break
            tokens = self._estimate_tokens(r["content"])
            if token_count + tokens > MAX_CONTEXT_TOKENS:
                break
            kept.append(r)
            token_count += tokens

        # Validate minimums — if we don't have enough, add more from remaining
        text_count = len([x for x in kept if x.get("source") != "graph"])
        graph_count = len([x for x in kept if x.get("source") == "graph"])

        if text_count < actual_min_text:
            remaining = [r for r in text_chunks if r not in kept]
            for r in remaining[: (actual_min_text - text_count)]:
                kept.append(r)

        if graph_count < actual_min_graph:
            remaining = [r for r in graph_chunks if r not in kept]
            for r in remaining[: (actual_min_graph - graph_count)]:
                kept.append(r)

        # Sort kept by score descending
        kept.sort(key=lambda r: r.get("score", 0), reverse=True)

        logger.info(
            f"Relevance filter: {len(candidates)} → {len(kept)} "
            f"(text={text_count}, graph={graph_count}, tokens={token_count})"
        )
        return kept

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        """Rough token estimate: ~2 chars per token for Chinese, ~4 for English."""
        return max(1, len(text) // 2)
