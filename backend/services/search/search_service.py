"""Search orchestrator — query analysis → hybrid search → rerank."""

import time

from services.search.query_analyzer import analyze_query
from services.search.hybrid_searcher import HybridSearcher
from services.search.reranker import get_reranker


class SearchService:
    def __init__(self, hybrid_searcher: HybridSearcher):
        self._searcher = hybrid_searcher
        self._reranker = get_reranker()

    async def search(
        self, query: str, top_k: int = 10, filters: dict | None = None
    ) -> dict:
        t0 = time.time()

        # 1. Query analysis
        analysis = analyze_query(query)

        # 2. Hybrid search (three-way parallel)
        candidates = await self._searcher.search(
            query, analysis["query_type"], top_k, filters
        )

        # 3. Rerank
        reranked = self._reranker.rerank(query, candidates)

        # 4. Return
        results = [
            {
                "content": r["content"],
                "chunk_id": r.get("chunk_id", r.get("entity_id", "")),
                "doc_id": r.get("doc_id", ""),
                "score": r.get("score", 0),
                "source": r.get("source", "unknown"),
            }
            for r in reranked[:top_k]
        ]

        return {
            "results": results,
            "query_analysis": analysis,
            "took_ms": (time.time() - t0) * 1000,
        }
