"""Search API endpoints."""

import logging

from fastapi import APIRouter, Depends

from schemas.search import SearchRequest, SearchResponse, AnswerRequest, AnswerResponse
from api.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["search"])


def _get_search_service():
    """Lazy-import and wire up the search service with real stores."""
    from services.search.search_service import SearchService
    from services.search.hybrid_searcher import HybridSearcher
    from services.indexing.es_store import ESStore
    from services.indexing.milvus_store import MilvusStore
    from services.graphrag.neo4j_store import Neo4jStore

    es = None
    milvus = None
    neo4j = None

    try:
        es = ESStore()
    except Exception as e:
        logger.warning(f"ES store unavailable: {e}")

    try:
        milvus = MilvusStore()
    except Exception as e:
        logger.warning(f"Milvus store unavailable: {e}")

    try:
        neo4j = Neo4jStore()
    except Exception as e:
        logger.warning(f"Neo4j store unavailable: {e}")

    return SearchService(HybridSearcher(es, milvus, neo4j))


@router.post("/search", response_model=SearchResponse)
async def search(
    body: SearchRequest,
    user: dict = Depends(get_current_user),
):
    """Unified hybrid search with reranking.

    Routes queries through BM25 (ES), vector (Milvus), and graph (Neo4j)
    recall in parallel, fuses results with weighted Min-Max normalization,
    and reranks with BGE-Reranker v2-m3.
    """
    svc = _get_search_service()
    result = await svc.search(
        query=body.query,
        top_k=body.top_k,
        filters=body.filters.model_dump(exclude_none=True) if body.filters else None,
    )

    return SearchResponse(**result)


@router.post("/search/answer", response_model=AnswerResponse)
async def search_answer(
    body: AnswerRequest,
    user: dict = Depends(get_current_user),
):
    """Corrective RAG: search + relevance filter + LLM answer generation.

    Retrieves relevant chunks via hybrid search, filters out irrelevant
    results with BGE-Reranker at threshold 0.3, then generates a cited
    answer via the configured GraphRAG LLM.
    """
    from services.corrective_rag.relevance_checker import RelevanceChecker
    from services.corrective_rag.answer_generator import AnswerGenerator

    svc = _get_search_service()
    search_result = await svc.search(
        query=body.query,
        top_k=body.top_k,
        filters=body.filters.model_dump(exclude_none=True) if body.filters else None,
    )

    checker = RelevanceChecker()
    relevant = checker.filter(body.query, search_result["results"])

    generator = AnswerGenerator()
    result = await generator.generate(body.query, relevant)

    return AnswerResponse(**result)
