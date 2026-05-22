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


DEMO_DATA = {
    "请假": {
        "answer": "根据《考勤管理制度》（第3章第2节），员工请假流程如下：\n\n1. 请假3天以内：由直属经理审批[1]\n2. 请假3-7天：直属经理审批后，需部门总监加签[2]\n3. 请假超过7天：需提交人力资源部备案，由分管副总裁审批[3]",
        "citations": [
            {"chunk_id": "demo-chunk-001", "doc_id": "demo-doc-001", "page_start": 12, "quote": "员工请假3天以内由直属经理审批。"},
            {"chunk_id": "demo-chunk-002", "doc_id": "demo-doc-001", "page_start": 12, "quote": "请假3-7天需经部门总监加签。"},
            {"chunk_id": "demo-chunk-003", "doc_id": "demo-doc-001", "page_start": 13, "quote": "超过7天提交人力资源部备案，由分管副总裁审批。"},
        ],
    },
    "合同": {
        "answer": "根据《合同管理制度》第5章，合同审查流程如下：\n\n1. 业务部门发起合同申请，填写合同审批表[1]\n2. 法务部在2个工作日内完成合规审查[2]\n3. 金额超过100万的合同需经财务总监审核[3]",
        "citations": [
            {"chunk_id": "demo-chunk-101", "doc_id": "demo-doc-002", "page_start": 28, "quote": "业务部门发起合同申请，填写合同审批表。"},
            {"chunk_id": "demo-chunk-102", "doc_id": "demo-doc-002", "page_start": 29, "quote": "法务部应在2个工作日内完成合规审查。"},
            {"chunk_id": "demo-chunk-103", "doc_id": "demo-doc-002", "page_start": 29, "quote": "金额超过100万的合同需经财务总监审核。"},
        ],
    },
    "数据安全": {
        "answer": "根据《数据安全管理制度》，公司数据分级为：\n\n1. 公开数据：可对外发布，无需审批[1]\n2. 内部数据：公司内部分享，需部门负责人审批[2]\n3. 机密数据：仅授权人员访问，需数据安全委员会审批[3]",
        "citations": [
            {"chunk_id": "demo-chunk-201", "doc_id": "demo-doc-003", "page_start": 5, "quote": "公开数据可对外发布，无需审批。"},
            {"chunk_id": "demo-chunk-202", "doc_id": "demo-doc-003", "page_start": 6, "quote": "内部数据公司内部分享，需部门负责人审批。"},
            {"chunk_id": "demo-chunk-203", "doc_id": "demo-doc-003", "page_start": 6, "quote": "机密数据仅授权人员访问，需数据安全委员会审批。"},
        ],
    },
}


def _match_demo(query: str) -> dict | None:
    for keyword, data in DEMO_DATA.items():
        if keyword in query:
            return data
    return None


@router.post("/search/answer", response_model=AnswerResponse)
async def search_answer(
    body: AnswerRequest,
    user: dict = Depends(get_current_user),
):
    """Corrective RAG: search + relevance filter + LLM answer generation."""
    # Demo mode: if no LLM configured, return demo data for known queries
    from core.config import settings

    demo = _match_demo(body.query)
    if demo and not settings.graphrag_llm_api_key:
        return AnswerResponse(
            answer=demo["answer"],
            citations=demo["citations"],
            confidence=1.0,
            no_answer=False,
            is_fallback=False,
        )

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
