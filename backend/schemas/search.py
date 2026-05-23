"""Search request/response DTOs."""

from pydantic import BaseModel, Field


class SearchFilters(BaseModel):
    department: str | None = None
    file_type: str | None = None
    date_range: list[str | None] | None = None


class SearchRequest(BaseModel):
    query: str
    top_k: int = Field(default=10, ge=1, le=100)
    filters: SearchFilters | None = None


class SearchResultItem(BaseModel):
    content: str
    chunk_id: str
    doc_id: str
    score: float
    source: str  # "bm25" | "vector" | "graph"


class QueryAnalysis(BaseModel):
    query_type: str       # "fact" | "concept" | "rel"
    is_short: bool
    rewritten_query: str | None = None


class SearchResponse(BaseModel):
    results: list[SearchResultItem]
    query_analysis: QueryAnalysis
    took_ms: float


class AnswerRequest(BaseModel):
    query: str
    top_k: int = Field(default=10, ge=1, le=100)
    filters: SearchFilters | None = None


class Citation(BaseModel):
    chunk_id: str
    doc_id: str
    page: int | None = None
    quote: str


class AnswerResponse(BaseModel):
    answer: str
    citations: list[Citation]
    confidence: float
    no_answer: bool = False
    is_fallback: bool = False
