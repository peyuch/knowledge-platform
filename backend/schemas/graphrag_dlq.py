"""Pydantic schemas for GraphRAG DLQ API."""

from datetime import datetime
from pydantic import BaseModel


class DlqGraphragItem(BaseModel):
    id: str
    chunk_id: str
    error_type: str | None = None
    error_message: str | None = None
    retry_count: int
    created_at: datetime

    model_config = {"from_attributes": True}


class DlqGraphragListResponse(BaseModel):
    items: list[DlqGraphragItem]
    total: int
    page: int
    page_size: int


class DlqGraphragRetryResponse(BaseModel):
    id: str
    status: str
