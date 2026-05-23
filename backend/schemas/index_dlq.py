"""Pydantic schemas for indexing DLQ management API."""

from datetime import datetime
from pydantic import BaseModel


class DlqItem(BaseModel):
    id: str
    chunk_id: str
    topic: str
    partition: int
    kafka_offset: int
    error_type: str | None = None
    error_message: str | None = None
    retry_count: int
    created_at: datetime

    model_config = {"from_attributes": True}


class DlqListResponse(BaseModel):
    items: list[DlqItem]
    total: int
    page: int
    page_size: int


class DlqRetryResponse(BaseModel):
    id: str
    status: str


class DlqDeleteResponse(BaseModel):
    id: str
    status: str
