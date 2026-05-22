"""Pydantic schemas for document API."""

from datetime import datetime
from pydantic import BaseModel


class DocumentUploadResponse(BaseModel):
    task_id: str
    doc_id: str | None = None
    file_hash: str
    status: str
    duplicate: bool = False


class ImportFileItem(BaseModel):
    url: str
    filename: str
    metadata: dict | None = None


class DocumentImportRequest(BaseModel):
    files: list[ImportFileItem]


class DocumentImportResponse(BaseModel):
    batch_id: str
    tasks: list[dict]


class ChunkItem(BaseModel):
    chunk_id: str
    heading_level: str
    granularity: str
    heading_path: list[str]
    content: str
    page_start: int | None = None
    page_end: int | None = None


class ChunkPage(BaseModel):
    items: list[ChunkItem]
    page: int
    page_size: int
    total: int


class DocumentDetailResponse(BaseModel):
    doc_id: str
    filename: str
    file_type: str
    file_hash: str
    original_url: str | None = None
    markdown_url: str | None = None
    departments: list[str] = []
    chunks: ChunkPage | None = None
    created_at: datetime | None = None
