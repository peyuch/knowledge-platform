"""Document upload / import / detail API endpoints."""

import uuid
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from schemas.document import (
    DocumentUploadResponse,
    DocumentImportRequest,
    DocumentImportResponse,
    DocumentDetailResponse,
    ChunkItem,
    ChunkPage,
)
from api.dependencies import get_current_user
from utils.hash import sha256_hex

router = APIRouter(prefix="/api/v1/documents", tags=["documents"])
limiter = Limiter(key_func=get_remote_address)


@router.post("/upload", response_model=DocumentUploadResponse, status_code=201)
@limiter.limit("5/second")
async def upload_document(
    request: Request,
    file: UploadFile = File(...),
    metadata: str = Form(default="{}"),
    user: dict = Depends(get_current_user),
):
    """Upload a single document file. Returns task_id for status polling."""
    content = await file.read()
    file_hash = sha256_hex(content)
    task_id = str(uuid.uuid4())

    return DocumentUploadResponse(
        task_id=task_id,
        file_hash=file_hash,
        status="pending",
        duplicate=False,
    )


@router.post("/import", response_model=DocumentImportResponse, status_code=201)
@limiter.limit("10/second")
async def import_documents(
    request: Request,
    body: DocumentImportRequest,
    user: dict = Depends(get_current_user),
):
    """Bulk import documents from MinIO URLs."""
    batch_id = str(uuid.uuid4())
    tasks = []
    for item in body.files[:100]:
        tasks.append({
            "task_id": str(uuid.uuid4()),
            "file_hash": sha256_hex(item.url.encode()),
            "filename": item.filename,
        })
    return DocumentImportResponse(batch_id=batch_id, tasks=tasks)


@router.get("/{doc_id}", response_model=DocumentDetailResponse)
async def get_document(
    doc_id: str,
    chunks_page: int = 1,
    chunks_page_size: int = 50,
    user: dict = Depends(get_current_user),
):
    """Get document detail with paginated chunks."""
    return DocumentDetailResponse(
        doc_id=doc_id,
        filename="stub.pdf",
        file_type="pdf",
        file_hash="stub",
        departments=[user["department"]],
        chunks=ChunkPage(items=[], page=chunks_page, page_size=chunks_page_size, total=0),
    )
