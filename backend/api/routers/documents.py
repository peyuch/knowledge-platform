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
    import os, uuid as uuid_mod, json as json_mod
    from datetime import datetime, timezone
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from core.config import settings

    content = await file.read()
    file_hash = sha256_hex(content)
    meta_dict = json_mod.loads(metadata or "{}")
    task_id = str(uuid_mod.uuid4())
    doc_id = str(uuid_mod.uuid4())

    # Save to local disk (MinIO alternative for quick demo)
    safe_name = os.path.basename(file.filename) if file.filename else "upload.pdf"
    os.makedirs("data/raw", exist_ok=True)
    local_path = f"data/raw/{task_id}_{safe_name}"
    with open(local_path, "wb") as f:
        f.write(content)

    # Insert ingestion task + document into PG
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)
    with Session() as db:
        from models.document import Document
        from models.ingestion_task import IngestionTask
        from common.enums import TaskStatus

        ext = safe_name.rsplit(".", 1)[-1].lower() if "." in safe_name else "pdf"
        ft = ext if ext in ("pdf","docx","pptx","xlsx","txt","md","png","jpg","mp4","mp3") else "pdf"

        doc = Document(
            id=uuid_mod.UUID(doc_id), filename=safe_name, file_type=ft,
            file_hash=file_hash, file_size_bytes=len(content),
            raw_url=local_path, created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
        )
        db.add(doc)

        task = IngestionTask(
            id=uuid_mod.UUID(task_id), doc_id=uuid_mod.UUID(doc_id),
            trace_id=str(uuid_mod.uuid4()), file_hash=file_hash,
            original_filename=file.filename, file_type=ft,
            file_size_bytes=len(content), status=TaskStatus.PENDING.value,
            task_metadata=meta_dict, created_by=user.get("user_id",""),
            created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
        )
        db.add(task)
        db.commit()

    # Dispatch Celery parse task
    from workers.tasks.parse import parse_document
    parse_document.apply_async(args=[task_id], queue="gpu_queue")

    return DocumentUploadResponse(
        task_id=task_id, doc_id=doc_id, file_hash=file_hash,
        status=TaskStatus.PENDING.value, duplicate=False,
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
