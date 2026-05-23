"""Ingestion lifecycle service: duplicate check, task creation, audit logging."""

import uuid
import logging
from datetime import datetime, timezone
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from models.document import Document
from models.ingestion_task import IngestionTask
from models.audit_log import AuditLog
from common.enums import TaskStatus

logger = logging.getLogger(__name__)


@dataclass
class DuplicateResult:
    is_duplicate: bool
    task_id: str | None = None
    doc_id: str | None = None
    existing_status: str | None = None


async def check_duplicate(
    db: AsyncSession,
    file_hash: str,
    department: str,
) -> DuplicateResult:
    """Check if same file was already uploaded by same department."""
    doc_result = await db.execute(select(Document).where(Document.file_hash == file_hash))
    existing_doc = doc_result.scalar_one_or_none()

    task_result = await db.execute(
        select(IngestionTask)
        .where(IngestionTask.file_hash == file_hash)
        .order_by(IngestionTask.created_at.desc())
        .limit(1)
    )
    existing_task = task_result.scalar_one_or_none()

    if existing_task and existing_task.task_metadata.get("department") == department:
        if existing_task.status == TaskStatus.DONE:
            return DuplicateResult(is_duplicate=True, task_id=str(existing_task.id),
                                   doc_id=str(existing_task.doc_id) if existing_task.doc_id else None,
                                   existing_status="done")
        elif existing_task.status in (TaskStatus.FAILED, TaskStatus.CANCELLED):
            return DuplicateResult(is_duplicate=False,
                                   doc_id=str(existing_doc.id) if existing_doc else None)
        else:
            return DuplicateResult(is_duplicate=True, task_id=str(existing_task.id),
                                   existing_status=existing_task.status.value)

    return DuplicateResult(is_duplicate=False,
                           doc_id=str(existing_doc.id) if existing_doc else None)


async def create_task(
    db: AsyncSession,
    *,
    file_hash: str,
    filename: str,
    file_type: str,
    file_size_bytes: int | None,
    raw_url: str,
    metadata: dict,
    user: dict,
    trace_id: str,
    batch_id: str | None = None,
    reuse_doc_id: str | None = None,
) -> tuple[str, str]:
    """Create document (if new) + ingestion_task + audit_log in one transaction."""
    now = datetime.now(timezone.utc)
    task_id = uuid.uuid4()

    if reuse_doc_id:
        doc_id = uuid.UUID(reuse_doc_id)
    else:
        doc_id = uuid.uuid4()
        doc = Document(
            id=doc_id, filename=filename, file_type=file_type,
            file_hash=file_hash, file_size_bytes=file_size_bytes,
            raw_url=raw_url, created_at=now, updated_at=now,
        )
        db.add(doc)

    task = IngestionTask(
        id=task_id, batch_id=uuid.UUID(batch_id) if batch_id else None,
        doc_id=doc_id, trace_id=trace_id, file_hash=file_hash,
        original_filename=filename, file_type=file_type,
        file_size_bytes=file_size_bytes, status=TaskStatus.PENDING,
        task_metadata=metadata, created_by=user["user_id"],
        created_at=now, updated_at=now,
    )
    db.add(task)

    audit = AuditLog(
        id=uuid.uuid4(), user_id=user["user_id"], action="upload",
        resource_type="document", resource_id=doc_id,
        details={"filename": filename, "file_hash": file_hash,
                  "department": metadata.get("department", "")},
        trace_id=trace_id, created_at=now,
    )
    db.add(audit)

    await db.commit()
    logger.info(f"Created task {task_id} for document {doc_id} (hash={file_hash[:12]}...)")
    return str(task_id), str(doc_id)
