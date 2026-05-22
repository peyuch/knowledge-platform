"""Document CRUD service with department-filtered queries."""

import uuid
import logging
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from models.document import Document
from models.ingestion_task import IngestionTask
from models.chunk import Chunk
from common.enums import TaskStatus

logger = logging.getLogger(__name__)


async def get_document(db: AsyncSession, doc_id: str, user_department: str) -> Optional[dict]:
    """Get document detail if user's department has access."""
    result = await db.execute(select(Document).where(Document.id == uuid.UUID(doc_id)))
    doc = result.scalar_one_or_none()
    if not doc:
        return None

    dept_result = await db.execute(
        select(IngestionTask.task_metadata["department"].astext)
        .where(IngestionTask.doc_id == doc.id, IngestionTask.status == TaskStatus.DONE)
        .distinct()
    )
    departments = [row[0] for row in dept_result.all() if row[0]]

    if user_department not in departments and "admin" not in departments:
        return None

    return {
        "doc_id": str(doc.id), "filename": doc.filename, "file_type": doc.file_type,
        "file_hash": doc.file_hash, "original_url": doc.raw_url,
        "markdown_url": doc.markdown_url, "departments": departments,
        "page_count": doc.page_count, "created_at": doc.created_at,
    }


async def get_document_chunks(db: AsyncSession, doc_id: str, page: int = 1, page_size: int = 50) -> dict:
    """Get paginated chunks for a document."""
    offset = (page - 1) * page_size

    count_result = await db.execute(select(func.count(Chunk.id)).where(Chunk.doc_id == uuid.UUID(doc_id)))
    total = count_result.scalar() or 0

    chunks_result = await db.execute(
        select(Chunk).where(Chunk.doc_id == uuid.UUID(doc_id))
        .order_by(Chunk.sequence).offset(offset).limit(page_size)
    )
    chunks = chunks_result.scalars().all()

    items = [{
        "chunk_id": str(c.id), "heading_level": c.heading_level,
        "granularity": c.granularity.value if hasattr(c.granularity, 'value') else str(c.granularity),
        "heading_path": c.heading_path, "content": c.content,
        "page_start": c.page_start, "page_end": c.page_end,
    } for c in chunks]

    return {"items": items, "page": page, "page_size": page_size, "total": total}
