"""DLQ management endpoints — query, retry, delete dead-lettered index records."""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from core.database import get_db
from models.dead_letter_index import DeadLetterIndex
from schemas.index_dlq import DlqListResponse, DlqItem, DlqRetryResponse, DlqDeleteResponse
from api.dependencies import get_current_user

router = APIRouter(prefix="/api/v1/indexing/dlq", tags=["indexing-dlq"])


@router.get("/", response_model=DlqListResponse)
async def list_dlq(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List dead-lettered index records, most recent first."""
    offset = (page - 1) * page_size

    total_result = await db.execute(select(func.count(DeadLetterIndex.id)))
    total = total_result.scalar() or 0

    items_result = await db.execute(
        select(DeadLetterIndex)
        .order_by(DeadLetterIndex.created_at.desc())
        .offset(offset)
        .limit(page_size)
    )
    items = [DlqItem.model_validate(row) for row in items_result.scalars().all()]

    return DlqListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("/{dlq_id}/retry", response_model=DlqRetryResponse)
async def retry_dlq(
    dlq_id: str,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually retry a dead-lettered message by republishing to Kafka."""
    from core.kafka import get_kafka

    result = await db.execute(select(DeadLetterIndex).where(DeadLetterIndex.id == uuid.UUID(dlq_id)))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="DLQ record not found")

    kafka = get_kafka()
    kafka.send_single(record.chunk_id, record.payload)
    record.retry_count += 1
    record.updated_at = datetime.now(timezone.utc)
    await db.commit()

    return DlqRetryResponse(id=dlq_id, status="republished")


@router.delete("/{dlq_id}", response_model=DlqDeleteResponse)
async def delete_dlq(
    dlq_id: str,
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a dead-lettered record (acknowledge as handled)."""
    result = await db.execute(select(DeadLetterIndex).where(DeadLetterIndex.id == uuid.UUID(dlq_id)))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="DLQ record not found")

    await db.delete(record)
    await db.commit()

    return DlqDeleteResponse(id=dlq_id, status="deleted")
