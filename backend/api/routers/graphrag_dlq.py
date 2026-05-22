"""DLQ management for GraphRAG extraction failures."""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from core.database import get_db
from core.kafka import get_kafka
from models.dead_letter_graphrag import DeadLetterGraphrag
from schemas.graphrag_dlq import DlqGraphragItem, DlqGraphragListResponse, DlqGraphragRetryResponse
from api.dependencies import get_current_user

router = APIRouter(prefix="/api/v1/graphrag/dlq", tags=["graphrag-dlq"])


@router.get("/", response_model=DlqGraphragListResponse)
async def list_dlq(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * page_size
    total = (await db.execute(select(func.count(DeadLetterGraphrag.id)))).scalar() or 0
    items = (await db.execute(
        select(DeadLetterGraphrag).order_by(DeadLetterGraphrag.created_at.desc()).offset(offset).limit(page_size)
    )).scalars().all()
    return DlqGraphragListResponse(
        items=[DlqGraphragItem.model_validate(i) for i in items],
        total=total, page=page, page_size=page_size,
    )


@router.post("/{dlq_id}/retry", response_model=DlqGraphragRetryResponse)
async def retry_dlq(dlq_id: str, user: dict = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DeadLetterGraphrag).where(DeadLetterGraphrag.id == uuid.UUID(dlq_id)))
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="DLQ record not found")
    kafka = get_kafka()
    kafka.send_single(record.chunk_id, record.payload)
    record.retry_count += 1
    record.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return DlqGraphragRetryResponse(id=dlq_id, status="republished")
