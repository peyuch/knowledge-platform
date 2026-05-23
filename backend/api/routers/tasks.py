"""Task status / list / cancel / retry API endpoints."""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from schemas.task import (
    TaskStatusResponse,
    TaskListItem,
    TaskRetryResponse,
    TaskCancelResponse,
)
from schemas.common import PaginationResponse
from api.dependencies import get_current_user
from common.enums import TaskStatus

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])
limiter = Limiter(key_func=get_remote_address)


@router.get("/{task_id}", response_model=TaskStatusResponse)
async def get_task(task_id: str, user: dict = Depends(get_current_user)):
    """Get task status and progress."""
    now = datetime.now(timezone.utc)
    return TaskStatusResponse(
        task_id=task_id,
        original_filename="stub.pdf",
        file_type="pdf",
        file_hash="stub",
        status=TaskStatus.PENDING,
        progress=0,
        retry_count=0,
        created_at=now,
        updated_at=now,
    )


@router.get("/", response_model=PaginationResponse[TaskListItem])
async def list_tasks(
    status: TaskStatus | None = None,
    page: int = 1,
    page_size: int = 20,
    sort: str = "created_at:desc",
    user: dict = Depends(get_current_user),
):
    """List ingestion tasks with filtering and pagination."""
    return PaginationResponse(items=[], total=0, page=page, page_size=page_size)


@router.post("/{task_id}/cancel", status_code=200)
async def cancel_task(task_id: str, user: dict = Depends(get_current_user)):
    """Cancel a running task."""
    return TaskCancelResponse(task_id=task_id, status="cancelled")


@router.post("/{task_id}/retry", status_code=201)
async def retry_task(task_id: str, user: dict = Depends(get_current_user)):
    """Retry a failed task."""
    new_id = str(uuid.uuid4())
    return TaskRetryResponse(task_id=new_id, status="pending", retry_of=task_id)
