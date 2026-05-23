"""Pydantic schemas for task API responses."""

from datetime import datetime
from pydantic import BaseModel
from common.enums import TaskStatus


class TaskStatusResponse(BaseModel):
    task_id: str
    batch_id: str | None = None
    retry_of: str | None = None
    doc_id: str | None = None
    original_filename: str
    file_type: str
    file_hash: str
    status: TaskStatus
    progress: int
    current_step: str | None = None
    error_message: str | None = None
    retry_count: int
    stats: dict | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TaskListItem(BaseModel):
    task_id: str
    original_filename: str
    file_type: str
    status: TaskStatus
    progress: int
    department: str = ""
    created_at: datetime

    model_config = {"from_attributes": True}


class TaskRetryResponse(BaseModel):
    task_id: str
    status: str
    retry_of: str


class TaskCancelResponse(BaseModel):
    task_id: str
    status: str
