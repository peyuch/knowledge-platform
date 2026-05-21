"""IngestionTask — tracks one upload/import lifecycle. Carries department metadata."""

import uuid
from datetime import datetime

from sqlalchemy import String, SmallInteger, BigInteger, DateTime, Text, ForeignKey, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB

from models import Base
from common.enums import TaskStatus


class IngestionTask(Base):
    __tablename__ = "ingestion_tasks"
    __table_args__ = (
        CheckConstraint(
            "status != 'done' OR doc_id IS NOT NULL",
            name="chk_done_has_doc",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    batch_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("batches.id", ondelete="SET NULL"), nullable=True
    )
    doc_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("documents.id", ondelete="SET NULL"), nullable=True
    )
    retry_of: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ingestion_tasks.id", ondelete="SET NULL"), nullable=True
    )
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)

    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(512), nullable=False)
    file_type: Mapped[str] = mapped_column(String(16), nullable=False)
    file_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    status: Mapped[TaskStatus] = mapped_column(default=TaskStatus.PENDING, nullable=False)
    progress: Mapped[int] = mapped_column(SmallInteger, default=0)
    current_step: Mapped[str | None] = mapped_column(String(256), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(SmallInteger, default=0)

    mineru_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    asr_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    last_heartbeat: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    parse_duration_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    chunk_duration_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    task_metadata: Mapped[dict] = mapped_column("metadata", JSONB, default=dict)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
