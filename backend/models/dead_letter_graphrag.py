"""Dead letter queue for GraphRAG extraction failures."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Integer, BigInteger, SmallInteger, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB

from models import Base


class DeadLetterGraphrag(Base):
    __tablename__ = "dead_letter_graphrag"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    chunk_id: Mapped[str] = mapped_column(String(36), nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(256), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(SmallInteger, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
