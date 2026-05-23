"""Entity normalization dictionary — maps aliases to standard entity names."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID

from models import Base


class EntityNormalization(Base):
    __tablename__ = "entity_normalization"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    standard_name: Mapped[str] = mapped_column(String(256), nullable=False)
    alias: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
