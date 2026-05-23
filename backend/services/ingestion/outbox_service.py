"""Transactional outbox: write atomically with chunks, publish to Kafka via poller."""

import uuid
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from models.outbox import Outbox
from core.kafka import get_kafka
from common.constants import OUTBOX_BATCH_SIZE

logger = logging.getLogger(__name__)


def build_outbox_records(
    trace_id: str,
    chunk_payloads: list[dict],
) -> list[Outbox]:
    """Build Outbox ORM objects for a batch of chunks."""
    records = []
    for payload in chunk_payloads:
        outbox = Outbox(
            id=uuid.uuid4(),
            aggregate_type="chunk",
            aggregate_id=uuid.UUID(payload["chunk_id"]),
            event_type="chunk.created",
            payload=payload,
            trace_id=trace_id,
            created_at=datetime.now(timezone.utc),
        )
        records.append(outbox)
    return records


def publish_pending_outbox(db: Session) -> int:
    """Publish all unpublished outbox records to Kafka. Returns count published."""
    unpublished = (
        db.query(Outbox)
        .filter(Outbox.published_at.is_(None))
        .order_by(Outbox.created_at)
        .limit(OUTBOX_BATCH_SIZE)
        .all()
    )
    if not unpublished:
        return 0

    kafka = get_kafka()
    messages = [(str(record.aggregate_id), record.payload) for record in unpublished]
    kafka.send_batch(messages)

    now = datetime.now(timezone.utc)
    for record in unpublished:
        record.published_at = now
    db.commit()

    logger.info(f"Published {len(unpublished)} outbox records to Kafka")
    return len(unpublished)
