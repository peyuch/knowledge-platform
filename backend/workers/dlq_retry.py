"""Celery Beat: retries dead-lettered index records with retry_count < 10."""

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from common.constants import INDEX_DLQ_MAX_RETRY
from core.kafka import get_kafka

logger = logging.getLogger(__name__)


@app.task(name="workers.dlq_retry.retry_dlq_records")
def retry_dlq_records():
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        from models.dead_letter_index import DeadLetterIndex

        records = db.query(DeadLetterIndex).filter(
            DeadLetterIndex.retry_count < INDEX_DLQ_MAX_RETRY
        ).limit(100).all()

        kafka = get_kafka()
        for record in records:
            try:
                kafka.send_single(record.chunk_id, record.payload)
                record.retry_count += 1
                record.updated_at = datetime.now(timezone.utc)
            except Exception as e:
                logger.warning(f"DLQ retry failed for {record.chunk_id}: {e}")

        db.commit()
        if records:
            logger.info(f"DLQ retry: republished {len(records)} records to Kafka")
