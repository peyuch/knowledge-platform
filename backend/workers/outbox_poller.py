"""Celery Beat task: publishes pending outbox records to Kafka every 5s."""

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from services.ingestion.outbox_service import publish_pending_outbox

logger = logging.getLogger(__name__)


@app.task(name="workers.outbox_poller.publish_pending_outbox")
def publish_pending_outbox_task():
    """Publish all unpublished outbox records to Kafka."""
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)
    with Session() as db:
        count = publish_pending_outbox(db)
        if count > 0:
            logger.info(f"Outbox poller published {count} messages")
