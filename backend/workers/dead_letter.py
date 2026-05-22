"""Dead letter queue management for tasks that exhausted retries."""

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config import settings
from common.enums import TaskStatus

logger = logging.getLogger(__name__)


def route_to_dlq(task_id: str, error_message: str) -> None:
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        from models.ingestion_task import IngestionTask

        task = db.query(IngestionTask).get(task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.error_message = error_message
            task.updated_at = datetime.now(timezone.utc)
            db.commit()
            logger.info(f"Task {task_id} routed to DLQ: {error_message}")
