"""Celery Beat: cancels tasks with stale heartbeats (>120s)."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from common.enums import TaskStatus
from common.constants import HEARTBEAT_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


@app.task(name="workers.heartbeat_checker.check_heartbeats")
def check_heartbeats():
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        from models.ingestion_task import IngestionTask
        from models.audit_log import AuditLog
        import uuid

        deadline = datetime.now(timezone.utc) - timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS)
        stale = db.query(IngestionTask).filter(
            IngestionTask.status.in_([TaskStatus.PARSING, TaskStatus.CHUNKING]),
            IngestionTask.last_heartbeat < deadline,
        ).all()

        for task in stale:
            task.status = TaskStatus.CANCELLED
            task.error_message = f"心跳超时（{HEARTBEAT_TIMEOUT_SECONDS}s），自动取消"
            task.updated_at = datetime.now(timezone.utc)

            audit = AuditLog(
                id=uuid.uuid4(),
                user_id="system",
                action="auto_cancel",
                resource_type="task",
                resource_id=task.id,
                details={"reason": "heartbeat_timeout"},
                trace_id=task.trace_id,
                created_at=datetime.now(timezone.utc),
            )
            db.add(audit)
            logger.warning(f"Auto-cancelled task {task.id} due to heartbeat timeout")

        if stale:
            db.commit()
