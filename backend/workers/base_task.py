"""Base Celery task class — enforces the "tasks only orchestrate" rule."""

from datetime import datetime, timezone

from celery import Task


class IngestionBaseTask(Task):
    """All ingestion tasks MUST inherit from this base class.

    RULES:
    - Tasks in workers/tasks/ only orchestrate: call services + update status.
    - Business logic lives in services/.
    - NEVER import a task from another task; use .delay() or .apply_async().
    """

    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Record error message in ingestion_tasks on failure."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from core.config import settings
        from models.ingestion_task import IngestionTask
        from common.enums import TaskStatus

        try:
            task_uuid = args[0] if args else None
            if task_uuid:
                engine = create_engine(settings.database_url_sync)
                Session = sessionmaker(bind=engine)
                with Session() as session:
                    task = session.query(IngestionTask).get(task_uuid)
                    if task and task.status != TaskStatus.CANCELLED:
                        task.error_message = f"{type(exc).__name__}: {str(exc)[:500]}"
                        task.status = TaskStatus.FAILED
                        task.updated_at = datetime.now(timezone.utc)
                        session.commit()
        except Exception:
            pass

        super().on_failure(exc, task_id, args, kwargs, einfo)
