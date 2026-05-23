"""Celery Beat: ensures MinerU tasks for cancelled ingestion tasks are also cancelled."""

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from common.enums import TaskStatus

logger = logging.getLogger(__name__)


@app.task(name="workers.orphan_checker.check_orphans")
def check_orphans():
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        from models.ingestion_task import IngestionTask
        from services.ingestion.mineru_client import get_mineru_client
        from services.ingestion.asr_client import get_asr_client

        orphans = db.query(IngestionTask).filter(
            IngestionTask.status == TaskStatus.CANCELLED,
            (
                IngestionTask.mineru_task_id.isnot(None)
                | IngestionTask.asr_task_id.isnot(None)
            ),
        ).all()

        for task in orphans:
            if task.mineru_task_id:
                try:
                    mineru = get_mineru_client()
                    mineru.cancel(task.mineru_task_id)
                except Exception as e:
                    logger.warning(f"Orphan cancelling failed for MinerU task {task.mineru_task_id}: {e}")

            if task.asr_task_id:
                try:
                    asr = get_asr_client()
                    asr.cancel(task.asr_task_id)
                except Exception as e:
                    logger.warning(f"Orphan cancelling failed for ASR task {task.asr_task_id}: {e}")
