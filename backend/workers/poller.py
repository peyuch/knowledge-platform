"""Celery Beat task: polls MinerU/ASR task status every 5s, triggers chunk on completion."""

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from common.enums import TaskStatus
from services.ingestion.mineru_client import get_mineru_client
from services.ingestion.asr_client import get_asr_client

logger = logging.getLogger(__name__)


@app.task(name="workers.poller.poll_parse_tasks")
def poll_parse_tasks():
    """Check all in-progress parse tasks and advance them if complete."""
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        from models.ingestion_task import IngestionTask

        tasks = db.query(IngestionTask).filter(
            IngestionTask.status == TaskStatus.PARSING,
        ).all()

        for task in tasks:
            task.last_heartbeat = datetime.now(timezone.utc)

            if task.mineru_task_id:
                try:
                    mineru = get_mineru_client()
                    result = mineru.query(task.mineru_task_id)
                    if result.status == "done":
                        import os
                        os.makedirs("/tmp/parsed", exist_ok=True)
                        with open(f"/tmp/parsed/{task.id}.md", "w", encoding="utf-8") as f:
                            f.write(result.markdown or "")
                        task.status = TaskStatus.CHUNKING
                        task.current_step = "解析完成，开始分块"
                        db.commit()
                        from workers.tasks.chunk import chunk_document
                        chunk_document.apply_async(args=[str(task.id)], queue="cpu_queue")
                    elif result.status == "failed":
                        task.status = TaskStatus.FAILED
                        task.error_message = "MinerU 解析失败"
                        db.commit()
                except Exception as e:
                    logger.warning(f"Poll error for task {task.id}: {e}")
                    continue

            elif task.asr_task_id:
                try:
                    asr = get_asr_client()
                    result = asr.query(task.asr_task_id)
                    if result.status == "done":
                        import os
                        os.makedirs("/tmp/parsed", exist_ok=True)
                        with open(f"/tmp/parsed/{task.id}.md", "w", encoding="utf-8") as f:
                            f.write(result.text or "")
                        task.status = TaskStatus.CHUNKING
                        task.current_step = "ASR 完成，开始分块"
                        db.commit()
                        from workers.tasks.chunk import chunk_document
                        chunk_document.apply_async(args=[str(task.id)], queue="cpu_queue")
                    elif result.status == "failed":
                        task.status = TaskStatus.FAILED
                        task.error_message = "ASR 识别失败"
                        db.commit()
                except Exception as e:
                    logger.warning(f"ASR poll error for task {task.id}: {e}")
                    continue
