"""Parse document task — submits to MinerU/ASR and releases immediately."""

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from workers.base_task import IngestionBaseTask
from common.enums import TaskStatus
from services.ingestion.mineru_client import get_mineru_client
from services.ingestion.asr_client import get_asr_client

logger = logging.getLogger(__name__)


@app.task(bind=True, base=IngestionBaseTask, queue="gpu_queue", max_retries=0)
def parse_document(self, task_id: str):
    """Submit document to MinerU or ASR, persist task IDs, release Worker slot."""
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        from models.ingestion_task import IngestionTask

        task = db.query(IngestionTask).get(task_id)
        if not task:
            logger.error(f"IngestionTask {task_id} not found")
            return

        task.status = TaskStatus.PARSING
        task.last_heartbeat = datetime.now(timezone.utc)
        db.commit()

        try:
            if task.file_type in ("txt", "md"):
                task.current_step = "纯文本文件，跳过解析"
                task.status = TaskStatus.CHUNKING
                task.last_heartbeat = datetime.now(timezone.utc)
                db.commit()
                from workers.tasks.chunk import chunk_document
                chunk_document.apply_async(args=[task_id], queue="cpu_queue")
                return

            elif task.file_type in ("mp4", "mp3"):
                asr = get_asr_client()
                asr_task_id = asr.submit(task.raw_url or "")
                task.asr_task_id = asr_task_id
                task.current_step = "ASR 语音识别中"

            else:
                mineru = get_mineru_client()
                mineru_task_id = mineru.submit(task.raw_url or "")
                task.mineru_task_id = mineru_task_id
                task.current_step = "MinerU 解析中"

            task.last_heartbeat = datetime.now(timezone.utc)
            db.commit()

        except Exception as e:
            logger.error(f"Failed to submit parse task {task_id}: {e}")
            task.status = TaskStatus.FAILED
            task.error_message = str(e)[:500]
            db.commit()
            raise
