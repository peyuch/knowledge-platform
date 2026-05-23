"""Chunk document task — heading-aware splitting + transactional outbox write."""

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.celery import app
from core.config import settings
from workers.base_task import IngestionBaseTask
from common.enums import TaskStatus
from services.ingestion.chunker import chunk_markdown
from services.ingestion.outbox_service import build_outbox_records
from models.chunk import Chunk
from models.document import Document
from models.ingestion_task import IngestionTask
from utils.text import count_tokens

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    base=IngestionBaseTask,
    queue="cpu_queue",
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
)
def chunk_document(self, task_id: str):
    """Read markdown, run heading-aware chunker, write chunks + outbox in one transaction."""
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        task = db.query(IngestionTask).get(task_id)
        if not task:
            logger.error(f"IngestionTask {task_id} not found")
            return

        task.status = TaskStatus.CHUNKING
        task.current_step = "标题感知分块中"
        task.last_heartbeat = datetime.now(timezone.utc)
        db.commit()

        doc = db.query(Document).get(task.doc_id)
        if not doc:
            task.status = TaskStatus.FAILED
            task.error_message = f"Document {task.doc_id} not found"
            db.commit()
            return

        # In production: fetch markdown from MinIO. For now: use local stub.
        import os
        stub_path = f"/tmp/parsed/{task.id}.md"
        if os.path.exists(stub_path):
            with open(stub_path, "r", encoding="utf-8") as f:
                markdown_text = f.read()
        else:
            markdown_text = f"# {doc.filename}\n\n[Content placeholder for document {doc.id}]"

        chunk_start = datetime.now(timezone.utc)
        chunk_drafts = chunk_markdown(markdown_text, str(doc.id))

        if not chunk_drafts:
            task.status = TaskStatus.FAILED
            task.error_message = "Chunker produced no chunks"
            db.commit()
            return

        # Build chunk ORM objects + outbox records
        chunk_payloads = []
        chunk_models = []
        for draft in chunk_drafts:
            payload = {
                "event": "chunk.created",
                "outbox_id": draft.chunk_id,
                "chunk_id": draft.chunk_id,
                "doc_id": str(doc.id),
                "parent_id": draft.parent_id,
                "heading_level": draft.heading_level,
                "granularity": draft.granularity,
                "heading_path": draft.heading_path,
                "content": draft.content,
                "content_hash": draft.content_hash,
                "page_start": draft.page_start,
                "page_end": draft.page_end,
                "trace_id": task.trace_id,
                "metadata": {
                    "file_type": doc.file_type,
                    "original_filename": doc.filename,
                    "department": task.task_metadata.get("department", "") if hasattr(task, 'task_metadata') else "",
                },
            }
            chunk_payloads.append(payload)

            chunk_model = Chunk(
                id=draft.chunk_id,
                doc_id=doc.id,
                parent_id=draft.parent_id,
                outbox_id=draft.chunk_id,
                heading_level=draft.heading_level,
                granularity=draft.granularity,
                heading_path=draft.heading_path,
                content=draft.content,
                content_hash=draft.content_hash,
                token_count=draft.token_count,
                page_start=draft.page_start,
                page_end=draft.page_end,
                has_table=draft.has_table,
                has_formula=draft.has_formula,
                has_image=draft.has_image,
                sequence=draft.sequence,
            )
            chunk_models.append(chunk_model)

        outbox_records = build_outbox_records(task.trace_id, chunk_payloads)

        # Transaction: chunks + outbox together
        db.add_all(chunk_models)
        db.add_all(outbox_records)

        # Update document stats
        doc.chunk_count = len(chunk_models)
        doc.total_tokens = sum(c.token_count or 0 for c in chunk_models)

        chunk_end = datetime.now(timezone.utc)
        task.chunk_duration_ms = int((chunk_end - chunk_start).total_seconds() * 1000)
        task.status = TaskStatus.DONE
        task.progress = 100
        task.current_step = "处理完成"
        task.last_heartbeat = datetime.now(timezone.utc)
        db.commit()

        logger.info(f"Chunked doc {doc.id}: {len(chunk_models)} chunks, {doc.total_tokens} tokens, {task.chunk_duration_ms}ms")
