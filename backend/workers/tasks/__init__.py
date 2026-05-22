"""Task exports for Celery auto-discovery."""
from workers.tasks.parse import parse_document
from workers.tasks.chunk import chunk_document

__all__ = ["parse_document", "chunk_document"]
