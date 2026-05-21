"""Shared enumerations used across models, schemas, and services."""

from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    PARSING = "parsing"
    CHUNKING = "chunking"
    STORING = "storing"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ChunkGranularity(str, Enum):
    LARGE = "LARGE"
    SMALL = "SMALL"


class FileType(str, Enum):
    PDF = "pdf"
    DOCX = "docx"
    PPTX = "pptx"
    XLSX = "xlsx"
    PNG = "png"
    JPG = "jpg"
    TXT = "txt"
    MD = "md"
    MP4 = "mp4"
    MP3 = "mp3"
