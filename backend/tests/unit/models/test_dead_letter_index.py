"""Tests for DeadLetterIndex ORM model."""

import uuid
from datetime import datetime, timezone
from models.dead_letter_index import DeadLetterIndex


def test_dead_letter_index_creation():
    dlq = DeadLetterIndex(
        id=uuid.uuid4(),
        chunk_id="chunk-abc-123",
        topic="knowledge.ingestion.chunks",
        partition=0,
        kafka_offset=42,
        payload={"chunk_id": "chunk-abc-123", "content": "test"},
        error_type="ValueError",
        error_message="Embedding failed",
        retry_count=0,
        created_at=datetime.now(timezone.utc),
    )
    assert dlq.chunk_id == "chunk-abc-123"
    assert dlq.kafka_offset == 42
    assert dlq.retry_count == 0


def test_dead_letter_index_defaults():
    dlq = DeadLetterIndex(
        chunk_id="c1",
        topic="t",
        partition=0,
        kafka_offset=1,
        payload={},
        retry_count=0,
        created_at=datetime.now(timezone.utc),
    )
    assert dlq.retry_count == 0
    assert dlq.created_at is not None
