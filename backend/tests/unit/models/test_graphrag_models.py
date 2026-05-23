"""Tests for GraphRAG PG models."""

import uuid
from datetime import datetime, timezone
from models.entity_normalization import EntityNormalization
from models.dead_letter_graphrag import DeadLetterGraphrag


def test_entity_normalization_creation():
    en = EntityNormalization(
        id=uuid.uuid4(),
        standard_name="首席执行官",
        alias="CEO",
        entity_type="Person",
        created_at=datetime.now(timezone.utc),
    )
    assert en.standard_name == "首席执行官"
    assert en.alias == "CEO"


def test_dead_letter_graphrag_creation():
    dlq = DeadLetterGraphrag(
        id=uuid.uuid4(),
        chunk_id="chunk-123",
        topic="knowledge.ingestion.chunks",
        partition=0,
        kafka_offset=99,
        payload={"text": "failed chunk"},
        error_type="LLMTimeoutError",
        error_message="LLM API timeout after 30s",
        retry_count=0,
        created_at=datetime.now(timezone.utc),
    )
    assert dlq.chunk_id == "chunk-123"
    assert dlq.retry_count == 0
