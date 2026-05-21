"""Kafka producer wrapper for chunk events."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from kafka import KafkaProducer

from core.config import settings

if TYPE_CHECKING:
    from collections.abc import Iterable


class KafkaChunkProducer:
    """Kafka producer specialised for publishing chunk records."""

    def __init__(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True,
            linger_ms=100,
            compression_type="gzip",
        )
        self._topic = settings.kafka_topic_chunks

    # ------------------------------------------------------------------
    # send
    # ------------------------------------------------------------------

    def send_single(self, key: str | None, value: dict) -> None:
        """Send a single message, blocking until acknowledged."""
        self._producer.send(self._topic, key=key, value=value).get(timeout=30)

    def send_batch(self, messages: Iterable[tuple[str | None, dict]]) -> None:
        """Send a batch of messages and flush after each batch.

        Each element of *messages* is a (key, value) pair where *key* may be
        ``None`` for round-robin partitioning.
        """
        for key, value in messages:
            self._producer.send(self._topic, key=key, value=value)
        self._producer.flush(timeout=60)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Flush any pending messages and close the producer."""
        self._producer.flush(timeout=30)
        self._producer.close(timeout=30)


# ------------------------------------------------------------------
# singleton
# ------------------------------------------------------------------

_kafka_producer: KafkaChunkProducer | None = None


def get_kafka() -> KafkaChunkProducer:
    """Return the module-level KafkaChunkProducer singleton."""
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaChunkProducer()
    return _kafka_producer
