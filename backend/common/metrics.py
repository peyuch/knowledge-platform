"""Prometheus metrics for the indexing consumer."""

from prometheus_client import Counter, Gauge, Histogram

kafka_consumer_messages_consumed = Counter(
    "kafka_consumer_messages_consumed_total",
    "Total Kafka messages consumed by the indexer",
    ["topic"],
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Consumer lag per partition",
    ["topic", "partition"],
)

indexing_batch_size_avg = Histogram(
    "indexing_batch_size",
    "Batch size distribution",
    buckets=[10, 25, 50, 100, 200, 500],
)

indexing_embedding_duration = Histogram(
    "indexing_embedding_duration_seconds",
    "Time spent on embedding per batch",
)

indexing_milvus_write_duration = Histogram(
    "indexing_milvus_write_duration_seconds",
    "Time spent writing to Milvus per batch",
)

indexing_es_write_duration = Histogram(
    "indexing_es_write_duration_seconds",
    "Time spent writing to ES per batch",
)

indexing_batch_success = Counter(
    "indexing_batch_success_total",
    "Total successfully processed batches",
)

indexing_batch_failure = Counter(
    "indexing_batch_failure_total",
    "Total failed batches",
)

indexing_dlq_messages = Counter(
    "indexing_dlq_messages_total",
    "Total messages routed to DLQ",
    ["error_type"],
)
