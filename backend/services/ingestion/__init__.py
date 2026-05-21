"""
ingestion_service.py  — "one ingestion" lifecycle (idempotency, scheduling, state)
document_service.py   — Document entity CRUD (for router queries)
mineru_client.py      — MinerU API client + circuit breaker
asr_client.py         — ASR API client (primary aliyun / fallback tencent) + circuit breaker
chunker.py            — Heading-aware markdown chunking algorithm
outbox_service.py     — Outbox write + publish
"""
