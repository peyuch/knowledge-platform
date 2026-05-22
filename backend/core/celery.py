"""Celery application instance with GPU/CPU queue definitions."""

from celery import Celery
from celery.schedules import crontab

from core.config import settings

app = Celery(
    "knowledge_platform",
    broker=settings.celery_broker_url,
    include=[
        "workers.tasks.parse",
        "workers.tasks.chunk",
        "workers.poller",
        "workers.outbox_poller",
        "workers.heartbeat_checker",
        "workers.orphan_checker",
        "workers.dlq_retry",
        "workers.graph_postprocessor",
        "workers.graph_rebuilder",
        "workers.raptor_timeout_scanner",
    ],
)

app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_queue="cpu_queue",
    task_queues={
        "gpu_queue": {"exchange": "gpu", "routing_key": "gpu"},
        "cpu_queue": {"exchange": "cpu", "routing_key": "cpu"},
    },
    task_routes={
        "workers.tasks.parse.parse_document": {"queue": "gpu_queue"},
        "workers.tasks.chunk.chunk_document": {"queue": "cpu_queue"},
    },
    beat_schedule={
        "poll-parse-tasks": {
            "task": "workers.poller.poll_parse_tasks",
            "schedule": 5.0,
        },
        "publish-outbox": {
            "task": "workers.outbox_poller.publish_pending_outbox",
            "schedule": 5.0,
        },
        "heartbeat-check": {
            "task": "workers.heartbeat_checker.check_heartbeats",
            "schedule": 30.0,
        },
        "orphan-check": {
            "task": "workers.orphan_checker.check_orphans",
            "schedule": 300.0,
        },
        "retry-dlq": {
            "task": "workers.dlq_retry.retry_dlq_records",
            "schedule": crontab(hour=2, minute=0),  # Daily at 2am
        },
        "graphrag-rebuild": {
            "task": "workers.graph_rebuilder.rebuild_recent_docs",
            "schedule": crontab(hour=3, minute=0),
        },
        "raptor-timeout-scan": {
            "task": "workers.raptor_timeout_scanner.scan_timeouts",
            "schedule": 600.0,  # every 10 minutes
        },
    },
    broker_connection_retry_on_startup=True,
)
