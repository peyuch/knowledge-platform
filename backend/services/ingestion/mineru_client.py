"""MinerU API client with circuit breaker pattern."""

import logging
from dataclasses import dataclass
from typing import Optional

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from core.config import settings
from core.exceptions import MinerUError, CircuitBreakerOpenError
from common.constants import (
    CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    CIRCUIT_BREAKER_RECOVERY_SECONDS,
)

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Simple circuit breaker: opens after N consecutive failures, auto-recovers after timeout."""

    def __init__(self, failure_threshold: int = 5, recovery_seconds: int = 30):
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._failure_count = 0
        self._last_failure_time: float = 0
        self._state = "closed"

    @property
    def is_open(self) -> bool:
        import time
        if self._state == "closed":
            return False
        if self._state == "open":
            if time.time() - self._last_failure_time >= self.recovery_seconds:
                self._state = "half-open"
                return False
            return True
        return False  # half-open

    def record_success(self) -> None:
        self._failure_count = 0
        self._state = "closed"

    def record_failure(self) -> None:
        import time
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = "open"

    @property
    def state_name(self) -> str:
        return self._state


_mineru_breaker = CircuitBreaker(
    failure_threshold=CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    recovery_seconds=CIRCUIT_BREAKER_RECOVERY_SECONDS,
)


@dataclass
class MinerUTaskResult:
    task_id: str
    status: str
    markdown: Optional[str] = None
    json_content: Optional[str] = None


class MinerUClient:
    """HTTP client for MinerU 3.1 async API."""

    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip("/")
        self._client = httpx.Client(timeout=600)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=30, min=10, max=120),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    def _post(self, path: str, **kwargs) -> httpx.Response:
        if _mineru_breaker.is_open:
            raise CircuitBreakerOpenError("MinerU circuit breaker is open")
        try:
            resp = self._client.post(f"{self.api_url}{path}", **kwargs)
            resp.raise_for_status()
            _mineru_breaker.record_success()
            return resp
        except Exception as e:
            _mineru_breaker.record_failure()
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=5, min=2, max=30),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    def _get(self, path: str) -> httpx.Response:
        if _mineru_breaker.is_open:
            raise CircuitBreakerOpenError("MinerU circuit breaker is open")
        try:
            resp = self._client.get(f"{self.api_url}{path}")
            resp.raise_for_status()
            _mineru_breaker.record_success()
            return resp
        except Exception as e:
            _mineru_breaker.record_failure()
            raise

    def submit(self, file_url: str) -> str:
        """Submit a document for parsing. Returns mineru_task_id."""
        resp = self._post("/tasks", json={"file_url": file_url})
        return resp.json()["task_id"]

    def query(self, mineru_task_id: str) -> MinerUTaskResult:
        """Query the status of a MinerU task."""
        resp = self._get(f"/tasks/{mineru_task_id}")
        data = resp.json()
        return MinerUTaskResult(
            task_id=mineru_task_id,
            status=data.get("status", "unknown"),
            markdown=data.get("result", {}).get("markdown"),
            json_content=data.get("result", {}).get("json"),
        )

    def cancel(self, mineru_task_id: str) -> bool:
        """Cancel a MinerU task."""
        try:
            self._client.delete(f"{self.api_url}/tasks/{mineru_task_id}")
            return True
        except Exception as e:
            logger.warning(f"Failed to cancel MinerU task {mineru_task_id}: {e}")
            return False

    def close(self) -> None:
        self._client.close()


def get_mineru_client() -> MinerUClient:
    return MinerUClient(settings.mineru_api_url)
