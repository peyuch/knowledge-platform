"""Redis connection pool and lock utilities."""

from __future__ import annotations

import contextlib
from typing import AsyncIterator

import redis as sync_redis
import redis.asyncio as aioredis

from common.constants import REDIS_LOCK_TTL_SECONDS
from core.config import settings

# ------------------------------------------------------------------
# async connection pool (created lazily)
# ------------------------------------------------------------------

_pool: aioredis.ConnectionPool | None = None


def _get_pool() -> aioredis.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = aioredis.ConnectionPool.from_url(settings.redis_url, decode_responses=True)
    return _pool


# ------------------------------------------------------------------
# public API (async)
# ------------------------------------------------------------------


def get_redis() -> aioredis.Redis:
    """Return an async Redis client backed by the shared connection pool."""
    pool = _get_pool()
    return aioredis.Redis(connection_pool=pool)


# ------------------------------------------------------------------
# public API (sync — for Celery workers / standalone consumers)
# ------------------------------------------------------------------

_sync_client: sync_redis.Redis | None = None


def get_sync_redis() -> sync_redis.Redis:
    """Return a synchronous Redis client for use in Celery workers and consumers."""
    global _sync_client
    if _sync_client is None:
        _sync_client = sync_redis.Redis.from_url(
            settings.redis_url, decode_responses=True
        )
    return _sync_client


async def close_redis() -> None:
    """Close the shared connection pool, releasing all connections."""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None


@contextlib.asynccontextmanager
async def acquire_lock(
    lock_name: str,
    timeout: int = REDIS_LOCK_TTL_SECONDS,
    blocking: bool = False,
) -> AsyncIterator[bool]:
    """Async context manager that acquires a Redis distributed lock.

    Yields ``True`` when the lock was acquired, ``False`` otherwise.

    Parameters
    ----------
    lock_name:
        Unique key used as the lock identifier.
    timeout:
        Maximum seconds the lock is held before expiring.
    blocking:
        If ``True``, block until the lock is available.
    """
    client = get_redis()
    lock = client.lock(lock_name, timeout=timeout)
    acquired = await lock.acquire(blocking=blocking)
    try:
        yield acquired
    finally:
        if acquired:
            await lock.release()
