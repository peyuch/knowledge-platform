"""Redis connection pool and lock utilities."""

from __future__ import annotations

import contextlib
from typing import AsyncIterator

import redis.asyncio as aioredis

from common.constants import REDIS_LOCK_TTL_SECONDS
from core.config import settings

# ------------------------------------------------------------------
# connection pool (created lazily)
# ------------------------------------------------------------------

_pool: aioredis.ConnectionPool | None = None


def _get_pool() -> aioredis.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = aioredis.ConnectionPool.from_url(settings.redis_url, decode_responses=True)
    return _pool


# ------------------------------------------------------------------
# public API
# ------------------------------------------------------------------


def get_redis() -> aioredis.Redis:
    """Return a Redis client backed by the shared connection pool."""
    pool = _get_pool()
    return aioredis.Redis(connection_pool=pool)


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
