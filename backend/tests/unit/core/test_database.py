"""Tests for database connectivity and engine setup."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from core.database import async_engine, async_session_factory


@pytest.mark.asyncio
async def test_async_engine_creates():
    """Engine should be an async SQLAlchemy engine."""
    assert async_engine is not None
    assert hasattr(async_engine, 'connect')


@pytest.mark.asyncio
async def test_session_factory_yields_async_session():
    """Session factory should produce AsyncSession instances."""
    async with async_session_factory() as session:
        assert isinstance(session, AsyncSession)


@pytest.mark.asyncio
async def test_session_executes_simple_query():
    """Should be able to execute SELECT 1."""
    try:
        async with async_session_factory() as session:
            result = await session.execute(text("SELECT 1"))
            assert result.scalar() == 1
    except ConnectionRefusedError:
        pytest.skip("PostgreSQL is not running — start with: docker compose -f docker/docker-compose.yml up -d")
