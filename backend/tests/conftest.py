"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from core.minio import MinioClient, get_minio


@pytest.fixture
def minio_client() -> MinioClient:
    """Return a MinioClient singleton for use in integration tests."""
    return get_minio()
