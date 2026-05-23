"""End-to-end integration tests: search + answer pipeline."""

import pytest
from httpx import AsyncClient, ASGITransport
from api.main import app


@pytest.mark.asyncio
async def test_search_endpoint_returns_200():
    """POST /api/v1/search should return 200 with results structure."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/search",
            json={"query": "请假流程", "top_k": 5},
            headers={"Authorization": "Bearer dev-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert "query_analysis" in data
        assert "took_ms" in data


@pytest.mark.asyncio
async def test_search_answer_endpoint_returns_200():
    """POST /api/v1/search/answer should return 200 with answer structure."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/search/answer",
            json={"query": "请假流程", "top_k": 5},
            headers={"Authorization": "Bearer dev-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "answer" in data
        assert "citations" in data
        assert "no_answer" in data
        assert "is_fallback" in data


@pytest.mark.asyncio
async def test_search_without_auth_returns_401():
    """Requests without JWT should be rejected."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/search",
            json={"query": "test", "top_k": 5},
        )
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_search_empty_query_returns_422():
    """Empty query should fail validation."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/search",
            json={"query": "", "top_k": 5},
            headers={"Authorization": "Bearer dev-token"},
        )
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_search_answer_with_filters():
    """Answer endpoint should accept filters."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/search/answer",
            json={
                "query": "合同审查流程",
                "top_k": 10,
                "filters": {"department": "法务部", "file_type": "pdf"},
            },
            headers={"Authorization": "Bearer dev-token"},
        )
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_health_endpoint():
    """GET /health should return ok."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
