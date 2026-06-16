import pytest
from httpx import ASGITransport, AsyncClient

from lemonade_backend.main import app


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_list_operations(client):
    resp = await client.get("/v1/operations")
    assert resp.status_code == 200
    data = resp.json()
    assert "operations" in data
    assert len(data["operations"]) >= 25


@pytest.mark.asyncio
async def test_get_operation(client):
    resp = await client.get("/v1/operations/linear_regression")
    assert resp.status_code == 200
    assert resp.json()["name"] == "linear_regression"


@pytest.mark.asyncio
async def test_get_unknown_operation(client):
    resp = await client.get("/v1/operations/nonexistent")
    assert resp.status_code == 404
