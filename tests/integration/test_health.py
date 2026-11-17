from __future__ import annotations

import httpx
from pydantic import SecretStr

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings


async def test_health_ok(client: httpx.AsyncClient) -> None:
    resp = await client.get("/health", headers={"X-API-Key": ""})
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "components": {"postgres": {"status": "ok"}, "redis": {"status": "ok"}},
    }


async def _health_with(settings: Settings) -> httpx.Response:
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            return await c.get("/health")


async def test_health_503_when_redis_down(test_settings: Settings) -> None:
    settings = test_settings.model_copy(update={"redis_url": SecretStr("redis://127.0.0.1:1/0")})
    resp = await _health_with(settings)
    assert resp.status_code == 503
    body = resp.json()
    assert body["components"]["postgres"] == {"status": "ok"}
    assert body["components"]["redis"]["status"] == "error"


async def test_health_503_when_postgres_down(test_settings: Settings) -> None:
    bad = "postgresql+psycopg://nobody:nothing@127.0.0.1:1/none"
    settings = test_settings.model_copy(update={"database_url": SecretStr(bad)})
    resp = await _health_with(settings)
    assert resp.status_code == 503
    body = resp.json()
    assert body["components"]["postgres"]["status"] == "error"
    assert body["components"]["redis"] == {"status": "ok"}
    assert "nothing" not in resp.text  # never echo connection details
