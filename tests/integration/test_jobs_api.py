from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy import func, select

from tests.integration.conftest import API_KEY_A, API_KEY_B, wait_for_status
from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.core.idempotency import api_key_id
from workflow_engine.db.models import IdempotencyKey, Job
from workflow_engine.worker.runner import Worker

SLEEP_JOB = {"type": "sleep", "payload": {"seconds": 0}}


async def _job_count(app: FastAPI) -> int:
    async with app.state.session_factory() as session:
        return int(await session.scalar(select(func.count()).select_from(Job)) or 0)


async def _stream_len(settings: Settings, priority: str = "normal") -> int:
    redis = Redis.from_url(settings.redis_url.get_secret_value())
    try:
        return int(await redis.xlen(f"{settings.redis_stream_prefix}:{priority}"))
    finally:
        await redis.aclose()


# --------------------------------------------------------------------------- auth


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/jobs"), ("GET", "/jobs"), ("GET", f"/jobs/{uuid.uuid4()}")],
)
async def test_missing_or_wrong_key_is_401(
    app: FastAPI, headers: dict[str, str], method: str, path: str
) -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t", headers=headers) as c:
        resp = await c.request(method, path, json=SLEEP_JOB)
    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"] == "ApiKey"


async def test_health_needs_no_key(app: FastAPI) -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        assert (await c.get("/health")).status_code == 200


# --------------------------------------------------------------------------- validation


@pytest.mark.parametrize(
    ("body", "loc", "fragment"),
    [
        ({"type": "nope"}, ["body", "type"], "unknown job type"),
        ({"type": "sleep", "payload": {"seconds": -1}}, ["body", "payload", "seconds"], ""),
        ({"type": "sleep", "payload": {"bogus": 1}}, ["body", "payload", "bogus"], ""),
        ({"type": "sleep", "priority": "urgent"}, ["body", "priority"], ""),
        ({"type": "sleep", "unexpected": 1}, ["body", "unexpected"], ""),
        ({"type": "sleep", "payload": [1, 2]}, ["body", "payload"], ""),
        ({"type": "sleep", "run_at": "2030-01-01T00:00:00"}, ["body", "run_at"], "timezone"),
        ({"type": "cpu", "payload": {"seed": "x" * 70_000}}, ["body", "payload"], "limit"),
    ],
)
async def test_invalid_input_is_422(
    client: httpx.AsyncClient, body: dict[str, Any], loc: list[str], fragment: str
) -> None:
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 422, resp.text
    errors = resp.json()["detail"]
    assert any(e["loc"] == loc for e in errors), errors
    assert any(fragment in e["msg"] for e in errors)


async def test_invalid_idempotency_key_is_422(client: httpx.AsyncClient) -> None:
    resp = await client.post("/jobs", json=SLEEP_JOB, headers={"Idempotency-Key": "k" * 256})
    assert resp.status_code == 422


# --------------------------------------------------------------------------- submission


async def test_submit_returns_201_and_enqueues_after_commit(
    client: httpx.AsyncClient, test_settings: Settings
) -> None:
    resp = await client.post("/jobs", json={**SLEEP_JOB, "max_attempts": 3})
    assert resp.status_code == 201
    job = resp.json()
    assert job["status"] == "queued"
    assert job["priority"] == "normal"
    assert job["attempts"] == 0
    assert job["max_attempts"] == 3
    assert job["payload"] == {"seconds": 0.0}
    assert await _stream_len(test_settings) == 1


async def test_default_max_attempts(client: httpx.AsyncClient, test_settings: Settings) -> None:
    job = (await client.post("/jobs", json=SLEEP_JOB)).json()
    assert job["max_attempts"] == test_settings.default_max_attempts


async def test_priority_selects_stream(client: httpx.AsyncClient, test_settings: Settings) -> None:
    await client.post("/jobs", json={**SLEEP_JOB, "priority": "high"})
    assert await _stream_len(test_settings, "high") == 1
    assert await _stream_len(test_settings, "normal") == 0


async def test_future_run_at_is_pending_and_not_enqueued(
    client: httpx.AsyncClient, test_settings: Settings
) -> None:
    run_at = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    job = (await client.post("/jobs", json={**SLEEP_JOB, "run_at": run_at})).json()
    assert job["status"] == "pending"
    assert await _stream_len(test_settings) == 0


async def test_submit_succeeds_when_redis_is_down(test_settings: Settings, registry: Any) -> None:
    settings = test_settings.model_copy(update={"redis_url": SecretStr("redis://127.0.0.1:1/0")})
    app = create_app(settings, registry)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://t", headers={"X-API-Key": API_KEY_A}
        ) as c:
            resp = await c.post("/jobs", json=SLEEP_JOB)
    # Committed to Postgres; the sweeper (Milestone 3) re-adds the missing stream entry.
    assert resp.status_code == 201
    assert resp.json()["status"] == "queued"


async def test_sleep_job_succeeds_with_one_attempt(
    client: httpx.AsyncClient, running_worker: Worker
) -> None:
    job_id = (
        await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0.1}})
    ).json()["id"]
    job = await wait_for_status(client, job_id, {"succeeded", "dead"})
    assert job["status"] == "succeeded"
    assert job["result"] == {"slept_seconds": 0.1}
    assert job["attempts"] == 1
    assert job["finished_at"] is not None
    (attempt,) = job["attempt_history"]
    assert attempt["status"] == "succeeded"
    assert attempt["attempt_no"] == 1
    assert attempt["worker_id"] == running_worker.worker_id
    assert attempt["lease_token"] == 1


async def test_get_unknown_job_is_404(client: httpx.AsyncClient) -> None:
    assert (await client.get(f"/jobs/{uuid.uuid4()}")).status_code == 404
    assert (await client.get("/jobs/not-a-uuid")).status_code == 422


# --------------------------------------------------------------------------- idempotency


async def test_concurrent_duplicates_create_exactly_one_job(
    client: httpx.AsyncClient, app: FastAPI
) -> None:
    headers = {"Idempotency-Key": "same-key"}
    responses = await asyncio.gather(
        *(client.post("/jobs", json=SLEEP_JOB, headers=headers) for _ in range(20))
    )
    codes = sorted(r.status_code for r in responses)
    assert codes == [200] * 19 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert await _job_count(app) == 1


async def test_replay_returns_original_job(client: httpx.AsyncClient) -> None:
    headers = {"Idempotency-Key": "k1"}
    first = await client.post("/jobs", json=SLEEP_JOB, headers=headers)
    # Same request with key order changed and a default spelled out: still the same body.
    same = {"priority": "normal", "payload": {"seconds": 0}, "type": "sleep"}
    second = await client.post("/jobs", json=same, headers=headers)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["id"] == second.json()["id"]


async def test_same_key_different_body_is_409(client: httpx.AsyncClient, app: FastAPI) -> None:
    headers = {"Idempotency-Key": "k2"}
    assert (await client.post("/jobs", json=SLEEP_JOB, headers=headers)).status_code == 201
    other = {"type": "sleep", "payload": {"seconds": 2}}
    resp = await client.post("/jobs", json=other, headers=headers)
    assert resp.status_code == 409
    assert "Idempotency-Key" in resp.json()["detail"]
    assert await _job_count(app) == 1


async def test_keys_are_scoped_per_api_key(client: httpx.AsyncClient, app: FastAPI) -> None:
    headers = {"Idempotency-Key": "shared"}
    a = await client.post("/jobs", json=SLEEP_JOB, headers=headers)
    b = await client.post("/jobs", json=SLEEP_JOB, headers={**headers, "X-API-Key": API_KEY_B})
    assert (a.status_code, b.status_code) == (201, 201)
    assert a.json()["id"] != b.json()["id"]


async def test_expired_key_can_be_reused(client: httpx.AsyncClient, app: FastAPI) -> None:
    headers = {"Idempotency-Key": "old"}
    first = (await client.post("/jobs", json=SLEEP_JOB, headers=headers)).json()
    async with app.state.session_factory() as session, session.begin():
        row = await session.get(IdempotencyKey, (api_key_id(API_KEY_A), "old"))
        assert row is not None
        row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    other = {"type": "sleep", "payload": {"seconds": 3}}
    resp = await client.post("/jobs", json=other, headers=headers)
    assert resp.status_code == 201
    assert resp.json()["id"] != first["id"]


# --------------------------------------------------------------------------- listing


async def test_list_filters_and_keyset_pagination(client: httpx.AsyncClient) -> None:
    created = []
    for i in range(5):
        created.append((await client.post("/jobs", json=SLEEP_JOB)).json()["id"])
        if i % 2 == 0:
            await client.post("/jobs", json={"type": "cpu", "payload": {"iterations": 1}})

    seen: list[str] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"type": "sleep", "limit": 2}
        if cursor:
            params["cursor"] = cursor
        page = (await client.get("/jobs", params=params)).json()
        assert len(page["items"]) <= 2
        seen.extend(item["id"] for item in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == list(reversed(created))  # newest first, no gaps or repeats

    cpu = (await client.get("/jobs", params={"type": "cpu"})).json()["items"]
    assert len(cpu) == 3
    queued = (await client.get("/jobs", params={"status": "queued", "limit": 200})).json()
    assert len(queued["items"]) == 8
    assert (await client.get("/jobs", params={"status": "succeeded"})).json()["items"] == []


@pytest.mark.parametrize(
    "params", [{"cursor": "garbage"}, {"limit": 0}, {"limit": 201}, {"status": "nope"}]
)
async def test_list_rejects_bad_params(client: httpx.AsyncClient, params: dict[str, Any]) -> None:
    assert (await client.get("/jobs", params=params)).status_code == 422
