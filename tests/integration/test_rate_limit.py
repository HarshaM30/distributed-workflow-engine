from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr
from redis.asyncio import Redis

from tests.integration.conftest import (
    API_KEY_A,
    API_KEY_B,
    run_in_background,
    wait_for_status,
)
from workflow_engine.api.app import create_app
from workflow_engine.config import RateLimit, Settings
from workflow_engine.dispatch.rate_limit import TokenBucket
from workflow_engine.maintenance.main import Maintenance
from workflow_engine.worker.runner import Worker


@pytest.fixture
async def redis(test_settings: Settings) -> AsyncIterator[Redis]:
    client = Redis.from_url(test_settings.redis_url.get_secret_value(), decode_responses=True)
    yield client
    await client.aclose()


# --------------------------------------------------------------------------- the Lua bucket


async def test_bucket_allows_burst_then_refills(redis: Redis) -> None:
    bucket = TokenBucket(redis, "t")
    limit = RateLimit(rate=10, burst=3)
    decisions = [await bucket.acquire("s", "k", limit) for _ in range(4)]
    assert [d.allowed for d in decisions] == [True, True, True, False]
    assert 0 < decisions[-1].retry_after <= 0.1 + 1e-6
    await asyncio.sleep(decisions[-1].retry_after + 0.02)
    assert (await bucket.acquire("s", "k", limit)).allowed
    assert 0 < await redis.pttl(bucket.key("s", "k")) <= 1300


async def test_buckets_are_independent(redis: Redis) -> None:
    bucket = TokenBucket(redis, "t")
    limit = RateLimit(rate=1, burst=1)
    assert (await bucket.acquire("s", "a", limit)).allowed
    assert not (await bucket.acquire("s", "a", limit)).allowed
    assert (await bucket.acquire("s", "b", limit)).allowed


async def test_concurrent_callers_never_overspend(redis: Redis) -> None:
    bucket = TokenBucket(redis, "t")
    limit = RateLimit(rate=0.001, burst=10)
    decisions = await asyncio.gather(*(bucket.acquire("s", "k", limit) for _ in range(50)))
    assert sum(d.allowed for d in decisions) == 10


async def test_bucket_fails_open_when_redis_is_down() -> None:
    dead = Redis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.5)
    decision = await TokenBucket(dead, "t").acquire("s", "k", RateLimit(rate=1, burst=1))
    assert decision.allowed
    assert decision.failed_open
    await dead.aclose()


# --------------------------------------------------------------------------- execution throttle


async def test_execution_throttle_caps_start_rate_and_every_job_completes(
    client: httpx.AsyncClient,
    make_worker: Callable[..., Worker],
    running_maintenance: Maintenance,
    test_settings: Settings,
) -> None:
    rate = 5.0
    settings = test_settings.model_copy(
        update={"rate_limits": {"count": RateLimit(rate=rate, burst=1)}, "worker_concurrency": 2}
    )
    workers = [make_worker(f"w{i}", settings=settings) for i in range(2)]
    runs = [await run_in_background(w) for w in workers]
    ids = [(await client.post("/jobs", json={"type": "count"})).json()["id"] for _ in range(20)]

    starts: list[datetime] = []
    for job_id in ids:
        job = await wait_for_status(client, job_id, {"succeeded", "dead"}, within=20)
        assert job["status"] == "succeeded"
        # Throttled claims are undone, so each job shows exactly the attempt that ran.
        (attempt,) = job["attempt_history"]
        assert job["attempts"] == 1
        starts.append(datetime.fromisoformat(attempt["started_at"]))
    for stop, task in runs:
        stop.set()
        await task

    starts.sort()
    span = (starts[-1] - starts[0]).total_seconds()
    observed = (len(starts) - 1) / span
    assert observed <= rate * 1.2, f"{observed:.2f}/s exceeds {rate}/s"
    assert observed >= rate * 0.5, f"{observed:.2f}/s is far below {rate}/s"
    # No one-second window holds more than the burst plus one second of refill.
    for i, first in enumerate(starts):
        in_window = sum(1 for s in starts[i:] if (s - first).total_seconds() < 1)
        assert in_window <= 1 + rate


async def test_unthrottled_types_are_unaffected(
    client: httpx.AsyncClient, make_worker: Callable[..., Worker], test_settings: Settings
) -> None:
    settings = test_settings.model_copy(
        update={"rate_limits": {"cpu": RateLimit(rate=0.01, burst=1)}}
    )
    worker = make_worker(settings=settings)
    stop, task = await run_in_background(worker)
    ids = [(await client.post("/jobs", json={"type": "count"})).json()["id"] for _ in range(5)]
    for job_id in ids:
        await wait_for_status(client, job_id, {"succeeded"}, within=3)
    stop.set()
    await task


# --------------------------------------------------------------------------- submission limit


@pytest.fixture
async def limited_app(test_settings: Settings, registry: Any) -> AsyncIterator[FastAPI]:
    settings = test_settings.model_copy(
        update={"submit_rate_limit_per_second": 0.5, "submit_rate_limit_burst": 3}
    )
    app = create_app(settings, registry)
    async with app.router.lifespan_context(app):
        yield app


def _client(app: FastAPI, key: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t", headers={"X-API-Key": key}
    )


async def test_submission_limit_returns_429_per_api_key(limited_app: FastAPI) -> None:
    body = {"type": "count"}
    async with _client(limited_app, API_KEY_A) as a, _client(limited_app, API_KEY_B) as b:
        first = await a.post("/jobs", json=body, headers={"Idempotency-Key": "kept"})
        assert first.status_code == 201
        assert [(await a.post("/jobs", json=body)).status_code for _ in range(2)] == [201, 201]

        limited = await a.post("/jobs", json=body)
        assert limited.status_code == 429
        retry_after = int(limited.headers["Retry-After"])
        assert 1 <= retry_after <= 2  # one token at 0.5/s

        # Another key has its own bucket.
        assert (await b.post("/jobs", json=body)).status_code == 201

        # A replay of an existing Idempotency-Key costs nothing and still works.
        replay = await a.post("/jobs", json=body, headers={"Idempotency-Key": "kept"})
        assert replay.status_code == 200
        assert replay.json()["id"] == first.json()["id"]

        # A new key is a new submission, so it is limited.
        fresh = await a.post("/jobs", json=body, headers={"Idempotency-Key": "fresh"})
        assert fresh.status_code == 429

        await asyncio.sleep(retry_after)
        assert (await a.post("/jobs", json=body)).status_code == 201


async def test_submission_limit_fails_open_without_redis(
    test_settings: Settings, registry: Any
) -> None:
    settings = test_settings.model_copy(
        update={
            "redis_url": SecretStr("redis://127.0.0.1:1/0"),
            "submit_rate_limit_per_second": 0.01,
            "submit_rate_limit_burst": 1,
        }
    )
    app = create_app(settings, registry)
    async with app.router.lifespan_context(app), _client(app, API_KEY_A) as c:
        codes = [(await c.post("/jobs", json={"type": "count"})).status_code for _ in range(3)]
    assert codes == [201, 201, 201]
