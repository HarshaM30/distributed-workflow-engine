from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import func, select, update

from tests.integration.conftest import (
    API_KEY_A,
    ExecutionCounter,
    run_in_background,
    wait_for_status,
    wait_until,
)
from workflow_engine.config import Settings
from workflow_engine.core.idempotency import api_key_id
from workflow_engine.core.states import JobStatus
from workflow_engine.db.models import IdempotencyKey, Job
from workflow_engine.maintenance.main import Maintenance
from workflow_engine.worker.runner import Worker


def _redis(settings: Settings) -> Redis:
    return Redis.from_url(settings.redis_url.get_secret_value(), decode_responses=True)


async def _stream_len(settings: Settings, priority: str = "normal") -> int:
    redis = _redis(settings)
    try:
        return int(await redis.xlen(f"{settings.redis_stream_prefix}:{priority}"))
    finally:
        await redis.aclose()


async def _age_jobs(app: FastAPI, seconds: float) -> None:
    async with app.state.session_factory() as session, session.begin():
        await session.execute(
            update(Job).values(updated_at=func.now() - timedelta(seconds=seconds))
        )


# --------------------------------------------------------------------------- scheduler


async def test_scheduler_promotes_only_due_jobs(
    client: httpx.AsyncClient,
    app: FastAPI,
    make_maintenance: Callable[..., Maintenance],
    test_settings: Settings,
) -> None:
    soon = (datetime.now(UTC) + timedelta(seconds=0.3)).isoformat()
    later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    due = (await client.post("/jobs", json={"type": "count", "run_at": soon})).json()["id"]
    await client.post("/jobs", json={"type": "count", "run_at": later})
    maintenance = make_maintenance()
    assert await maintenance.scheduler_tick() == 0
    await asyncio.sleep(0.4)
    assert await maintenance.scheduler_tick() == 1
    assert (await client.get(f"/jobs/{due}")).json()["status"] == "queued"
    assert await _stream_len(test_settings) == 1
    assert await maintenance.scheduler_tick() == 0


async def test_scheduler_drains_more_than_one_batch(
    client: httpx.AsyncClient,
    make_maintenance: Callable[..., Maintenance],
    test_settings: Settings,
) -> None:
    soon = (datetime.now(UTC) + timedelta(seconds=0.2)).isoformat()
    for _ in range(7):
        await client.post("/jobs", json={"type": "count", "run_at": soon})
    await asyncio.sleep(0.3)
    small = make_maintenance(
        settings=test_settings.model_copy(update={"maintenance_batch_size": 3})
    )
    assert await small.scheduler_tick() == 7


async def test_delayed_job_does_not_start_early(
    client: httpx.AsyncClient,
    running_worker: Worker,
    running_maintenance: Maintenance,
) -> None:
    run_at = datetime.now(UTC) + timedelta(seconds=10)
    resp = await client.post("/jobs", json={"type": "count", "run_at": run_at.isoformat()})
    job_id = resp.json()["id"]
    assert resp.json()["status"] == "pending"
    await asyncio.sleep(9)
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "pending"
    job = await wait_for_status(client, job_id, {"succeeded"}, within=5)
    started = datetime.fromisoformat(job["attempt_history"][0]["started_at"])
    assert started >= run_at


# --------------------------------------------------------------------------- sweeper


async def test_sweeper_readds_jobs_whose_xadd_was_lost(
    client: httpx.AsyncClient,
    app: FastAPI,
    make_maintenance: Callable[..., Maintenance],
    test_settings: Settings,
) -> None:
    await client.post("/jobs", json={"type": "count"})
    redis = _redis(test_settings)
    await redis.delete(f"{test_settings.redis_stream_prefix}:normal")  # entry lost
    await redis.aclose()
    maintenance = make_maintenance()
    assert await maintenance.sweeper_tick() == 0  # not stale yet
    await _age_jobs(app, 5)
    assert await maintenance.sweeper_tick() == 1
    assert await _stream_len(test_settings) == 1
    # Touched on re-send, so the next pass leaves it alone instead of piling up duplicates.
    assert await maintenance.sweeper_tick() == 0


async def test_sweeper_reclaims_idle_pel_entries(
    client: httpx.AsyncClient,
    app: FastAPI,
    make_worker: Callable[..., Worker],
    make_maintenance: Callable[..., Maintenance],
    test_settings: Settings,
) -> None:
    job_id = (await client.post("/jobs", json={"type": "count"})).json()["id"]
    dead_consumer = make_worker("dead-consumer")
    await dead_consumer.start()
    (message,) = await dead_consumer.consumer.read(block_ms=100)  # read, never ACKed
    redis = _redis(test_settings)
    stream = message.stream
    assert (await redis.xpending(stream, test_settings.redis_consumer_group))["pending"] == 1

    await asyncio.sleep(test_settings.sweeper_stale_seconds + 0.1)
    # Keep the job "fresh" so only the PEL path can re-send it.
    async with app.state.session_factory() as session, session.begin():
        await session.execute(update(Job).values(updated_at=func.now()))
    assert await make_maintenance().sweeper_tick() == 1
    assert (await redis.xpending(stream, test_settings.redis_consumer_group))["pending"] == 0
    assert int(await redis.xlen(stream)) == 2  # original entry + the re-send
    await redis.aclose()

    worker = make_worker("healthy")
    stop, task = await run_in_background(worker)
    await wait_for_status(client, job_id, {"succeeded"})
    stop.set()
    await task


async def test_every_job_completes_after_redis_is_wiped(
    client: httpx.AsyncClient,
    app: FastAPI,
    make_worker: Callable[..., Worker],
    running_maintenance: Maintenance,
    counter: ExecutionCounter,
    test_settings: Settings,
) -> None:
    queued = [(await client.post("/jobs", json={"type": "count"})).json()["id"] for _ in range(5)]
    soon = (datetime.now(UTC) + timedelta(seconds=0.5)).isoformat()
    delayed = [
        (await client.post("/jobs", json={"type": "count", "run_at": soon})).json()["id"]
        for _ in range(3)
    ]
    high = (await client.post("/jobs", json={"type": "count", "priority": "high"})).json()["id"]

    # FLUSHDB on the test database (the same effect as FLUSHALL, scoped to DB 15 so a
    # developer's own Redis data survives the test run): streams, groups, PELs all gone.
    redis = _redis(test_settings)
    await redis.flushdb()
    await redis.aclose()

    worker = make_worker()
    stop, task = await run_in_background(worker)
    for job_id in [*queued, *delayed, high]:
        job = await wait_for_status(client, job_id, {"succeeded", "dead"}, within=15)
        assert job["status"] == "succeeded"
    assert sorted(map(str, counter.calls)) == sorted([*queued, *delayed, high])
    stop.set()
    await task


async def test_sweeper_deletes_expired_idempotency_keys(
    client: httpx.AsyncClient, app: FastAPI, make_maintenance: Callable[..., Maintenance]
) -> None:
    for key in ("old", "new"):
        await client.post("/jobs", json={"type": "count"}, headers={"Idempotency-Key": key})
    async with app.state.session_factory() as session, session.begin():
        await session.execute(
            update(IdempotencyKey)
            .where(IdempotencyKey.key == "old")
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    await make_maintenance().sweeper_tick()
    async with app.state.session_factory() as session:
        keys = (await session.scalars(select(IdempotencyKey.key))).all()
        assert keys == ["new"]
        assert await session.get(IdempotencyKey, (api_key_id(API_KEY_A), "new")) is not None
        # The job itself stays.
        assert await session.scalar(select(func.count()).select_from(Job)) == 2


# --------------------------------------------------------------------------- leader election


async def test_only_one_maintenance_process_leads(
    make_maintenance: Callable[..., Maintenance],
) -> None:
    first, second = make_maintenance("first"), make_maintenance("second")
    stop_first, first_task = await run_in_background(first)
    await wait_until(lambda: first.is_leader, message="first to lead")
    stop_second, second_task = await run_in_background(second)
    await asyncio.sleep(0.6)  # several leader-retry periods
    assert first.is_leader
    assert not second.is_leader

    stop_first.set()  # leader goes away; the standby takes over
    await asyncio.wait_for(first_task, timeout=10)
    await wait_until(lambda: second.is_leader, within=5, message="standby to take over")
    stop_second.set()
    await asyncio.wait_for(second_task, timeout=10)


async def test_leader_keeps_running_loops(
    client: httpx.AsyncClient,
    running_maintenance: Maintenance,
    running_worker: Worker,
) -> None:
    soon = (datetime.now(UTC) + timedelta(seconds=0.2)).isoformat()
    job_id = (await client.post("/jobs", json={"type": "count", "run_at": soon})).json()["id"]
    await wait_for_status(client, job_id, {"succeeded"})
    assert running_maintenance.is_leader


async def test_sweeper_ignores_running_jobs(
    client: httpx.AsyncClient,
    app: FastAPI,
    make_maintenance: Callable[..., Maintenance],
) -> None:
    job_id = uuid.UUID((await client.post("/jobs", json={"type": "count"})).json()["id"])
    async with app.state.session_factory() as session, session.begin():
        await session.execute(update(Job).values(status=JobStatus.RUNNING))
    await _age_jobs(app, 5)
    assert await make_maintenance().sweeper_tick() == 0
    async with app.state.session_factory() as session:
        job = await session.get(Job, job_id)
    assert job is not None
    assert job.status == JobStatus.RUNNING
