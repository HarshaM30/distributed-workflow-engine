from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
from fastapi import FastAPI
from sqlalchemy import func, select, update

from tests.integration.conftest import (
    run_in_background,
    wait_for_status,
    wait_until,
)
from workflow_engine.config import Settings
from workflow_engine.core import jobs
from workflow_engine.core.states import JobStatus
from workflow_engine.db.models import DeadLetter, Job
from workflow_engine.maintenance.main import Maintenance
from workflow_engine.worker.runner import Worker


async def _submit(client: httpx.AsyncClient, body: dict[str, object]) -> uuid.UUID:
    resp = await client.post("/jobs", json=body)
    assert resp.status_code == 201, resp.text
    return uuid.UUID(resp.json()["id"])


async def _expire_lease(app: FastAPI, job_id: uuid.UUID) -> None:
    async with app.state.session_factory() as session, session.begin():
        await session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )


async def _job(app: FastAPI, job_id: uuid.UUID) -> Job:
    async with app.state.session_factory() as session:
        job = await session.get(Job, job_id)
    assert job is not None
    return job


def worker_env(settings: Settings, **overrides: str) -> dict[str, str]:
    """Environment for a worker subprocess pointed at the test database and Redis DB."""
    env = {
        **os.environ,
        "DATABASE_URL": settings.sqlalchemy_url,
        "REDIS_URL": settings.redis_url.get_secret_value(),
        "REDIS_STREAM_PREFIX": settings.redis_stream_prefix,
        "REDIS_CONSUMER_GROUP": settings.redis_consumer_group,
        "RATE_LIMITS": "{}",
        "WORKER_BLOCK_MS": "100",
        "LOG_LEVEL": "WARNING",
    }
    env.update(overrides)
    return env


def spawn_worker(settings: Settings, **overrides: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "workflow_engine.worker"],
        env=worker_env(settings, **overrides),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


# --------------------------------------------------------------------------- heartbeat


async def test_heartbeat_extends_the_lease(
    client: httpx.AsyncClient,
    app: FastAPI,
    make_worker: Callable[..., Worker],
    test_settings: Settings,
) -> None:
    worker = make_worker(settings=test_settings.model_copy(update={"worker_lease_seconds": 1}))
    stop, task = await run_in_background(worker)
    job_id = await _submit(client, {"type": "count", "payload": {"seconds": 1.8}})
    await wait_for_status(client, str(job_id), {"running"})
    first = (await _job(app, job_id)).lease_expires_at
    await asyncio.sleep(0.6)
    second = (await _job(app, job_id)).lease_expires_at
    assert first is not None
    assert second is not None
    assert second > first
    job = await wait_for_status(client, str(job_id), {"succeeded", "queued", "dead"})
    assert job["status"] == "succeeded"  # outlived the 1s lease thanks to heartbeats
    assert job["attempts"] == 1
    stop.set()
    await task


# --------------------------------------------------------------------------- reaper


async def test_reaper_requeues_expired_lease(
    client: httpx.AsyncClient, app: FastAPI, make_maintenance: Callable[..., Maintenance]
) -> None:
    job_id = await _submit(client, {"type": "count", "max_attempts": 3})
    sf = app.state.session_factory
    claim = await jobs.claim_job(sf, job_id, "crashed", lease_seconds=30)
    assert claim is not None
    maintenance = make_maintenance()
    assert await maintenance.reaper_tick() == 0  # lease still valid

    await _expire_lease(app, job_id)
    assert await maintenance.reaper_tick() == 1
    detail = (await client.get(f"/jobs/{job_id}")).json()
    assert detail["status"] == "queued"
    assert detail["last_error"] == "lease_expired"
    (attempt,) = detail["attempt_history"]
    assert (attempt["status"], attempt["error"]) == ("lease_expired", "lease_expired")
    assert attempt["finished_at"] is not None


async def test_reaper_dead_letters_when_attempts_exhausted(
    client: httpx.AsyncClient, app: FastAPI, make_maintenance: Callable[..., Maintenance]
) -> None:
    job_id = await _submit(client, {"type": "count", "max_attempts": 1})
    sf = app.state.session_factory
    assert await jobs.claim_job(sf, job_id, "crashed", lease_seconds=30) is not None
    await _expire_lease(app, job_id)
    assert await make_maintenance().reaper_tick() == 1
    assert (await _job(app, job_id)).status == JobStatus.DEAD
    async with sf() as session:
        reasons = (await session.scalars(select(DeadLetter.reason))).all()
    assert reasons == ["lease_expired"]


async def test_reaper_honours_pending_cancel(
    client: httpx.AsyncClient, app: FastAPI, make_maintenance: Callable[..., Maintenance]
) -> None:
    job_id = await _submit(client, {"type": "count"})
    sf = app.state.session_factory
    assert await jobs.claim_job(sf, job_id, "crashed", lease_seconds=30) is not None
    assert (await client.post(f"/jobs/{job_id}/cancel")).status_code == 202
    await _expire_lease(app, job_id)
    await make_maintenance().reaper_tick()
    assert (await _job(app, job_id)).status == JobStatus.CANCELLED


async def test_zombie_is_fenced_out_after_reap(
    client: httpx.AsyncClient, app: FastAPI, make_maintenance: Callable[..., Maintenance]
) -> None:
    job_id = await _submit(client, {"type": "count", "max_attempts": 3})
    sf = app.state.session_factory
    zombie = await jobs.claim_job(sf, job_id, "zombie", lease_seconds=30)
    assert zombie is not None
    await _expire_lease(app, job_id)
    await make_maintenance().reaper_tick()

    # While the job is queued again, the zombie can't touch it.
    assert await jobs.heartbeat(sf, zombie, 30) is None
    assert await jobs.complete_job(sf, zombie, {"from": "zombie"}) is False

    owner = await jobs.claim_job(sf, job_id, "owner", lease_seconds=30)
    assert owner is not None
    assert owner.lease_token == zombie.lease_token + 1
    assert owner.attempt == 2

    # The zombie wakes up late: every write it tries is rejected by the fencing token.
    assert await jobs.heartbeat(sf, zombie, 30) is None
    assert await jobs.complete_job(sf, zombie, {"from": "zombie"}) is False
    assert await jobs.fail_job(sf, zombie, "zombie", retry_delay=0) is None
    assert await jobs.release_job(sf, zombie) is False
    assert await jobs.cancel_running_job(sf, zombie) is False
    assert await jobs.defer_job(sf, zombie, 1) is False

    assert await jobs.complete_job(sf, owner, {"from": "owner"}) is True
    job = await _job(app, job_id)
    assert (job.status, job.result) == (JobStatus.SUCCEEDED, {"from": "owner"})


async def test_handler_stops_when_lease_is_lost(
    client: httpx.AsyncClient, app: FastAPI, running_worker: Worker
) -> None:
    slow = await _submit(client, {"type": "count", "payload": {"seconds": 5}})
    await wait_for_status(client, str(slow), {"running"})
    # Partition: the job is reaped (no new stream entry) while the handler is still running.
    async with app.state.session_factory() as session, session.begin():
        await session.execute(
            update(Job).where(Job.id == slow).values(status=JobStatus.QUEUED, lease_owner=None)
        )
    # The next heartbeat sees the lost lease and cancels the handler, so this single-slot
    # worker is free long before the 5s handler would have finished.
    quick = await _submit(client, {"type": "count"})
    done = await wait_for_status(client, str(quick), {"succeeded"}, within=2)
    assert done["status"] == "succeeded"
    job = await _job(app, slow)
    assert (job.status, job.result) == (JobStatus.QUEUED, None)


# --------------------------------------------------------------------------- kill -9


async def test_killed_worker_job_is_reaped_and_completed(
    client: httpx.AsyncClient,
    app: FastAPI,
    test_settings: Settings,
    make_worker: Callable[..., Worker],
    running_maintenance: Maintenance,
) -> None:
    lease = 1.0
    victim = spawn_worker(
        test_settings,
        WORKER_ID="victim",
        WORKER_LEASE_SECONDS=str(lease),
        WORKER_HEARTBEAT_SECONDS="0.3",
    )
    try:
        job_id = await _submit(client, {"type": "sleep", "payload": {"seconds": 1.5}})
        running = await wait_for_status(client, str(job_id), {"running"}, within=20)
        assert running["attempt_history"][0]["worker_id"] == "victim"
        victim.send_signal(signal.SIGKILL)
        killed_at = asyncio.get_running_loop().time()
        await asyncio.to_thread(victim.wait, 10)
    finally:
        if victim.poll() is None:
            victim.kill()

    rescuer = make_worker("rescuer")
    stop, task = await run_in_background(rescuer)

    async def reclaimed() -> bool:
        detail = (await client.get(f"/jobs/{job_id}")).json()
        return any(a["worker_id"] == "rescuer" for a in detail["attempt_history"])

    await wait_until(reclaimed, within=10, message="rescuer to claim the job")
    recovery = asyncio.get_running_loop().time() - killed_at
    # Lease time + reaper interval, plus a little for the stream read and claim.
    assert recovery <= lease + test_settings.reaper_interval_seconds + 1.0

    job = await wait_for_status(client, str(job_id), {"succeeded", "dead"})
    assert job["status"] == "succeeded"
    assert [(a["worker_id"], a["status"]) for a in job["attempt_history"]] == [
        ("victim", "lease_expired"),
        ("rescuer", "succeeded"),
    ]
    stop.set()
    await task
    async with app.state.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
