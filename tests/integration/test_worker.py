from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable

import httpx
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy import select

from tests.integration.conftest import ExecutionCounter, wait_for_status
from workflow_engine.config import Settings
from workflow_engine.core import jobs
from workflow_engine.core.states import Priority
from workflow_engine.db.models import DeadLetter, Job
from workflow_engine.dispatch.streams import StreamMessage, StreamProducer
from workflow_engine.worker.runner import Worker


async def _submit(client: httpx.AsyncClient, **payload: object) -> str:
    resp = await client.post("/jobs", json={"type": "count", "payload": payload})
    assert resp.status_code == 201, resp.text
    return str(resp.json()["id"])


async def _pending_count(worker: Worker, settings: Settings) -> int:
    redis = Redis.from_url(settings.redis_url.get_secret_value())
    try:
        total = 0
        for stream in worker.consumer.streams:
            info = await redis.xpending(stream, worker.consumer.group)
            total += int(info["pending"])
        return total
    finally:
        await redis.aclose()


async def test_same_message_delivered_twice_runs_handler_once(
    client: httpx.AsyncClient,
    make_worker: Callable[..., Worker],
    counter: ExecutionCounter,
    test_settings: Settings,
) -> None:
    job_id = await _submit(client)
    worker = make_worker()
    await worker.start()
    (message,) = await worker.consumer.read(block_ms=100)
    assert message.job_id == uuid.UUID(job_id)

    await worker.process_message(message)
    await worker.process_message(message)  # redelivery of the very same entry

    assert counter.calls == [uuid.UUID(job_id)]
    job = await wait_for_status(client, job_id, {"succeeded"})
    assert len(job["attempt_history"]) == 1
    assert await _pending_count(worker, test_settings) == 0  # both deliveries ACKed


async def test_duplicate_stream_entries_run_handler_once(
    client: httpx.AsyncClient,
    app: FastAPI,
    running_worker: Worker,
    counter: ExecutionCounter,
    test_settings: Settings,
) -> None:
    job_id = await _submit(client, seconds=0.2)
    producer: StreamProducer = app.state.producer
    for _ in range(3):  # e.g. sweeper re-adds; all extra hints must be dropped
        await producer.enqueue(uuid.UUID(job_id), Priority.NORMAL)

    job = await wait_for_status(client, job_id, {"succeeded"})
    assert job["attempts"] == 1
    for _ in range(50):
        if await _pending_count(running_worker, test_settings) == 0:
            break
        await asyncio.sleep(0.05)
    assert counter.calls == [uuid.UUID(job_id)]
    assert await _pending_count(running_worker, test_settings) == 0


async def test_failure_dead_letters_the_job(
    client: httpx.AsyncClient, app: FastAPI, running_worker: Worker
) -> None:
    job_id = await _submit(client, fail=True)
    job = await wait_for_status(client, job_id, {"dead"})
    assert job["last_error"] == "RuntimeError: asked to fail"
    (attempt,) = job["attempt_history"]
    assert attempt["status"] == "failed"
    assert attempt["error"] == "RuntimeError: asked to fail"
    async with app.state.session_factory() as session:
        letters = (await session.scalars(select(DeadLetter))).all()
    assert [(d.job_id, d.reason) for d in letters] == [(uuid.UUID(job_id), "failed")]


async def test_handler_timeout_fails_the_job(
    client: httpx.AsyncClient, running_worker: Worker
) -> None:
    job_id = await _submit(client, seconds=5)  # `count` has a 2s timeout
    job = await wait_for_status(client, job_id, {"dead", "succeeded"}, within=10)
    assert job["status"] == "dead"
    assert job["last_error"] == "timed out after 2s"


async def test_stale_lease_token_cannot_overwrite_result(
    client: httpx.AsyncClient, app: FastAPI
) -> None:
    job_id = uuid.UUID(await _submit(client))
    sf = app.state.session_factory
    claim = await jobs.claim_job(sf, job_id, "zombie", lease_seconds=30)
    assert claim is not None
    assert await jobs.claim_job(sf, job_id, "other", lease_seconds=30) is None  # CAS rejects

    stale = jobs.Claim(**{**claim.__dict__, "lease_token": claim.lease_token - 1})
    assert await jobs.complete_job(sf, stale, {"from": "zombie"}) is False
    assert await jobs.fail_job(sf, stale, "zombie error") is False
    assert await jobs.complete_job(sf, claim, {"from": "owner"}) is True
    assert await jobs.complete_job(sf, claim, {"again": True}) is False  # already terminal

    async with sf() as session:
        job = await session.get(Job, job_id)
    assert job is not None
    assert job.result == {"from": "owner"}


async def test_malformed_entry_is_acked_and_dropped(
    make_worker: Callable[..., Worker], test_settings: Settings, counter: ExecutionCounter
) -> None:
    worker = make_worker()
    await worker.start()
    redis = Redis.from_url(test_settings.redis_url.get_secret_value())
    stream = worker.consumer.streams[1]
    await redis.xadd(stream, {"job_id": "not-a-uuid"})
    await redis.aclose()
    (message,) = await worker.consumer.read(block_ms=100)
    assert message == StreamMessage(stream, message.message_id, None)
    await worker.process_message(message)
    assert counter.calls == []
    assert await _pending_count(worker, test_settings) == 0


async def test_workers_poll_high_priority_first(
    client: httpx.AsyncClient, make_worker: Callable[..., Worker]
) -> None:
    low = (await client.post("/jobs", json={"type": "count", "priority": "low"})).json()["id"]
    high = (await client.post("/jobs", json={"type": "count", "priority": "high"})).json()["id"]
    worker = make_worker()
    await worker.start()
    first = await worker.consumer.read(block_ms=100)
    second = await worker.consumer.read(block_ms=100)
    assert [str(m.job_id) for m in first + second] == [high, low]
