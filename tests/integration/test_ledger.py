"""The `ledger` handler records every execution, so duplicates can be counted."""

from __future__ import annotations

import uuid

import httpx
from fastapi import FastAPI
from sqlalchemy import select, update

from tests.integration.conftest import wait_for_status
from workflow_engine.core.states import JobStatus, Priority
from workflow_engine.db.models import Job, JobExecution
from workflow_engine.worker.runner import Worker


async def _executions(app: FastAPI, job_id: str) -> list[JobExecution]:
    async with app.state.session_factory() as session:
        rows = await session.scalars(
            select(JobExecution)
            .where(JobExecution.job_id == uuid.UUID(job_id))
            .order_by(JobExecution.id)
        )
        return list(rows.all())


async def test_each_execution_is_recorded_and_a_redelivery_counts_as_a_duplicate(
    client: httpx.AsyncClient, app: FastAPI, running_worker: Worker
) -> None:
    job_id = (
        await client.post("/jobs", json={"type": "ledger", "payload": {"seconds": 0}})
    ).json()["id"]
    job = await wait_for_status(client, job_id, {"succeeded", "dead"})
    assert job["status"] == "succeeded"
    (first,) = await _executions(app, job_id)
    assert first.attempt_no == 1
    assert first.worker_id == "test-worker"
    assert first.finished_at is not None
    assert job["result"]["execution_id"] == first.id

    # Run it again (as after a reaped lease): the ledger shows two executions of one job.
    async with app.state.session_factory() as session, session.begin():
        await session.execute(
            update(Job).where(Job.id == uuid.UUID(job_id)).values(status=JobStatus.QUEUED)
        )
    await app.state.producer.enqueue(uuid.UUID(job_id), Priority.NORMAL)
    await wait_for_status(client, job_id, {"succeeded"})
    executions = await _executions(app, job_id)
    assert len(executions) == 2
    assert [e.attempt_no for e in executions] == [1, 2]
