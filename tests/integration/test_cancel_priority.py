from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx

from tests.integration.conftest import (
    ExecutionCounter,
    run_in_background,
    wait_for_status,
)
from workflow_engine.worker.runner import Worker

# --------------------------------------------------------------------------- cancellation


async def test_cancel_pending_job(client: httpx.AsyncClient) -> None:
    later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    job_id = (await client.post("/jobs", json={"type": "count", "run_at": later})).json()["id"]
    resp = await client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert resp.json()["finished_at"] is not None


async def test_cancel_queued_job_and_drop_its_stream_entry(
    client: httpx.AsyncClient, make_worker: Callable[..., Worker], counter: ExecutionCounter
) -> None:
    job_id = (await client.post("/jobs", json={"type": "count"})).json()["id"]
    resp = await client.post(f"/jobs/{job_id}/cancel")
    assert (resp.status_code, resp.json()["status"]) == (200, "cancelled")

    worker = make_worker()
    await worker.start()
    (message,) = await worker.consumer.read(block_ms=100)
    await worker.process_message(message)  # the CAS claim rejects the leftover hint
    assert counter.calls == []
    assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "cancelled"


async def test_cancel_running_job_stops_the_handler(
    client: httpx.AsyncClient, running_worker: Worker, counter: ExecutionCounter
) -> None:
    job_id = (
        await client.post("/jobs", json={"type": "count", "payload": {"seconds": 1.8}})
    ).json()["id"]
    await wait_for_status(client, job_id, {"running"})
    resp = await client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 202
    assert (resp.json()["status"], resp.json()["cancel_requested"]) == ("running", True)

    # The heartbeat (every 0.2s here) notices well before the 1.8s handler would finish.
    job = await wait_for_status(client, job_id, {"cancelled", "succeeded"}, within=1.2)
    assert job["status"] == "cancelled"
    assert job["result"] is None
    (attempt,) = job["attempt_history"]
    assert attempt["status"] == "cancelled"
    assert counter.calls == [uuid.UUID(job_id)]


async def test_cancel_terminal_job_is_409(
    client: httpx.AsyncClient, running_worker: Worker
) -> None:
    job_id = (await client.post("/jobs", json={"type": "count"})).json()["id"]
    await wait_for_status(client, job_id, {"succeeded"})
    resp = await client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 409
    assert "succeeded" in resp.json()["detail"]
    pending = (
        await client.post(
            "/jobs",
            json={"type": "count", "run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
        )
    ).json()["id"]
    await client.post(f"/jobs/{pending}/cancel")
    assert (await client.post(f"/jobs/{pending}/cancel")).status_code == 409  # cancelled


async def test_cancel_unknown_job_is_404(client: httpx.AsyncClient) -> None:
    assert (await client.post(f"/jobs/{uuid.uuid4()}/cancel")).status_code == 404


# --------------------------------------------------------------------------- priorities


async def test_mixed_backlog_runs_high_priority_first(
    client: httpx.AsyncClient, make_worker: Callable[..., Worker], counter: ExecutionCounter
) -> None:
    ids: dict[str, list[str]] = {"low": [], "normal": [], "high": []}
    # Lowest priority submitted first, so FIFO order alone would get this backwards.
    for priority in ("low", "normal", "high"):
        for _ in range(4):
            resp = await client.post("/jobs", json={"type": "count", "priority": priority})
            ids[priority].append(resp.json()["id"])

    worker = make_worker()  # one slot: jobs start strictly one after another
    stop, task = await run_in_background(worker)
    for job_id in ids["low"]:
        await wait_for_status(client, job_id, {"succeeded"})
    stop.set()
    await task

    order = [str(job_id) for job_id in counter.calls]
    assert order[:4] == ids["high"]
    assert order[4:8] == ids["normal"]
    assert order[8:] == ids["low"]
