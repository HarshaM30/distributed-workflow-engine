from __future__ import annotations

import asyncio
import signal
import uuid
from collections.abc import Callable

import httpx
from fastapi import FastAPI

from tests.integration.conftest import ExecutionCounter, run_in_background, wait_for_status
from tests.integration.test_leases import spawn_worker
from workflow_engine.config import Settings
from workflow_engine.worker.runner import Worker


async def test_sigterm_lets_inflight_job_finish_and_exits_zero(
    client: httpx.AsyncClient, test_settings: Settings
) -> None:
    proc = spawn_worker(test_settings, WORKER_ID="graceful", WORKER_SHUTDOWN_GRACE_SECONDS="30")
    try:
        job_id = (
            await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 5}})
        ).json()["id"]
        await wait_for_status(client, job_id, {"running"}, within=20)
        proc.send_signal(signal.SIGTERM)
        # A second job submitted after SIGTERM must not be picked up.
        later = await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})
        code = await asyncio.to_thread(proc.wait, 15)
    finally:
        if proc.poll() is None:
            proc.kill()
    stderr = proc.stderr.read().decode() if proc.stderr else ""
    assert code == 0, stderr
    job = (await client.get(f"/jobs/{job_id}")).json()
    assert job["status"] == "succeeded"
    assert job["attempt_history"][0]["worker_id"] == "graceful"
    assert (await client.get(f"/jobs/{later.json()['id']}")).json()["status"] == "queued"


async def test_job_still_running_after_grace_is_released(
    client: httpx.AsyncClient,
    app: FastAPI,
    make_worker: Callable[..., Worker],
    test_settings: Settings,
    counter: ExecutionCounter,
) -> None:
    impatient = make_worker(
        "impatient",
        settings=test_settings.model_copy(update={"worker_shutdown_grace_seconds": 0.3}),
    )
    stop, task = await run_in_background(impatient)
    job_id = (
        await client.post("/jobs", json={"type": "count", "payload": {"seconds": 1.5}})
    ).json()["id"]
    await wait_for_status(client, job_id, {"running"})
    stop.set()
    await asyncio.wait_for(task, timeout=3)

    released = (await client.get(f"/jobs/{job_id}")).json()
    assert released["status"] == "queued"
    assert released["attempts"] == 0  # a shutdown isn't the job's fault
    (attempt,) = released["attempt_history"]
    assert attempt["status"] == "released"

    # It was re-sent on release, so the next worker picks it up without the sweeper.
    successor = make_worker("successor")
    stop2, task2 = await run_in_background(successor)
    job = await wait_for_status(client, job_id, {"succeeded"})
    assert [a["worker_id"] for a in job["attempt_history"]] == ["impatient", "successor"]
    assert counter.calls == [uuid.UUID(job_id)] * 2
    stop2.set()
    await task2
