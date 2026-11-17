from __future__ import annotations

import itertools
from datetime import datetime

import httpx
from fastapi import FastAPI

from tests.integration.conftest import ExecutionCounter, wait_for_status
from workflow_engine.config import Settings
from workflow_engine.core.backoff import backoff_ceiling
from workflow_engine.maintenance.main import Maintenance
from workflow_engine.worker.runner import Worker

# Scheduler tick + stream read + claim, on top of the backoff delay itself.
SCHEDULING_SLACK_SECONDS = 0.6


async def test_flaky_job_retries_with_backoff_then_dead_letters(
    client: httpx.AsyncClient,
    running_worker: Worker,
    running_maintenance: Maintenance,
    test_settings: Settings,
) -> None:
    resp = await client.post(
        "/jobs",
        json={"type": "flaky", "payload": {"failure_rate": 1.0}, "max_attempts": 3},
    )
    job_id = resp.json()["id"]
    job = await wait_for_status(client, job_id, {"dead"}, within=15)

    assert job["attempts"] == 3
    history = job["attempt_history"]
    assert [a["attempt_no"] for a in history] == [1, 2, 3]
    assert all(a["status"] == "failed" for a in history)
    assert all("injected failure" in a["error"] for a in history)

    # Gap n runs from attempt n finishing to attempt n+1 starting. Each one is a full-jitter
    # draw from a window whose ceiling doubles, so the windows grow and the gaps vary.
    base, cap = test_settings.retry_base_delay_seconds, test_settings.retry_max_delay_seconds
    for n, (prev, nxt) in enumerate(itertools.pairwise(history), start=1):
        gap = (
            datetime.fromisoformat(nxt["started_at"]) - datetime.fromisoformat(prev["finished_at"])
        ).total_seconds()
        assert 0 <= gap <= backoff_ceiling(n, base, cap) + SCHEDULING_SLACK_SECONDS
    assert backoff_ceiling(2, base, cap) == 2 * backoff_ceiling(1, base, cap)

    dlq = (await client.get("/dlq")).json()
    (entry,) = dlq["items"]
    assert entry["job_id"] == job_id
    assert entry["job_type"] == "flaky"
    assert entry["reason"] == "failed"
    assert entry["requeued_at"] is None


async def test_retry_then_success(
    client: httpx.AsyncClient,
    running_worker: Worker,
    running_maintenance: Maintenance,
    counter: ExecutionCounter,
) -> None:
    resp = await client.post(
        "/jobs", json={"type": "count", "payload": {"fail_times": 2}, "max_attempts": 5}
    )
    job = await wait_for_status(client, resp.json()["id"], {"succeeded", "dead"}, within=15)
    assert job["status"] == "succeeded"
    assert [a["status"] for a in job["attempt_history"]] == ["failed", "failed", "succeeded"]
    assert job["last_error"] is None
    assert (await client.get("/dlq")).json()["items"] == []


async def test_requeued_dlq_job_runs_again_and_succeeds(
    client: httpx.AsyncClient,
    running_worker: Worker,
    running_maintenance: Maintenance,
    counter: ExecutionCounter,
) -> None:
    resp = await client.post(
        "/jobs", json={"type": "count", "payload": {"fail_times": 1}, "max_attempts": 1}
    )
    job_id = resp.json()["id"]
    await wait_for_status(client, job_id, {"dead"})
    (entry,) = (await client.get("/dlq")).json()["items"]

    requeued = await client.post(f"/dlq/{entry['id']}/requeue")
    assert requeued.status_code == 200, requeued.text
    assert requeued.json()["status"] == "queued"
    assert requeued.json()["attempts"] == 0

    job = await wait_for_status(client, job_id, {"succeeded", "dead"})
    assert job["status"] == "succeeded"
    assert job["attempts"] == 1  # reset by the requeue
    assert [a["status"] for a in job["attempt_history"]] == ["failed", "succeeded"]
    assert len(counter.calls) == 2

    # The entry is hidden by default, visible on request, and can't be requeued twice.
    assert (await client.get("/dlq")).json()["items"] == []
    (old,) = (await client.get("/dlq", params={"include_requeued": True})).json()["items"]
    assert old["requeued_at"] is not None
    assert (await client.post(f"/dlq/{entry['id']}/requeue")).status_code == 409


async def test_requeue_unknown_entry_is_404(client: httpx.AsyncClient) -> None:
    assert (await client.post("/dlq/999999/requeue")).status_code == 404


async def test_unknown_type_is_not_retried(
    client: httpx.AsyncClient, app: FastAPI, running_worker: Worker
) -> None:
    resp = await client.post("/jobs", json={"type": "count", "max_attempts": 5})
    job_id = resp.json()["id"]
    # Simulate a deploy where no worker knows this type any more.
    del running_worker.registry._specs["count"]
    job = await wait_for_status(client, job_id, {"dead"})
    assert job["attempts"] == 1
    (entry,) = (await client.get("/dlq")).json()["items"]
    assert entry["reason"] == "non_retryable"


async def test_dlq_pagination(client: httpx.AsyncClient, running_worker: Worker) -> None:
    ids = []
    for _ in range(5):
        resp = await client.post(
            "/jobs", json={"type": "count", "payload": {"fail": True}, "max_attempts": 1}
        )
        ids.append(resp.json()["id"])
    for job_id in ids:
        await wait_for_status(client, job_id, {"dead"})

    seen: list[str] = []
    cursor = None
    while True:
        params: dict[str, object] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        page = (await client.get("/dlq", params=params)).json()
        seen.extend(item["job_id"] for item in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert sorted(seen) == sorted(ids)
    assert len(seen) == 5
    assert (await client.get("/dlq", params={"cursor": "x"})).status_code == 422
