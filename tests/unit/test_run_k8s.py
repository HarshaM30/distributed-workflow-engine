"""The kind load/chaos script's invariant checks and recovery timings."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from benchmarks.run_k8s import api_key_from_secrets, recovery_seconds, summarize

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def at(seconds: float) -> str:
    return (T0 + timedelta(seconds=seconds)).isoformat()


def attempt(worker: str, status: str, start: float, end: float | None) -> dict[str, Any]:
    return {
        "worker_id": worker,
        "status": status,
        "started_at": at(start),
        "finished_at": at(end) if end is not None else None,
    }


def job(
    job_id: str, status: str, created: float, finished: float | None, *attempts: Any
) -> dict[str, Any]:
    return {
        "id": job_id,
        "status": status,
        "created_at": at(created),
        "finished_at": at(finished) if finished is not None else None,
        "attempt_history": list(attempts),
    }


def test_summary_of_a_clean_run() -> None:
    jobs = [
        job(str(i), "succeeded", 0, 2 + i, attempt("w-1", "succeeded", 1, 2 + i)) for i in range(4)
    ]
    s = summarize(jobs, expected=4)
    assert s.failures == []
    assert s.drain_seconds == 5.0
    assert s.throughput_jobs_per_s == pytest.approx(4 / 5)
    assert s.e2e_seconds["p50"] == 3.0


def test_summary_flags_lost_unfinished_and_double_success() -> None:
    jobs = [
        job("a", "running", 0, None, attempt("w-1", "running", 1, None)),
        job(
            "b",
            "succeeded",
            0,
            3,
            attempt("w-1", "succeeded", 1, 2),
            attempt("w-2", "succeeded", 2, 3),
        ),
    ]
    failures = " | ".join(summarize(jobs, expected=3).failures)
    assert "found 2" in failures
    assert "1 jobs are not terminal" in failures
    assert "b has 2 succeeded attempts" in failures


def test_recovery_measures_reap_and_reclaim_for_killed_pods_only() -> None:
    kills = {"worker-abc": T0 + timedelta(seconds=10)}
    jobs = [
        job(
            "orphan",
            "succeeded",
            0,
            40,
            attempt("worker-abc-1", "lease_expired", 5, 26),
            attempt("worker-def-1", "succeeded", 30, 40),
        ),
        # Interrupted on a pod that wasn't deleted by the run: not counted.
        job(
            "other",
            "succeeded",
            0,
            40,
            attempt("worker-xyz-1", "lease_expired", 5, 26),
            attempt("worker-def-1", "succeeded", 30, 40),
        ),
    ]
    assert recovery_seconds(jobs, kills) == {"reaped": [16.0], "reclaimed": [20.0]}


def test_api_key_comes_from_env_or_secrets_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text("# comment\nPOSTGRES_PASSWORD=x\nAPI_KEYS= first , second\n")
    monkeypatch.delenv("K8S_API_KEY", raising=False)
    assert api_key_from_secrets(secrets) == "first"
    monkeypatch.setenv("K8S_API_KEY", "from-env")
    assert api_key_from_secrets(secrets) == "from-env"
