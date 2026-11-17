"""Throughput and latency at 1, 2, 4, and 8 workers (one worker = one process, one job at a time).

Two measurements per worker count, each repeated `--runs` times (median reported):

- Drain: a backlog of N queued jobs (bulk-inserted, then one XADD each) is worked off.
  Throughput = jobs / (last `finished_at` - first claim). This is the pool's capacity.
  Run for `sleep(0)` jobs (pure engine overhead) and short `cpu` jobs (~10 ms of hashing).
- Steady load: jobs are POSTed through the API at a fixed open-loop rate, and end-to-end
  latency (`finished_at - created_at`, both Postgres timestamps) is reported as p50/p95/p99.

    uv run python -m benchmarks.run_throughput            # full run (what `make bench` does)
    uv run python -m benchmarks.run_throughput --workers 1 2 --runs 1 --sleep-jobs 500 ...
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

from benchmarks.harness import (
    RAW_DIR,
    Stack,
    StackConfig,
    e2e_latencies,
    insert_queued,
    log,
    machine_specs,
    percentiles,
    wait_terminal,
    write_json,
)


def drain(stack: Stack, jobs: int, job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    stack.reset()
    insert_queued(stack, jobs, job_type, payload)
    wait_terminal(stack, timeout=max(120, jobs / 20), expected=jobs)
    with stack.pg() as conn:
        row = conn.execute(
            """
            SELECT count(*) FILTER (WHERE j.status = 'succeeded'),
                   extract(epoch FROM max(j.finished_at)
                           - (SELECT min(started_at) FROM job_attempts))::float8
            FROM jobs j
            """
        ).fetchone()
        service = [
            float(r[0])
            for r in conn.execute(
                "SELECT extract(epoch FROM finished_at - started_at)::float8 FROM job_attempts "
                "WHERE status = 'succeeded'"
            ).fetchall()
        ]
    assert row is not None
    succeeded, window = row
    return {
        "jobs": jobs,
        "succeeded": succeeded,
        "window_seconds": round(window, 4),
        "jobs_per_second": round(jobs / window, 1),
        "service_ms": {k: round(v * 1000, 2) for k, v in percentiles(service).items()},
    }


async def _steady(stack: Stack, rate: float, seconds: float) -> dict[str, Any]:
    total = int(rate * seconds)
    ids: list[uuid.UUID] = []
    request_ms: list[float] = []
    errors = 0
    limits = httpx.Limits(max_connections=64, max_keepalive_connections=64)
    async with httpx.AsyncClient(
        base_url=stack.api_url,
        headers={"X-API-Key": stack.config.api_key},
        limits=limits,
        timeout=30,
    ) as client:

        async def submit() -> None:
            nonlocal errors
            started = time.perf_counter()
            try:
                resp = await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})
            except httpx.HTTPError:
                errors += 1
                return
            request_ms.append((time.perf_counter() - started) * 1000)
            if resp.status_code == 201:
                ids.append(uuid.UUID(resp.json()["id"]))
            else:
                errors += 1

        loop = asyncio.get_running_loop()
        t0 = loop.time()
        tasks = []
        for i in range(total):
            # Open loop: request i goes out at t0 + i/rate, however slow earlier ones were.
            delay = t0 + i / rate - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(submit()))
        await asyncio.gather(*tasks)
        offered_seconds = loop.time() - t0
    await asyncio.to_thread(wait_terminal, stack, 300.0, len(ids))
    e2e = e2e_latencies(stack, ids)
    return {
        "offered_rate": rate,
        "achieved_rate": round(len(ids) / offered_seconds, 1),
        "submitted": len(ids),
        "errors": errors,
        "e2e_ms": {k: round(v * 1000, 2) for k, v in percentiles(e2e).items()},
        "api_request_ms": {k: round(v, 2) for k, v in percentiles(request_ms).items()},
    }


def steady(stack: Stack, rate: float, seconds: float) -> dict[str, Any]:
    stack.reset()
    return asyncio.run(_steady(stack, rate, seconds))


def _median(runs: list[dict[str, Any]]) -> dict[str, Any]:
    """Per-field median across runs (nested dicts too); non-numeric fields from the first run."""
    out: dict[str, Any] = {}
    for key, first in runs[0].items():
        values = [r[key] for r in runs]
        if isinstance(first, dict):
            out[key] = _median(values)
        elif isinstance(first, int | float) and not isinstance(first, bool):
            out[key] = round(statistics.median(values), 2)
        else:
            out[key] = first
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--sleep-jobs", type=int, default=8000)
    parser.add_argument("--cpu-jobs", type=int, default=3000)
    parser.add_argument("--cpu-iterations", type=int, default=60_000)
    parser.add_argument("--latency-rate", type=float, default=200)
    parser.add_argument("--latency-seconds", type=float, default=15)
    parser.add_argument("--out", type=Path, default=RAW_DIR / "throughput.json")
    parser.add_argument("--keep", action="store_true", help="leave the stack running")
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args(argv)
    if max(args.workers) > 8:
        parser.error("worker counts are capped at 8 (laptop-friendly)")

    stack = Stack(StackConfig())
    if not args.no_build:
        stack.build()
    results: dict[str, Any] = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "config": {k: v for k, v in vars(args).items() if k not in {"out", "keep", "no_build"}},
        "by_workers": {},
    }
    try:
        stack.up(min(args.workers))
        results["specs"] = machine_specs(stack)
        for n in sorted(args.workers):
            stack.scale_workers(n)
            entry: dict[str, Any] = {}
            scenarios = {
                "drain_sleep": lambda: drain(stack, args.sleep_jobs, "sleep", {"seconds": 0}),
                "drain_cpu": lambda: drain(
                    stack, args.cpu_jobs, "cpu", {"iterations": args.cpu_iterations}
                ),
                "steady": lambda: steady(stack, args.latency_rate, args.latency_seconds),
            }
            for name, fn in scenarios.items():
                runs = []
                for i in range(args.runs):
                    run = fn()
                    log(f"workers={n} {name} run {i + 1}: {_summary(name, run)}")
                    runs.append(run)
                entry[name] = {"median": _median(runs), "runs": runs}
            results["by_workers"][str(n)] = entry
    finally:
        if not args.keep:
            stack.down()
    write_json(args.out, results)


def _summary(name: str, run: dict[str, Any]) -> str:
    if name.startswith("drain"):
        return f"{run['jobs_per_second']} jobs/s"
    e2e = run["e2e_ms"]
    return f"{run['achieved_rate']}/s offered, e2e p50={e2e.get('p50')}ms p95={e2e.get('p95')}ms"


if __name__ == "__main__":
    main()
