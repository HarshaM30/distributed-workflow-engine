"""Chaos run: kill workers and restart Redis under load, then check the invariants.

While jobs are submitted through the API, random worker containers get `docker kill`
(SIGKILL, no drain) and are started again a moment later, and Redis is restarted (it runs
without persistence, so every stream, consumer group, and pending entry is lost). Afterwards:

- every submitted job is terminal and none is lost or invented;
- succeeded + dead == submitted;
- each job has exactly one terminal outcome (never two successful attempts);
- jobs designed to fail (`flaky`, p=1) end `dead`.

Duplicate executions are *measured* with the `ledger` handler's side table
(executions - jobs), and recovery time is the gap between a kill and the orphaned job's next
claim. Exit status is non-zero if any invariant fails.

    uv run python -m benchmarks.run_chaos
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from benchmarks.harness import (
    RAW_DIR,
    Stack,
    StackConfig,
    docker,
    log,
    machine_specs,
    percentiles,
    wait_terminal,
    write_json,
)

# Short leases and fast maintenance loops, so recovery happens in seconds.
CHAOS_STACK = StackConfig(
    lease_seconds=4,
    heartbeat_seconds=1,
    reaper_interval_seconds=1,
    sweeper_interval_seconds=2,
    sweeper_stale_seconds=3,
)


@dataclass(frozen=True)
class ChaosConfig:
    workers: int = 4
    # About half the pool's capacity, so "claimed again" measures recovery, not queueing
    # behind a backlog (an overloaded pool puts a requeued job at the back of the line).
    jobs: int = 400
    submit_seconds: float = 60
    kill_every_seconds: float = 4  # mean gap between worker kills
    restart_after_seconds: float = 2  # a killed container comes back after this long
    redis_restarts: int = 1
    ledger_min_seconds: float = 0.1
    ledger_max_seconds: float = 0.5
    flaky_share: float = 0.10  # flaky p=0.3, plenty of attempts: should succeed
    doomed_share: float = 0.10  # flaky p=1.0, two attempts: must end dead
    drain_timeout_seconds: float = 240
    seed: int = 7


def _job_body(kind: str, rng: random.Random, cfg: ChaosConfig) -> dict[str, Any]:
    if kind == "ledger":
        seconds = round(rng.uniform(cfg.ledger_min_seconds, cfg.ledger_max_seconds), 3)
        return {"type": "ledger", "payload": {"seconds": seconds}, "max_attempts": 8}
    if kind == "flaky":
        return {
            "type": "flaky",
            "payload": {"failure_rate": 0.3, "seconds": 0.2},
            "max_attempts": 10,
        }
    return {"type": "flaky", "payload": {"failure_rate": 1.0}, "max_attempts": 2}


async def _submit_all(
    stack: Stack, cfg: ChaosConfig, rng: random.Random
) -> tuple[dict[uuid.UUID, str], int]:
    """POST every job, retrying transient failures under the same Idempotency-Key."""
    kinds = []
    for _ in range(cfg.jobs):
        roll = rng.random()
        kinds.append(
            "doomed"
            if roll < cfg.doomed_share
            else "flaky"
            if roll < cfg.doomed_share + cfg.flaky_share
            else "ledger"
        )
    bodies = [_job_body(kind, rng, cfg) for kind in kinds]
    submitted: dict[uuid.UUID, str] = {}
    retries = 0
    async with httpx.AsyncClient(
        base_url=stack.api_url, headers={"X-API-Key": stack.config.api_key}, timeout=10
    ) as client:

        async def submit(kind: str, body: dict[str, Any]) -> None:
            nonlocal retries
            key = str(uuid.uuid4())
            for attempt in range(20):
                try:
                    resp = await client.post("/jobs", json=body, headers={"Idempotency-Key": key})
                    if resp.status_code in (200, 201):
                        submitted[uuid.UUID(resp.json()["id"])] = kind
                        return
                except httpx.HTTPError:
                    pass
                retries += 1
                await asyncio.sleep(min(2.0, 0.1 * 2**attempt))
            raise RuntimeError("could not submit a job after 20 tries")

        loop = asyncio.get_running_loop()
        t0 = loop.time()
        tasks = []
        for i, (kind, body) in enumerate(zip(kinds, bodies, strict=True)):
            delay = t0 + i * cfg.submit_seconds / cfg.jobs - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(submit(kind, body)))
        await asyncio.gather(*tasks)
    return submitted, retries


def _db_now(stack: Stack) -> datetime:
    with stack.pg() as conn:
        row = conn.execute("SELECT clock_timestamp()").fetchone()
    assert row is not None
    now: datetime = row[0]
    return now


async def _chaos(
    stack: Stack, cfg: ChaosConfig, rng: random.Random, stop: asyncio.Event
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kills: list[dict[str, Any]] = []
    redis_events: list[dict[str, Any]] = []
    restarts: list[asyncio.Task[None]] = []
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    # Redis restarts spread evenly through the submission window.
    redis_due = [
        t0 + cfg.submit_seconds * (i + 1) / (cfg.redis_restarts + 1)
        for i in range(cfg.redis_restarts)
    ]

    async def restart_later(container: str) -> None:
        await asyncio.sleep(cfg.restart_after_seconds)
        await asyncio.to_thread(docker, "start", container)

    while not stop.is_set():
        gap = rng.uniform(0.5, 1.5) * cfg.kill_every_seconds
        try:
            await asyncio.wait_for(stop.wait(), timeout=gap)
            break
        except TimeoutError:
            pass
        if redis_due and loop.time() >= redis_due[0]:
            redis_due.pop(0)
            started = await asyncio.to_thread(_db_now, stack)
            t = time.monotonic()
            await asyncio.to_thread(stack.restart_redis)
            redis_events.append({"at": started, "restart_seconds": round(time.monotonic() - t, 2)})
            log(f"restarted redis ({redis_events[-1]['restart_seconds']}s)")
            continue
        running = await asyncio.to_thread(stack.worker_containers)
        if not running:
            continue
        container = rng.choice(running)
        await asyncio.to_thread(docker, "kill", container)
        kill_at = await asyncio.to_thread(_db_now, stack)
        kills.append(
            {"container": container[:12], "worker_id": f"{container[:12]}-1", "at": kill_at}
        )
        log(f"killed worker {container[:12]}")
        restarts.append(asyncio.create_task(restart_later(container)))
    await asyncio.gather(*restarts)
    return kills, redis_events


def _recovery_times(stack: Stack, kills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """For each attempt orphaned by a kill: seconds from the kill until the reaper took the
    lease back (`reaped`), and until another worker claimed the job (`reclaimed`). The second
    also includes any wait for a free worker."""
    out = []
    with stack.pg() as conn:
        for wid in {k["worker_id"] for k in kills}:
            kill_times = sorted(k["at"] for k in kills if k["worker_id"] == wid)
            rows = conn.execute(
                """
                SELECT a.job_id, a.started_at, a.finished_at,
                       (SELECT min(b.started_at) FROM job_attempts b
                        WHERE b.job_id = a.job_id AND b.started_at > a.started_at)
                FROM job_attempts a
                WHERE a.worker_id = %s AND a.status = 'lease_expired'
                """,
                (wid,),
            ).fetchall()
            for job_id, started_at, reaped_at, next_start in rows:
                # The kill that orphaned it: the first kill of this container after it started.
                kill_at = next((t for t in kill_times if t >= started_at), None)
                if kill_at is None or next_start is None:
                    continue
                out.append(
                    {
                        "job_id": str(job_id),
                        "worker_id": wid,
                        "reaped_seconds": round((reaped_at - kill_at).total_seconds(), 3),
                        "recovery_seconds": round((next_start - kill_at).total_seconds(), 3),
                    }
                )
    return out


def analyse(
    stack: Stack,
    submitted: dict[uuid.UUID, str],
    kills: list[dict[str, Any]],
) -> dict[str, Any]:
    with stack.pg() as conn:
        rows = {r[0]: r[1] for r in conn.execute("SELECT id, status::text FROM jobs").fetchall()}
        multi_success = conn.execute(
            "SELECT count(*) FROM (SELECT job_id FROM job_attempts WHERE status = 'succeeded' "
            "GROUP BY job_id HAVING count(*) > 1) s"
        ).fetchone()
        inconsistent = conn.execute(
            """
            SELECT count(*) FROM jobs j
            WHERE (j.status = 'succeeded' AND (SELECT count(*) FROM job_attempts a
                    WHERE a.job_id = j.id AND a.status = 'succeeded') <> 1)
               OR (j.status = 'dead' AND (EXISTS (SELECT 1 FROM job_attempts a
                    WHERE a.job_id = j.id AND a.status = 'succeeded')
                    OR NOT EXISTS (SELECT 1 FROM dead_letters d WHERE d.job_id = j.id)))
            """
        ).fetchone()
        ledger = conn.execute(
            "SELECT count(*), count(DISTINCT job_id), count(*) FILTER (WHERE finished_at IS NULL)"
            " FROM job_executions"
        ).fetchone()
        reasons = dict(
            conn.execute("SELECT reason, count(*) FROM dead_letters GROUP BY reason").fetchall()
        )
        expired = conn.execute(
            "SELECT count(*) FROM job_attempts WHERE status = 'lease_expired'"
        ).fetchone()
    assert multi_success is not None
    assert inconsistent is not None
    assert ledger is not None
    assert expired is not None

    statuses: dict[str, int] = {}
    for status in rows.values():
        statuses[status] = statuses.get(status, 0) + 1
    by_kind: dict[str, dict[str, int]] = {}
    for job_id, kind in submitted.items():
        status = rows.get(job_id, "missing")
        by_kind.setdefault(kind, {}).setdefault(status, 0)
        by_kind[kind][status] += 1
    lost = [str(j) for j in submitted if rows.get(j) not in ("succeeded", "dead", "cancelled")]
    phantom = len(set(rows) - set(submitted))
    ledger_jobs = sum(1 for k in submitted.values() if k == "ledger")
    executions, executed_jobs, interrupted = ledger
    recovery = _recovery_times(stack, kills)
    recovery_s = [r["recovery_seconds"] for r in recovery]
    reaped_s = [r["reaped_seconds"] for r in recovery]

    succeeded, dead = statuses.get("succeeded", 0), statuses.get("dead", 0)
    invariants = {
        "no_lost_jobs": not lost,
        "every_job_terminal": all(s in ("succeeded", "dead", "cancelled") for s in rows.values()),
        "no_phantom_jobs": phantom == 0,
        "succeeded_plus_dead_equals_submitted": succeeded + dead == len(submitted),
        "one_terminal_outcome_per_job": multi_success[0] == 0 and inconsistent[0] == 0,
        "doomed_jobs_dead": by_kind.get("doomed", {}).get("dead", 0)
        == sum(by_kind.get("doomed", {}).values()),
    }
    return {
        "submitted": len(submitted),
        "statuses": statuses,
        "by_kind": by_kind,
        "lost": len(lost),
        "lost_ids": lost[:20],
        "phantom": phantom,
        "dead_letter_reasons": reasons,
        "lease_expired_attempts": expired[0],
        "duplicates": {
            "ledger_jobs": ledger_jobs,
            "executions": executions,
            "jobs_executed": executed_jobs,
            "executions_minus_jobs": executions - ledger_jobs,
            "interrupted_executions": interrupted,
        },
        "recovery": {
            "orphaned_jobs": len(recovery),
            "seconds": {
                **{k: round(v, 3) for k, v in percentiles(recovery_s).items()},
                **(
                    {
                        "min": min(recovery_s),
                        "max": max(recovery_s),
                        "mean": round(statistics.mean(recovery_s), 3),
                    }
                    if recovery_s
                    else {}
                ),
            },
            "reaped_seconds": {
                **{k: round(v, 3) for k, v in percentiles(reaped_s).items()},
                **({"max": max(reaped_s)} if reaped_s else {}),
            },
            # Lease (measured from the last heartbeat, at most one interval before the kill)
            # plus one reaper interval.
            "bound_seconds": stack.config.lease_seconds + stack.config.reaper_interval_seconds,
        },
        "invariants": invariants,
        "passed": all(invariants.values()),
    }


async def _run(stack: Stack, cfg: ChaosConfig) -> dict[str, Any]:
    rng = random.Random(cfg.seed)  # noqa: S311
    stop = asyncio.Event()
    chaos_task = asyncio.create_task(_chaos(stack, cfg, random.Random(cfg.seed + 1), stop))  # noqa: S311
    started = time.monotonic()
    try:
        submitted, retries = await _submit_all(stack, cfg, rng)
    finally:
        stop.set()
    kills, redis_events = await chaos_task
    log(f"submitted {len(submitted)} jobs ({retries} client retries); waiting for the drain")
    drained = True
    try:
        await asyncio.to_thread(wait_terminal, stack, cfg.drain_timeout_seconds, len(submitted))
    except TimeoutError as exc:
        log(f"drain timed out: {exc}")
        drained = False
    report = analyse(stack, submitted, kills)
    report.update(
        {
            "config": asdict(cfg),
            "stack": asdict(stack.config),
            "client_retries": retries,
            "kills": len(kills),
            "redis_restarts": redis_events,
            "drained": drained,
            "total_seconds": round(time.monotonic() - started, 1),
        }
    )
    return report


def run_chaos(stack: Stack, cfg: ChaosConfig) -> dict[str, Any]:
    """Reset the (already running) stack, scale to cfg.workers, and run one chaos round."""
    stack.scale_workers(cfg.workers)
    stack.reset()
    return asyncio.run(_run(stack, cfg))


def main(argv: list[str] | None = None) -> None:
    defaults = ChaosConfig()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workers", type=int, default=defaults.workers)
    parser.add_argument("--jobs", type=int, default=defaults.jobs)
    parser.add_argument("--submit-seconds", type=float, default=defaults.submit_seconds)
    parser.add_argument("--kill-every", type=float, default=defaults.kill_every_seconds)
    parser.add_argument("--redis-restarts", type=int, default=defaults.redis_restarts)
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--out", type=Path, default=RAW_DIR / "chaos.json")
    parser.add_argument("--keep", action="store_true", help="leave the stack running")
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args(argv)
    if args.workers > 8:
        parser.error("worker counts are capped at 8 (laptop-friendly)")
    cfg = ChaosConfig(
        workers=args.workers,
        jobs=args.jobs,
        submit_seconds=args.submit_seconds,
        kill_every_seconds=args.kill_every,
        redis_restarts=args.redis_restarts,
        seed=args.seed,
    )
    stack = Stack(CHAOS_STACK)
    if not args.no_build:
        stack.build()
    try:
        stack.up(cfg.workers)
        report = run_chaos(stack, cfg)
        report["specs"] = machine_specs(stack)
    finally:
        if not args.keep:
            stack.down()
    write_json(args.out, report)
    for name, ok in report["invariants"].items():
        log(f"{'PASS' if ok else 'FAIL'} {name}")
    dup = report["duplicates"]
    rec = report["recovery"]["seconds"]
    log(
        f"kills={report['kills']} duplicates={dup['executions_minus_jobs']} "
        f"recovery p50={rec.get('p50')}s max={rec.get('max')}s"
    )
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
