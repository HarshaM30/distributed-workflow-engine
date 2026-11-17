"""A smaller version of `benchmarks/run_chaos.py`: same harness, fewer jobs and kills.

Needs Docker; builds the image and runs a throwaway `wfe-bench` compose project, which is
removed afterwards. Run with `make test-chaos` (never part of the push CI).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from benchmarks.harness import Stack
from benchmarks.run_chaos import CHAOS_STACK, ChaosConfig, run_chaos

WORKERS = 3


@pytest.fixture(scope="module")
def stack() -> Iterator[Stack]:
    s = Stack(CHAOS_STACK)
    s.build()
    try:
        s.up(WORKERS)
        yield s
    finally:
        s.down()


def test_invariants_hold_under_worker_kills_and_a_redis_restart(stack: Stack) -> None:
    report = run_chaos(
        stack,
        ChaosConfig(
            workers=WORKERS,
            jobs=150,
            submit_seconds=25,
            kill_every_seconds=2.5,
            redis_restarts=1,
            seed=11,
        ),
    )
    assert report["drained"], report
    failed = [name for name, ok in report["invariants"].items() if not ok]
    assert not failed, (failed, report)
    assert report["lost"] == 0
    assert report["statuses"].get("succeeded", 0) + report["statuses"].get("dead", 0) == 150
    assert report["kills"] >= 3
    assert len(report["redis_restarts"]) == 1

    # At-least-once: kills can cause re-executions, never missing ones.
    dup = report["duplicates"]
    assert dup["jobs_executed"] == dup["ledger_jobs"]
    assert dup["executions_minus_jobs"] >= 0
    # Every cut-short execution was run again. (A kill between the ledger write and the
    # result commit re-runs a *finished* execution too, so this is <=, not ==.)
    assert dup["interrupted_executions"] <= dup["executions_minus_jobs"]

    # The reaper takes an orphaned lease back within lease + one reaper interval (+ slack).
    reaped = report["recovery"]["reaped_seconds"]
    if reaped:
        assert reaped["max"] <= report["recovery"]["bound_seconds"] + 1.5


def test_stack_is_isolated_from_development_ports(stack: Stack) -> None:
    # The chaos stack must never point at the dev Postgres (5433) or Redis (6379).
    assert stack.config.postgres_port != 5433
    assert stack.config.redis_port != 6379
