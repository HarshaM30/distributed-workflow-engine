"""Shared harness for the throughput benchmark, the chaos run, and tests/chaos.

It drives a dedicated compose project (`wfe-bench`, see benchmarks/compose.bench.yml) with its
own Postgres, Redis, API, maintenance, and N worker containers. Each worker is one process
running one job at a time (WORKER_CONCURRENCY=1), the benchmark unit from PLAN.md.

The stack is throwaway: it publishes on localhost only, uses a fixed local password, and
`Stack.down()` removes its containers and volumes.
"""

from __future__ import annotations

import json
import math
import os
import platform
import subprocess
import time
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import psycopg
import redis

from workflow_engine.core.states import PRIORITY_ORDER, Priority
from workflow_engine.dispatch.streams import stream_name

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "benchmarks" / "raw"
PROJECT = "wfe-bench"
COMPOSE_FILES = ("docker-compose.yml", "benchmarks/compose.bench.yml")
STREAM_PREFIX = "jobs"
GROUP = "workers"
TERMINAL = ("succeeded", "dead", "cancelled")
TABLES = "jobs, job_attempts, dead_letters, idempotency_keys, embeddings, job_executions"


@dataclass(frozen=True)
class StackConfig:
    postgres_port: int = 5434
    redis_port: int = 6380
    api_port: int = 8001
    # Throwaway credentials for a localhost-only stack that is deleted after each run.
    password: str = "bench-only-password"  # noqa: S105
    api_key: str = "bench-key"
    log_level: str = "WARNING"
    lease_seconds: float = 30
    heartbeat_seconds: float = 10
    reaper_interval_seconds: float = 5
    sweeper_interval_seconds: float = 15
    sweeper_stale_seconds: float = 30


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Stack:
    def __init__(self, config: StackConfig | None = None) -> None:
        self.config = config or StackConfig()

    # ------------------------------------------------------------------ docker compose

    def env(self) -> dict[str, str]:
        c = self.config
        return {
            **os.environ,
            "POSTGRES_PASSWORD": c.password,
            "POSTGRES_PORT": str(c.postgres_port),
            "REDIS_PORT": str(c.redis_port),
            "API_PORT": str(c.api_port),
            "BENCH_API_KEY": c.api_key,
            "BENCH_LOG_LEVEL": c.log_level,
            "BENCH_LEASE_SECONDS": str(c.lease_seconds),
            "BENCH_HEARTBEAT_SECONDS": str(c.heartbeat_seconds),
            "BENCH_REAPER_INTERVAL_SECONDS": str(c.reaper_interval_seconds),
            "BENCH_SWEEPER_INTERVAL_SECONDS": str(c.sweeper_interval_seconds),
            "BENCH_SWEEPER_STALE_SECONDS": str(c.sweeper_stale_seconds),
        }

    def compose(self, *args: str, capture: bool = False) -> str:
        cmd = ["docker", "compose", "-p", PROJECT]
        for f in COMPOSE_FILES:
            cmd += ["-f", f]
        cmd += ["--profile", "app", *args]
        result = subprocess.run(  # noqa: S603
            cmd, cwd=ROOT, env=self.env(), check=True, capture_output=capture, text=True
        )
        return result.stdout if capture else ""

    def build(self) -> None:
        log("building image")
        self.compose("build", "-q", "api")

    def up(self, workers: int) -> None:
        log(f"starting stack ({workers} workers)")
        self.compose("up", "-d", "--wait", "postgres", "redis")
        self.compose("run", "--rm", "-T", "migrate")
        self.compose("up", "-d", "--wait", "--no-deps", "api")
        self.compose("up", "-d", "--no-deps", "maintenance")
        self.scale_workers(workers)

    def scale_workers(self, n: int) -> None:
        self.compose("up", "-d", "--no-deps", "--no-recreate", "--scale", f"worker={n}", "worker")
        # A killed-then-started container may be stopped if the last run ended mid-chaos.
        for cid in self.worker_containers(running_only=False):
            if cid not in self.worker_containers():
                docker("start", cid)
        self.wait_workers_ready(n)

    def worker_containers(self, *, running_only: bool = True) -> list[str]:
        args = ["ps", "-q", "--no-trunc"]
        if not running_only:
            args.append("-a")
        args += [
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--filter",
            "label=com.docker.compose.service=worker",
        ]
        return sorted(docker(*args).split())

    def restart_redis(self) -> None:
        self.compose("restart", "redis")

    def down(self) -> None:
        log("removing stack (containers + volumes)")
        self.compose("down", "-v", "--remove-orphans")

    # ------------------------------------------------------------------ clients

    def pg(self) -> psycopg.Connection[Any]:
        c = self.config
        return psycopg.connect(
            host="localhost",
            port=c.postgres_port,
            user="workflow",
            password=c.password,
            dbname="workflow",
            autocommit=True,
        )

    def redis(self) -> redis.Redis:
        return redis.Redis(port=self.config.redis_port, decode_responses=True)

    @property
    def api_url(self) -> str:
        return f"http://localhost:{self.config.api_port}"

    # ------------------------------------------------------------------ state

    def reset(self) -> None:
        """Empty every table and stream (keeping the consumer groups the workers use)."""
        with self.pg() as conn:
            conn.execute(f"TRUNCATE {TABLES} CASCADE")
        r = self.redis()
        for priority in PRIORITY_ORDER:
            key = stream_name(STREAM_PREFIX, priority)
            if r.exists(key):
                r.xtrim(key, maxlen=0, approximate=False)
        r.close()

    def active_consumers(self, idle_ms: int = 3000) -> int:
        r = self.redis()
        try:
            consumers = r.xinfo_consumers(stream_name(STREAM_PREFIX, Priority.HIGH), GROUP)
        except redis.ResponseError:
            return 0
        finally:
            r.close()
        return sum(1 for c in consumers if int(c["idle"]) < idle_ms)

    def wait_workers_ready(self, n: int, timeout: float = 90) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.worker_containers()) == n and self.active_consumers() >= n:
                return
            time.sleep(0.5)
        raise TimeoutError(f"{n} workers did not become ready within {timeout}s")


def docker(*args: str) -> str:
    result = subprocess.run(  # noqa: S603
        ["docker", *args],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


# --------------------------------------------------------------------------- jobs


def insert_queued(
    stack: Stack,
    n: int,
    job_type: str,
    payload: dict[str, Any],
    *,
    max_attempts: int = 5,
) -> list[uuid.UUID]:
    """Bulk-create `n` queued jobs, then XADD a hint for each (commit first, as the API does).

    COPY plus one pipelined XADD batch puts the whole backlog in place in well under a second,
    so a drain measurement is bounded by the workers, not by the submitter.
    """
    ids = [uuid.uuid4() for _ in range(n)]
    body = json.dumps(payload)
    with stack.pg() as conn, conn.cursor() as cur:
        copy_sql = "COPY jobs (id, type, payload, status, priority, max_attempts) FROM STDIN"
        with cur.copy(copy_sql) as copy:
            for job_id in ids:
                copy.write_row((job_id, job_type, body, "queued", "normal", max_attempts))
    r = stack.redis()
    stream = stream_name(STREAM_PREFIX, Priority.NORMAL)
    for start in range(0, n, 1000):
        pipe = r.pipeline(transaction=False)
        for job_id in ids[start : start + 1000]:
            pipe.xadd(stream, {"job_id": str(job_id)})
        pipe.execute()
    r.close()
    return ids


def wait_terminal(stack: Stack, timeout: float, expected: int | None = None) -> float:
    """Block until no job is pending/queued/running. Returns the seconds waited."""
    started = time.monotonic()
    with stack.pg() as conn:
        while True:
            row = conn.execute(
                "SELECT count(*) FILTER (WHERE status NOT IN ('succeeded', 'dead', 'cancelled')),"
                " count(*) FROM jobs"
            ).fetchone()
            assert row is not None
            active, total = row
            if active == 0 and (expected is None or total >= expected):
                return time.monotonic() - started
            if time.monotonic() - started > timeout:
                raise TimeoutError(f"{active} of {total} jobs still active after {timeout}s")
            time.sleep(0.2)


def percentiles(
    values: Sequence[float], points: Iterable[float] = (0.5, 0.95, 0.99)
) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)
    out = {}
    for p in points:
        rank = max(1, math.ceil(p * len(ordered)))  # nearest-rank
        out[f"p{round(p * 100)}"] = ordered[rank - 1]
    return out


def e2e_latencies(stack: Stack, ids: Sequence[uuid.UUID] | None = None) -> list[float]:
    """`finished_at - created_at` per finished job, in seconds (both Postgres timestamps)."""
    sql = (
        "SELECT extract(epoch FROM finished_at - created_at)::float8 FROM jobs "
        "WHERE finished_at IS NOT NULL"
    )
    with stack.pg() as conn:
        if ids is None:
            rows = conn.execute(sql).fetchall()
        else:
            rows = conn.execute(sql + " AND id = ANY(%s)", (list(ids),)).fetchall()
    return [float(r[0]) for r in rows]


# --------------------------------------------------------------------------- environment


def _run(*cmd: str) -> str:
    try:
        return subprocess.run(  # noqa: S603
            cmd, check=True, capture_output=True, text=True, timeout=10
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def machine_specs(stack: Stack) -> dict[str, Any]:
    """Host and Docker details for the results file."""
    system = platform.system()
    if system == "Darwin":
        chip = _run("sysctl", "-n", "machdep.cpu.brand_string")
        mem_bytes = int(_run("sysctl", "-n", "hw.memsize") or 0)
        os_name = f"macOS {_run('sw_vers', '-productVersion')}"
    else:
        chip = ""
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    chip = line.split(":", 1)[1].strip()
                    break
        except OSError:
            pass
        mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        os_name = platform.platform()
    info = json.loads(_run("docker", "info", "--format", "{{json .}}") or "{}")
    config = json.loads(stack.compose("config", "--format", "json", capture=True))
    limits = {
        name: svc.get("mem_limit")
        for name, svc in config.get("services", {}).items()
        if name in {"postgres", "redis", "api", "worker", "maintenance"}
    }
    return {
        "chip": chip or platform.processor(),
        "cpu_count": os.cpu_count(),
        "ram_gb": round(mem_bytes / 2**30, 1),
        "os": os_name,
        "python": platform.python_version(),
        "docker": {
            "server_version": info.get("ServerVersion"),
            "os": info.get("OperatingSystem"),
            "cpus": info.get("NCPU"),
            "memory_gb": round(int(info.get("MemTotal") or 0) / 2**30, 1),
        },
        "container_mem_limits_bytes": limits,
    }


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str) + "\n")
    shown = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
    log(f"wrote {shown}")


def as_dict(obj: Any) -> dict[str, Any]:
    return asdict(obj)
