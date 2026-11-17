"""Scripted demo: submit jobs, SIGKILL the worker running one of them, watch it recover.

`make demo` starts the app stack (API, maintenance, 3 workers) with the short leases in
demo/compose.demo.yml and then runs this script. Everything goes through the public API except
the `docker kill`, which plays the part of a crashed machine.

    uv run python -m demo.run [--jobs 4] [--seconds 3]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from workflow_engine.config import get_settings

PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "workflow-engine")
TERMINAL = frozenset({"succeeded", "dead", "cancelled"})
TICK = 0.25

BOLD, DIM, RESET = "\033[1m", "\033[2m", "\033[0m"
COLORS = {
    "pending": "\033[90m",
    "queued": "\033[33m",
    "running": "\033[36m",
    "succeeded": "\033[32m",
    "dead": "\033[31m",
    "cancelled": "\033[35m",
    "lease_expired": "\033[31m",
}


def paint(text: str, key: str) -> str:
    return f"{COLORS.get(key, '')}{text}{RESET}"


def say(msg: str = "") -> None:
    print(msg, flush=True)


def step(n: int, title: str) -> None:
    say(f"\n{BOLD}[{n}/4] {title}{RESET}")


def docker(*args: str) -> str:
    out = subprocess.run(["docker", *args], check=True, capture_output=True, text=True)  # noqa: S603, S607
    return out.stdout


def container_of(worker_id: str) -> str:
    """Worker ids are `<hostname>-<pid>`, and a container's hostname is its short id."""
    return worker_id.rsplit("-", 1)[0][:12]


def running_workers() -> dict[str, str]:
    """Running worker containers: short container id -> display name (`worker-1`)."""
    out = docker(
        "ps",
        "--filter",
        f"label=com.docker.compose.project={PROJECT}",
        "--filter",
        "label=com.docker.compose.service=worker",
        "--format",
        "{{.ID}} {{.Names}}",
    )
    found = {}
    for line in out.splitlines():
        cid, name = line.split()
        found[cid[:12]] = name.removeprefix(f"{PROJECT}-")
    return found


@dataclass
class Board:
    """A job table redrawn in place."""

    names: dict[str, str]
    victim: str | None = None
    victim_attempt: int = 0
    lines: int = 0

    def worker_name(self, worker_id: str) -> str:
        return self.names.get(container_of(worker_id), worker_id[:14])

    def note(self, job: dict[str, Any]) -> str:
        history = job["attempt_history"]
        if job["id"] != self.victim:
            return ""
        if len(history) == self.victim_attempt and job["status"] == "running":
            return paint("its worker is dead; the lease runs out in <= 5 s", "dead")
        if job["status"] == "queued":
            return paint("lease reaped -> back to queued", "queued")
        return f"recovered by {self.worker_name(history[-1]['worker_id'])}"

    def draw(self, jobs: list[dict[str, Any]]) -> None:
        if self.lines:
            sys.stdout.write(f"\033[{self.lines}F\033[J")
        rows = [f"  {DIM}{'job':<10}{'status':<12}{'attempt':<9}{'worker':<11}note{RESET}"]
        for job in jobs:
            history = job["attempt_history"]
            worker = self.worker_name(history[-1]["worker_id"]) if history else "-"
            status = paint(f"{job['status']:<12}", job["status"])
            attempt = str(len(history)) if history else "-"
            rows.append(f"  {job['id'][:8]:<10}{status}{attempt:<9}{worker:<11}{self.note(job)}")
        sys.stdout.write("\n".join(rows) + "\n")
        sys.stdout.flush()
        self.lines = len(rows)


def wait_healthy(client: httpx.Client, timeout: float = 60) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        try:
            resp = client.get("/health")
            if resp.status_code == 200:
                body: dict[str, Any] = resp.json()
                return body
        except httpx.TransportError:
            pass
        if time.monotonic() > deadline:
            raise SystemExit("API did not become healthy; is the stack up? (`make demo`)")
        time.sleep(0.5)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--seconds", type=float, default=3.0, help="duration of each sleep job")
    parser.add_argument("--kill-after", type=float, default=1.0, help="seconds into the victim job")
    args = parser.parse_args(argv)

    settings = get_settings()
    api_key = os.environ.get("DEMO_API_KEY") or next(iter(settings.api_key_values), "")
    if not api_key:
        raise SystemExit("API_KEYS is empty; copy .env.example to .env first")
    client = httpx.Client(
        base_url=os.environ.get("DEMO_API_URL", f"http://localhost:{settings.api_port}"),
        headers={"X-API-Key": api_key},
        timeout=10,
    )

    health = wait_healthy(client)
    names = running_workers()
    step(1, f"Stack: API, maintenance, {len(names)} workers (lease 5 s, heartbeat 1 s)")
    parts = ", ".join(f"{k} {v['status']}" for k, v in health["components"].items())
    say(f"  GET /health -> 200 ({parts})")
    say(f"  workers: {', '.join(sorted(names.values()))}")

    step(2, f"Submit {args.jobs} sleep({args.seconds:g}s) jobs, each with an Idempotency-Key")
    ids: list[str] = []
    run = uuid.uuid4().hex[:6]
    body = {"type": "sleep", "payload": {"seconds": args.seconds}}
    for i in range(args.jobs):
        resp = client.post("/jobs", json=body, headers={"Idempotency-Key": f"demo-{run}-{i}"})
        resp.raise_for_status()
        ids.append(resp.json()["id"])
        say(f"  POST /jobs -> {resp.status_code}  {ids[-1][:8]}")
    replay = client.post("/jobs", json=body, headers={"Idempotency-Key": f"demo-{run}-0"})
    same = "same job, no duplicate" if replay.json()["id"] == ids[0] else "DIFFERENT JOB"
    say(f"  POST /jobs, key reused -> {replay.status_code}  {ids[0][:8]}  {DIM}{same}{RESET}")

    step(3, "Workers claim jobs; SIGKILL the worker running one of them")
    board = Board(names=dict(names))
    seen_running: dict[str, float] = {}
    killed: str | None = None
    killed_wall: datetime | None = None
    jobs: list[dict[str, Any]] = []
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        jobs = [client.get(f"/jobs/{i}").json() for i in ids]
        now = time.monotonic()
        for job in jobs:
            if job["status"] == "running":
                seen_running.setdefault(job["id"], now)
        if killed is None:
            for job in jobs:
                started = seen_running.get(job["id"])
                if job["status"] == "running" and started and now - started >= args.kill_after:
                    killed = container_of(job["attempt_history"][-1]["worker_id"])
                    board.draw(jobs)  # the last view before the kill stays on screen
                    docker("kill", "--signal", "KILL", killed)
                    killed_wall = datetime.now(UTC)
                    board.victim, board.victim_attempt = job["id"], len(job["attempt_history"])
                    say(
                        f"  {paint('SIGKILL', 'dead')} -> {board.names[killed]} "
                        f"while it runs {job['id'][:8]} (no shutdown, no cleanup)\n"
                    )
                    board.lines = 0
                    break
        board.draw(jobs)
        if killed and all(j["status"] in TERMINAL for j in jobs):
            break
        time.sleep(TICK)

    step(4, "Result")
    if killed is None or killed_wall is None or board.victim is None:
        say("  no job was caught running; try a longer --seconds")
        return 1
    final = next(j for j in jobs if j["id"] == board.victim)
    history = final["attempt_history"]
    say(f"  attempt history of {board.victim[:8]}:")
    for a in history:
        say(
            f"    #{a['attempt_no']}  {board.worker_name(a['worker_id']):<10}"
            f"lease token {a['lease_token']}  {paint(a['status'], a['status'])}"
        )
    if len(history) > 1:
        reclaimed = datetime.fromisoformat(history[-1]["started_at"])
        gap = (reclaimed - killed_wall).total_seconds()
        say(f"  kill -> claimed again: {gap:.1f} s (lease expiry + reaper pass)")
    succeeded = sum(j["status"] == "succeeded" for j in jobs)
    saved = sum(a["status"] == "succeeded" for a in history)
    say(f"  {succeeded}/{len(jobs)} jobs succeeded; the killed job's result was saved {saved}x")
    say(f"  {DIM}at-least-once execution, with each job's result saved exactly once{RESET}")

    docker("start", killed)
    say(f"\n  {DIM}{board.names[killed]} started again; `make down` stops the stack{RESET}")
    return 0 if succeeded == len(jobs) and saved == 1 else 1


if __name__ == "__main__":
    sys.exit(main())
