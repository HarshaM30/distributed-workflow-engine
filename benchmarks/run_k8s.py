"""Load and chaos runs against the kind deployment (`make k8s-up`).

Everything goes through the API published by the kind port mapping (127.0.0.1:8800), so this
measures the cluster exactly as a client sees it. Two modes:

    uv run python -m benchmarks.run_k8s load  --jobs 200 --seconds 0.2
    uv run python -m benchmarks.run_k8s chaos --jobs 40 --kills 4 --force

`load` submits a backlog of `sleep` jobs and reports throughput (jobs / (last finished_at -
first created_at)) and end-to-end latency at the current worker replica count. Run it before
and after `make k8s-scale N=4` to compare.

`chaos` submits `sleep` jobs and, while they run, deletes random worker pods (the Deployment
replaces them). By default a deletion is graceful: SIGTERM, then the worker drains. With
`--force` each one is `--grace-period=0 --force`, so the pod dies mid-job and the reaper has
to recover the lease. Afterwards it checks that every job is terminal and succeeded, and
reports interrupted attempts and how long they took to be claimed again. Exit status is
non-zero if an invariant fails.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import random
import statistics
import subprocess
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from benchmarks.harness import log, percentiles

ROOT = Path(__file__).resolve().parents[1]
SECRETS_FILE = ROOT / "deploy" / "k8s" / "secrets.env"
NAMESPACE = "workflow-engine"
WORKER_SELECTOR = "app.kubernetes.io/component=worker"
TERMINAL = frozenset({"succeeded", "dead", "cancelled"})
INTERRUPTED = frozenset({"lease_expired", "released"})


def api_key_from_secrets(path: Path = SECRETS_FILE) -> str:
    """First key in API_KEYS: `K8S_API_KEY` if set, else deploy/k8s/secrets.env."""
    if key := os.environ.get("K8S_API_KEY"):
        return key
    for line in path.read_text().splitlines():
        name, sep, value = line.partition("=")
        if sep and name.strip() == "API_KEYS":
            return value.split(",")[0].strip()
    raise SystemExit(f"API_KEYS not found in {path}; run `make k8s-up` first")


def kubectl(*args: str) -> str:
    out = subprocess.run(  # noqa: S603
        ["kubectl", "-n", NAMESPACE, *args],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout


def worker_pods() -> list[str]:
    """Names of worker pods that are running and not already being deleted."""
    data = json.loads(kubectl("get", "pods", "-l", WORKER_SELECTOR, "-o", "json"))
    return [
        p["metadata"]["name"]
        for p in data["items"]
        if p["status"].get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")
    ]


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


@dataclass
class RunSummary:
    submitted: int
    by_status: dict[str, int]
    throughput_jobs_per_s: float
    e2e_seconds: dict[str, float]
    drain_seconds: float
    attempts_total: int
    interrupted_attempts: int
    jobs_with_retries: int
    failures: list[str] = field(default_factory=list)


def summarize(jobs: Sequence[dict[str, Any]], expected: int) -> RunSummary:
    """Invariants and timings from final `GET /jobs/{id}` bodies (with attempt_history)."""
    by_status: dict[str, int] = {}
    for job in jobs:
        by_status[job["status"]] = by_status.get(job["status"], 0) + 1
    created = [_ts(j["created_at"]) for j in jobs]
    finished = [_ts(j["finished_at"]) for j in jobs]
    starts = [c for c in created if c]
    ends = [f for f in finished if f]
    drain = (max(ends) - min(starts)).total_seconds() if starts and ends else 0.0
    e2e = [(f - c).total_seconds() for c, f in zip(created, finished, strict=True) if c and f]
    attempts = [a for j in jobs for a in j.get("attempt_history", [])]

    failures = []
    if len(jobs) != expected:
        failures.append(f"submitted {expected} jobs but found {len(jobs)}")
    not_terminal = sum(n for s, n in by_status.items() if s not in TERMINAL)
    if not_terminal:
        failures.append(f"{not_terminal} jobs are not terminal")
    if by_status.get("succeeded", 0) != expected:
        failures.append(f"expected {expected} succeeded, got {by_status}")
    for job in jobs:
        wins = [a for a in job.get("attempt_history", []) if a["status"] == "succeeded"]
        if len(wins) > 1:
            failures.append(f"job {job['id']} has {len(wins)} succeeded attempts")

    return RunSummary(
        submitted=expected,
        by_status=by_status,
        throughput_jobs_per_s=len(ends) / drain if drain > 0 else 0.0,
        e2e_seconds={k: round(v, 3) for k, v in percentiles(e2e, (0.5, 0.95)).items()},
        drain_seconds=round(drain, 2),
        attempts_total=len(attempts),
        interrupted_attempts=sum(a["status"] in INTERRUPTED for a in attempts),
        jobs_with_retries=sum(len(j.get("attempt_history", [])) > 1 for j in jobs),
        failures=failures,
    )


def recovery_seconds(
    jobs: Sequence[dict[str, Any]], kills: dict[str, datetime]
) -> dict[str, list[float]]:
    """For each job a killed pod was running: kill -> lease reaped, and kill -> next claim.

    The interrupted attempt's `finished_at` is when the reaper (or the drain) closed it.
    "Claimed again" also includes any wait for a free worker.
    """
    out: dict[str, list[float]] = {"reaped": [], "reclaimed": []}
    for job in jobs:
        history = sorted(job.get("attempt_history", []), key=lambda a: a["started_at"])
        for prev, nxt in itertools.pairwise(history):
            pod = next((p for p in kills if prev["worker_id"].startswith(p)), None)
            if not pod or prev["status"] not in INTERRUPTED:
                continue
            for name, ts in (("reaped", prev["finished_at"]), ("reclaimed", nxt["started_at"])):
                when = _ts(ts)
                if when is not None:
                    out[name].append(max(0.0, (when - kills[pod]).total_seconds()))
    return out


class Client:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.http = httpx.AsyncClient(base_url=base_url, headers={"X-API-Key": api_key}, timeout=10)

    async def submit(self, body: dict[str, Any]) -> str:
        key = str(uuid.uuid4())  # a retried POST can't create a second job
        for attempt in range(20):
            try:
                resp = await self.http.post("/jobs", json=body, headers={"Idempotency-Key": key})
                if resp.status_code in (200, 201):
                    return str(resp.json()["id"])
            except httpx.HTTPError:
                pass
            await asyncio.sleep(min(2.0, 0.1 * 2**attempt))
        raise RuntimeError("could not submit a job after 20 tries")

    async def submit_many(self, bodies: Sequence[dict[str, Any]], parallel: int = 16) -> list[str]:
        sem = asyncio.Semaphore(parallel)

        async def one(body: dict[str, Any]) -> str:
            async with sem:
                return await self.submit(body)

        return list(await asyncio.gather(*(one(b) for b in bodies)))

    async def wait_terminal(self, ids: Sequence[str], limit_seconds: float) -> list[dict[str, Any]]:
        """Poll until every job is terminal (or the timeout passes); returns final bodies."""
        done: dict[str, dict[str, Any]] = {}
        sem = asyncio.Semaphore(16)
        deadline = time.monotonic() + limit_seconds

        async def fetch(job_id: str) -> None:
            async with sem:
                try:
                    resp = await self.http.get(f"/jobs/{job_id}")
                except httpx.HTTPError:
                    return
                if resp.status_code == 200 and resp.json()["status"] in TERMINAL:
                    done[job_id] = resp.json()

        while len(done) < len(ids) and time.monotonic() < deadline:
            await asyncio.gather(*(fetch(i) for i in ids if i not in done))
            if len(done) < len(ids):
                await asyncio.sleep(1)
        # Whatever is still open is reported as-is, so the summary can flag it.
        for job_id in ids:
            if job_id not in done:
                resp = await self.http.get(f"/jobs/{job_id}")
                if resp.status_code == 200:
                    done[job_id] = resp.json()
        return list(done.values())

    async def aclose(self) -> None:
        await self.http.aclose()


async def run_load(client: Client, args: argparse.Namespace) -> RunSummary:
    replicas = len(worker_pods())
    log(f"load: {args.jobs} sleep({args.seconds}s) jobs, {replicas} worker pod(s)")
    ids = await client.submit_many(
        [{"type": "sleep", "payload": {"seconds": args.seconds}}] * args.jobs
    )
    jobs = await client.wait_terminal(ids, args.timeout)
    summary = summarize(jobs, len(ids))
    log(
        f"{replicas} worker(s): {summary.throughput_jobs_per_s:.2f} jobs/s, "
        f"drain {summary.drain_seconds}s, e2e {summary.e2e_seconds}"
    )
    return summary


async def run_chaos(client: Client, args: argparse.Namespace) -> RunSummary:
    """Paced submissions with pod deletions in between.

    Jobs (2-5 s) outlast the ~2 s the kubelet still allows a force-deleted pod, so a forced
    deletion lands mid-job. Submissions are spread over `--submit-seconds` to keep the pool
    at roughly 60% load, so a re-claim measures recovery rather than a backlog queue.
    """
    rng = random.Random(args.seed)  # noqa: S311
    bodies = [
        {"type": "sleep", "payload": {"seconds": round(rng.uniform(2.0, 5.0), 2)}}
        for _ in range(args.jobs)
    ]
    mode = "forced" if args.force else "graceful"
    log(
        f"chaos: {args.jobs} sleep jobs over {args.submit_seconds:.0f}s, "
        f"{args.kills} {mode} worker-pod deletions every {args.interval:.0f}s"
    )

    async def submit_paced() -> list[str]:
        gap = args.submit_seconds / max(1, args.jobs)
        tasks = []
        for body in bodies:
            tasks.append(asyncio.create_task(client.submit(body)))
            await asyncio.sleep(gap)
        return list(await asyncio.gather(*tasks))

    kills: dict[str, datetime] = {}

    async def delete_pods() -> None:
        for _ in range(args.kills):
            await asyncio.sleep(args.interval)
            pods = worker_pods()
            if not pods:
                log("no running worker pod to delete; waiting for replacements")
                continue
            victim = rng.choice(pods)
            kills[victim] = datetime.now(UTC)
            extra = ["--grace-period=0", "--force"] if args.force else []
            # --wait=false: a graceful drain can take a while; the next kill shouldn't wait.
            await asyncio.to_thread(kubectl, "delete", "pod", victim, "--wait=false", *extra)
            log(f"deleted {victim} ({mode})")

    ids, _ = await asyncio.gather(submit_paced(), delete_pods())
    jobs = await client.wait_terminal(ids, args.timeout)
    summary = summarize(jobs, len(ids))
    log(
        f"{summary.by_status}; {summary.interrupted_attempts} interrupted attempts, "
        f"{summary.jobs_with_retries} jobs ran more than once"
    )
    for name, values in recovery_seconds(jobs, kills).items():
        if values:
            log(
                f"kill -> {name}: median {statistics.median(values):.1f}s, "
                f"max {max(values):.1f}s over {len(values)} orphaned jobs"
            )
    return summary


async def main_async(args: argparse.Namespace) -> int:
    client = Client(args.api_url, api_key_from_secrets())
    try:
        resp = await client.http.get("/health")
        if resp.status_code != 200:
            raise SystemExit(f"API not healthy at {args.api_url}: {resp.text}")
        runner = run_load if args.mode == "load" else run_chaos
        summary = await runner(client, args)
    finally:
        await client.aclose()
    for failure in summary.failures:
        log(f"FAIL: {failure}")
    if not summary.failures:
        log("all invariants hold")
    return 1 if summary.failures else 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=["load", "chaos"])
    parser.add_argument("--api-url", default=os.environ.get("K8S_API_URL", "http://127.0.0.1:8800"))
    parser.add_argument("--jobs", type=int, default=None)
    parser.add_argument("--seconds", type=float, default=0.2, help="load: sleep per job")
    parser.add_argument("--kills", type=int, default=4, help="chaos: pod deletions")
    parser.add_argument("--interval", type=float, default=10.0, help="chaos: seconds between")
    parser.add_argument(
        "--submit-seconds", type=float, default=60.0, help="chaos: spread submissions over"
    )
    parser.add_argument("--force", action="store_true", help="chaos: --grace-period=0 --force")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--timeout", type=float, default=300.0)
    args = parser.parse_args(argv)
    if args.jobs is None:
        args.jobs = 200 if args.mode == "load" else 40
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
