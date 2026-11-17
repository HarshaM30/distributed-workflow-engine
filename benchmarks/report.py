"""Render benchmarks/raw/*.json into the generated blocks of benchmarks/results.md.

Only the text between `<!-- BEGIN:name -->` and `<!-- END:name -->` is rewritten; the method
and discussion around those blocks are hand-written and kept. A missing results file is
created from a skeleton.

    uv run python -m benchmarks.report
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from benchmarks.harness import RAW_DIR, ROOT

RESULTS = ROOT / "benchmarks" / "results.md"
SKELETON = """# Benchmark results

<!-- BEGIN:specs -->
<!-- END:specs -->

## Throughput

<!-- BEGIN:throughput -->
<!-- END:throughput -->

## Latency under steady load

<!-- BEGIN:latency -->
<!-- END:latency -->

## Chaos

<!-- BEGIN:chaos -->
<!-- END:chaos -->
"""


def _mb(n: int | str | None) -> str:
    # `docker compose config --format json` reports limits as byte strings.
    return f"{int(n) / 2**20:g} MB" if n else "unlimited"


def render_specs(throughput: dict[str, Any], chaos: dict[str, Any] | None) -> str:
    specs = throughput.get("specs") or (chaos or {}).get("specs") or {}
    docker = specs.get("docker", {})
    limits = specs.get("container_mem_limits_bytes", {})
    rows = [
        ("Chip", specs.get("chip")),
        ("CPU cores (host)", specs.get("cpu_count")),
        ("RAM (host)", f"{specs.get('ram_gb')} GB"),
        ("OS", specs.get("os")),
        (
            "Docker",
            f"{docker.get('server_version')} on {docker.get('os')}: "
            f"{docker.get('cpus')} CPUs, {docker.get('memory_gb')} GB for the VM",
        ),
        (
            "Container memory limits",
            ", ".join(f"{name} {_mb(limits.get(name))}" for name in sorted(limits)),
        ),
        ("Python (harness)", specs.get("python")),
        ("Run started", throughput.get("started_at")),
    ]
    lines = ["| | |", "|---|---|", *[f"| {k} | {v} |" for k, v in rows]]
    return "\n".join(lines)


def render_throughput(throughput: dict[str, Any]) -> str:
    by_workers = throughput["by_workers"]
    cfg = throughput["config"]
    base = by_workers[min(by_workers, key=int)]
    base_sleep = base["drain_sleep"]["median"]["jobs_per_second"]
    base_cpu = base["drain_cpu"]["median"]["jobs_per_second"]
    lines = [
        f"Drain a backlog: {cfg['sleep_jobs']:,} `sleep(0)` jobs, or {cfg['cpu_jobs']:,} `cpu` "
        f"jobs of {cfg['cpu_iterations']:,} SHA-256 rounds each. Median of {cfg['runs']} runs.",
        "",
        "| Workers | `sleep(0)` jobs/s | Speed-up | `cpu` jobs/s | Speed-up | "
        "`cpu` service time p50 |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for n in sorted(by_workers, key=int):
        sleep = by_workers[n]["drain_sleep"]["median"]
        cpu = by_workers[n]["drain_cpu"]["median"]
        sleep_rate, cpu_rate = sleep["jobs_per_second"], cpu["jobs_per_second"]
        lines.append(
            f"| {n} | {sleep_rate:,.0f} | {sleep_rate / base_sleep:.2f}x"
            f" | {cpu_rate:,.0f} | {cpu_rate / base_cpu:.2f}x"
            f" | {cpu['service_ms']['p50']:.1f} ms |"
        )
    spread = []
    for n in sorted(by_workers, key=int):
        runs = [r["jobs_per_second"] for r in by_workers[n]["drain_sleep"]["runs"]]
        spread.append(f"{n}: {min(runs):,.0f}-{max(runs):,.0f}")
    lines += ["", f"Run-to-run range, `sleep(0)` jobs/s: {'; '.join(spread)}."]
    return "\n".join(lines)


def render_latency(throughput: dict[str, Any]) -> str:
    by_workers = throughput["by_workers"]
    cfg = throughput["config"]
    lines = [
        f"`sleep(0)` jobs POSTed through the API at {cfg['latency_rate']:g} jobs/s (open loop) for "
        f"{cfg['latency_seconds']:g} s. End-to-end = `finished_at - created_at` in Postgres. "
        f"Median of {cfg['runs']} runs.",
        "",
        "| Workers | Achieved rate | E2E p50 | E2E p95 | E2E p99 | API request p50 | Errors |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for n in sorted(by_workers, key=int):
        m = by_workers[n]["steady"]["median"]
        e2e, api = m["e2e_ms"], m["api_request_ms"]
        lines.append(
            f"| {n} | {m['achieved_rate']:g}/s | {e2e['p50']:.1f} ms | {e2e['p95']:.1f} ms"
            f" | {e2e['p99']:.1f} ms | {api['p50']:.1f} ms | {m['errors']:g} |"
        )
    return "\n".join(lines)


def render_chaos(chaos: dict[str, Any] | None) -> str:
    if chaos is None:
        return "_No chaos run recorded yet (`make bench`)._"
    cfg, stack = chaos["config"], chaos["stack"]
    dup, rec = chaos["duplicates"], chaos["recovery"]
    st = chaos["statuses"]
    restarts = ", ".join(f"{r['restart_seconds']} s" for r in chaos["redis_restarts"]) or "none"
    lines = [
        f"{cfg['workers']} workers, {chaos['submitted']} jobs submitted over "
        f"{cfg['submit_seconds']:g} s ({_mix(chaos)}). A random worker got SIGKILL about every "
        f"{cfg['kill_every_seconds']:g} s and was started again {cfg['restart_after_seconds']:g} s "
        f"later; Redis was restarted {len(chaos['redis_restarts'])} time(s) mid-run (no "
        f"persistence, so streams, groups, and pending entries were lost). Lease "
        f"{stack['lease_seconds']:g} s, heartbeat {stack['heartbeat_seconds']:g} s, reaper every "
        f"{stack['reaper_interval_seconds']:g} s, sweeper every "
        f"{stack['sweeper_interval_seconds']:g} s.",
        "",
        "| Measure | Result |",
        "|---|---|",
        f"| Jobs submitted | {chaos['submitted']} |",
        f"| Succeeded / dead / other | {st.get('succeeded', 0)} / {st.get('dead', 0)} / "
        f"{chaos['submitted'] - st.get('succeeded', 0) - st.get('dead', 0)} |",
        f"| **Lost jobs** | **{chaos['lost']}** |",
        f"| Jobs not submitted by the client (phantoms) | {chaos['phantom']} |",
        f"| Worker kills / Redis restarts | {chaos['kills']} / {len(chaos['redis_restarts'])} "
        f"({restarts}) |",
        f"| Attempts orphaned by a kill (lease expired) | {chaos['lease_expired_attempts']} |",
        f"| `ledger` executions / jobs | {dup['executions']} / {dup['ledger_jobs']} |",
        f"| **Duplicate executions** (executions - jobs) | **{dup['executions_minus_jobs']}** "
        f"({dup['interrupted_executions']} cut short by a kill) |",
        f"| Kill to lease reaped, p50 / max | {_s(rec.get('reaped_seconds', {}), 'p50')} / "
        f"{_s(rec.get('reaped_seconds', {}), 'max')} (bound: {rec['bound_seconds']:g} s) |",
        f"| Kill to job claimed again, p50 / max | {_s(rec['seconds'], 'p50')} / "
        f"{_s(rec['seconds'], 'max')} (includes any wait for a free worker) |",
        f"| Client retries during the run | {chaos['client_retries']} |",
        "",
        "| Invariant | Result |",
        "|---|---|",
        *[
            f"| {name.replace('_', ' ')} | {'pass' if ok else '**FAIL**'} |"
            for name, ok in chaos["invariants"].items()
        ],
    ]
    return "\n".join(lines)


def _s(d: dict[str, Any], key: str) -> str:
    return f"{d[key]:.2f} s" if key in d else "n/a"


def _mix(chaos: dict[str, Any]) -> str:
    kinds = {k: sum(v.values()) for k, v in chaos["by_kind"].items()}
    cfg = chaos["config"]
    names = {
        "ledger": f"`ledger` {cfg['ledger_min_seconds']:g}-{cfg['ledger_max_seconds']:g} s",
        "flaky": "`flaky` p=0.3",
        "doomed": "`flaky` p=1.0, 2 attempts",
    }
    return ", ".join(f"{kinds[k]} {names[k]}" for k in ("ledger", "flaky", "doomed") if k in kinds)


def replace_block(text: str, name: str, body: str) -> str:
    pattern = re.compile(rf"(<!-- BEGIN:{name} -->\n).*?(<!-- END:{name} -->)", re.DOTALL)
    if not pattern.search(text):
        raise ValueError(f"results file has no {name} block")
    return pattern.sub(lambda m: m.group(1) + body.rstrip() + "\n" + m.group(2), text)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--throughput", type=Path, default=RAW_DIR / "throughput.json")
    parser.add_argument("--chaos", type=Path, default=RAW_DIR / "chaos.json")
    parser.add_argument("--results", type=Path, default=RESULTS)
    args = parser.parse_args(argv)
    throughput = json.loads(args.throughput.read_text())
    chaos = json.loads(args.chaos.read_text()) if args.chaos.exists() else None
    text = args.results.read_text() if args.results.exists() else SKELETON
    text = replace_block(text, "specs", render_specs(throughput, chaos))
    text = replace_block(text, "throughput", render_throughput(throughput))
    text = replace_block(text, "latency", render_latency(throughput))
    text = replace_block(text, "chaos", render_chaos(chaos))
    args.results.parent.mkdir(parents=True, exist_ok=True)
    args.results.write_text(text)
    print(f"updated {args.results}")


if __name__ == "__main__":
    main()
