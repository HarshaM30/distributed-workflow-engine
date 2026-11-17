# Benchmark results

`make bench` reproduces every table on this page with one command. The tables sit between
`BEGIN`/`END` markers and are regenerated from `benchmarks/raw/*.json`. The method and
discussion are written by hand, and the numbers they quote come from the committed run.

## Machine

<!-- BEGIN:specs -->
| | |
|---|---|
| Chip | Apple M5 |
| CPU cores (host) | 10 |
| RAM (host) | 16.0 GB |
| OS | macOS 26.6.2 |
| Docker | 29.9.3 on Docker Desktop: 10 CPUs, 7.7 GB for the VM |
| Container memory limits | api 512 MB, maintenance 192 MB, postgres 512 MB, redis 128 MB, worker 512 MB |
| Python (harness) | 3.12.16 |
| Run started | 2026-11-16T21:05:25-0500 |
<!-- END:specs -->

## Method

**The unit is one worker: one process running one job at a time** (`WORKER_CONCURRENCY=1`).
Scaling from 1 to 8 workers means 1 to 8 worker containers.

The harness (`benchmarks/harness.py`) runs its own compose project, `wfe-bench`. It has its own
Postgres, Redis, API, maintenance process, and N workers, built from the same image and
`docker-compose.yml` as development, with the settings that affect results pinned in
`benchmarks/compose.bench.yml`. Rate limits are off, logs are at `WARNING`, and tracing export
is off. The stack is torn down with its volumes when the run ends, and it never touches the
development database or Redis.

- **Drain (throughput).** A backlog of N jobs is bulk-inserted as `queued` (one `COPY`), and
  then one stream entry is XADDed per job. This is the same state `POST /jobs` leaves behind,
  but it is in place in well under a second, so the submitter is never the bottleneck.
  Throughput = jobs / (last `finished_at` - first claim). Two job types are used: `sleep(0)`,
  which measures the engine's own per-job cost (stream read, CAS claim, attempt row,
  completion, XACK), and `cpu` with 60,000 SHA-256 rounds (about 10 ms of real work).
- **Steady load (latency).** `sleep(0)` jobs are POSTed through the API at a fixed open-loop
  rate. End-to-end latency is `finished_at - created_at`, both timestamps written by Postgres,
  so client and container clocks don't matter. The rate is the same for every worker count, so
  the rows compare the same offered load.
- **Repeats.** Each measurement runs 3 times on a freshly emptied database. The median is
  reported (per field), and the raw runs are in the JSON.
- **Chaos.** `benchmarks/run_chaos.py` submits a mixed workload through the API, each request
  with an `Idempotency-Key` so a client retry can't create a second job. While it does, it
  SIGKILLs a random worker container every few seconds (no graceful drain), starts that
  container again 2 s later, and restarts Redis mid-run. Redis runs without persistence, so a
  restart loses every stream, consumer group, and pending entry. Leases are shortened to 4 s so
  recovery happens within the run. After the drain, every invariant is checked in Postgres.
- **Duplicate executions are measured, not assumed.** The `ledger` handler writes a row to
  `job_executions` when it starts and stamps it when it finishes. Duplicates = executions -
  jobs.

## Throughput

<!-- BEGIN:throughput -->
Drain a backlog: 8,000 `sleep(0)` jobs, or 3,000 `cpu` jobs of 60,000 SHA-256 rounds each. Median of 3 runs.

| Workers | `sleep(0)` jobs/s | Speed-up | `cpu` jobs/s | Speed-up | `cpu` service time p50 |
|---:|---:|---:|---:|---:|---:|
| 1 | 536 | 1.00x | 81 | 1.00x | 11.2 ms |
| 2 | 1,002 | 1.87x | 151 | 1.87x | 12.0 ms |
| 4 | 1,298 | 2.42x | 236 | 2.92x | 14.9 ms |
| 8 | 1,673 | 3.12x | 322 | 3.99x | 22.0 ms |

Run-to-run range, `sleep(0)` jobs/s: 1: 530-541; 2: 1,002-1,005; 4: 1,245-1,338; 8: 1,634-1,678.
<!-- END:throughput -->

## Latency under steady load

<!-- BEGIN:latency -->
`sleep(0)` jobs POSTed through the API at 200 jobs/s (open loop) for 15 s. End-to-end = `finished_at - created_at` in Postgres. Median of 3 runs.

| Workers | Achieved rate | E2E p50 | E2E p95 | E2E p99 | API request p50 | Errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 200/s | 1.9 ms | 11.6 ms | 32.8 ms | 3.4 ms | 0 |
| 2 | 200/s | 2.0 ms | 2.3 ms | 3.6 ms | 2.8 ms | 0 |
| 4 | 200/s | 2.2 ms | 2.5 ms | 3.9 ms | 3.0 ms | 0 |
| 8 | 200/s | 2.3 ms | 2.6 ms | 3.5 ms | 3.0 ms | 0 |
<!-- END:latency -->

## Chaos

<!-- BEGIN:chaos -->
4 workers, 400 jobs submitted over 60 s (307 `ledger` 0.1-0.5 s, 49 `flaky` p=0.3, 44 `flaky` p=1.0, 2 attempts). A random worker got SIGKILL about every 4 s and was started again 2 s later; Redis was restarted 1 time(s) mid-run (no persistence, so streams, groups, and pending entries were lost). Lease 4 s, heartbeat 1 s, reaper every 1 s, sweeper every 2 s.

| Measure | Result |
|---|---|
| Jobs submitted | 400 |
| Succeeded / dead / other | 356 / 44 / 0 |
| **Lost jobs** | **0** |
| Jobs not submitted by the client (phantoms) | 0 |
| Worker kills / Redis restarts | 14 / 1 (0.33 s) |
| Attempts orphaned by a kill (lease expired) | 10 |
| `ledger` executions / jobs | 316 / 307 |
| **Duplicate executions** (executions - jobs) | **9** (9 cut short by a kill) |
| Kill to lease reaped, p50 / max | 4.37 s / 4.75 s (bound: 5 s) |
| Kill to job claimed again, p50 / max | 4.37 s / 4.76 s (includes any wait for a free worker) |
| Client retries during the run | 0 |

| Invariant | Result |
|---|---|
| no lost jobs | pass |
| every job terminal | pass |
| no phantom jobs | pass |
| succeeded plus dead equals submitted | pass |
| one terminal outcome per job | pass |
| doomed jobs dead | pass |
<!-- END:chaos -->

## Discussion

**Scaling.** Going from one worker to two nearly doubles throughput (1.87x for both job types).
After that, `sleep(0)` jobs flatten out: 2.4x at 4 workers and 3.1x at 8. A `sleep(0)` job does
no work, so its cost is the engine's own bookkeeping: a stream read, the CAS-claim
transaction (update plus attempt row), the completion transaction, and an XACK. Eight workers
all doing that against one Postgres container (`shared_buffers=128MB`, 512 MB limit) and one
Redis, inside a Docker VM that also runs the workers, spend more and more time waiting on
those shared services. `cpu` jobs spend most of their time in the handler, so they scale
better (4.0x at 8). Their service time still rises from 11 ms to 22 ms at 8 workers, which
points to CPU contention: 8 hashing workers plus Postgres and Redis on the VM's 10 CPUs, with
some of the chip's cores being efficiency cores. Throughput is still rising at 8, but the
returns per worker shrink, so on this machine the bottleneck at 8 workers is the shared
Postgres/host, not the number of workers.

**Latency.** At 200 jobs/s offered through the API, the median job finishes about 2 ms after
it was created, whatever the worker count. That covers the commit, XADD, stream read, claim,
handler, and completion. One worker is about 37% busy at this rate (200 of its ~536 jobs/s),
so arrivals that bunch up queue behind each other: p95 is 11.6 ms and p99 33 ms. With two or
more workers a second worker absorbs the bursts, and p99 stays under 4 ms. The first
one-worker run, right after start-up, had a p95 of 85 ms; the table shows the median of three
runs, and every run is in `raw/throughput.json`.

**Chaos.** Under 14 SIGKILLs and a Redis restart that wiped every stream and consumer group,
all 400 jobs reached exactly one terminal state, with no job lost or invented. Every `flaky`
p=1.0 job ended dead, as designed. The reaper took each orphaned lease back 4.4 s (p50) after
the kill, inside the 4 s lease + 1 s reaper bound, because the lease runs from the last
heartbeat (at most 1 s before the kill). The run kept the pool at about half load, so a free
worker claimed each reaped job at once and "claimed again" matches "reaped". An earlier run
that offered more work than the pool could do put requeued jobs at the back of a growing
backlog, and its "claimed again" figure measured that queue (tens of seconds) instead of
recovery. The Redis restart needed no client retries. The API commits to Postgres before it
XADDs, so a failed XADD only delays a job until the sweeper re-sends it.

**Duplicates are real and expected.** There were 9 duplicate executions, all of them
executions cut short by a kill and then run again on another worker. That is the "at-least-once
execution" half of the guarantee. The other half, "each job's result saved exactly once", held:
no job has two successful attempts, because every write after a claim is fenced by the lease
token. SIGKILL leaves no zombie, so none of these duplicates came from a stale worker finishing
late; that case is covered by the fencing integration tests. The takeaway for handler authors:
anything a handler does before its result is committed can happen twice, so it has to be
idempotent (like `embed_texts`, which upserts on `(job_id, item_index)`).

**Caveats.** These numbers come from one laptop running everything in one Docker Desktop VM,
with an untuned Postgres and a single API process. They show how the design behaves and
scales on this machine, not what a production deployment would sustain.
