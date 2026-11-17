# Architecture

The engine has three process types, two stores, and one rule: **Postgres decides, Redis
hints.** Every fact about a job (its state, its owner, when it should run, how many times it
has been tried) is a row in Postgres. Redis Streams carry "job X may be ready" messages, and
Redis also holds the rate limiters' token buckets. If Redis loses everything, the system slows
down but loses nothing.

## Processes

| Process | Command | Replicas | Job |
|---|---|---|---|
| `api` | `python -m workflow_engine.api` | any | Auth, validation, idempotent submission, submission rate limit, reads, cancel, DLQ, search, `/health`, `/metrics` |
| `worker` | `python -m workflow_engine.worker` | any (one job at a time each, by default) | Read streams, claim, heartbeat, run the handler, write the fenced result |
| `maintenance` | `python -m workflow_engine.maintenance` | any (one active) | Scheduler, reaper, sweeper, expired idempotency-key cleanup |

All three are built from one Docker image and differ only in the command.

## Job state machine

```
            submit (run_at > now)                  scheduler: run_at due
  client ───────────────────────► PENDING ───────────────────────────────► QUEUED
     │ submit (run_at <= now)        ▲                                       │ worker CAS claim
     └───────────────────────────────┼──────────────────────────────► QUEUED │
                                     │ retry (run_at = now + backoff)        ▼
                                     └──────────────────────────────── RUNNING ──► SUCCEEDED
                                                                          │
                                       attempts >= max_attempts ──────────┼──► DEAD (dead_letters row)
                                       cancel_requested ──────────────────┘──► CANCELLED
  PENDING / QUEUED ──cancel──► CANCELLED          DEAD ──requeue──► QUEUED (attempts reset)
  RUNNING + lease expired ──reaper──► QUEUED (or DEAD if attempts exhausted)
  RUNNING + SIGTERM grace exceeded ──release──► QUEUED (attempt not counted)
```

The allowed transitions are listed in `core/states.py`, and a Hypothesis property test checks
that no sequence of operations can reach a state outside them. All transitions are implemented in
`core/jobs.py`.

## Data model

| Table | Purpose | Key columns |
|---|---|---|
| `jobs` | One row per job, the source of truth | `id`, `type`, `payload`, `status`, `priority`, `run_at`, `attempts`, `max_attempts`, `lease_owner`, `lease_token`, `lease_expires_at`, `cancel_requested`, `result`, `last_error`, timestamps |
| `job_attempts` | One row per claim | `job_id`, `attempt_no`, `worker_id`, `lease_token`, `status` (`running`, `succeeded`, `failed`, `lease_expired`, `released`, `cancelled`), `error`, timestamps |
| `dead_letters` | Jobs that ran out of attempts | `job_id`, `reason` (`failed`, `lease_expired`, `non_retryable`), `last_error`, `dead_at`, `requeued_at` |
| `idempotency_keys` | Submission dedup | primary key `(api_key_id, key)`, `request_hash`, `job_id`, `expires_at` |
| `embeddings` | `embed_texts` output | `job_id`, `item_index`, `text`, `embedding vector(384)`, `model`; unique on `(job_id, item_index)`; HNSW index |
| `job_executions` | Side-effect ledger for the chaos tests | one row per `ledger` handler start, stamped on finish |

Indexes support the hot queries: `jobs(status, run_at)` for the scheduler,
`jobs(status, lease_expires_at)` for the reaper, and a partial index on active statuses.

## Redis layout

| Key | Type | Use |
|---|---|---|
| `jobs:high`, `jobs:normal`, `jobs:low` | Stream, consumer group `workers` | One entry per hint: the job id plus trace context (`traceparent`, `tracestate`). Trimmed approximately at `STREAM_MAXLEN`. |
| token buckets | Hash, updated by `dispatch/lua/token_bucket.lua` | Per job type (execution throttle) and per API key (submission limit) |

Workers read the streams in priority order: `high` first, then `normal`, then `low`, so a
backlog of low-priority work never delays a high-priority job by more than the job a worker is
already running.

## Path of a job

1. **Submit.** The API validates the type and the payload against the handler's Pydantic model,
   checks the `Idempotency-Key` (a replay returns the original job), charges the client's token
   bucket (429 when empty), then inserts the job and the key in one transaction. After commit it
   XADDs a hint to the priority's stream.
2. **Claim.** A worker blocked on `XREADGROUP` gets the entry and runs the CAS claim. It now
   holds a lease with a fresh token, and an attempt row exists.
3. **Throttle check.** If the job type has an execution limit and its bucket is empty, the claim
   is undone: the job goes back to `pending` with a short delay, and the attempt is deleted
   (the handler never ran).
4. **Run.** The handler runs with a timeout while a heartbeat task extends the lease. The
   heartbeat also notices cancellation requests and lost leases.
5. **Finish.** A fenced update writes `succeeded`, a retry (`pending` with a backoff `run_at`),
   or `dead` with a dead-letter row. Then the worker XACKs the entry.
6. **Repair, if needed.** The reaper, sweeper, and scheduler handle crashes, lost hints, and
   timers. See [delivery-guarantees.md](delivery-guarantees.md).

## Maintenance and leader election

The maintenance process holds a session-level `pg_try_advisory_lock` on a dedicated connection
and checks it every 2 s. Only the holder runs the loops:

- **Scheduler** (every `SCHEDULER_INTERVAL_SECONDS`): `pending` jobs with `run_at <= now()` move
  to `queued` and get a hint, in batches, using `FOR UPDATE SKIP LOCKED`.
- **Reaper** (every `REAPER_INTERVAL_SECONDS`): `running` jobs whose lease has expired go back to
  `queued` (or to `dead`, or to `cancelled` if a cancel was requested).
- **Sweeper** (every `SWEEPER_INTERVAL_SECONDS`): `queued` jobs untouched for
  `SWEEPER_STALE_SECONDS` get a fresh hint, stream entries idle that long are reclaimed with
  `XAUTOCLAIM`, and expired idempotency keys are deleted.

## Observability

- **Logs:** structlog JSON on stdout, including stdlib loggers. Worker lines carry `job_id`,
  `job_type`, `attempt`, `worker_id`, and `trace_id`; API lines carry `request_id`.
- **Traces:** OpenTelemetry. The API's trace context rides in each stream entry, so one trace
  covers `POST /jobs` → `job.enqueue` → `job.process` → `job.claim` and `job.handler`. Export
  (OTLP/HTTP to Jaeger) is on when `OTEL_ENABLED=true`.
- **Metrics:** Prometheus. The API serves `/metrics`; workers and maintenance serve it on
  `WORKER_METRICS_PORT` and `MAINTENANCE_METRICS_PORT`. Main series: `jobs_submitted_total`,
  `jobs_completed_total{type,status}`, `job_duration_seconds`, `job_e2e_latency_seconds`,
  `queue_depth{status,priority}`, `retries_total`, `dlq_size`, `leases_reaped_total`,
  `sweeper_requeued_total`, `rate_limited_total{limiter,type}`.
