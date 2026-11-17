# Distributed Workflow Engine: Implementation Plan

A distributed job-processing platform. Clients submit jobs over an HTTP API, and a pool of
workers runs them asynchronously with at-least-once delivery, retries, dead-lettering,
idempotency, rate limiting, and crash recovery. PostgreSQL is the source of truth. Redis
only dispatches work and never holds state that can't be rebuilt from Postgres.

---

## 1. Core design decisions

These decisions shape every milestone, so they are settled here first.

1. **Redis messages are hints, and Postgres decides.** A worker that reads a stream entry
   claims the job with a conditional update:
   `UPDATE jobs SET status='running', lease_owner=…, lease_token=lease_token+1, lease_expires_at=…
   WHERE id=… AND status='queued' RETURNING lease_token`.
   If no row comes back, the message is stale or a duplicate. The worker ACKs it and drops
   it. As a result, re-enqueueing too often is harmless, and the sweeper and reaper can act
   aggressively.
2. **Enqueue order is commit first, then XADD.** If the API crashes between the two steps,
   the job sits in `queued` with no stream entry. The reconciliation sweeper finds it and
   re-adds it.
3. **Leases live in Postgres and use fencing tokens.** Heartbeats extend `lease_expires_at`
   only while `lease_token` still matches. Completion, retry, and dead-letter writes all
   include `WHERE lease_token = :mine`. A slow "zombie" worker whose lease was reaped can't
   overwrite the result of the worker that took over. This is what lets the client see
   **one** terminal outcome even though a handler may run more than once.
4. **Delayed jobs and retry backoff both use `run_at`.** Jobs with a future `run_at` stay in
   `pending` in Postgres. A scheduler loop moves due jobs to `queued` and XADDs them. Redis
   never holds timers.
5. **Three fixed priority levels, each with its own stream.** Redis Streams have no
   priority ordering, so `high`, `normal`, and `low` each get a stream (`jobs:high`,
   `jobs:normal`, `jobs:low`), and workers poll them in that order. The API accepts only
   these three values.
6. **Maintenance loops run in their own process.** The reaper, sweeper, and scheduler run in
   a `maintenance` process that holds a Postgres advisory lock (leader election). You can
   run several copies safely, and only one is active at a time.
7. **Rate limiting happens at two layers, both using token buckets in an atomic Redis Lua
   script.**
   - *Execution throttle, per job type:* gates when a worker may *start* a job of that
     type. A worker that finds the bucket empty puts the job back in `pending` with a short
     `run_at` delay. Jobs are delayed, never rejected.
   - *Submission limit, per client (API key):* `POST /jobs` returns **429** with a
     `Retry-After` header when a client goes over its limit. Idempotent replays of a job
     that already exists don't use up tokens.
   - If Redis is down, both limiters fail open and log a warning. Postgres stays the
     source of truth, so an outage in the limiter never loses jobs.
8. **Delivery guarantee: at-least-once execution, with each job's result saved exactly
   once.** A handler may run more than once (for example, after a worker crash), but fencing
   tokens make sure only one attempt's result is ever committed. Handlers are written to be
   idempotent. README, docs, and benchmarks all use this exact wording.
9. **Benchmark unit: one worker is one process running one job at a time.** Scaling from 1
   to 8 workers means 1 to 8 processes, which gives a clean comparison.

### Job state machine

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
```

### Data model (Postgres)

| Table | Key columns |
|---|---|
| `jobs` | `id uuid`, `type`, `payload jsonb`, `status`, `priority`, `run_at`, `attempts`, `max_attempts`, `lease_owner`, `lease_token`, `lease_expires_at`, `cancel_requested`, `result jsonb`, `last_error`, `created_at`, `updated_at`, `finished_at` |
| `job_attempts` | `id`, `job_id`, `attempt_no`, `worker_id`, `lease_token`, `status`, `error`, `started_at`, `finished_at` |
| `dead_letters` | `id`, `job_id`, `reason`, `last_error`, `dead_at`, `requeued_at` |
| `idempotency_keys` | `(api_key_id, key)` primary key, `request_hash`, `job_id`, `created_at`, `expires_at` |
| `embeddings` | `id`, `job_id`, `item_index`, `text`, `embedding vector(384)`, `model`, `created_at`, with a unique constraint on `(job_id, item_index)` |

Indexes: `jobs(status, run_at)`, `jobs(status, lease_expires_at)`, a partial index on
`jobs(status) WHERE status IN ('pending','queued','running')`, and HNSW on
`embeddings.embedding`.

---

## 2. Folder structure

```
distributed-workflow-engine/
├── src/workflow_engine/
│   ├── config.py                 # pydantic-settings; reads env / .env
│   ├── logging.py                # structured JSON logging (structlog)
│   ├── telemetry.py              # OpenTelemetry setup
│   ├── metrics.py                # Prometheus metric definitions
│   ├── db/
│   │   ├── base.py               # declarative base, naming conventions
│   │   ├── models.py             # Job, JobAttempt, DeadLetter, IdempotencyKey, Embedding
│   │   └── session.py            # async engine + session factory
│   ├── core/
│   │   ├── states.py             # JobStatus enum + allowed transitions
│   │   ├── jobs.py               # submit / claim / complete / fail / cancel (all transitions)
│   │   ├── backoff.py            # exponential backoff with full jitter
│   │   └── idempotency.py        # key lookup, request hashing, conflict detection
│   ├── dispatch/
│   │   ├── streams.py            # Redis Streams producer/consumer, consumer groups
│   │   ├── rate_limit.py         # token buckets: per job type (execution) + per client (submission)
│   │   └── lua/token_bucket.lua
│   ├── handlers/
│   │   ├── registry.py           # @handler decorator, per-type config (max_attempts, timeout)
│   │   ├── sleep.py
│   │   ├── cpu.py
│   │   ├── flaky.py
│   │   └── embed_texts.py        # fastembed (ONNX) + pgvector upsert
│   ├── api/
│   │   ├── app.py                # FastAPI factory, lifespan, middleware
│   │   ├── auth.py               # X-API-Key dependency
│   │   ├── schemas.py            # Pydantic request/response models
│   │   └── routes/{jobs,dlq,embeddings,health}.py
│   ├── worker/
│   │   ├── main.py               # entrypoint: `python -m workflow_engine.worker`
│   │   ├── runner.py             # read → claim → execute → finalize loop
│   │   ├── heartbeat.py          # lease renewal task
│   │   └── shutdown.py           # SIGTERM handling, drain
│   └── maintenance/
│       ├── main.py               # leader-elected loop runner
│       ├── scheduler.py          # pending + due → queued
│       ├── reaper.py             # expired leases → queued / dead
│       └── sweeper.py            # queued but missing from Redis → re-XADD
├── migrations/                   # Alembic env + versions
├── tests/
│   ├── unit/                     # pure logic: backoff, states, schemas, registry
│   ├── integration/              # real Postgres + Redis (compose or CI services)
│   └── chaos/                    # kill workers / restart Redis, assert invariants
├── benchmarks/
│   ├── run_throughput.py         # 1/2/4/8 workers, jobs/s, p50/p95
│   ├── run_chaos.py              # injected failures, lost/duplicate counts, recovery time
│   └── results.md
├── demo/                         # `make demo`: scripted kill-and-recover run + compose override
├── deploy/k8s/                   # kind config + kustomize manifests
├── docs/
│   ├── architecture.md
│   ├── delivery-guarantees.md
│   ├── deploying-to-the-cloud.md
│   ├── demo.tape                 # vhs script that records demo.gif
│   └── demo.gif
├── .github/workflows/
│   ├── ci.yml                    # every push: lint, unit, integration
│   └── chaos-bench.yml           # manual trigger only (workflow_dispatch)
├── Dockerfile                    # one multi-stage image; API / worker / maintenance chosen by command
├── docker-compose.yml
├── Makefile
├── alembic.ini
├── pyproject.toml / uv.lock
├── .env.example
├── PLAN.md
└── README.md
```

---

## 3. Milestones

### Milestone 1: Scaffold

**Tasks**
- [x] `uv init` with Python 3.12 and a `src/` layout. Add runtime deps (fastapi, uvicorn,
      sqlalchemy[asyncio], psycopg[binary], alembic, pydantic-settings, redis, structlog)
      and dev deps (pytest, pytest-asyncio, httpx, ruff, mypy).
- [x] Configure ruff (lint + format) and pytest markers (`unit`, `integration`, `chaos`) in
      `pyproject.toml`.
- [x] `config.py` with typed settings for every variable in `.env.example`.
- [x] SQLAlchemy models for all five tables, plus the `JobStatus` enum and transition table.
- [x] Alembic setup and an initial migration (including `CREATE EXTENSION vector`).
- [x] `docker-compose.yml`: `pgvector/pgvector:pg16` and `redis:7-alpine` with healthchecks,
      named volumes, and memory limits. API/worker services go behind a profile.
- [x] Multi-stage `Dockerfile` (uv-based, non-root user, arm64- and amd64-compatible).
- [x] FastAPI app with `GET /health`, which checks Postgres and Redis and returns 200 or 503
      with details for each component.
- [x] `Makefile` targets: `install`, `up`, `down`, `migrate`, `api`, `worker`,
      `maintenance`, `lint`, `fmt`, `test-unit`, `test-integration`, `test-chaos`, `bench`.
- [x] GitHub Actions `ci.yml`, run on every push: ruff check + format check, then unit
      tests, then integration tests against Postgres (pgvector image) and Redis service
      containers. Cache uv. Chaos tests and benchmarks are left out (see Milestone 5).

**Done when**
- [x] `make up && make migrate` on a clean checkout creates all tables.
- [x] `curl localhost:8000/health` returns 200 with both components `ok`, and returns 503
      when Redis is stopped.
- [x] `alembic downgrade base && alembic upgrade head` round-trips cleanly.
- [x] `make lint` and `make test-unit` pass locally.
- [x] CI is green on the first push and runs all three stages.

**Implementation notes**
- Compose publishes Postgres on host port **5433** (`POSTGRES_PORT`) so it doesn't clash with
  another local Postgres on 5432. `DATABASE_URL` is now optional: when empty, the URL is built
  from the `POSTGRES_*` parts, so the password lives in one place.
- The CI lint stage also runs `mypy --strict` (an adopted addition).
- `make maintenance` runs a stub entrypoint until Milestone 3, and `make bench` exits with a
  "not implemented" message until Milestone 5.
- `WORKER_CONCURRENCY` defaults to 1 (one job at a time per process, matching the benchmark
  unit). New settings: `MAX_PAYLOAD_BYTES` (Milestone 2 payload cap) and `STREAM_MAXLEN`
  (approximate per-stream trim; the sweeper repairs anything trimmed).

### Milestone 2: Submission API, dispatch, and a basic worker

**Tasks**
- [x] `X-API-Key` dependency. Keys come from `API_KEYS` and are compared in constant time.
      Applies to every route except `/health` and `/metrics`.
- [x] Pydantic schemas: job type must be a registered handler, payload validated against
      that handler's own payload model, priority enum, optional `run_at`, size limits.
- [x] `POST /jobs` (returns 201 with the job, or 200 for an idempotent replay).
- [x] `GET /jobs/{id}` with attempt history.
- [x] `GET /jobs?status=&type=&limit=&cursor=` with keyset pagination.
- [x] Idempotency: `Idempotency-Key` header scoped per API key, with a SHA-256 hash of the
      canonical request body. Same key and same body returns the original job. Same key
      with a different body returns 409. Handles concurrent duplicates with
      `INSERT … ON CONFLICT`. Expired keys are cleaned up by maintenance.
- [x] Streams producer: XADD to `jobs:{priority}` after commit. Consumer group created at
      startup with `MKSTREAM`.
- [x] Handler registry plus the `sleep`, `cpu`, and `flaky` handlers. `cpu` runs in a
      process or thread executor so it doesn't block the event loop.
- [x] Worker loop: XREADGROUP, then CAS claim in Postgres, then execute, then write the
      result and an attempt row, then XACK. Single attempt only in this milestone.

**Done when**
- [x] Requests without a key or with a wrong key get 401. `/health` stays open.
- [x] Invalid input (unknown type, bad payload, oversized body) gets 422 with a clear error.
- [x] Submitting a `sleep` job leads to `succeeded` in `GET /jobs/{id}` with one attempt.
- [x] An integration test fires 20 concurrent POSTs with the same Idempotency-Key and gets
      exactly one job row.
- [x] Reusing a key with a different body returns 409.
- [x] Delivering the same stream message twice runs the handler only once, because the
      CAS claim rejects the second delivery (integration test).

**Implementation notes**
- Single attempt only: a handler error, timeout, or unknown type sends the job straight to
  `dead` with a `dead_letters` row (reason `failed`). Milestone 3 replaces this with retries.
- The per-handler timeout (an adopted addition) is already enforced with `asyncio.wait_for`.
- Oversized payloads (`MAX_PAYLOAD_BYTES`, serialized JSON) get 422, as the checklist says.
- If the XADD fails after commit, the API logs a warning and still returns 201. The job stays
  `queued` until the Milestone 3 sweeper re-adds it.
- An expired idempotency key is taken over in place by the `INSERT … ON CONFLICT DO UPDATE …
  WHERE expires_at <= now()`. Deleting expired keys in bulk is still a maintenance task.
- `GET /jobs/{id}` returns the attempts as `attempt_history`. Reads aren't scoped per API key.
- Workers already read `high`, then `normal`, then `low`. The Milestone 3 priority task still
  needs its mixed-backlog check.

### Milestone 3: Reliability

**Tasks**
- [x] Retries: on failure, set `run_at = now + backoff(attempt)` using full jitter capped at
      `RETRY_MAX_DELAY_SECONDS`, and set status to `pending`. `max_attempts` comes from the
      job, falling back to the handler default and then the global default.
- [x] Dead-lettering: when attempts run out, set status to `dead` and insert a
      `dead_letters` row in the same transaction.
- [x] `GET /dlq` (paginated) and `POST /dlq/{id}/requeue`, which resets attempts, sets
      `requeued_at`, and re-enqueues the job.
- [x] Leases: claim sets `lease_expires_at`, a heartbeat task renews it every
      `WORKER_HEARTBEAT_SECONDS`, and every write checks the fencing token.
- [x] Reaper: `running` jobs with an expired lease go back to `queued` (counted as a failed
      attempt with error `lease_expired`), or to `dead` if attempts are exhausted. Uses
      `FOR UPDATE SKIP LOCKED`.
- [x] Sweeper: `queued` jobs older than a threshold get re-XADDed. This is safe because of
      the CAS claim. Also uses XAUTOCLAIM to clean up stale pending-entry-list entries.
- [x] Scheduler: due `pending` jobs move to `queued` and get XADDed, in batches.
- [x] Maintenance process with Postgres advisory-lock leader election.
- [x] Graceful shutdown: on SIGTERM, stop reading new messages, let in-flight jobs finish
      within the grace period, and if a job is still running after that, release its lease
      (back to `queued`) instead of abandoning it.
- [x] Priorities: workers poll `high`, then `normal`, then `low`.
- [x] Delayed jobs: `run_at` on submission.
- [x] Cancellation via `POST /jobs/{id}/cancel`. A `pending` or `queued` job becomes
      `cancelled` immediately. A `running` job gets `cancel_requested` set, the heartbeat
      sees it and cancels the handler task, and the final state is `cancelled`. Terminal
      jobs return 409.
- [x] Token-bucket Lua script shared by both limiters.
- [x] Execution throttle per job type (configured by `RATE_LIMITS`). An empty bucket
      delays the job and never rejects it.
- [x] Submission limit per client on `POST /jobs`, keyed by API key (configured by
      `SUBMIT_RATE_LIMIT_PER_SECOND` / `SUBMIT_RATE_LIMIT_BURST`). Returns 429 with
      `Retry-After`, and idempotent replays are exempt.

**Done when**
- [x] A `flaky` job with p=1.0 and max_attempts=3 ends up `dead` with exactly 3 attempts.
      Attempt timestamps show growing, jittered gaps.
- [x] A requeued DLQ job runs again and can succeed.
- [x] After `kill -9` on a worker mid-job, the job is reaped and completed by another worker
      within lease time plus the reaper interval.
- [x] A zombie worker's late completion is rejected by the fencing token (integration test).
- [x] After `FLUSHALL` on Redis, every non-terminal job still completes thanks to the
      sweeper (integration test).
- [x] After SIGTERM, an in-flight 5-second sleep job finishes and the worker exits 0.
- [x] With a mixed backlog, high-priority jobs start before low-priority ones.
- [x] A job with `run_at` 10s in the future doesn't start before then.
- [x] Cancel works in each state (pending, queued, running, terminal) and each case is
      tested.
- [x] An execution limit of 5/s on a type caps execution starts at roughly 5/s, within
      tolerance, and every throttled job still completes.
- [x] A client that goes over its submission limit gets 429 with a valid `Retry-After`.
      Other API keys aren't affected, and replaying an existing Idempotency-Key still
      returns 200.

**Implementation notes**
- Backoff is full jitter, so each gap is a random draw from a window whose ceiling doubles
  (`base * 2**(n-1)`, capped). Gaps grow in their upper bound and on average, not on every
  single run. The integration test checks each gap against its window; a unit test checks
  that the mean grows.
- Exhausted retries dead-letter with reason `failed`; an expired lease with no attempts left
  uses `lease_expired`. An unknown job type or a payload that no longer validates goes
  straight to `dead` with reason `non_retryable` (retrying can't help).
- `POST /dlq/{id}/requeue` takes the dead-letter entry id. `GET /dlq` hides requeued entries
  unless `include_requeued=true`, and pages newest first by id. Requeuing twice returns 409.
- `POST /jobs/{id}/cancel` returns 200 when the job is cancelled at once (pending/queued),
  202 when it is running and only `cancel_requested` is set, 404 and 409 otherwise. A
  running job stops within one heartbeat interval. The reaper also honours a pending cancel:
  an expired lease on a job with `cancel_requested` ends in `cancelled`.
- The heartbeat also detects a lost lease and cancels the handler, so the worker stops
  wasting time on a job someone else now owns.
- On shutdown, a job released after the grace period goes back to `queued` with its attempt
  marked `released` and *not counted* (`attempts` is decremented). It is XADDed right away,
  so the next worker doesn't wait for the sweeper. Its next attempt reuses the attempt number.
- The execution throttle is checked after the claim (that is when the type is known). A
  throttled claim is undone: the job returns to `pending` with `run_at = now + retry_after`
  (plus jitter), `attempts` is decremented, and the attempt row is deleted, since the handler
  never ran.
- Before charging the submission limit, the API checks for a live Idempotency-Key, so a replay
  is free. Concurrent first-time duplicates may each use a token. Invalid requests (422) are
  rejected before the limiter and use no tokens.
- The sweeper finds "stale" queued jobs by `updated_at` older than `SWEEPER_STALE_SECONDS`, and
  bumps `updated_at` when it re-sends, so each job gets at most one extra hint per window and
  a large backlog doesn't flood the streams. XAUTOCLAIM uses the same idle threshold: reclaimed
  entries are ACKed, and any whose job is still `queued` is re-sent. The sweeper also deletes
  expired idempotency keys.
- Leader election uses `pg_try_advisory_lock` on a dedicated unpooled connection, which is
  checked every 2s. Losing that connection makes the leader step down, and a standby retries
  every `MAINTENANCE_LEADER_RETRY_SECONDS`.
- New settings: `SWEEPER_STALE_SECONDS`, `MAINTENANCE_BATCH_SIZE`,
  `MAINTENANCE_LEADER_RETRY_SECONDS`. Lease, heartbeat, and grace timings are now floats, and
  config rejects a heartbeat interval that isn't shorter than the lease.
- The Redis-wipe test runs `FLUSHDB` on the test database (DB 15) instead of `FLUSHALL`, so it
  can't wipe a developer's local Redis data. The effect on the engine is the same: streams,
  groups, and PELs are all gone.
- The kill -9 and SIGTERM checks run real worker subprocesses inside the integration suite.
  The Docker-based chaos harness is still Milestone 5.
- `docker-compose.yml` gains a `maintenance` service under the `app` profile.

### Milestone 4: Observability and the ML job

**Tasks**
- [x] structlog JSON output with `job_id`, `job_type`, `attempt`, `worker_id`, `trace_id`
      bound in context, and a request-ID middleware on the API.
- [x] OpenTelemetry: FastAPI, SQLAlchemy, and Redis auto-instrumentation. Trace context is
      carried inside the stream message so a job's trace runs from the API through the
      worker. OTLP exporter goes to Jaeger all-in-one, behind a `tracing` compose profile.
- [x] Prometheus metrics: `jobs_submitted_total`, `jobs_completed_total{type,status}`,
      `job_duration_seconds` histogram, `job_e2e_latency_seconds` histogram, `queue_depth`
      gauge, `retries_total`, `dlq_size`, `leases_reaped_total`, `sweeper_requeued_total`,
      `rate_limited_total`. API serves `/metrics`; workers serve it on `WORKER_METRICS_PORT`.
- [x] `embed_texts` handler: payload is a list of texts (with a cap), embedded in batches
      with fastembed `BAAI/bge-small-en-v1.5` (384 dimensions). Upserts on
      `(job_id, item_index)` so retries stay idempotent. Model is cached in a volume.
- [x] `GET /embeddings/search?q=&k=&job_id=`, which requires an API key. It embeds the
      query with the same model and returns the top-k nearest stored texts by cosine
      distance, using the HNSW index. `k` is validated and capped, and `job_id` optionally
      limits the search to one job's texts. The model is loaded once per process, lazily.

**Done when**
- [x] Every log line is valid JSON, and a single job's lines can be filtered by `job_id`.
- [x] Jaeger shows one trace that spans the API request, enqueue, worker claim, and handler.
- [x] `/metrics` scrapes cleanly and the counters move when jobs run.
- [x] An `embed_texts` job with 100 texts stores 100 vectors. Retrying it leaves the row
      count at 100.
- [x] `GET /embeddings/search` returns a semantically related text as the top hit for a
      paraphrased query (integration test with a small fixed corpus). EXPLAIN shows the
      HNSW index being used.
- [x] The worker image stays reasonably small with no PyTorch in the dependency tree
      (verified with `uv tree`).

**Implementation notes**
- A tracer provider is always installed, so every process has trace IDs for its logs and the
  API's trace context rides along in each stream entry (`traceparent`/`tracestate` fields).
  Spans are only exported (OTLP/HTTP) when `OTEL_ENABLED=true`, and SQLAlchemy and Redis
  auto-instrumentation is only switched on then. The SQLAlchemy instrumentor's declared range
  stops below SQLAlchemy 2.1, so it is enabled with `skip_dep_check=True` (a test checks it
  still emits `db.system=postgresql` spans).
- Span chain: `POST /jobs` → `job.enqueue` → (stream) → `job.process` → `job.claim` and
  `job.handler`. A hint re-sent later (scheduler, sweeper, reaper, shutdown release) carries the
  sender's context, so a delayed or retried attempt starts its own trace rather than joining the
  original request's.
- stdlib loggers (uvicorn, alembic, libraries) render through the same structlog chain, so every
  stdout line is JSON. The request-ID middleware echoes a safe `X-Request-ID` (else generates
  one), binds `request_id`, and writes one `http_request` access line per request.
- Workers and maintenance serve `/metrics` from a small asyncio server on their own event loop.
  `prometheus_client.start_http_server` was dropped because its bind does a reverse-DNS lookup
  (`getfqdn`) that stalled worker start-up for seconds on macOS. Port `0` disables the server, and
  a taken port (several workers on one host) is logged rather than fatal. New setting:
  `MAINTENANCE_METRICS_PORT`.
- `queue_depth{status,priority}` and `dlq_size` are read from Postgres when the API is scraped.
  `job_duration_seconds` is labelled by `type` and `outcome`; `rate_limited_total` by `limiter`
  (`submission` or `execution`) and `type`. The reaper also counts its dead/cancelled outcomes
  in `jobs_completed_total`.
- `embed_texts` takes up to 512 texts of up to 4,000 characters (and the payload byte cap still
  applies). It commits per batch and skips items already stored for the job, so a retry resumes
  where the last attempt stopped. Handlers that write their own results get a session factory
  on `JobContext`. Model loading and inference run in a thread so heartbeats keep going. New
  setting: `EMBED_THREADS` (ONNX threads per process, default 2).
- `GET /embeddings/search`: `k` outside `1..EMBED_SEARCH_MAX_K` gets 422. `hnsw.ef_search` is
  raised to at least `k`, and a `job_id` filter turns on pgvector's iterative index scan so a
  filter can't shrink the result below `k`. Each hit returns `distance` and `score = 1 - distance`.
- The EXPLAIN test seeds 3,000 filler vectors first: on a near-empty table Postgres rightly picks
  a sequential scan, at realistic sizes it picks the HNSW index (with or without `job_id`).
- Compose: Jaeger 2.11 behind the `tracing` profile (`make tracing-up`, UI on :16686), a
  `modelcache` volume shared by the API and workers, and 512 MB limits for both (the ONNX model
  is loaded on first use). CI caches the model.
- The image is 630 MB (the venv is 307 MB; ONNX Runtime is the largest package at 63 MB).
  `uv tree` has no `torch`.

### Milestone 5: Benchmarks and chaos testing

**Tasks**
- [x] Load generator: submits N `sleep(0)` or short `cpu` jobs and measures end-to-end
      latency (`finished_at - created_at`) from Postgres.
- [x] Throughput runs at 1, 2, 4, and 8 workers, where each worker is one process running
      one job at a time. 3 runs each, median reported.
- [x] Chaos harness: randomly `docker kill` workers during a run, restart Redis mid-run,
      then check the invariants: every job is terminal, none lost, and
      `succeeded + dead == submitted`.
- [x] Duplicate-execution counter: handlers record each execution in a side table, so
      duplicates = executions − jobs.
- [x] Recovery-time measurement: time from worker kill to the orphaned job being claimed
      again.
- [x] `tests/chaos/` pytest suite, a smaller version of the harness, marked `chaos` and run
      via `make test-chaos`.
- [x] `benchmarks/results.md`: machine specs (chip, RAM, OS, Docker resource limits), the
      method (including the definition of a worker), tables, and a discussion of the results.
- [x] `.github/workflows/chaos-bench.yml`, triggered only by `workflow_dispatch`. It runs
      the chaos suite and a reduced benchmark and uploads the results as an artifact. It
      never runs on push.

**Done when**
- [x] `make bench` reproduces the results tables in a single command.
- [x] Chaos runs report **0 lost jobs** and exactly one terminal state per job.
- [x] Duplicate executions are reported honestly (expected to be above 0 under kills) and
      explained.
- [x] `results.md` is committed with specs, method, and numbers.
- [x] `chaos-bench.yml` succeeds when triggered by hand from the Actions tab, and `ci.yml`
      still doesn't run chaos tests or benchmarks.

**Implementation notes**
- Benchmarks and chaos runs use their own compose project, `wfe-bench`
  (`benchmarks/compose.bench.yml` layered on `docker-compose.yml`), with separate Postgres,
  Redis, API, maintenance, and worker containers on ports 5434/6380/8001. Every setting that
  affects results is pinned there. Restarting Redis mid-run therefore can't touch development
  data, and the project is removed with its volumes at the end. `REDIS_PORT` is a new compose
  variable so the two stacks don't collide.
- The drain benchmark bulk-inserts the backlog (`COPY`) and pipelines one XADD per job, leaving
  the same state `POST /jobs` would, so the submitter never limits throughput. The latency
  benchmark goes through the real API at a fixed open-loop rate (200 jobs/s). Results also
  cover `cpu` jobs of 60,000 SHA-256 rounds (about 10 ms).
- The side-effect ledger is a `ledger` handler plus a `job_executions` table (migration 0002).
  A row is written when the handler starts and stamped when it finishes, so cut-short
  executions are counted too.
- Recovery is reported two ways: kill to lease reaped, and kill to job claimed again. The
  second includes any wait for a free worker. The first full chaos run offered more work than 4
  workers could do, so its re-claims waited tens of seconds behind the backlog. The defaults
  now keep the pool at about half load (400 jobs over 60 s, 0.1-0.5 s each).
- Chaos submissions carry an `Idempotency-Key` so a retried POST can't create a second job,
  which is what makes the "no phantom jobs" invariant checkable.
- `results.md` tables are regenerated between `BEGIN`/`END` markers by `benchmarks/report.py`;
  the method and discussion around them are hand-written. Raw JSON stays in the ignored
  `benchmarks/raw/`.
- `chaos-bench.yml` runs the chaos suite, a reduced benchmark (1/2/4 workers, one run each), and
  a smaller chaos run, then uploads `benchmarks/raw/` (including a rendered `results-ci.md`).

### Milestone 6: Kubernetes (kind)

**Tasks**
- [x] kind cluster config (single node, port mappings) and a local image loaded with
      `kind load docker-image`.
- [x] Manifests via kustomize: Postgres StatefulSet + PVC, Redis Deployment, a migration
      Job, API Deployment + Service, worker Deployment, maintenance Deployment, ConfigMap,
      and a Secret built from a committed example.
- [x] Readiness and liveness probes (`/health` for the API, a heartbeat-file or metrics
      probe for workers), `terminationGracePeriodSeconds` set above the drain timeout, and
      resource requests and limits.
- [x] `make k8s-up`, `k8s-scale N=4`, `k8s-chaos` (deletes a random worker pod during load),
      and `k8s-down` (deletes the cluster).

**Done when**
- [x] Jobs submitted through a port-forwarded API complete inside the cluster.
- [x] `kubectl scale deploy/worker --replicas=4` increases throughput.
- [x] Deleting a worker pod mid-job still leaves every job completed.
- [x] `make k8s-down` removes the cluster and `kind get clusters` is empty.

**Implementation notes**
- Tested with kind v0.33. The single node maps the API's NodePort 30800 to `127.0.0.1:8800`;
  `make k8s-port-forward` (`kubectl port-forward svc/api 8000:8000`) works too, and the "done
  when" submission was checked through it.
- The ConfigMap is generated from `deploy/k8s/config.env` and the Secret from a git-ignored
  `deploy/k8s/secrets.env`, which `make k8s-up` copies from the committed `secrets.example.env`
  when it is missing. Both get hash suffixes, so a config change rolls the Deployments. A unit
  test keeps `config.env` + `secrets.example.env` in step with `.env.example` (minus the
  compose-only `REDIS_PORT`). The cluster uses a 15 s lease, 5 s heartbeat, and 2 s reaper, and
  a raised submission limit so the load script is never throttled.
- The migrate Job retries `alembic upgrade head` until Postgres accepts connections, so
  everything is applied in one go; `make k8s-up` deletes the finished Job first because a Job's
  pod template is immutable. API, worker, and maintenance pods hold in a `wait-for-migrations`
  init container until `alembic current` reports head.
- Probes: the API's readiness probe is `/health`, but its liveness probe is a TCP check, since a
  Redis outage shouldn't restart every API pod. Workers and maintenance use the metrics probe
  (`/metrics` on 9100/9101); that server runs on the process's own event loop, so an answer
  means the loop, and with it heartbeats, is still turning. `terminationGracePeriodSeconds` is
  45 for workers against a 30 s drain.
- App pods run as the image's non-root user with a read-only root filesystem and all
  capabilities dropped; `/tmp` and the model cache are per-pod `emptyDir`s (kind has no shared
  RWX volume). The kind node used about 1.35 GiB with 4 workers.
- The worker Deployment has no `replicas` field, so re-running `make k8s-up` keeps the count
  set by `make k8s-scale`. Re-running it after a code change reloads the same `workflow-engine:k8s`
  tag and restarts the app Deployments.
- `benchmarks/run_k8s.py` drives the cluster through the API only (new targets `k8s-load` and
  `k8s-port-forward`). `load`: 200 `sleep(0.2)` jobs went from 4.76 jobs/s with 1 worker to
  19.00 jobs/s with 4 (one run each). `k8s-chaos` force-deletes random worker pods
  (`--grace-period=0 --force`) while paced 2-5 s jobs run at about 60% load: in the recorded run
  4 deletions interrupted 3 jobs, each was reaped about 16 s after the kill (15 s lease plus the
  reaper pass) and claimed again right away, and all 40 jobs succeeded. A force-deleted pod
  still gets SIGTERM and about 2 s before SIGKILL, which is why the jobs outlast that window.
  A graceful run (120 jobs, 3 deletions) drained cleanly with no interrupted attempts.

### Milestone 7: Documentation and polish

**Tasks**
- [x] README: pitch, Mermaid architecture diagram, one-command quickstart
      (`make up && make demo`), API examples, a link to the benchmark results, a delivery
      guarantees section, design trade-offs (including why not use Postgres
      `SKIP LOCKED` alone, and why Streams over lists), and a testing strategy section.
- [x] `docs/delivery-guarantees.md`: "at-least-once execution, with each job's result saved
      exactly once", fencing tokens, idempotent
      handler patterns (upsert on natural keys, side-effect tables), and failure scenarios
      laid out step by step.
- [x] Demo GIF recorded with `vhs` (scripted terminal recording, so it can be reproduced).
- [x] A "Deploying to the cloud" guide, clearly worded as a guide (managed Postgres with
      pgvector, managed Redis, container platform, secrets, scaling). It should not claim
      anything has been deployed.

- [x] Pre-release history check across every commit on every branch: no secrets or
      credentials (scanned with `gitleaks`), no `.env` files, no locally excluded files, no
      attribution trailers in commit messages, and author identity is consistent. Fix
      anything found before the repo goes public.

**Done when**
- [x] A fresh clone gets a working demo by following only the README.
- [x] The Mermaid diagram renders on GitHub.
- [x] Every number in the README matches `benchmarks/results.md`.
- [x] The history check passes with zero findings.
- [ ] The repo owner switches the repo from private to public. This is a manual step, done
      only after everything above is complete.

**Implementation notes**
- `make demo` (new targets `demo` and `demo-stack`) starts the API, maintenance, and 3 workers in
  the normal compose project, with `demo/compose.demo.yml` shortening the lease to 5 s (heartbeat
  1 s, reaper 1 s). `demo/run.py` submits 4 `sleep(3)` jobs with Idempotency-Keys, replays one key,
  SIGKILLs the worker running a job, redraws a job board until the reaper hands that job to
  another worker, prints its attempt history (`lease_expired` under token 1, `succeeded` under
  token 2), and starts the killed container again. It talks to the API only, except for the
  `docker kill`. `demo-stack` writes compose output to a temp log and prints it only on failure.
- `make up` now creates `.env` from `.env.example` when it is missing (the same pattern `k8s-up`
  uses for `secrets.env`), so the README quickstart on a fresh clone is `make up && make demo`.
- The GIF is recorded with vhs from `docs/demo.tape`; the stack is started off camera so the
  recording skips the image build. `make demo-stack && vhs docs/demo.tape` re-records it.
- Docs: `docs/architecture.md` (processes, state machine, data model, Redis layout, the path of a
  job), `docs/delivery-guarantees.md`, and `docs/deploying-to-the-cloud.md`, which opens by saying
  that nothing has been deployed and that the provider names are untested examples.
- The README's Mermaid diagram was checked by rendering it with Mermaid 11.4.1 in headless
  Chrome (no parse errors); labels use only `<br/>`, which GitHub's sanitizer keeps. Every README
  number is taken from `benchmarks/results.md`; the kind throughput numbers stay in this file only.
- mypy now also checks `demo/`, and `tests/unit/test_demo.py` covers the demo's helpers.
- History check: `gitleaks git --log-opts=--all` found nothing in any commit; no `.env`,
  `secrets.env`, `.cache/`, or locally excluded file was ever committed; no commit message has an
  attribution trailer; every commit has the same author and committer identity. (`gitleaks dir`
  on the working tree flags only the git-ignored local `.env`.)

---

## 4. Decisions log

These questions came up during planning and are now settled.

| # | Topic | Decision |
|---|---|---|
| 1 | Rate limiting | **Both.** Execution is throttled per job type (jobs are delayed, never rejected), and submissions over a per-client (API key) limit get **429** with `Retry-After`. |
| 2 | Delivery guarantee wording | **"At-least-once execution, with each job's result saved exactly once."** Used everywhere the guarantee is described. |
| 3 | Benchmark unit | **One worker = one process running one job at a time.** |
| 4 | Priorities | **Three fixed levels** (`high`, `normal`, `low`), each with its own Redis stream. |
| 5 | Embedding search | **Added:** `GET /embeddings/search` (Milestone 4). |
| 6 | Repo visibility | **Private until Milestone 7 and the history check are complete.** The owner then makes it public by hand. |
| 7 | CI scope | Every push runs **lint, unit, and integration** tests. Chaos tests and benchmarks run **locally or by manual `workflow_dispatch` only**. |

### Adopted additions
- **Per-job timeout** (configured on the handler) so a hung handler can't hold a lease
  forever through heartbeats.
- **A side-effect ledger for the chaos tests**, so duplicate executions are *measured*
  rather than assumed.
- **Property-based tests** (Hypothesis) for the state machine and the backoff bounds.
- **`mypy --strict`** on `src/` in CI.
- **Jaeger behind a compose profile** so the default `make up` stays light on memory.
