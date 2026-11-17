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
5. **Priorities map to separate streams.** Redis Streams have no priority ordering, so each
   priority level (`high`, `normal`, `low`) gets its own stream, and workers poll them in
   that order. The API takes a priority value and maps it to one of the three levels.
6. **Maintenance loops run in their own process.** The reaper, sweeper, and scheduler run in
   a `maintenance` process that holds a Postgres advisory lock (leader election). You can
   run several copies safely, and only one is active at a time.
7. **Rate limiting happens at dispatch.** A token bucket per job type, implemented as an
   atomic Redis Lua script, gates when a worker may *start* a job of that type. A worker
   that finds the bucket empty puts the job back with a short delay. If Redis is down, the
   limiter fails open and logs a warning. See question Q1.

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
│   │   ├── rate_limit.py         # token bucket wrapper
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
│   │   └── routes/{jobs,dlq,health}.py
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
├── deploy/k8s/                   # kind config + kustomize manifests
├── docs/
│   ├── architecture.md
│   ├── delivery-guarantees.md
│   └── demo.gif
├── .github/workflows/ci.yml
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
- [ ] `uv init` with Python 3.12 and a `src/` layout. Add runtime deps (fastapi, uvicorn,
      sqlalchemy[asyncio], psycopg[binary], alembic, pydantic-settings, redis, structlog)
      and dev deps (pytest, pytest-asyncio, httpx, ruff, mypy).
- [ ] Configure ruff (lint + format) and pytest markers (`unit`, `integration`, `chaos`) in
      `pyproject.toml`.
- [ ] `config.py` with typed settings for every variable in `.env.example`.
- [ ] SQLAlchemy models for all five tables, plus the `JobStatus` enum and transition table.
- [ ] Alembic setup and an initial migration (including `CREATE EXTENSION vector`).
- [ ] `docker-compose.yml`: `pgvector/pgvector:pg16` and `redis:7-alpine` with healthchecks,
      named volumes, and memory limits. API/worker services go behind a profile.
- [ ] Multi-stage `Dockerfile` (uv-based, non-root user, arm64- and amd64-compatible).
- [ ] FastAPI app with `GET /health`, which checks Postgres and Redis and returns 200 or 503
      with details for each component.
- [ ] `Makefile` targets: `install`, `up`, `down`, `migrate`, `api`, `worker`,
      `maintenance`, `lint`, `fmt`, `test-unit`, `test-integration`, `test-chaos`, `bench`.
- [ ] GitHub Actions `ci.yml`: ruff check + format check, then unit tests, then integration
      tests against Postgres (pgvector image) and Redis service containers. Cache uv.

**Done when**
- [ ] `make up && make migrate` on a clean checkout creates all tables.
- [ ] `curl localhost:8000/health` returns 200 with both components `ok`, and returns 503
      when Redis is stopped.
- [ ] `alembic downgrade base && alembic upgrade head` round-trips cleanly.
- [ ] `make lint` and `make test-unit` pass locally.
- [ ] CI is green on the first push and runs all three stages.

### Milestone 2: Submission API, dispatch, and a basic worker

**Tasks**
- [ ] `X-API-Key` dependency. Keys come from `API_KEYS` and are compared in constant time.
      Applies to every route except `/health` and `/metrics`.
- [ ] Pydantic schemas: job type must be a registered handler, payload validated against
      that handler's own payload model, priority enum, optional `run_at`, size limits.
- [ ] `POST /jobs` (returns 201 with the job, or 200 for an idempotent replay).
- [ ] `GET /jobs/{id}` with attempt history.
- [ ] `GET /jobs?status=&type=&limit=&cursor=` with keyset pagination.
- [ ] Idempotency: `Idempotency-Key` header scoped per API key, with a SHA-256 hash of the
      canonical request body. Same key and same body returns the original job. Same key
      with a different body returns 409. Handles concurrent duplicates with
      `INSERT … ON CONFLICT`. Expired keys are cleaned up by maintenance.
- [ ] Streams producer: XADD to `jobs:{priority}` after commit. Consumer group created at
      startup with `MKSTREAM`.
- [ ] Handler registry plus the `sleep`, `cpu`, and `flaky` handlers. `cpu` runs in a
      process or thread executor so it doesn't block the event loop.
- [ ] Worker loop: XREADGROUP, then CAS claim in Postgres, then execute, then write the
      result and an attempt row, then XACK. Single attempt only in this milestone.

**Done when**
- [ ] Requests without a key or with a wrong key get 401. `/health` stays open.
- [ ] Invalid input (unknown type, bad payload, oversized body) gets 422 with a clear error.
- [ ] Submitting a `sleep` job leads to `succeeded` in `GET /jobs/{id}` with one attempt.
- [ ] An integration test fires 20 concurrent POSTs with the same Idempotency-Key and gets
      exactly one job row.
- [ ] Reusing a key with a different body returns 409.
- [ ] Delivering the same stream message twice runs the handler only once, because the
      CAS claim rejects the second delivery (integration test).

### Milestone 3: Reliability

**Tasks**
- [ ] Retries: on failure, set `run_at = now + backoff(attempt)` using full jitter capped at
      `RETRY_MAX_DELAY_SECONDS`, and set status to `pending`. `max_attempts` comes from the
      job, falling back to the handler default and then the global default.
- [ ] Dead-lettering: when attempts run out, set status to `dead` and insert a
      `dead_letters` row in the same transaction.
- [ ] `GET /dlq` (paginated) and `POST /dlq/{id}/requeue`, which resets attempts, sets
      `requeued_at`, and re-enqueues the job.
- [ ] Leases: claim sets `lease_expires_at`, a heartbeat task renews it every
      `WORKER_HEARTBEAT_SECONDS`, and every write checks the fencing token.
- [ ] Reaper: `running` jobs with an expired lease go back to `queued` (counted as a failed
      attempt with error `lease_expired`), or to `dead` if attempts are exhausted. Uses
      `FOR UPDATE SKIP LOCKED`.
- [ ] Sweeper: `queued` jobs older than a threshold get re-XADDed. This is safe because of
      the CAS claim. Also uses XAUTOCLAIM to clean up stale pending-entry-list entries.
- [ ] Scheduler: due `pending` jobs move to `queued` and get XADDed, in batches.
- [ ] Maintenance process with Postgres advisory-lock leader election.
- [ ] Graceful shutdown: on SIGTERM, stop reading new messages, let in-flight jobs finish
      within the grace period, and if a job is still running after that, release its lease
      (back to `queued`) instead of abandoning it.
- [ ] Priorities: workers poll `high`, then `normal`, then `low`.
- [ ] Delayed jobs: `run_at` on submission.
- [ ] Cancellation via `POST /jobs/{id}/cancel`. A `pending` or `queued` job becomes
      `cancelled` immediately. A `running` job gets `cancel_requested` set, the heartbeat
      sees it and cancels the handler task, and the final state is `cancelled`. Terminal
      jobs return 409.
- [ ] Token-bucket rate limiter (Lua script, per job type, configured by `RATE_LIMITS`).

**Done when**
- [ ] A `flaky` job with p=1.0 and max_attempts=3 ends up `dead` with exactly 3 attempts.
      Attempt timestamps show growing, jittered gaps.
- [ ] A requeued DLQ job runs again and can succeed.
- [ ] After `kill -9` on a worker mid-job, the job is reaped and completed by another worker
      within lease time plus the reaper interval.
- [ ] A zombie worker's late completion is rejected by the fencing token (integration test).
- [ ] After `FLUSHALL` on Redis, every non-terminal job still completes thanks to the
      sweeper (integration test).
- [ ] After SIGTERM, an in-flight 5-second sleep job finishes and the worker exits 0.
- [ ] With a mixed backlog, high-priority jobs start before low-priority ones.
- [ ] A job with `run_at` 10s in the future doesn't start before then.
- [ ] Cancel works in each state (pending, queued, running, terminal) and each case is
      tested.
- [ ] A rate limit of 5/s on a type caps execution starts at roughly 5/s, within tolerance.

### Milestone 4: Observability and the ML job

**Tasks**
- [ ] structlog JSON output with `job_id`, `job_type`, `attempt`, `worker_id`, `trace_id`
      bound in context, and a request-ID middleware on the API.
- [ ] OpenTelemetry: FastAPI, SQLAlchemy, and Redis auto-instrumentation. Trace context is
      carried inside the stream message so a job's trace runs from the API through the
      worker. OTLP exporter goes to Jaeger all-in-one, behind a `tracing` compose profile.
- [ ] Prometheus metrics: `jobs_submitted_total`, `jobs_completed_total{type,status}`,
      `job_duration_seconds` histogram, `job_e2e_latency_seconds` histogram, `queue_depth`
      gauge, `retries_total`, `dlq_size`, `leases_reaped_total`, `sweeper_requeued_total`,
      `rate_limited_total`. API serves `/metrics`; workers serve it on `WORKER_METRICS_PORT`.
- [ ] `embed_texts` handler: payload is a list of texts (with a cap), embedded in batches
      with fastembed `BAAI/bge-small-en-v1.5` (384 dimensions). Upserts on
      `(job_id, item_index)` so retries stay idempotent. Model is cached in a volume.
- [ ] Optional nearest-neighbour endpoint `GET /embeddings/search?q=` to show the stored
      vectors are usable (see Q5).

**Done when**
- [ ] Every log line is valid JSON, and a single job's lines can be filtered by `job_id`.
- [ ] Jaeger shows one trace that spans the API request, enqueue, worker claim, and handler.
- [ ] `/metrics` scrapes cleanly and the counters move when jobs run.
- [ ] An `embed_texts` job with 100 texts stores 100 vectors. Retrying it leaves the row
      count at 100.
- [ ] The worker image stays reasonably small with no PyTorch in the dependency tree
      (verified with `uv tree`).

### Milestone 5: Benchmarks and chaos testing

**Tasks**
- [ ] Load generator: submits N `sleep(0)` or short `cpu` jobs and measures end-to-end
      latency (`finished_at - created_at`) from Postgres.
- [ ] Throughput runs at 1, 2, 4, and 8 workers, with 3 runs each and the median reported.
- [ ] Chaos harness: randomly `docker kill` workers during a run, restart Redis mid-run,
      then check the invariants: every job is terminal, none lost, and
      `succeeded + dead == submitted`.
- [ ] Duplicate-execution counter: handlers record each execution in a side table, so
      duplicates = executions − jobs.
- [ ] Recovery-time measurement: time from worker kill to the orphaned job being claimed
      again.
- [ ] `tests/chaos/` pytest suite, a smaller version of the harness, marked `chaos` and run
      via `make test-chaos`.
- [ ] `benchmarks/results.md`: machine specs (chip, RAM, OS, Docker resource limits), the
      method, tables, and a discussion of the results.

**Done when**
- [ ] `make bench` reproduces the results tables in a single command.
- [ ] Chaos runs report **0 lost jobs** and exactly one terminal state per job.
- [ ] Duplicate executions are reported honestly (expected to be above 0 under kills) and
      explained.
- [ ] `results.md` is committed with specs, method, and numbers.

### Milestone 6: Kubernetes (kind)

**Tasks**
- [ ] kind cluster config (single node, port mappings) and a local image loaded with
      `kind load docker-image`.
- [ ] Manifests via kustomize: Postgres StatefulSet + PVC, Redis Deployment, a migration
      Job, API Deployment + Service, worker Deployment, maintenance Deployment, ConfigMap,
      and a Secret built from a committed example.
- [ ] Readiness and liveness probes (`/health` for the API, a heartbeat-file or metrics
      probe for workers), `terminationGracePeriodSeconds` set above the drain timeout, and
      resource requests and limits.
- [ ] `make k8s-up`, `k8s-scale N=4`, `k8s-chaos` (deletes a random worker pod during load),
      and `k8s-down` (deletes the cluster).

**Done when**
- [ ] Jobs submitted through a port-forwarded API complete inside the cluster.
- [ ] `kubectl scale deploy/worker --replicas=4` increases throughput.
- [ ] Deleting a worker pod mid-job still leaves every job completed.
- [ ] `make k8s-down` removes the cluster and `kind get clusters` is empty.

### Milestone 7: Documentation and polish

**Tasks**
- [ ] README: pitch, Mermaid architecture diagram, one-command quickstart
      (`make up && make demo`), API examples, a link to the benchmark results, a delivery
      guarantees section, design trade-offs (including why not use Postgres
      `SKIP LOCKED` alone, and why Streams over lists), and a testing strategy section.
- [ ] `docs/delivery-guarantees.md`: at-least-once execution, fencing tokens, idempotent
      handler patterns (upsert on natural keys, side-effect tables), and failure scenarios
      laid out step by step.
- [ ] Demo GIF recorded with `vhs` (scripted terminal recording, so it can be reproduced).
- [ ] A "Deploying to the cloud" guide, clearly worded as a guide (managed Postgres with
      pgvector, managed Redis, container platform, secrets, scaling). It should not claim
      anything has been deployed.

**Done when**
- [ ] A fresh clone gets a working demo by following only the README.
- [ ] The Mermaid diagram renders on GitHub.
- [ ] Every number in the README matches `benchmarks/results.md`.

---

## 4. Questions and suggested changes

**Q1. Where should rate limiting apply?** The plan throttles at *execution* (backpressure,
with jobs delayed rather than rejected). The alternative is rejecting *submissions* with 429
and `Retry-After`. Execution-side throttling fits a job queue better, for example protecting
a downstream API, but I could do both. Which do you want?

**Q2. Exactly-once from the client's view.** I'd describe this as "at-least-once execution,
with exactly-once *result commitment* through fencing tokens and idempotent handlers". That's
more accurate than saying "exactly once" alone, and it holds up better in interviews. OK?

**Q3. What counts as a "worker" in benchmarks?** I suggest 1 worker = 1 process with
concurrency 1, so scaling from 1 to 8 is a clean comparison. I'd add one extra run with
in-process concurrency for comparison.

**Q4. Priority levels.** Three fixed levels (`high`, `normal`, `low`) instead of arbitrary
integers, because Streams can't order by priority. OK?

**Q5. Embedding search endpoint.** I suggest adding `GET /embeddings/search` (outside the
spec) so the ML use case covers storage *and* retrieval. It's small and makes a better
demo. Add it?

**Q6. Repo visibility.** The repo is private for now. Recruiters can't see a private repo, so
plan to make it public once it's polished, probably after Milestone 5 or 7.

**Q7. CI minutes.** Private repos get a limited number of free Actions minutes each month.
Per-push CI covers lint, unit, and integration tests. Chaos tests and benchmarks run only
through `workflow_dispatch` and locally, because they're slow and noisy on shared runners.

**Suggested additions**
- **Per-job timeout** (configured on the handler) so a hung handler can't hold a lease
  forever through heartbeats.
- **A side-effect ledger for the chaos tests**, so duplicate executions are *measured*
  rather than assumed.
- **Property-based tests** (Hypothesis) for the state machine and the backoff bounds.
- **`mypy --strict`** on `src/` in CI. It's cheap and signals quality.
- **Keep Jaeger behind a compose profile** so the default `make up` stays light on memory.
