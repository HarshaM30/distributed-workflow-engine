# Deploying to the cloud: a guide

> This is a guide to how you *could* run the engine on managed cloud services. The project itself
> has only been run locally: with Docker Compose and on a local kind cluster. Nothing here has
> been deployed to a cloud provider, and the provider names below are examples, not tested
> configurations. Check each provider's current documentation before relying on a detail.

## What you need

| Piece | Requirement |
|---|---|
| PostgreSQL | Version 16 (the version tested), with the `vector` extension (pgvector) available |
| Redis | Redis 7 compatible, with **Streams and consumer groups** (`XADD`, `XREADGROUP`, `XAUTOCLAIM`) and **Lua scripting** (`EVAL`/`EVALSHA`) |
| Container runtime | Long-running containers for the API, the workers, and maintenance, plus a one-off task for migrations |
| Secrets | A place to keep `POSTGRES_PASSWORD` (or `DATABASE_URL`), `REDIS_URL`, and `API_KEYS` |

One image (the repository's `Dockerfile`) serves every role; the command picks it:

| Role | Command |
|---|---|
| Migrations (run once per release) | `alembic upgrade head` |
| API | `python -m workflow_engine.api` |
| Worker | `python -m workflow_engine.worker` |
| Maintenance | `python -m workflow_engine.maintenance` |

## 1. Managed PostgreSQL with pgvector

Most managed Postgres services can enable pgvector, for example Amazon RDS or Aurora PostgreSQL,
Google Cloud SQL or AlloyDB, Azure Database for PostgreSQL (Flexible Server), Neon, and
Supabase. The first migration runs `CREATE EXTENSION vector`, so either let the migration user
create extensions or create it once by hand as an admin.

Things to plan for:

- **Connections.** Each process opens up to `DB_POOL_SIZE + DB_MAX_OVERFLOW` connections (10 + 5
  by default). A worker running one job at a time needs only a few, so lower the pool size for
  workers (2 + 2 is plenty) and do the arithmetic against the server's `max_connections`.
- **Connection poolers.** If you put PgBouncer (or a provider's pooler) in front, keep the
  **maintenance** process on a direct connection or a session-mode pool: leader election uses a
  session-level advisory lock, which transaction pooling can't hold. In transaction mode also
  check how the pooler handles the driver's prepared statements.
- **Vacuum.** The `jobs` and `job_attempts` tables are update-heavy. Default autovacuum is usually
  fine; watch table bloat at high throughput, and consider archiving old terminal jobs.
- **Backups and high availability.** Postgres is the source of truth, so turn on automated
  backups and a standby. After a failover, workers reconnect, and any lease that lapsed during the
  outage is reaped as usual.

## 2. Managed Redis

Options include Amazon ElastiCache or MemoryDB, Google Memorystore, Azure Cache for Redis, and
Redis Cloud. Before choosing, confirm that the service supports Streams with consumer groups and
Lua scripts; some serverless or "Redis-compatible" offerings restrict one or the other.

Redis holds no state that can't be rebuilt, so persistence, replication, and failover are about
*speed of recovery*, not correctness. A full Redis loss means the sweeper re-sends every queued
job, which takes up to `SWEEPER_STALE_SECONDS`. A small single-node instance is a reasonable
start. Set an eviction policy that doesn't silently drop stream keys (`noeviction`, or size the
instance so eviction never happens); `STREAM_MAXLEN` keeps the streams bounded either way.

If the service requires TLS, use a `rediss://` URL in `REDIS_URL`.

## 3. A container platform

Any platform that runs long-lived containers works. Some patterns:

- **Kubernetes (EKS, GKE, AKS).** The manifests in `deploy/k8s/` are a starting point. Replace
  the in-cluster Postgres StatefulSet and Redis Deployment with the managed services, push the
  image to a registry, and swap the generated Secret for your secret store (for example the
  External Secrets Operator). Keep the probes, resource limits, and the worker's
  `terminationGracePeriodSeconds`.
- **Amazon ECS on Fargate.** One service per role, plus a one-off task for migrations that your
  release pipeline runs before updating the services.
- **Google Cloud Run.** The API fits Cloud Run services well. Workers and maintenance are
  background loops, not request handlers, so they need an always-on CPU option (for example
  Cloud Run worker pools, or a service with CPU always allocated and a minimum instance count).
  Check the current product options.
- **Fly.io, Render, Railway, and similar.** Run the API as a web service and the worker and
  maintenance roles as background workers.

Wherever they run, give workers a stop timeout longer than `WORKER_SHUTDOWN_GRACE_SECONDS`, so a
deploy lets in-flight jobs finish (the kind manifests use 45 s against a 30 s drain). A worker
that is killed anyway is still safe; its jobs come back after the lease expires.

## 4. Configuration and secrets

All settings come from environment variables (see `.env.example` for the full list). Don't bake
a `.env` into the image.

| Kind | Variables | Where to keep them |
|---|---|---|
| Secrets | `POSTGRES_PASSWORD` or `DATABASE_URL`, `REDIS_URL` (if it holds a password), `API_KEYS` | The platform's secret manager (AWS Secrets Manager or SSM, Google Secret Manager, Azure Key Vault, Kubernetes Secrets) |
| Config | `POSTGRES_HOST`, `WORKER_LEASE_SECONDS`, `RATE_LIMITS`, `LOG_LEVEL`, ... | Plain environment variables or a ConfigMap |

Generate long random API keys and rotate them by listing old and new keys in `API_KEYS` during
the change-over. Set `APP_ENV=production` and keep `LOG_JSON=true` so a log pipeline can parse
the output.

## 5. Networking and security

- Expose only the API, behind the platform's load balancer with TLS. Workers and maintenance
  need no inbound traffic except metrics scraping.
- Keep Postgres and Redis on private networking, reachable only from the app's containers.
- `/health` and `/metrics` are unauthenticated by design. Point the load balancer's health check
  at `/health`, and don't route `/metrics` from the public internet.
- The image runs as a non-root user and works with a read-only root filesystem, given writable
  `/tmp` and model-cache directories.

## 6. Scaling

- **Workers** are the main knob. One worker is one process running one job at a time
  (`WORKER_CONCURRENCY=1`); add replicas for throughput. Scale on queue depth: the API exports
  `queue_depth{status="queued"}`, which an autoscaler can read (for example KEDA with its
  Prometheus scaler, or a custom metric on ECS).
- **The API** is stateless; scale it on CPU or request rate.
- **Maintenance** needs one active copy. Run two for availability; the second waits on the
  advisory lock.
- **The bottleneck moves to Postgres.** The local benchmarks show per-job overhead flattening out
  as workers contend for one database (see [benchmarks/results.md](../benchmarks/results.md)).
  Size the database before adding many workers, and use the connection advice above.
- **Lease timing.** A shorter `WORKER_LEASE_SECONDS` recovers crashed jobs sooner but costs more
  heartbeat writes. Keep `WORKER_HEARTBEAT_SECONDS` at about a third of the lease, so one slow
  heartbeat doesn't lose it.
- **`embed_texts`** loads an ONNX model of about 65 MB on first use; give workers that run it
  enough memory (the compose and kind setups allow 512 MB) and a cache volume, or bake the model
  into the image.

## 7. Observability

- Ship stdout JSON logs to the platform's log service; filter by `job_id` or `trace_id`.
- Scrape `/metrics` from the API, and from workers and maintenance on their metrics ports, with a
  managed Prometheus or an agent that speaks the Prometheus format.
- Set `OTEL_ENABLED=true` and `OTEL_EXPORTER_OTLP_ENDPOINT` to an OTLP/HTTP collector to export
  traces.
- Useful alerts: `dlq_size` growing, `leases_reaped_total` rising (workers crashing),
  `queue_depth` of queued jobs growing for longer than a few minutes, and `/health` failing.

## Release checklist

1. Build the image and push it to a registry.
2. Run `alembic upgrade head` as a one-off task with the new image.
3. Roll the API, workers, and maintenance. Workers drain on SIGTERM.
4. Check `/health`, submit a `sleep` job, and confirm it reaches `succeeded`.
