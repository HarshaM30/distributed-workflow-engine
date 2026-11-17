# Delivery guarantees

> **At-least-once execution, with each job's result saved exactly once.**

This page explains what that sentence promises, how the engine keeps the promise, what it asks
of handler authors, and how each failure plays out.

## What is promised

| | Promised? | Why |
|---|---|---|
| Every submitted job reaches a terminal state (`succeeded`, `dead`, or `cancelled`) | Yes | Postgres holds every job, and the maintenance loops repair anything Redis or a worker loses. |
| A job's outcome is committed once and never overwritten | Yes | Every write after a claim is fenced by the lease token. |
| A handler runs exactly once | **No** | A worker can die after the handler has done its work but before the result is saved. Another worker must then run it again. |
| A job submitted twice with the same `Idempotency-Key` creates one job | Yes | The key is a unique row in Postgres, written in the same transaction as the job. |

No system whose workers can crash can promise exactly-once *execution* without the handler's
cooperation, because the side effect and the "done" marker can't be written atomically to two
different systems. What this engine promises instead is that duplicates are bounded to crash
and lease-loss cases, that they never produce two results, and that they are visible: every
attempt is a row in `job_attempts`.

## The mechanism

### 1. Redis messages are hints; the claim decides

A stream entry only says "job X might be ready". The worker turns it into ownership with one
conditional update:

```sql
UPDATE jobs
   SET status = 'running', lease_owner = :worker, lease_token = lease_token + 1,
       lease_expires_at = now() + :lease, attempts = attempts + 1
 WHERE id = :job_id AND status = 'queued'
RETURNING lease_token;
```

Postgres applies one of two competing updates first; the second sees `status = 'running'` and
matches no row. No row means the entry was stale or a duplicate, and the worker ACKs it and
moves on. Because of this, sending a hint twice is always harmless, which lets the sweeper,
reaper, and scheduler re-send freely.

The claim also inserts a `job_attempts` row carrying the same `lease_token`.

### 2. Leases and fencing tokens

The `lease_token` returned by the claim is the worker's fencing token. Every later write by that
worker includes it:

```sql
-- heartbeat, every WORKER_HEARTBEAT_SECONDS
UPDATE jobs SET lease_expires_at = now() + :lease
 WHERE id = :job_id AND lease_token = :mine AND status = 'running'
RETURNING cancel_requested;

-- completion (failure, retry, dead-letter, cancel, and release look the same)
UPDATE jobs SET status = 'succeeded', result = :result, finished_at = now(), ...
 WHERE id = :job_id AND lease_token = :mine AND status = 'running';
```

If the reaper has taken the job back and another worker has claimed it, the token has moved on,
and these updates match nothing:

- A heartbeat that matches nothing tells the worker it has lost the lease, so it cancels the
  handler instead of wasting time on a job someone else owns.
- A completion that matches nothing is discarded. The new owner's result stands.

The token only ever increases, so an old owner can never match again. This is what keeps one
terminal outcome per job even when a handler runs twice.

### 3. Everything else lives in Postgres

- **Commit, then XADD.** The API commits the job before it sends the hint. If it dies in between,
  the job sits in `queued` without a hint until the sweeper re-sends it.
- **Timers are rows.** Delays and retry backoff set `run_at` on a `pending` job; the scheduler
  promotes due jobs. Redis holds no timers to lose.
- **Repair loops.** The reaper returns jobs with expired leases to `queued` (or to `dead` once
  attempts run out). The sweeper re-sends `queued` jobs that have sat untouched for
  `SWEEPER_STALE_SECONDS`, and uses `XAUTOCLAIM` to clear stream entries left pending by dead
  consumers.

## Writing idempotent handlers

Anything a handler does before its result is committed can happen twice. These patterns make
that safe.

**Upsert on a natural key.** Derive the key from the job, not from the attempt, so a second run
writes the same rows. `embed_texts` stores one row per input text, unique on
`(job_id, item_index)`, with `INSERT … ON CONFLICT DO UPDATE`, and commits per batch. A retry
skips what the earlier attempt already stored and resumes where it stopped, and two overlapping
runs write the same rows; the row count stays at the number of texts.

**Record side effects in a table keyed by the job.** Before doing something that can't be
repeated, check a table for a row with the job id; write the row in the same transaction as the
effect, or with a unique constraint that rejects the second insert. The `ledger` handler used in
the chaos tests does a version of this to count executions: it writes a row when it starts and
stamps it when it finishes, and duplicates are `executions - jobs`.

**Pass the job id downstream.** When calling an external service that supports idempotency keys
(payment APIs, for example), use `ctx.job_id` as the key. The second call returns the first
call's result instead of repeating the effect.

**Prefer overwriting to appending.** "Set the thumbnail for image 42" is idempotent; "append a
thumbnail to image 42's list" is not.

**Use the timeout.** Each handler has a `timeout_seconds`. A hung handler is cancelled and its
attempt counted as failed, so it can't hold a lease forever through heartbeats.

## Failure scenarios, step by step

Each scenario is covered by an integration test or the chaos suite.

### The API dies between commit and XADD

1. The job row commits as `queued`.
2. The process dies before `XADD`, so no worker hears about the job.
3. After `SWEEPER_STALE_SECONDS` the sweeper sees a `queued` job whose `updated_at` is old,
   bumps `updated_at`, and XADDs a hint.
4. A worker claims it as usual. The job ran late, but it ran once.

The same path covers an XADD that fails because Redis is briefly down: the API logs a warning
and still returns 201.

### The same stream entry is delivered twice

1. Worker A reads the entry and claims the job (`queued → running`, token 1).
2. Worker B gets the same entry, for example from a sweeper re-send or `XAUTOCLAIM`.
3. B's claim finds `status = 'running'`, matches no row, and B ACKs the entry and drops it. The
   handler runs once.

### A worker is killed mid-job (`kill -9`, OOM, lost machine)

1. Worker A claims the job (token 1) and starts the handler. Heartbeats extend the lease.
2. A dies. Heartbeats stop; its stream entry stays un-ACKed.
3. The lease expires `WORKER_LEASE_SECONDS` after the last heartbeat. On its next pass the reaper
   marks A's attempt `lease_expired`, counts it as a failed attempt, sets the job back to
   `queued`, and XADDs a hint. If no attempts are left, the job goes to `dead` with reason
   `lease_expired` instead.
4. Worker B claims it (token 2) and runs the handler from the start. **This is the duplicate
   execution:** whatever A did before dying may happen again.
5. B's completion matches token 2 and commits.

Recovery takes at most the lease plus one reaper interval. In the chaos run (4 s lease, 1 s
reaper) the p50 from kill to reaped was 4.37 s; `make demo` shows the same thing with a 5 s
lease.

### A zombie worker comes back late

1. Worker A claims the job (token 1), then stalls: a long GC pause, a frozen VM, or a network
   partition from Postgres.
2. Its lease expires; the reaper requeues the job; worker B claims it (token 2).
3. A wakes up and finishes the handler. Its completion runs
   `… WHERE lease_token = 1 AND status = 'running'`, matches nothing, and is discarded.
4. B finishes and commits under token 2. The job has one result, B's.

If A wakes before finishing, its next heartbeat matches nothing, and it cancels its own handler.

### Redis loses everything

A restart without persistence or a `FLUSHALL` removes every stream, consumer group, pending
entry, and token bucket.

1. Workers get `NOGROUP` on their next read, recreate the groups, and keep reading (from empty
   streams).
2. Running jobs are unaffected: leases and heartbeats live in Postgres. Their final `XACK` fails
   harmlessly.
3. `queued` jobs have lost their hints. The sweeper re-sends each one once it has been untouched
   for `SWEEPER_STALE_SECONDS`.
4. `pending` jobs never depended on Redis; the scheduler promotes them when due.
5. The rate limiters fail open (and log it) until Redis is back.

No job is lost; some start later than they would have.

### A worker is told to stop (SIGTERM)

1. The worker stops reading new entries.
2. In-flight jobs get `WORKER_SHUTDOWN_GRACE_SECONDS` to finish and commit normally.
3. A job still running after that is *released*: a fenced update sets it back to `queued`, marks
   the attempt `released`, and gives the attempt back (it doesn't count against
   `max_attempts`). The worker XADDs a hint so the next worker doesn't wait for the sweeper.
4. The worker exits 0.

### A handler fails

1. The handler raises or times out.
2. A fenced update records the failed attempt. If attempts remain, the job goes to `pending` with
   `run_at = now + backoff`, where backoff is full jitter: a random delay between 0 and
   `min(RETRY_MAX_DELAY_SECONDS, RETRY_BASE_DELAY_SECONDS * 2^(attempt-1))`.
3. The scheduler promotes it when due, and it is claimed again with a new token.
4. When attempts run out, the job becomes `dead` and a `dead_letters` row is written in the same
   transaction. `POST /dlq/{id}/requeue` resets attempts and queues it again.

An unknown job type, or a payload that no longer validates, goes straight to `dead` with reason
`non_retryable`, since retrying can't help.

### The client retries a submission

1. The client POSTs with `Idempotency-Key: abc`, and the response is lost.
2. It retries with the same key and body. The key's row already exists, so the API returns the
   original job with 200 and doesn't spend a rate-limit token.
3. Concurrent first submissions race on `INSERT … ON CONFLICT`; one wins and the rest get its job.
4. The same key with a *different* body gets 409.

### A job is cancelled while running

1. `POST /jobs/{id}/cancel` sets `cancel_requested` and returns 202.
2. The worker's next heartbeat reads the flag and cancels the handler task.
3. A fenced update moves the job to `cancelled`.
4. If the worker died first, the reaper sees the flag on the expired lease and ends the job as
   `cancelled` rather than requeueing it.

### The maintenance leader dies

1. Every maintenance process tries `pg_try_advisory_lock`; one holds it, and the others wait.
2. The leader dies, so its Postgres session ends and the lock is released.
3. A standby gets the lock within `MAINTENANCE_LEADER_RETRY_SECONDS` and resumes the loops.
   Nothing is lost while no loop runs; repairs simply happen a little later.

## How it's verified

- **Integration tests** (every push): concurrent idempotent submissions, a duplicate stream
  delivery, a zombie worker's late completion rejected by the fencing token, `kill -9` and
  SIGTERM on real worker processes, and a Redis wipe followed by every job completing.
- **Chaos suite and benchmark** (by hand): random SIGKILLs and a Redis restart under load, with a
  ledger that counts every execution. The committed run in
  [benchmarks/results.md](../benchmarks/results.md) had 0 lost jobs, one terminal state per job,
  and 9 duplicate executions, all of them runs cut short by a kill.
