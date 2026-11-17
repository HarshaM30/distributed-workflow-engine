"""Job state transitions. Every write a worker makes after claiming is fenced by lease_token."""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from redis.exceptions import RedisError
from sqlalchemy import delete, func, insert, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from workflow_engine.core.idempotency import IdempotencyConflictError, IdempotencyRequest
from workflow_engine.core.states import ACTIVE_STATUSES, AttemptStatus, JobStatus, Priority
from workflow_engine.db.models import DeadLetter, IdempotencyKey, Job, JobAttempt
from workflow_engine.db.session import SessionFactory
from workflow_engine.logging import get_logger

log = get_logger(__name__)

MAX_ERROR_CHARS = 4000


class Dispatcher(Protocol):
    async def enqueue(self, job_id: uuid.UUID, priority: Priority) -> str: ...


@dataclass(frozen=True)
class JobSpec:
    type: str
    payload: dict[str, Any]
    priority: Priority
    max_attempts: int
    run_at: datetime | None = None


@dataclass(frozen=True)
class Enqueue:
    """A job that became `queued` in a committed transaction and now needs an XADD."""

    job_id: uuid.UUID
    priority: Priority


@dataclass(frozen=True)
class SubmitResult:
    job: Job
    created: bool


@dataclass(frozen=True)
class Claim:
    job_id: uuid.UUID
    lease_token: int
    attempt: int
    max_attempts: int
    type: str
    payload: dict[str, Any]
    worker_id: str
    priority: Priority = Priority.NORMAL
    created_at: datetime | None = None  # for end-to-end latency metrics


class _ReplayError(Exception):
    """Internal: the idempotency key already exists, so roll back the new job."""


def resolve_max_attempts(requested: int | None, handler_default: int | None, default: int) -> int:
    """Job value, then handler default, then the global default."""
    for value in (requested, handler_default):
        if value is not None:
            return value
    return default


# --------------------------------------------------------------------------- submission


async def submit_job(
    session_factory: SessionFactory,
    dispatcher: Dispatcher,
    spec: JobSpec,
    *,
    idempotency: IdempotencyRequest | None = None,
    idempotency_ttl: timedelta = timedelta(hours=24),
) -> SubmitResult:
    """Insert the job (and idempotency key) in one transaction, then XADD after commit."""
    for _ in range(3):
        try:
            job = await _insert_job(session_factory, spec, idempotency, idempotency_ttl)
        except _ReplayError:
            assert idempotency is not None
            existing = await _load_replay(session_factory, idempotency)
            if existing is None:
                continue  # key vanished between the conflict and the read; try again
            return SubmitResult(job=existing, created=False)
        if job.status == JobStatus.QUEUED:
            await enqueue_safely(dispatcher, job.id, job.priority)
        return SubmitResult(job=job, created=True)
    raise RuntimeError("idempotency key kept changing during submission")


async def _insert_job(
    session_factory: SessionFactory,
    spec: JobSpec,
    idempotency: IdempotencyRequest | None,
    ttl: timedelta,
) -> Job:
    now = datetime.now(UTC)
    due = spec.run_at is None or spec.run_at <= now
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "type": spec.type,
        "payload": spec.payload,
        "priority": spec.priority,
        "status": JobStatus.QUEUED if due else JobStatus.PENDING,
        "max_attempts": spec.max_attempts,
    }
    if spec.run_at is not None:
        values["run_at"] = spec.run_at
    async with session_factory() as session, session.begin():
        job = (await session.execute(insert(Job).values(**values).returning(Job))).scalar_one()
        if idempotency is not None:
            stmt = pg_insert(IdempotencyKey).values(
                api_key_id=idempotency.api_key_id,
                key=idempotency.key,
                request_hash=idempotency.request_hash,
                job_id=job.id,
                expires_at=func.now() + ttl,
            )
            # Concurrent duplicates block on the unique index until the winner commits.
            # An expired key is taken over in place; a live one makes this a replay.
            upsert = stmt.on_conflict_do_update(
                index_elements=[IdempotencyKey.api_key_id, IdempotencyKey.key],
                set_={
                    "request_hash": stmt.excluded.request_hash,
                    "job_id": stmt.excluded.job_id,
                    "created_at": func.now(),
                    "expires_at": stmt.excluded.expires_at,
                },
                where=IdempotencyKey.expires_at <= func.now(),
            ).returning(IdempotencyKey.job_id)
            if (await session.execute(upsert)).scalar_one_or_none() is None:
                raise _ReplayError
        return job


async def _load_replay(session_factory: SessionFactory, idem: IdempotencyRequest) -> Job | None:
    async with session_factory() as session:
        row = (
            await session.execute(
                select(IdempotencyKey.request_hash, Job)
                .join(Job, Job.id == IdempotencyKey.job_id)
                .where(IdempotencyKey.api_key_id == idem.api_key_id, IdempotencyKey.key == idem.key)
            )
        ).one_or_none()
    if row is None:
        return None
    stored_hash, job = row
    if stored_hash != idem.request_hash:
        raise IdempotencyConflictError
    return job


async def enqueue_safely(dispatcher: Dispatcher, job_id: uuid.UUID, priority: Priority) -> None:
    """XADD after commit. Failure isn't fatal: the job stays queued and the sweeper re-adds it."""
    try:
        await dispatcher.enqueue(job_id, priority)
    except (RedisError, OSError) as exc:
        log.warning("enqueue_failed", job_id=str(job_id), error=type(exc).__name__)


# --------------------------------------------------------------------------- reads


async def get_job(
    session_factory: SessionFactory, job_id: uuid.UUID
) -> tuple[Job, list[JobAttempt]] | None:
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        if job is None:
            return None
        attempts = (
            await session.scalars(
                select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.id)
            )
        ).all()
    return job, list(attempts)


class InvalidCursorError(ValueError):
    pass


def encode_cursor(created_at: datetime, job_id: uuid.UUID) -> str:
    raw = json.dumps([created_at.isoformat(), str(job_id)]).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        created_at, job_id = json.loads(raw)
        parsed = datetime.fromisoformat(created_at)
        if parsed.tzinfo is None:
            raise ValueError("naive timestamp")
        return parsed, uuid.UUID(job_id)
    except (binascii.Error, ValueError, TypeError, UnicodeDecodeError) as exc:
        raise InvalidCursorError("invalid cursor") from exc


async def list_jobs(
    session_factory: SessionFactory,
    *,
    status: JobStatus | None = None,
    job_type: str | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> tuple[list[Job], str | None]:
    """Newest first, keyset-paginated on (created_at, id)."""
    stmt = select(Job).order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1)
    if status is not None:
        stmt = stmt.where(Job.status == status)
    if job_type is not None:
        stmt = stmt.where(Job.type == job_type)
    if cursor is not None:
        created_at, job_id = decode_cursor(cursor)
        stmt = stmt.where(tuple_(Job.created_at, Job.id) < tuple_(created_at, job_id))
    async with session_factory() as session:
        rows = list((await session.scalars(stmt)).all())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = encode_cursor(rows[-1].created_at, rows[-1].id)
    return rows, next_cursor


# --------------------------------------------------------------------------- worker side


async def claim_job(
    session_factory: SessionFactory, job_id: uuid.UUID, worker_id: str, lease_seconds: float
) -> Claim | None:
    """CAS claim: only a `queued` job can be claimed. None means stale/duplicate message."""
    async with session_factory() as session, session.begin():
        row = (
            await session.execute(
                update(Job)
                .where(Job.id == job_id, Job.status == JobStatus.QUEUED)
                .values(
                    status=JobStatus.RUNNING,
                    lease_owner=worker_id,
                    lease_token=Job.lease_token + 1,
                    lease_expires_at=func.now() + timedelta(seconds=lease_seconds),
                    attempts=Job.attempts + 1,
                )
                .returning(
                    Job.lease_token,
                    Job.attempts,
                    Job.max_attempts,
                    Job.type,
                    Job.payload,
                    Job.priority,
                    Job.created_at,
                )
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
        if row is None:
            return None
        lease_token, attempt, max_attempts, job_type, payload, priority, created_at = row
        await session.execute(
            insert(JobAttempt).values(
                job_id=job_id,
                attempt_no=attempt,
                worker_id=worker_id,
                lease_token=lease_token,
                status=AttemptStatus.RUNNING,
            )
        )
    return Claim(
        job_id,
        lease_token,
        attempt,
        max_attempts,
        job_type,
        payload,
        worker_id,
        priority,
        created_at,
    )


def _fenced(claim: Claim) -> Any:
    return (
        (Job.id == claim.job_id)
        & (Job.lease_token == claim.lease_token)
        & (Job.status == JobStatus.RUNNING)
    )


async def _finish_attempt(
    session: Any, claim: Claim, status: AttemptStatus, error: str | None
) -> None:
    await session.execute(
        update(JobAttempt)
        .where(
            JobAttempt.job_id == claim.job_id,
            JobAttempt.lease_token == claim.lease_token,
            JobAttempt.status == AttemptStatus.RUNNING,
        )
        .values(status=status, error=error, finished_at=func.now())
        .execution_options(synchronize_session=False)
    )


async def complete_job(session_factory: SessionFactory, claim: Claim, result: Any) -> bool:
    """Record success. Returns False if this worker no longer holds the lease (fenced out)."""
    async with session_factory() as session, session.begin():
        updated = await session.execute(
            update(Job)
            .where(_fenced(claim))
            .values(
                status=JobStatus.SUCCEEDED,
                result=result,
                last_error=None,
                finished_at=func.now(),
                lease_owner=None,
                lease_expires_at=None,
            )
            .returning(Job.id)
            .execution_options(synchronize_session=False)
        )
        if updated.one_or_none() is None:
            return False
        await _finish_attempt(session, claim, AttemptStatus.SUCCEEDED, None)
    return True


_RELEASE_LEASE: dict[str, Any] = {"lease_owner": None, "lease_expires_at": None}


async def fail_job(
    session_factory: SessionFactory,
    claim: Claim,
    error: str,
    *,
    retry_delay: float,
    retryable: bool = True,
) -> JobStatus | None:
    """Record a failed attempt: retry later (`pending`) or dead-letter once attempts run out.

    Returns the new status, or None if this worker was fenced out.
    """
    error = error[:MAX_ERROR_CHARS]
    retry = retryable and claim.attempt < claim.max_attempts
    values: dict[str, Any] = {"last_error": error, **_RELEASE_LEASE}
    if retry:
        values |= {
            "status": JobStatus.PENDING,
            "run_at": func.now() + timedelta(seconds=retry_delay),
        }
    else:
        values |= {"status": JobStatus.DEAD, "finished_at": func.now()}
    async with session_factory() as session, session.begin():
        updated = await session.execute(
            update(Job)
            .where(_fenced(claim))
            .values(**values)
            .returning(Job.id)
            .execution_options(synchronize_session=False)
        )
        if updated.one_or_none() is None:
            return None
        await _finish_attempt(session, claim, AttemptStatus.FAILED, error)
        if not retry:
            reason = "failed" if retryable else "non_retryable"
            await session.execute(
                insert(DeadLetter).values(job_id=claim.job_id, reason=reason, last_error=error)
            )
    return JobStatus.PENDING if retry else JobStatus.DEAD


@dataclass(frozen=True)
class LeaseState:
    cancel_requested: bool


async def heartbeat(
    session_factory: SessionFactory, claim: Claim, lease_seconds: float
) -> LeaseState | None:
    """Extend the lease while the fencing token still matches. None means the lease is lost."""
    async with session_factory() as session, session.begin():
        row = (
            await session.execute(
                update(Job)
                .where(_fenced(claim))
                .values(lease_expires_at=func.now() + timedelta(seconds=lease_seconds))
                .returning(Job.cancel_requested)
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
    return None if row is None else LeaseState(cancel_requested=bool(row[0]))


async def _fenced_transition(
    session_factory: SessionFactory,
    claim: Claim,
    values: dict[str, Any],
    attempt_status: AttemptStatus | None,
    attempt_error: str | None = None,
) -> bool:
    """Fenced update of the job plus its attempt row. `attempt_status=None` deletes the row."""
    async with session_factory() as session, session.begin():
        updated = await session.execute(
            update(Job)
            .where(_fenced(claim))
            .values(**values, **_RELEASE_LEASE)
            .returning(Job.id)
            .execution_options(synchronize_session=False)
        )
        if updated.one_or_none() is None:
            return False
        if attempt_status is None:
            await session.execute(
                delete(JobAttempt).where(
                    JobAttempt.job_id == claim.job_id,
                    JobAttempt.lease_token == claim.lease_token,
                )
            )
        else:
            await _finish_attempt(session, claim, attempt_status, attempt_error)
    return True


async def cancel_running_job(session_factory: SessionFactory, claim: Claim) -> bool:
    """The heartbeat saw `cancel_requested` and the handler was stopped: record `cancelled`."""
    return await _fenced_transition(
        session_factory,
        claim,
        {"status": JobStatus.CANCELLED, "finished_at": func.now()},
        AttemptStatus.CANCELLED,
        "cancelled",
    )


async def release_job(session_factory: SessionFactory, claim: Claim) -> bool:
    """Shutdown grace ran out: hand the job back (`queued`). The attempt isn't counted."""
    return await _fenced_transition(
        session_factory,
        claim,
        {"status": JobStatus.QUEUED, "attempts": Job.attempts - 1, "run_at": func.now()},
        AttemptStatus.RELEASED,
        "released on worker shutdown",
    )


async def defer_job(session_factory: SessionFactory, claim: Claim, delay: float) -> bool:
    """Execution throttle hit: put the job back in `pending` without counting an attempt.

    The handler never started, so the attempt row is removed instead of being marked.
    """
    return await _fenced_transition(
        session_factory,
        claim,
        {
            "status": JobStatus.PENDING,
            "attempts": Job.attempts - 1,
            "run_at": func.now() + timedelta(seconds=delay),
        },
        None,
    )


# --------------------------------------------------------------------------- cancellation


class JobNotFoundError(LookupError):
    pass


class InvalidTransitionError(Exception):
    def __init__(self, status: JobStatus) -> None:
        super().__init__(f"job is {status.value}")
        self.status = status


async def cancel_job(session_factory: SessionFactory, job_id: uuid.UUID) -> Job:
    """Pending/queued jobs become `cancelled` now; running jobs get `cancel_requested`.

    Raises JobNotFoundError, or InvalidTransitionError for a terminal job.
    """
    async with session_factory() as session, session.begin():
        job = await session.get(Job, job_id, with_for_update=True)
        if job is None:
            raise JobNotFoundError(job_id)
        if job.status in (JobStatus.PENDING, JobStatus.QUEUED):
            # A stream entry for a queued job may remain; the CAS claim drops it.
            job.status = JobStatus.CANCELLED
            job.finished_at = datetime.now(UTC)
        elif job.status == JobStatus.RUNNING:
            job.cancel_requested = True
        else:
            raise InvalidTransitionError(job.status)
        await session.flush()
        await session.refresh(job)
        return job


# --------------------------------------------------------------------------- dead letters


async def list_dead_letters(
    session_factory: SessionFactory,
    *,
    include_requeued: bool = False,
    limit: int = 50,
    cursor: int | None = None,
) -> tuple[list[tuple[DeadLetter, str]], int | None]:
    """Newest first, keyset-paginated on id. Each row comes with its job's type."""
    stmt = (
        select(DeadLetter, Job.type)
        .join(Job, Job.id == DeadLetter.job_id)
        .order_by(DeadLetter.id.desc())
        .limit(limit + 1)
    )
    if not include_requeued:
        stmt = stmt.where(DeadLetter.requeued_at.is_(None))
    if cursor is not None:
        stmt = stmt.where(DeadLetter.id < cursor)
    async with session_factory() as session:
        rows = [(letter, job_type) for letter, job_type in (await session.execute(stmt)).all()]
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = rows[-1][0].id
    return rows, next_cursor


async def requeue_dead_letter(session_factory: SessionFactory, dead_letter_id: int) -> Job:
    """Reset attempts, stamp `requeued_at`, and move the job back to `queued`.

    The caller XADDs after commit. Raises JobNotFoundError or InvalidTransitionError.
    """
    async with session_factory() as session, session.begin():
        letter = await session.get(DeadLetter, dead_letter_id, with_for_update=True)
        if letter is None:
            raise JobNotFoundError(dead_letter_id)
        job = await session.get(Job, letter.job_id, with_for_update=True)
        assert job is not None  # FK with ON DELETE CASCADE
        if letter.requeued_at is not None or job.status != JobStatus.DEAD:
            raise InvalidTransitionError(job.status)
        letter.requeued_at = datetime.now(UTC)
        job.status = JobStatus.QUEUED
        job.attempts = 0
        job.run_at = datetime.now(UTC)
        job.finished_at = None
        job.result = None
        job.cancel_requested = False
        await session.flush()
        await session.refresh(job)
        return job


# --------------------------------------------------------------------------- maintenance


async def promote_due(session_factory: SessionFactory, batch_size: int) -> list[Enqueue]:
    """Scheduler: due `pending` jobs become `queued`, oldest `run_at` first."""
    due = (
        select(Job.id)
        .where(Job.status == JobStatus.PENDING, Job.run_at <= func.now())
        .order_by(Job.run_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    async with session_factory() as session, session.begin():
        rows = (
            await session.execute(
                update(Job)
                .where(Job.id.in_(due), Job.status == JobStatus.PENDING)
                .values(status=JobStatus.QUEUED)
                .returning(Job.id, Job.priority)
                .execution_options(synchronize_session=False)
            )
        ).all()
    return [Enqueue(job_id, priority) for job_id, priority in rows]


@dataclass(frozen=True)
class Reaped:
    job_id: uuid.UUID
    priority: Priority
    status: JobStatus
    type: str = ""


LEASE_EXPIRED = "lease_expired"


async def reap_expired_leases(session_factory: SessionFactory, batch_size: int) -> list[Reaped]:
    """Reaper: `running` jobs whose lease expired go back to `queued`, or `dead` when out of
    attempts, or `cancelled` if a cancel was requested. The expired attempt counts as failed.

    The status change fences out the old owner: its writes all require `status='running'`
    plus its own lease_token, and the next claim bumps the token.
    """
    async with session_factory() as session, session.begin():
        expired = (
            await session.execute(
                select(
                    Job.id,
                    Job.type,
                    Job.priority,
                    Job.lease_token,
                    Job.attempts,
                    Job.max_attempts,
                    Job.cancel_requested,
                )
                .where(Job.status == JobStatus.RUNNING, Job.lease_expires_at < func.now())
                .order_by(Job.lease_expires_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        reaped: list[Reaped] = []
        for job_id, job_type, priority, token, attempts, max_attempts, cancel_requested in expired:
            values: dict[str, Any] = {"last_error": LEASE_EXPIRED, **_RELEASE_LEASE}
            if cancel_requested:
                status = JobStatus.CANCELLED
                values["finished_at"] = func.now()
            elif attempts >= max_attempts:
                status = JobStatus.DEAD
                values["finished_at"] = func.now()
            else:
                status = JobStatus.QUEUED
                values["run_at"] = func.now()
            await session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(status=status, **values)
                .execution_options(synchronize_session=False)
            )
            await session.execute(
                update(JobAttempt)
                .where(
                    JobAttempt.job_id == job_id,
                    JobAttempt.lease_token == token,
                    JobAttempt.status == AttemptStatus.RUNNING,
                )
                .values(
                    status=AttemptStatus.LEASE_EXPIRED,
                    error=LEASE_EXPIRED,
                    finished_at=func.now(),
                )
                .execution_options(synchronize_session=False)
            )
            if status == JobStatus.DEAD:
                await session.execute(
                    insert(DeadLetter).values(
                        job_id=job_id, reason=LEASE_EXPIRED, last_error=LEASE_EXPIRED
                    )
                )
            reaped.append(Reaped(job_id, priority, status, job_type))
    return reaped


async def touch_stale_queued(
    session_factory: SessionFactory, stale_seconds: float, batch_size: int
) -> list[Enqueue]:
    """Sweeper: `queued` jobs untouched for `stale_seconds` may have lost their stream entry.

    Bumping `updated_at` means each job is re-sent at most once per window, so a long but
    healthy backlog isn't flooded with duplicate hints (which the CAS claim would drop anyway).
    """
    stale = (
        select(Job.id)
        .where(
            Job.status == JobStatus.QUEUED,
            Job.updated_at < func.now() - timedelta(seconds=stale_seconds),
        )
        .order_by(Job.updated_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    async with session_factory() as session, session.begin():
        rows = (
            await session.execute(
                update(Job)
                .where(Job.id.in_(stale), Job.status == JobStatus.QUEUED)
                .values(updated_at=func.now())
                .returning(Job.id, Job.priority)
                .execution_options(synchronize_session=False)
            )
        ).all()
    return [Enqueue(job_id, priority) for job_id, priority in rows]


async def queued_among(session_factory: SessionFactory, job_ids: set[uuid.UUID]) -> list[Enqueue]:
    """The subset of `job_ids` that is still `queued` (needs a fresh stream entry)."""
    if not job_ids:
        return []
    async with session_factory() as session:
        rows = (
            await session.execute(
                select(Job.id, Job.priority).where(
                    Job.id.in_(job_ids), Job.status == JobStatus.QUEUED
                )
            )
        ).all()
    return [Enqueue(job_id, priority) for job_id, priority in rows]


async def delete_expired_idempotency_keys(session_factory: SessionFactory, batch_size: int) -> int:
    doomed = (
        select(IdempotencyKey.api_key_id, IdempotencyKey.key)
        .where(IdempotencyKey.expires_at <= func.now())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    async with session_factory() as session, session.begin():
        result = await session.execute(
            delete(IdempotencyKey)
            .where(tuple_(IdempotencyKey.api_key_id, IdempotencyKey.key).in_(doomed))
            .returning(IdempotencyKey.key)
        )
        return len(result.all())


async def idempotency_key_exists(session_factory: SessionFactory, idem: IdempotencyRequest) -> bool:
    """A live key means this request is a replay (exempt from the submission limit)."""
    async with session_factory() as session:
        found = await session.scalar(
            select(IdempotencyKey.job_id).where(
                IdempotencyKey.api_key_id == idem.api_key_id,
                IdempotencyKey.key == idem.key,
                IdempotencyKey.expires_at > func.now(),
            )
        )
    return found is not None


# --------------------------------------------------------------------------- metrics


async def active_job_counts(
    session_factory: SessionFactory,
) -> dict[tuple[JobStatus, Priority], int]:
    """Non-terminal jobs by (status, priority); served by the partial index on status."""
    async with session_factory() as session:
        rows = (
            await session.execute(
                select(Job.status, Job.priority, func.count())
                .where(Job.status.in_(sorted(ACTIVE_STATUSES)))
                .group_by(Job.status, Job.priority)
            )
        ).all()
    return {(status, priority): count for status, priority, count in rows}


async def dead_letter_count(session_factory: SessionFactory) -> int:
    """Dead-letter entries that are still dead (not requeued)."""
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(DeadLetter).where(DeadLetter.requeued_at.is_(None))
        )
    return int(count or 0)
