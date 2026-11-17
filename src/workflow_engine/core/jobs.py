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
from sqlalchemy import func, insert, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from workflow_engine.core.idempotency import IdempotencyConflictError, IdempotencyRequest
from workflow_engine.core.states import AttemptStatus, JobStatus, Priority
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
    session_factory: SessionFactory, job_id: uuid.UUID, worker_id: str, lease_seconds: int
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
                .returning(Job.lease_token, Job.attempts, Job.max_attempts, Job.type, Job.payload)
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
        if row is None:
            return None
        lease_token, attempt, max_attempts, job_type, payload = row
        await session.execute(
            insert(JobAttempt).values(
                job_id=job_id,
                attempt_no=attempt,
                worker_id=worker_id,
                lease_token=lease_token,
                status=AttemptStatus.RUNNING,
            )
        )
    return Claim(job_id, lease_token, attempt, max_attempts, job_type, payload, worker_id)


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


async def fail_job(
    session_factory: SessionFactory, claim: Claim, error: str, *, reason: str = "failed"
) -> bool:
    """Record a failed attempt. Single attempt for now: the job goes straight to `dead`.

    Returns False if this worker was fenced out.
    """
    error = error[:MAX_ERROR_CHARS]
    async with session_factory() as session, session.begin():
        updated = await session.execute(
            update(Job)
            .where(_fenced(claim))
            .values(
                status=JobStatus.DEAD,
                last_error=error,
                finished_at=func.now(),
                lease_owner=None,
                lease_expires_at=None,
            )
            .returning(Job.id)
            .execution_options(synchronize_session=False)
        )
        if updated.one_or_none() is None:
            return False
        await _finish_attempt(session, claim, AttemptStatus.FAILED, error)
        await session.execute(
            insert(DeadLetter).values(job_id=claim.job_id, reason=reason, last_error=error)
        )
    return True
