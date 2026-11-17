"""ORM models for the five tables in PLAN.md §1."""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from workflow_engine.core.states import AttemptStatus, JobStatus, Priority
from workflow_engine.db.base import Base

EMBEDDING_DIM = 384


def pg_enum(enum_cls: type[StrEnum], name: str) -> Enum:
    return Enum(
        enum_cls,
        name=name,
        values_callable=lambda members: [m.value for m in members],
        validate_strings=True,
    )


def _tz() -> DateTime:
    return DateTime(timezone=True)


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status: Mapped[JobStatus] = mapped_column(pg_enum(JobStatus, "job_status"))
    priority: Mapped[Priority] = mapped_column(
        pg_enum(Priority, "job_priority"), server_default=Priority.NORMAL.value
    )
    run_at: Mapped[datetime] = mapped_column(_tz(), server_default=func.now())
    attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer)
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_token: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    lease_expires_at: Mapped[datetime | None] = mapped_column(_tz())
    cancel_requested: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    result: Mapped[Any | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(_tz(), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        _tz(), server_default=func.now(), onupdate=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(_tz())

    attempt_rows: Mapped[list[JobAttempt]] = relationship(
        back_populates="job", order_by="JobAttempt.id", lazy="raise"
    )

    __table_args__ = (
        CheckConstraint("attempts >= 0", name="attempts_non_negative"),
        CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        Index("ix_jobs_status_run_at", "status", "run_at"),
        Index("ix_jobs_status_lease_expires_at", "status", "lease_expires_at"),
        Index(
            "ix_jobs_active_status",
            "status",
            postgresql_where=text("status IN ('pending', 'queued', 'running')"),
        ),
        Index("ix_jobs_created_at_id", "created_at", "id"),
    )


class JobAttempt(Base):
    __tablename__ = "job_attempts"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    attempt_no: Mapped[int] = mapped_column(Integer)
    worker_id: Mapped[str] = mapped_column(String(255))
    lease_token: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[AttemptStatus] = mapped_column(pg_enum(AttemptStatus, "attempt_status"))
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(_tz(), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(_tz())

    job: Mapped[Job] = relationship(back_populates="attempt_rows", lazy="raise")


class DeadLetter(Base):
    __tablename__ = "dead_letters"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    reason: Mapped[str] = mapped_column(String(64))
    last_error: Mapped[str | None] = mapped_column(Text)
    dead_at: Mapped[datetime] = mapped_column(_tz(), server_default=func.now())
    requeued_at: Mapped[datetime | None] = mapped_column(_tz())


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    api_key_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True
    )
    created_at: Mapped[datetime] = mapped_column(_tz(), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(_tz(), index=True)


class Embedding(Base):
    __tablename__ = "embeddings"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE")
    )
    item_index: Mapped[int] = mapped_column(Integer)
    text: Mapped[str] = mapped_column(Text)
    embedding: Mapped[list[float]] = mapped_column(Vector(EMBEDDING_DIM))
    model: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(_tz(), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("job_id", "item_index"),
        Index(
            "ix_embeddings_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )
