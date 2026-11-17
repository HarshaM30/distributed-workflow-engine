"""Initial schema: jobs, job_attempts, dead_letters, idempotency_keys, embeddings.

Revision ID: 0001
Revises:
Create Date: 2026-11-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

job_status = postgresql.ENUM(
    "pending", "queued", "running", "succeeded", "dead", "cancelled", name="job_status"
)
job_priority = postgresql.ENUM("high", "normal", "low", name="job_priority")
attempt_status = postgresql.ENUM(
    "running",
    "succeeded",
    "failed",
    "lease_expired",
    "released",
    "cancelled",
    name="attempt_status",
)


def _ts(name: str, *, nullable: bool = False, default: bool = False) -> sa.Column:  # type: ignore[type-arg]
    return sa.Column(
        name,
        sa.DateTime(timezone=True),
        nullable=nullable,
        server_default=sa.func.now() if default else None,
    )


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    bind = op.get_bind()
    for enum in (job_status, job_priority, attempt_status):
        enum.create(bind, checkfirst=True)

    op.create_table(
        "jobs",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("type", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", postgresql.ENUM(name="job_status", create_type=False), nullable=False),
        sa.Column(
            "priority",
            postgresql.ENUM(name="job_priority", create_type=False),
            nullable=False,
            server_default="normal",
        ),
        _ts("run_at", default=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(255), nullable=True),
        sa.Column("lease_token", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        _ts("lease_expires_at", nullable=True),
        sa.Column(
            "cancel_requested", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        _ts("created_at", default=True),
        _ts("updated_at", default=True),
        _ts("finished_at", nullable=True),
        sa.CheckConstraint("attempts >= 0", name="ck_jobs_attempts_non_negative"),
        sa.CheckConstraint("max_attempts >= 1", name="ck_jobs_max_attempts_positive"),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
    )
    op.create_index("ix_jobs_status_run_at", "jobs", ["status", "run_at"])
    op.create_index("ix_jobs_status_lease_expires_at", "jobs", ["status", "lease_expires_at"])
    op.create_index(
        "ix_jobs_active_status",
        "jobs",
        ["status"],
        postgresql_where=sa.text("status IN ('pending', 'queued', 'running')"),
    )
    op.create_index("ix_jobs_created_at_id", "jobs", ["created_at", "id"])

    op.create_table(
        "job_attempts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("lease_token", sa.BigInteger(), nullable=False),
        sa.Column(
            "status", postgresql.ENUM(name="attempt_status", create_type=False), nullable=False
        ),
        sa.Column("error", sa.Text(), nullable=True),
        _ts("started_at", default=True),
        _ts("finished_at", nullable=True),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_job_attempts_job_id_jobs", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_job_attempts"),
    )
    op.create_index("ix_job_attempts_job_id", "job_attempts", ["job_id"])

    op.create_table(
        "dead_letters",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("reason", sa.String(64), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        _ts("dead_at", default=True),
        _ts("requeued_at", nullable=True),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_dead_letters_job_id_jobs", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_dead_letters"),
    )
    op.create_index("ix_dead_letters_job_id", "dead_letters", ["job_id"])

    op.create_table(
        "idempotency_keys",
        sa.Column("api_key_id", sa.String(64), nullable=False),
        sa.Column("key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        _ts("created_at", default=True),
        _ts("expires_at"),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_idempotency_keys_job_id_jobs", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("api_key_id", "key", name="pk_idempotency_keys"),
    )
    op.create_index("ix_idempotency_keys_job_id", "idempotency_keys", ["job_id"])
    op.create_index("ix_idempotency_keys_expires_at", "idempotency_keys", ["expires_at"])

    op.create_table(
        "embeddings",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("item_index", sa.Integer(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(384), nullable=False),
        sa.Column("model", sa.String(128), nullable=False),
        _ts("created_at", default=True),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name="fk_embeddings_job_id_jobs", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_embeddings"),
        sa.UniqueConstraint("job_id", "item_index", name="uq_embeddings_job_id_item_index"),
    )
    op.create_index(
        "ix_embeddings_embedding_hnsw",
        "embeddings",
        ["embedding"],
        postgresql_using="hnsw",
        postgresql_ops={"embedding": "vector_cosine_ops"},
    )


def downgrade() -> None:
    op.drop_table("embeddings")
    op.drop_table("idempotency_keys")
    op.drop_table("dead_letters")
    op.drop_table("job_attempts")
    op.drop_table("jobs")
    bind = op.get_bind()
    for enum in (attempt_status, job_priority, job_status):
        enum.drop(bind, checkfirst=True)
    op.execute("DROP EXTENSION IF EXISTS vector")
