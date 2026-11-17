"""Job status enum and the allowed state transitions (see PLAN.md §1)."""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"
    CANCELLED = "cancelled"


class Priority(StrEnum):
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class AttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LEASE_EXPIRED = "lease_expired"
    RELEASED = "released"
    CANCELLED = "cancelled"


# Workers poll streams in this order.
PRIORITY_ORDER: tuple[Priority, ...] = (Priority.HIGH, Priority.NORMAL, Priority.LOW)

TERMINAL_STATUSES: frozenset[JobStatus] = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.CANCELLED}
)
ACTIVE_STATUSES: frozenset[JobStatus] = frozenset(
    {JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING}
)

TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    # scheduler promotes due jobs; cancel before start
    JobStatus.PENDING: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    # worker CAS claim; cancel before start
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.PENDING,  # retry with backoff (run_at in the future)
            JobStatus.QUEUED,  # lease reaped or released on shutdown
            JobStatus.DEAD,  # attempts exhausted
            JobStatus.CANCELLED,  # cancel_requested observed by heartbeat
        }
    ),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.DEAD: frozenset({JobStatus.QUEUED}),  # DLQ requeue
    JobStatus.CANCELLED: frozenset(),
}


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in TRANSITIONS[current]


def is_terminal(status: JobStatus) -> bool:
    return status in TERMINAL_STATUSES
