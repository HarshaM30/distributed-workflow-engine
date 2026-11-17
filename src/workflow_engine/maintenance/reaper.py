"""Reaper: `running` jobs whose lease expired (crashed or partitioned worker) are taken back."""

from __future__ import annotations

from collections import Counter

from workflow_engine.core import jobs
from workflow_engine.core.states import JobStatus
from workflow_engine.db.session import SessionFactory
from workflow_engine.logging import get_logger

log = get_logger(__name__)


async def reap_expired(
    session_factory: SessionFactory, dispatcher: jobs.Dispatcher, batch_size: int
) -> Counter[JobStatus]:
    reaped = await jobs.reap_expired_leases(session_factory, batch_size)
    for item in reaped:
        log.warning("lease_reaped", job_id=str(item.job_id), new_status=item.status.value)
        if item.status == JobStatus.QUEUED:
            await jobs.enqueue_safely(dispatcher, item.job_id, item.priority)
    return Counter(item.status for item in reaped)
