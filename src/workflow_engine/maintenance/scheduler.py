"""Scheduler: due `pending` jobs (delayed submissions, retry backoff, throttled jobs) move to
`queued` and get XADDed. Commit first, then XADD, as everywhere else."""

from __future__ import annotations

from workflow_engine.core import jobs
from workflow_engine.db.session import SessionFactory

# Bounds one tick so a huge backlog can't starve the other loops.
MAX_BATCHES_PER_TICK = 20


async def promote_due_jobs(
    session_factory: SessionFactory, dispatcher: jobs.Dispatcher, batch_size: int
) -> int:
    promoted = 0
    for _ in range(MAX_BATCHES_PER_TICK):
        batch = await jobs.promote_due(session_factory, batch_size)
        for item in batch:
            await jobs.enqueue_safely(dispatcher, item.job_id, item.priority)
        promoted += len(batch)
        if len(batch) < batch_size:
            break
    return promoted
