"""Sweeper: repairs Redis from Postgres. Re-sending is always safe thanks to the CAS claim.

1. `queued` jobs untouched for SWEEPER_STALE_SECONDS get a fresh stream entry. This covers
   a crash between commit and XADD, a failed XADD, stream trimming, and a Redis wipe.
2. XAUTOCLAIM picks up pending-entry-list entries idle that long (their consumer died) and
   ACKs them; any whose job is still `queued` is re-sent.
3. Expired idempotency keys are deleted.
"""

from __future__ import annotations

from dataclasses import dataclass

from redis.asyncio import Redis

from workflow_engine.core import jobs
from workflow_engine.db.session import SessionFactory
from workflow_engine.dispatch.streams import reclaim_idle
from workflow_engine.logging import get_logger

log = get_logger(__name__)

SWEEPER_CONSUMER = "maintenance-sweeper"


@dataclass(frozen=True)
class SweepResult:
    requeued: int
    reclaimed: int
    keys_deleted: int


async def sweep(
    session_factory: SessionFactory,
    dispatcher: jobs.Dispatcher,
    redis: Redis,
    *,
    streams: list[str],
    group: str,
    stale_seconds: float,
    batch_size: int,
) -> SweepResult:
    stale = await jobs.touch_stale_queued(session_factory, stale_seconds, batch_size)
    resend = {item.job_id: item for item in stale}

    reclaimed = 0
    for stream in streams:
        messages = await reclaim_idle(
            redis, stream, group, SWEEPER_CONSUMER, int(stale_seconds * 1000), count=batch_size
        )
        reclaimed += len(messages)
        ids = {m.job_id for m in messages if m.job_id is not None} - resend.keys()
        for item in await jobs.queued_among(session_factory, ids):
            resend[item.job_id] = item

    for item in resend.values():
        await jobs.enqueue_safely(dispatcher, item.job_id, item.priority)
    keys_deleted = await jobs.delete_expired_idempotency_keys(session_factory, batch_size)
    if resend or reclaimed:
        log.info("sweeper_repaired", requeued=len(resend), reclaimed=reclaimed)
    return SweepResult(requeued=len(resend), reclaimed=reclaimed, keys_deleted=keys_deleted)
