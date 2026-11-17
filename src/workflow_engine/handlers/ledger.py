"""`ledger`: sleeps, recording each execution in `job_executions`. Used by the chaos harness.

The row is written when the handler *starts* and stamped when it finishes, so the ledger shows
every execution, including ones cut short by a killed worker. Duplicates are then measured as
`executions - jobs`, which is what "at-least-once execution" predicts under crashes. Unlike a
real side effect, this one is deliberately *not* idempotent: it exists to be counted.
"""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, insert, update

from workflow_engine.db.models import JobExecution
from workflow_engine.handlers.registry import JobContext, handler


class LedgerPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seconds: float = Field(default=0.1, ge=0, le=60)


@handler("ledger", payload_model=LedgerPayload, timeout_seconds=120)
async def run(payload: LedgerPayload, ctx: JobContext) -> dict[str, Any]:
    if ctx.session_factory is None:
        raise RuntimeError("ledger needs a database session factory")
    async with ctx.session_factory() as session, session.begin():
        execution_id = await session.scalar(
            insert(JobExecution)
            .values(job_id=ctx.job_id, attempt_no=ctx.attempt, worker_id=ctx.worker_id)
            .returning(JobExecution.id)
        )
    await asyncio.sleep(payload.seconds)
    async with ctx.session_factory() as session, session.begin():
        await session.execute(
            update(JobExecution)
            .where(JobExecution.id == execution_id)
            .values(finished_at=func.now())
        )
    return {"slept_seconds": payload.seconds, "execution_id": execution_id}
