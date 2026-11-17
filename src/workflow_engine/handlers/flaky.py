"""`flaky`: fails with probability `failure_rate`. Used to exercise retries and the DLQ."""

from __future__ import annotations

import asyncio
import random
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from workflow_engine.handlers.registry import JobContext, handler


class FlakyError(RuntimeError):
    pass


class FlakyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_rate: float = Field(default=0.5, ge=0, le=1)
    seconds: float = Field(default=0, ge=0, le=60)


@handler("flaky", payload_model=FlakyPayload, timeout_seconds=120)
async def run(payload: FlakyPayload, ctx: JobContext) -> dict[str, Any]:
    if payload.seconds:
        await asyncio.sleep(payload.seconds)
    if random.random() < payload.failure_rate:
        raise FlakyError(f"injected failure (rate={payload.failure_rate})")
    return {"ok": True}
