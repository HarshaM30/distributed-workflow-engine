"""`sleep`: waits for a number of seconds. Used for latency and shutdown tests."""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from workflow_engine.handlers.registry import JobContext, handler


class SleepPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seconds: float = Field(default=1.0, ge=0, le=3600)


@handler("sleep", payload_model=SleepPayload, timeout_seconds=3660)
async def run(payload: SleepPayload, ctx: JobContext) -> dict[str, Any]:
    await asyncio.sleep(payload.seconds)
    return {"slept_seconds": payload.seconds}
