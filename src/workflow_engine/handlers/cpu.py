"""`cpu`: a chain of SHA-256 rounds, run off the event loop so heartbeats keep running."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from workflow_engine.handlers.registry import JobContext, handler


class CpuPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    iterations: int = Field(default=100_000, ge=1, le=50_000_000)
    seed: str = Field(default="", max_length=256)


def hash_chain(seed: str, iterations: int) -> str:
    digest = hashlib.sha256(seed.encode()).digest()
    for _ in range(iterations - 1):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


@handler("cpu", payload_model=CpuPayload, timeout_seconds=600)
async def run(payload: CpuPayload, ctx: JobContext) -> dict[str, Any]:
    digest = await asyncio.to_thread(hash_chain, payload.seed, payload.iterations)
    return {"iterations": payload.iterations, "digest": digest}
