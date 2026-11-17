"""Liveness/readiness: checks Postgres and Redis, returns 200 or 503 with per-component details."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

router = APIRouter(tags=["health"])

CHECK_TIMEOUT_SECONDS = 2.0


async def _check(coro: Any) -> dict[str, str]:
    try:
        await asyncio.wait_for(coro, timeout=CHECK_TIMEOUT_SECONDS)
    except TimeoutError:
        return {"status": "error", "error": "timeout"}
    except Exception as exc:
        # Type name only: driver messages can include hostnames or connection strings.
        return {"status": "error", "error": type(exc).__name__}
    return {"status": "ok"}


async def _ping_postgres(request: Request) -> None:
    async with request.app.state.engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def _ping_redis(request: Request) -> None:
    await request.app.state.redis.ping()


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    postgres, redis = await asyncio.gather(
        _check(_ping_postgres(request)), _check(_ping_redis(request))
    )
    components = {"postgres": postgres, "redis": redis}
    healthy = all(c["status"] == "ok" for c in components.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "unavailable", "components": components},
    )
