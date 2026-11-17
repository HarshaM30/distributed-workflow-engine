"""Prometheus scrape endpoint (no API key, like /health)."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest
from sqlalchemy.exc import SQLAlchemyError

from workflow_engine.core import jobs
from workflow_engine.core.states import ACTIVE_STATUSES, PRIORITY_ORDER
from workflow_engine.db.session import SessionFactory
from workflow_engine.logging import get_logger
from workflow_engine.metrics import DLQ_SIZE, QUEUE_DEPTH

router = APIRouter(tags=["metrics"])
log = get_logger(__name__)

GAUGE_REFRESH_TIMEOUT_SECONDS = 2.0


async def refresh_gauges(session_factory: SessionFactory) -> None:
    counts = await jobs.active_job_counts(session_factory)
    for status in ACTIVE_STATUSES:
        for priority in PRIORITY_ORDER:
            # Every combination is set, so a drained queue reads 0 instead of going stale.
            QUEUE_DEPTH.labels(status.value, priority.value).set(counts.get((status, priority), 0))
    DLQ_SIZE.set(await jobs.dead_letter_count(session_factory))


@router.get("/metrics", include_in_schema=False)
async def metrics(request: Request) -> Response:
    if not request.app.state.settings.metrics_enabled:
        raise HTTPException(status_code=404, detail="metrics are disabled")
    try:
        await asyncio.wait_for(
            refresh_gauges(request.app.state.session_factory), GAUGE_REFRESH_TIMEOUT_SECONDS
        )
    except (SQLAlchemyError, OSError, TimeoutError) as exc:
        # Still serve the counters; the gauges keep their last value.
        log.warning("metrics_gauge_refresh_failed", error=type(exc).__name__)
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
