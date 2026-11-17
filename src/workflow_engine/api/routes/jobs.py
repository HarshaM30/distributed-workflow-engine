"""Job submission, lookup, and cancellation."""

from __future__ import annotations

import math
import uuid
from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError

from workflow_engine.api.auth import Principal, require_api_key
from workflow_engine.api.schemas import (
    AttemptOut,
    JobCreate,
    JobDetail,
    JobList,
    JobOut,
    validate_job_payload,
)
from workflow_engine.config import Settings
from workflow_engine.core import jobs
from workflow_engine.core.idempotency import IdempotencyRequest, request_hash
from workflow_engine.core.states import JobStatus
from workflow_engine.dispatch.rate_limit import TokenBucket
from workflow_engine.handlers.registry import HandlerRegistry

router = APIRouter(prefix="/jobs", tags=["jobs"], dependencies=[Depends(require_api_key)])

MAX_LIST_LIMIT = 200
SUBMIT_SCOPE = "submit"


async def enforce_submit_limit(
    request: Request, principal: Principal, idem: IdempotencyRequest | None
) -> None:
    """Per-API-key token bucket. A replay of an existing Idempotency-Key costs nothing."""
    if idem is not None and await jobs.idempotency_key_exists(
        request.app.state.session_factory, idem
    ):
        return
    settings: Settings = request.app.state.settings
    limiter: TokenBucket = request.app.state.limiter
    decision = await limiter.acquire(SUBMIT_SCOPE, principal.key_id, settings.submit_rate_limit)
    if not decision.allowed:
        raise HTTPException(
            status_code=429,
            detail="submission rate limit exceeded for this API key",
            headers={"Retry-After": str(max(1, math.ceil(decision.retry_after)))},
        )


@router.post(
    "",
    status_code=201,
    response_model=JobOut,
    responses={
        200: {"description": "Idempotent replay of an existing job"},
        429: {"description": "Submission rate limit exceeded; see Retry-After"},
    },
)
async def create_job(
    body: JobCreate,
    request: Request,
    response: Response,
    principal: Annotated[Principal, Depends(require_api_key)],
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ] = None,
) -> JobOut:
    settings: Settings = request.app.state.settings
    registry: HandlerRegistry = request.app.state.registry
    payload = validate_job_payload(body, registry, settings.max_payload_bytes)
    spec = registry.require(body.type)
    job_spec = jobs.JobSpec(
        type=body.type,
        payload=payload,
        priority=body.priority,
        run_at=body.run_at,
        max_attempts=jobs.resolve_max_attempts(
            body.max_attempts, spec.max_attempts, settings.default_max_attempts
        ),
    )
    idem = None
    if idempotency_key is not None:
        canonical = body.model_copy(update={"payload": payload}).model_dump(mode="json")
        idem = IdempotencyRequest(principal.key_id, idempotency_key, request_hash(canonical))
    await enforce_submit_limit(request, principal, idem)
    result = await jobs.submit_job(
        request.app.state.session_factory,
        request.app.state.producer,
        job_spec,
        idempotency=idem,
        idempotency_ttl=timedelta(hours=settings.idempotency_key_ttl_hours),
    )
    if not result.created:
        response.status_code = 200
    return JobOut.model_validate(result.job)


@router.get("", response_model=JobList)
async def list_jobs(
    request: Request,
    status: JobStatus | None = None,
    type: Annotated[str | None, Query(max_length=64)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIST_LIMIT)] = 50,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> JobList:
    try:
        rows, next_cursor = await jobs.list_jobs(
            request.app.state.session_factory,
            status=status,
            job_type=type,
            limit=limit,
            cursor=cursor,
        )
    except jobs.InvalidCursorError as exc:
        raise RequestValidationError(
            [{"type": "value_error", "loc": ("query", "cursor"), "msg": str(exc), "input": cursor}]
        ) from exc
    return JobList(items=[JobOut.model_validate(r) for r in rows], next_cursor=next_cursor)


@router.get("/{job_id}", response_model=JobDetail)
async def get_job(job_id: uuid.UUID, request: Request) -> JobDetail:
    found = await jobs.get_job(request.app.state.session_factory, job_id)
    if found is None:
        raise HTTPException(status_code=404, detail="job not found")
    job, attempts = found
    return JobDetail(
        **JobOut.model_validate(job).model_dump(),
        attempt_history=[AttemptOut.model_validate(a) for a in attempts],
    )


@router.post(
    "/{job_id}/cancel",
    response_model=JobOut,
    responses={
        200: {"description": "Cancelled (the job was pending or queued)"},
        202: {"description": "Cancel requested; the running handler stops at its next heartbeat"},
        404: {"description": "Job not found"},
        409: {"description": "Job already finished"},
    },
)
async def cancel_job(job_id: uuid.UUID, request: Request, response: Response) -> JobOut:
    try:
        job = await jobs.cancel_job(request.app.state.session_factory, job_id)
    except jobs.JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail="job not found") from exc
    except jobs.InvalidTransitionError as exc:
        raise HTTPException(status_code=409, detail=f"job is already {exc.status.value}") from exc
    if job.status == JobStatus.RUNNING:
        response.status_code = 202
    return JobOut.model_validate(job)
