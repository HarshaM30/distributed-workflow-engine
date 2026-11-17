"""Job submission and lookup."""

from __future__ import annotations

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
from workflow_engine.handlers.registry import HandlerRegistry

router = APIRouter(prefix="/jobs", tags=["jobs"], dependencies=[Depends(require_api_key)])

MAX_LIST_LIMIT = 200


@router.post(
    "",
    status_code=201,
    response_model=JobOut,
    responses={200: {"description": "Idempotent replay of an existing job"}},
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
