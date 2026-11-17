"""Pydantic request/response models."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from fastapi.exceptions import RequestValidationError
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from workflow_engine.core.states import AttemptStatus, JobStatus, Priority
from workflow_engine.handlers.registry import HandlerRegistry


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    run_at: AwareDatetime | None = None
    max_attempts: int | None = Field(default=None, ge=1, le=100)


def _error(loc: tuple[str | int, ...], msg: str, input_value: Any) -> dict[str, Any]:
    return {"type": "value_error", "loc": loc, "msg": msg, "input": input_value}


def validate_job_payload(
    body: JobCreate, registry: HandlerRegistry, max_payload_bytes: int
) -> dict[str, Any]:
    """Check the type is registered and the payload fits its handler's model.

    Returns the normalised payload (defaults filled in). Raises RequestValidationError (422).
    """
    spec = registry.get(body.type)
    if spec is None:
        msg = f"unknown job type {body.type!r}; registered types: {', '.join(registry.names())}"
        raise RequestValidationError([_error(("body", "type"), msg, body.type)])
    size = len(json.dumps(body.payload, separators=(",", ":")).encode())
    if size > max_payload_bytes:
        msg = f"payload is {size} bytes; the limit is {max_payload_bytes}"
        raise RequestValidationError([_error(("body", "payload"), msg, None)])
    try:
        model = spec.payload_model.model_validate(body.payload)
    except ValidationError as exc:
        errors = []
        for err in exc.errors(include_url=False, include_context=False):
            errors.append({**err, "loc": ("body", "payload", *err["loc"])})
        raise RequestValidationError(errors) from exc
    return model.model_dump(mode="json")


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_no: int
    worker_id: str
    lease_token: int
    status: AttemptStatus
    error: str | None
    started_at: datetime
    finished_at: datetime | None


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: Priority
    run_at: datetime
    attempts: int
    max_attempts: int
    cancel_requested: bool
    result: Any | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


class JobDetail(JobOut):
    attempt_history: list[AttemptOut]


class JobList(BaseModel):
    items: list[JobOut]
    next_cursor: str | None
