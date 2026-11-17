"""Dead-letter queue: list dead jobs and requeue them."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError

from workflow_engine.api.auth import require_api_key
from workflow_engine.api.schemas import DeadLetterList, DeadLetterOut, JobOut
from workflow_engine.core import jobs

router = APIRouter(prefix="/dlq", tags=["dlq"], dependencies=[Depends(require_api_key)])

MAX_LIST_LIMIT = 200


def _parse_cursor(cursor: str | None) -> int | None:
    if cursor is None:
        return None
    if not cursor.isdigit():
        raise RequestValidationError(
            [
                {
                    "type": "value_error",
                    "loc": ("query", "cursor"),
                    "msg": "invalid cursor",
                    "input": cursor,
                }
            ]
        )
    return int(cursor)


@router.get("", response_model=DeadLetterList)
async def list_dead_letters(
    request: Request,
    include_requeued: bool = False,
    limit: Annotated[int, Query(ge=1, le=MAX_LIST_LIMIT)] = 50,
    cursor: Annotated[str | None, Query(max_length=32)] = None,
) -> DeadLetterList:
    """Newest first. Entries that were already requeued are hidden unless asked for."""
    rows, next_cursor = await jobs.list_dead_letters(
        request.app.state.session_factory,
        include_requeued=include_requeued,
        limit=limit,
        cursor=_parse_cursor(cursor),
    )
    items = [
        DeadLetterOut(
            id=letter.id,
            job_id=letter.job_id,
            job_type=job_type,
            reason=letter.reason,
            last_error=letter.last_error,
            dead_at=letter.dead_at,
            requeued_at=letter.requeued_at,
        )
        for letter, job_type in rows
    ]
    return DeadLetterList(
        items=items, next_cursor=None if next_cursor is None else str(next_cursor)
    )


@router.post(
    "/{dead_letter_id}/requeue",
    response_model=JobOut,
    responses={404: {"description": "No such entry"}, 409: {"description": "Already requeued"}},
)
async def requeue(dead_letter_id: int, request: Request) -> JobOut:
    """Reset the job's attempts, mark the entry requeued, and send the job back to a worker."""
    try:
        job = await jobs.requeue_dead_letter(request.app.state.session_factory, dead_letter_id)
    except jobs.JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail="dead-letter entry not found") from exc
    except jobs.InvalidTransitionError as exc:
        raise HTTPException(
            status_code=409, detail="dead-letter entry was already requeued"
        ) from exc
    await jobs.enqueue_safely(request.app.state.producer, job.id, job.priority)
    return JobOut.model_validate(job)
