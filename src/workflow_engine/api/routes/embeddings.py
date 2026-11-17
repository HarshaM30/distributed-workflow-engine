"""Semantic search over stored embeddings."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from fastapi.exceptions import RequestValidationError

from workflow_engine.api.auth import require_api_key
from workflow_engine.api.schemas import SearchHitOut, SearchResponse
from workflow_engine.config import Settings
from workflow_engine.core import embeddings
from workflow_engine.embedding import get_embedder

router = APIRouter(
    prefix="/embeddings", tags=["embeddings"], dependencies=[Depends(require_api_key)]
)

MAX_QUERY_CHARS = 1000


@router.get("/search", response_model=SearchResponse)
async def search(
    request: Request,
    q: Annotated[str, Query(min_length=1, max_length=MAX_QUERY_CHARS)],
    k: Annotated[int, Query(ge=1)] = 5,
    job_id: uuid.UUID | None = None,
) -> SearchResponse:
    settings: Settings = request.app.state.settings
    if k > settings.embed_search_max_k:
        msg = f"k must be at most {settings.embed_search_max_k}"
        raise RequestValidationError(
            [{"type": "value_error", "loc": ("query", "k"), "msg": msg, "input": k}]
        )
    query = q.strip()
    if not query:
        raise RequestValidationError(
            [{"type": "value_error", "loc": ("query", "q"), "msg": "q is blank", "input": q}]
        )
    embedder = get_embedder(settings)
    vector = await embedder.embed_query(query)
    hits = await embeddings.search(request.app.state.session_factory, vector, k, job_id)
    return SearchResponse(
        query=query,
        model=embedder.model_name,
        results=[
            SearchHitOut(
                job_id=h.job_id,
                item_index=h.item_index,
                text=h.text,
                distance=h.distance,
                score=1.0 - h.distance,
            )
            for h in hits
        ],
    )
