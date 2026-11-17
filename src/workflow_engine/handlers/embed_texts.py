"""`embed_texts`: embeds a list of texts and upserts the vectors into `embeddings`.

Idempotent by construction: rows are keyed on (job_id, item_index), and a retry skips items
that an earlier attempt already stored. A duplicate execution (e.g. a reaped zombie worker
finishing late) only rewrites identical rows.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from workflow_engine.config import get_settings
from workflow_engine.core.embeddings import EmbeddingRow, existing_indexes, upsert_embeddings
from workflow_engine.embedding import get_embedder
from workflow_engine.handlers.registry import JobContext, handler

MAX_TEXTS = 512
MAX_TEXT_CHARS = 4000

Text = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_TEXT_CHARS)
]


class EmbedTextsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    texts: list[Text] = Field(min_length=1, max_length=MAX_TEXTS)


@handler("embed_texts", payload_model=EmbedTextsPayload, timeout_seconds=900)
async def run(payload: EmbedTextsPayload, ctx: JobContext) -> dict[str, Any]:
    if ctx.session_factory is None:
        raise RuntimeError("embed_texts needs a database session factory")
    embedder = get_embedder(get_settings())
    done = await existing_indexes(ctx.session_factory, ctx.job_id, embedder.model_name)
    todo = [(i, t) for i, t in enumerate(payload.texts) if i not in done]
    stored = 0
    for start in range(0, len(todo), embedder.batch_size):
        batch = todo[start : start + embedder.batch_size]
        vectors = await embedder.embed_passages([t for _, t in batch])
        rows = [EmbeddingRow(i, t, v) for (i, t), v in zip(batch, vectors, strict=True)]
        # Committed per batch: a retry after a crash resumes where this attempt stopped.
        stored += await upsert_embeddings(
            ctx.session_factory, ctx.job_id, embedder.model_name, rows
        )
    return {
        "texts": len(payload.texts),
        "embedded": stored,
        "skipped": len(done),
        "model": embedder.model_name,
    }
