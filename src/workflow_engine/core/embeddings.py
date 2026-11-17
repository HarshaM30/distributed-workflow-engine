"""Storage and nearest-neighbour search for the `embeddings` table (pgvector)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Select, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from workflow_engine.db.models import Embedding
from workflow_engine.db.session import SessionFactory

# HNSW returns at most ef_search candidates, so it must be at least k (pgvector's default: 40).
MIN_EF_SEARCH = 40


@dataclass(frozen=True)
class EmbeddingRow:
    item_index: int
    text: str
    embedding: list[float]


@dataclass(frozen=True)
class SearchHit:
    job_id: uuid.UUID
    item_index: int
    text: str
    distance: float


async def existing_indexes(
    session_factory: SessionFactory, job_id: uuid.UUID, model: str
) -> set[int]:
    """Items of this job already embedded with this model (a retry skips them)."""
    async with session_factory() as session:
        rows = await session.scalars(
            select(Embedding.item_index).where(Embedding.job_id == job_id, Embedding.model == model)
        )
        return set(rows.all())


async def upsert_embeddings(
    session_factory: SessionFactory, job_id: uuid.UUID, model: str, rows: list[EmbeddingRow]
) -> int:
    """Insert or overwrite on the natural key (job_id, item_index), so re-runs are idempotent."""
    if not rows:
        return 0
    stmt = pg_insert(Embedding).values(
        [
            {
                "job_id": job_id,
                "item_index": r.item_index,
                "text": r.text,
                "embedding": r.embedding,
                "model": model,
            }
            for r in rows
        ]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[Embedding.job_id, Embedding.item_index],
        set_={
            "text": stmt.excluded.text,
            "embedding": stmt.excluded.embedding,
            "model": stmt.excluded.model,
            "created_at": func.now(),
        },
    )
    async with session_factory() as session, session.begin():
        await session.execute(stmt)
    return len(rows)


def search_query(query_vector: list[float], k: int, job_id: uuid.UUID | None) -> Select[Any]:
    distance = Embedding.embedding.cosine_distance(query_vector)
    stmt = select(
        Embedding.job_id, Embedding.item_index, Embedding.text, distance.label("distance")
    )
    if job_id is not None:
        stmt = stmt.where(Embedding.job_id == job_id)
    return stmt.order_by(distance).limit(k)


async def search(
    session_factory: SessionFactory, query_vector: list[float], k: int, job_id: uuid.UUID | None
) -> list[SearchHit]:
    """Top-k by cosine distance through the HNSW index.

    With a `job_id` filter, an iterative index scan keeps going until k matching rows are
    found, instead of filtering a fixed candidate list down to fewer than k.
    """
    async with session_factory() as session, session.begin():
        ef_search = max(MIN_EF_SEARCH, k)
        await session.execute(text(f"SET LOCAL hnsw.ef_search = {int(ef_search)}"))
        if job_id is not None:
            await session.execute(text("SET LOCAL hnsw.iterative_scan = strict_order"))
        rows = (await session.execute(search_query(query_vector, k, job_id))).all()
    return [SearchHit(r.job_id, r.item_index, r.text, float(r.distance)) for r in rows]
