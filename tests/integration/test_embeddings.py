"""`embed_texts` jobs and `GET /embeddings/search` (Milestone 4). Uses the real ONNX model."""

from __future__ import annotations

import uuid
from typing import Any

import httpx
from fastapi import FastAPI
from sqlalchemy import func, select, text, update

from tests.integration.conftest import wait_for_status
from workflow_engine.core.embeddings import search_query
from workflow_engine.core.states import JobStatus, Priority
from workflow_engine.db.models import EMBEDDING_DIM, Embedding, Job
from workflow_engine.worker.runner import Worker

CORPUS = [
    "The recipe calls for flour, water, salt and a sourdough starter to bake bread.",
    "Our quarterly revenue grew twelve percent compared with the same period last year.",
    "The goalkeeper saved a penalty in the final minute of the match.",
    "Regular exercise and a balanced diet lower the risk of heart disease.",
    "The new smartphone has a faster processor and a larger battery.",
    "Heavy rain and strong winds are expected across the region tomorrow.",
    "The museum exhibition features paintings from the Renaissance period.",
    "To reset your password, click the link we sent to your email address.",
]


async def _run_embed_job(client: httpx.AsyncClient, texts: list[str]) -> dict[str, Any]:
    resp = await client.post("/jobs", json={"type": "embed_texts", "payload": {"texts": texts}})
    assert resp.status_code == 201, resp.text
    return await wait_for_status(client, resp.json()["id"], {"succeeded", "dead"}, within=120)


async def _count(app: FastAPI, job_id: str) -> int:
    async with app.state.session_factory() as session:
        n = await session.scalar(
            select(func.count()).select_from(Embedding).where(Embedding.job_id == uuid.UUID(job_id))
        )
    return int(n or 0)


async def _requeue(app: FastAPI, worker: Worker, job_id: str) -> None:
    """Deliver the job a second time, as happens after a reaped lease or a duplicate hint."""
    async with app.state.session_factory() as session, session.begin():
        await session.execute(
            update(Job)
            .where(Job.id == uuid.UUID(job_id))
            .values(status=JobStatus.QUEUED, finished_at=None, result=None)
        )
    await app.state.producer.enqueue(uuid.UUID(job_id), Priority.NORMAL)


async def test_embed_100_texts_and_reruns_stay_at_100_rows(
    client: httpx.AsyncClient, app: FastAPI, running_worker: Worker
) -> None:
    texts = [f"Document {i}: notes about topic {i % 7} and item {i}." for i in range(100)]
    job = await _run_embed_job(client, texts)
    assert job["status"] == "succeeded", job["last_error"]
    assert job["result"]["embedded"] == 100
    assert await _count(app, job["id"]) == 100

    async with app.state.session_factory() as session:
        dims = await session.scalar(
            select(func.vector_dims(Embedding.embedding)).where(
                Embedding.job_id == uuid.UUID(job["id"])
            )
        )
        original_ids = set(
            (
                await session.scalars(
                    select(Embedding.id).where(Embedding.job_id == uuid.UUID(job["id"]))
                )
            ).all()
        )
    assert dims == EMBEDDING_DIM

    # A full re-run finds everything stored and writes nothing new.
    await _requeue(app, running_worker, job["id"])
    again = await wait_for_status(client, job["id"], {"succeeded", "dead"}, within=60)
    assert again["result"] == {**job["result"], "embedded": 0, "skipped": 100}
    assert await _count(app, job["id"]) == 100

    # A retry after a partial attempt resumes: only the missing items are embedded.
    async with app.state.session_factory() as session, session.begin():
        await session.execute(
            text("DELETE FROM embeddings WHERE job_id = :j AND item_index >= 60"),
            {"j": uuid.UUID(job["id"])},
        )
    await _requeue(app, running_worker, job["id"])
    resumed = await wait_for_status(client, job["id"], {"succeeded", "dead"}, within=60)
    assert resumed["result"]["embedded"] == 40
    assert resumed["result"]["skipped"] == 60
    assert await _count(app, job["id"]) == 100
    async with app.state.session_factory() as session:
        kept = set(
            (
                await session.scalars(
                    select(Embedding.id).where(
                        Embedding.job_id == uuid.UUID(job["id"]), Embedding.item_index < 60
                    )
                )
            ).all()
        )
    assert kept <= original_ids  # rows from the first attempt were left alone


async def test_search_returns_the_paraphrased_text_first(
    client: httpx.AsyncClient, app: FastAPI, running_worker: Worker
) -> None:
    corpus_job = await _run_embed_job(client, CORPUS)
    other_job = await _run_embed_job(
        client, ["Bread is baked in a hot oven.", "Penguins live in Antarctica."]
    )
    assert corpus_job["status"] == other_job["status"] == "succeeded"

    cases = {
        "how do I make a homemade loaf": 0,
        "company earnings went up this quarter": 1,
        "storm warning for the weekend weather": 5,
        "I forgot my login credentials": 7,
    }
    for query, expected in cases.items():
        resp = await client.get(
            "/embeddings/search", params={"q": query, "k": 3, "job_id": corpus_job["id"]}
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert len(body["results"]) == 3
        top = body["results"][0]
        assert top["text"] == CORPUS[expected], (query, body["results"])
        assert top["score"] > body["results"][1]["score"]
        assert abs(top["score"] - (1 - top["distance"])) < 1e-9

    # Without the filter both jobs are searched; with it, only that job's texts come back.
    unfiltered = (
        await client.get("/embeddings/search", params={"q": "baking bread", "k": 10})
    ).json()
    assert {r["job_id"] for r in unfiltered["results"]} == {corpus_job["id"], other_job["id"]}
    filtered = (
        await client.get(
            "/embeddings/search", params={"q": "baking bread", "k": 10, "job_id": other_job["id"]}
        )
    ).json()
    assert filtered["results"][0]["text"] == "Bread is baked in a hot oven."
    assert {r["job_id"] for r in filtered["results"]} == {other_job["id"]}
    assert len(filtered["results"]) == 2


async def test_search_validates_input_and_requires_a_key(
    client: httpx.AsyncClient, app: FastAPI
) -> None:
    max_k = app.state.settings.embed_search_max_k
    for params in ({"q": "x", "k": 0}, {"q": "x", "k": max_k + 1}, {"q": ""}, {"q": "   "}):
        resp = await client.get("/embeddings/search", params=params)
        assert resp.status_code == 422, (params, resp.text)
    assert (await client.get("/embeddings/search", params={"q": "x" * 1001})).status_code == 422
    resp = await client.get("/embeddings/search", params={"q": "x"}, headers={"X-API-Key": "nope"})
    assert resp.status_code == 401
    empty = await client.get("/embeddings/search", params={"q": "anything", "k": max_k})
    assert empty.status_code == 200
    assert empty.json()["results"] == []


async def test_search_query_uses_the_hnsw_index(app: FastAPI) -> None:
    sf = app.state.session_factory
    filler = uuid.uuid4()
    async with sf() as session, session.begin():
        session.add(
            Job(id=filler, type="embed_texts", payload={}, status="succeeded", max_attempts=1)
        )
        await session.flush()
        # Enough rows that the planner prefers the index over a sequential scan + sort, as it
        # would on any real table.
        assert EMBEDDING_DIM == 384
        await session.execute(
            text(
                "INSERT INTO embeddings (job_id, item_index, text, embedding, model) "
                "SELECT :j, g, 'filler ' || g, "
                "(SELECT array_agg(random())::vector(384) "
                "FROM generate_series(1, 384) WHERE g > 0), 'filler' "
                "FROM generate_series(1, 3000) AS g"
            ),
            {"j": filler},
        )
        await session.execute(text("ANALYZE embeddings"))

    probe = [0.05] * EMBEDDING_DIM
    for job_id in (None, filler):
        stmt = search_query(probe, 10, job_id)
        compiled = stmt.compile(dialect=app.state.engine.dialect)
        params = {
            k: ("[" + ",".join(map(str, v)) + "]" if isinstance(v, list) else v)
            for k, v in compiled.params.items()
        }
        async with app.state.engine.connect() as conn:
            if job_id is not None:
                await conn.exec_driver_sql("SET hnsw.iterative_scan = strict_order")
            plan = "\n".join(
                row[0] for row in await conn.exec_driver_sql("EXPLAIN " + compiled.string, params)
            )
        assert "Index Scan using ix_embeddings_embedding_hnsw" in plan, plan
