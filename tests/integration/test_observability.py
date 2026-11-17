"""Logs, traces, and metrics (Milestone 4)."""

from __future__ import annotations

import asyncio
import json
import signal
import subprocess
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi import FastAPI
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from prometheus_client import REGISTRY
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import update

from tests.integration.conftest import wait_for_status, wait_until
from tests.integration.test_leases import worker_env
from workflow_engine.config import Settings
from workflow_engine.core import jobs
from workflow_engine.db.models import Job
from workflow_engine.maintenance.main import Maintenance
from workflow_engine.telemetry import configure_tracing
from workflow_engine.worker.runner import Worker

# --------------------------------------------------------------------------- tracing


@pytest.fixture(scope="session")
def _session_exporter(test_settings: Settings) -> InMemorySpanExporter:
    # The process-wide provider (also used by create_app); spans are kept in memory.
    provider = configure_tracing(test_settings, "api")
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture
def spans(_session_exporter: InMemorySpanExporter) -> Iterator[InMemorySpanExporter]:
    _session_exporter.clear()
    yield _session_exporter
    _session_exporter.clear()


def _attr(span: ReadableSpan, key: str) -> object:
    return (span.attributes or {}).get(key)


async def test_one_trace_spans_api_request_enqueue_claim_and_handler(
    client: httpx.AsyncClient, running_worker: Worker, spans: InMemorySpanExporter
) -> None:
    job_id = (await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})).json()[
        "id"
    ]
    await wait_for_status(client, job_id, {"succeeded"})

    def process_span() -> ReadableSpan | None:
        for s in spans.get_finished_spans():
            if s.name == "job.process" and _attr(s, "job.id") == job_id:
                return s
        return None

    await wait_until(lambda: process_span() is not None, message="job.process span")
    process = process_span()
    assert process is not None
    assert process.context is not None
    trace_id = process.context.trace_id
    in_trace = [
        s
        for s in spans.get_finished_spans()
        if s.context is not None and s.context.trace_id == trace_id
    ]
    by_name = {s.name: s for s in in_trace}
    assert {"job.enqueue", "job.process", "job.claim", "job.handler"} <= by_name.keys()
    (server,) = [
        s for s in in_trace if s.kind == SpanKind.SERVER and _attr(s, "http.route") == "/jobs"
    ]

    def parent_id(span: ReadableSpan) -> int | None:
        return span.parent.span_id if span.parent is not None else None

    def span_id(span: ReadableSpan) -> int:
        assert span.context is not None
        return span.context.span_id

    # API request -> enqueue -> (stream) -> worker process -> claim / handler
    assert parent_id(by_name["job.enqueue"]) == span_id(server)
    assert parent_id(process) == span_id(by_name["job.enqueue"])
    assert parent_id(by_name["job.claim"]) == span_id(process)
    assert parent_id(by_name["job.handler"]) == span_id(process)
    assert _attr(process, "job.type") == "sleep"


# --------------------------------------------------------------------------- logs


def _spawn_logging_worker(env: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "workflow_engine.worker"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


async def test_worker_log_lines_are_json_and_filterable_by_job_id(
    client: httpx.AsyncClient, test_settings: Settings
) -> None:
    env = worker_env(
        test_settings,
        WORKER_ID="json-logger",
        LOG_LEVEL="INFO",
        LOG_JSON="true",
        WORKER_METRICS_PORT="0",
        PYTHONUNBUFFERED="1",
    )
    proc = _spawn_logging_worker(env)
    try:
        ids = [
            (await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})).json()[
                "id"
            ]
            for _ in range(3)
        ]
        for job_id in ids:
            await wait_for_status(client, job_id, {"succeeded"}, within=20)
        proc.send_signal(signal.SIGTERM)
        out, _ = await asyncio.to_thread(proc.communicate, None, 15)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 0
    lines = [json.loads(line) for line in out.decode().splitlines() if line.strip()]
    assert lines, "worker printed nothing"
    mine = [line for line in lines if line.get("job_id") == ids[1]]
    assert [line["event"] for line in mine] == ["job_started", "job_succeeded"]
    for line in mine:
        assert line["job_type"] == "sleep"
        assert line["attempt"] == 1
        assert line["worker_id"] == "json-logger"
        assert len(str(line["trace_id"])) == 32
    # The trace started in the API request carries through to the worker's lines.
    assert mine[0]["trace_id"] == mine[1]["trace_id"]


async def test_request_id_is_generated_or_echoed(client: httpx.AsyncClient) -> None:
    generated = (await client.get("/jobs")).headers["X-Request-ID"]
    assert len(generated) == 32
    echoed = await client.get("/jobs", headers={"X-Request-ID": "trace-me-123"})
    assert echoed.headers["X-Request-ID"] == "trace-me-123"


# --------------------------------------------------------------------------- metrics


def _sample(text: str, name: str, **labels: str) -> float:
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name == name and all(sample.labels.get(k) == v for k, v in labels.items()):
                return float(sample.value)
    return 0.0


async def _scrape(client: httpx.AsyncClient) -> str:
    # No API key: /metrics is open, like /health.
    resp = await client.get("/metrics", headers={"X-API-Key": ""})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    list(text_string_to_metric_families(resp.text))  # parses cleanly
    return resp.text


async def test_metrics_scrape_cleanly_and_counters_move(
    client: httpx.AsyncClient,
    running_worker: Worker,
    running_maintenance: Maintenance,
) -> None:
    before = await _scrape(client)
    ok = (await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})).json()
    bad = (
        await client.post(
            "/jobs", json={"type": "flaky", "payload": {"failure_rate": 1}, "max_attempts": 2}
        )
    ).json()
    await wait_for_status(client, ok["id"], {"succeeded"})
    await wait_for_status(client, bad["id"], {"dead"})
    after = await _scrape(client)

    def delta(name: str, **labels: str) -> float:
        return _sample(after, name, **labels) - _sample(before, name, **labels)

    assert delta("jobs_submitted_total", type="sleep", priority="normal") == 1
    assert delta("jobs_completed_total", type="sleep", status="succeeded") == 1
    assert delta("jobs_completed_total", type="flaky", status="dead") == 1
    assert delta("retries_total", type="flaky") == 1
    assert delta("job_duration_seconds_count", type="sleep", outcome="succeeded") == 1
    assert delta("job_duration_seconds_count", type="flaky", outcome="failed") == 2
    assert delta("job_e2e_latency_seconds_count", type="sleep") == 1
    assert _sample(after, "dlq_size") == 1
    assert "queue_depth" in after
    assert _sample(after, "queue_depth", status="queued", priority="normal") == 0


async def test_maintenance_counters_move(
    app: FastAPI, client: httpx.AsyncClient, make_maintenance: type[Maintenance]
) -> None:
    sf = app.state.session_factory
    maintenance = make_maintenance()  # type: ignore[call-arg]

    def value(name: str, **labels: str) -> float:
        return REGISTRY.get_sample_value(name, labels) or 0.0

    # A queued job nobody touched for a while: the sweeper re-sends it.
    stale = (await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 0}})).json()
    async with sf() as session, session.begin():
        await session.execute(
            update(Job)
            .where(Job.id == uuid.UUID(stale["id"]))
            .values(updated_at=datetime.now(UTC) - timedelta(minutes=5))
        )
    requeued = value("sweeper_requeued_total")
    await maintenance.sweeper_tick()
    assert value("sweeper_requeued_total") == requeued + 1

    # A running job whose lease expired: the reaper takes it back.
    claim = await jobs.claim_job(sf, uuid.UUID(stale["id"]), "ghost", 30)
    assert claim is not None
    async with sf() as session, session.begin():
        await session.execute(
            update(Job)
            .where(Job.id == claim.job_id)
            .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    reaped = value("leases_reaped_total", outcome="queued")
    assert await maintenance.reaper_tick() == 1
    assert value("leases_reaped_total", outcome="queued") == reaped + 1


async def test_client_instrumentation_records_sql_spans(
    test_settings: Settings, spans: InMemorySpanExporter
) -> None:
    from sqlalchemy import text

    from workflow_engine.db.session import create_engine
    from workflow_engine.telemetry import instrument_clients, tracer

    engine = create_engine(test_settings)
    try:
        instrument_clients(test_settings.model_copy(update={"otel_enabled": True}), engine)
        with tracer().start_as_current_span("root") as root:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
    finally:
        await engine.dispose()
    root_id = root.get_span_context().span_id
    children = [s for s in spans.get_finished_spans() if s.parent and s.parent.span_id == root_id]
    assert any(_attr(s, "db.system") == "postgresql" for s in children), children
