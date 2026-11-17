from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator

import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from pydantic import ValidationError
from starlette.requests import Request

from workflow_engine.api.app import request_id_from
from workflow_engine.dispatch.streams import _message
from workflow_engine.handlers.embed_texts import MAX_TEXT_CHARS, MAX_TEXTS, EmbedTextsPayload
from workflow_engine.logging import configure_logging
from workflow_engine.metrics import JOBS_SUBMITTED, serve
from workflow_engine.telemetry import extract_trace_context, inject_trace_context

# A private SDK tracer: never touches the global provider other tests may install.
_tracer = TracerProvider().get_tracer("test")


@pytest.fixture
def json_logs(capsys: pytest.CaptureFixture[str]) -> Iterator[pytest.CaptureFixture[str]]:
    root = logging.getLogger()
    saved = root.handlers[:], root.level
    # Tests call configure_logging themselves: capsys only swaps sys.stdout for the call phase.
    yield capsys
    root.handlers, level = saved
    root.setLevel(level)
    structlog.reset_defaults()


def _lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def test_structlog_and_stdlib_lines_are_json_with_bound_context(
    json_logs: pytest.CaptureFixture[str],
) -> None:
    configure_logging("DEBUG", json=True)
    log = structlog.get_logger("t")
    with structlog.contextvars.bound_contextvars(job_id="j-1", job_type="sleep", attempt=2):
        log.info("job_started", extra_field=1)
        logging.getLogger("uvicorn.error").warning("from %s", "stdlib")
        try:
            raise ValueError("boom")
        except ValueError:
            log.exception("job_crashed")
    lines = _lines(json_logs)
    assert [line["event"] for line in lines] == ["job_started", "from stdlib", "job_crashed"]
    assert all(line["job_id"] == "j-1" for line in lines)
    assert lines[1]["logger"] == "uvicorn.error"
    assert "ValueError: boom" in str(lines[2]["exception"])


def test_trace_id_is_added_inside_a_span(json_logs: pytest.CaptureFixture[str]) -> None:
    configure_logging("DEBUG", json=True)
    log = structlog.get_logger("t")
    log.info("outside")
    with _tracer.start_as_current_span("s") as span:
        log.info("inside")
        expected = format(span.get_span_context().trace_id, "032x")
    outside, inside = _lines(json_logs)
    assert "trace_id" not in outside
    assert inside["trace_id"] == expected


def test_trace_context_round_trips_through_stream_fields() -> None:
    assert inject_trace_context() == {}  # no active span: nothing to carry
    with _tracer.start_as_current_span("api") as span:
        fields = {"job_id": "8b0e8d3e-2f7c-4a0e-9d55-3a8f5f1d2c11", **inject_trace_context()}
        sent = span.get_span_context()
    assert fields["traceparent"].split("-")[1] == format(sent.trace_id, "032x")

    message = _message("jobs:normal", "1-0", {**fields, "unrelated": "x"})
    assert set(message.trace_context) == {"traceparent"}
    parent = trace.get_current_span(extract_trace_context(message.trace_context))
    assert parent.get_span_context().trace_id == sent.trace_id
    assert parent.get_span_context().span_id == sent.span_id


def _request(headers: dict[str, str]) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "headers": raw})


def test_request_id_is_echoed_only_when_safe() -> None:
    assert request_id_from(_request({"X-Request-ID": "abc-123.x"})) == "abc-123.x"
    generated = request_id_from(_request({"X-Request-ID": 'evil"\n{"admin":true}'}))
    assert len(generated) == 32
    assert len(request_id_from(_request({}))) == 32
    assert request_id_from(_request({"X-Request-ID": "x" * 129})) != "x" * 129


def test_embed_texts_payload_caps() -> None:
    assert EmbedTextsPayload.model_validate({"texts": ["  a  "]}).texts == ["a"]
    for bad in (
        {"texts": []},
        {"texts": ["ok", "   "]},
        {"texts": ["x" * (MAX_TEXT_CHARS + 1)]},
        {"texts": ["t"] * (MAX_TEXTS + 1)},
        {"texts": ["a"], "extra": 1},
    ):
        with pytest.raises(ValidationError):
            EmbedTextsPayload.model_validate(bad)


async def test_metrics_server_serves_scrapes_and_port_zero_disables() -> None:
    assert await serve(0, "test") is None
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()

    metrics_server = await serve(port, "test", host="127.0.0.1")
    assert metrics_server is not None
    try:
        JOBS_SUBMITTED.labels("unit_probe", "normal").inc()

        async def get(path: str) -> bytes:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            await writer.drain()
            body = await reader.read()
            writer.close()
            return body

        ok = await get("/metrics")
        assert ok.startswith(b"HTTP/1.1 200 OK")
        assert b'jobs_submitted_total{priority="normal",type="unit_probe"}' in ok
        assert (await get("/nope")).startswith(b"HTTP/1.1 404")
        # A second server on the same port is logged and skipped, not fatal.
        assert await serve(port, "test", host="127.0.0.1") is None
    finally:
        metrics_server.close()
        await metrics_server.wait_closed()
