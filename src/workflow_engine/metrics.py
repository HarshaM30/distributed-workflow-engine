"""Prometheus metric definitions.

Each process exposes the metrics it produces: the API on `/metrics`, workers on
WORKER_METRICS_PORT, and maintenance on MAINTENANCE_METRICS_PORT. The `queue_depth` and
`dlq_size` gauges are read from Postgres (the source of truth) when the API is scraped.
"""

from __future__ import annotations

import asyncio

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from workflow_engine.logging import get_logger

log = get_logger(__name__)

# Seconds; covers sleep(0) jobs through multi-minute handlers.
_DURATION_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    10,
    30,
    60,
    120,
    300,
)

JOBS_SUBMITTED = Counter(
    "jobs_submitted_total", "Jobs created through the API.", ["type", "priority"]
)
JOBS_COMPLETED = Counter(
    "jobs_completed_total",
    "Jobs that reached a terminal state (succeeded, dead, cancelled).",
    ["type", "status"],
)
JOB_DURATION = Histogram(
    "job_duration_seconds",
    "Handler run time of one attempt.",
    ["type", "outcome"],
    buckets=_DURATION_BUCKETS,
)
JOB_E2E_LATENCY = Histogram(
    "job_e2e_latency_seconds",
    "Time from job creation to successful completion.",
    ["type"],
    buckets=_DURATION_BUCKETS,
)
QUEUE_DEPTH = Gauge(
    "queue_depth", "Non-terminal jobs in Postgres, by status and priority.", ["status", "priority"]
)
RETRIES = Counter("retries_total", "Failed attempts that were scheduled for a retry.", ["type"])
DLQ_SIZE = Gauge("dlq_size", "Dead-letter entries that have not been requeued.")
LEASES_REAPED = Counter(
    "leases_reaped_total", "Running jobs whose lease expired, by the outcome.", ["outcome"]
)
SWEEPER_REQUEUED = Counter(
    "sweeper_requeued_total", "Queued jobs the sweeper re-sent to Redis Streams."
)
RATE_LIMITED = Counter(
    "rate_limited_total",
    "Requests or executions held back by a token bucket.",
    ["limiter", "type"],
)


async def _handle_scrape(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5)
        # Drain the headers; the body of a GET is empty.
        while (line := await asyncio.wait_for(reader.readline(), timeout=5)) not in (b"\r\n", b""):
            del line
        parts = request_line.decode("latin-1").split()
        if len(parts) >= 2 and parts[0] == "GET" and parts[1].split("?")[0] == "/metrics":
            status, ctype, body = "200 OK", CONTENT_TYPE_LATEST, generate_latest(REGISTRY)
        else:
            status, ctype, body = "404 Not Found", "text/plain", b"not found\n"
        head = (
            f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
        )
        writer.write(head.encode("latin-1") + body)
        await writer.drain()
    except (TimeoutError, ConnectionError, UnicodeDecodeError):
        pass
    finally:
        writer.close()


async def serve(port: int, component: str, host: str = "0.0.0.0") -> asyncio.Server | None:
    """Serve `GET /metrics` on the running event loop. Port 0 disables it.

    A small asyncio server instead of prometheus_client's threaded one, whose bind does a
    reverse-DNS lookup (`getfqdn`) that can stall start-up for seconds. A port that's already
    taken (several workers on one host) is logged, not fatal.
    """
    if port == 0:
        return None
    try:
        server = await asyncio.start_server(_handle_scrape, host, port)
    except OSError as exc:
        log.warning("metrics_port_unavailable", component=component, port=port, error=str(exc))
        return None
    log.info("metrics_serving", component=component, port=port)
    return server
