"""Worker entrypoint: `python -m workflow_engine.worker`."""

from __future__ import annotations

import asyncio
import os
import socket

from workflow_engine.config import Settings, get_settings
from workflow_engine.db.session import create_engine, create_session_factory
from workflow_engine.dispatch.rate_limit import TokenBucket
from workflow_engine.dispatch.streams import StreamConsumer, StreamProducer, create_redis
from workflow_engine.handlers.registry import load_builtin_handlers
from workflow_engine.logging import configure_logging
from workflow_engine.metrics import serve as serve_metrics
from workflow_engine.telemetry import configure_tracing, instrument_clients, shutdown_tracing
from workflow_engine.worker.runner import Worker
from workflow_engine.worker.shutdown import install_signal_handlers


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


async def run_worker(settings: Settings) -> None:
    configure_tracing(settings, "worker")
    engine = create_engine(settings)
    instrument_clients(settings, engine)
    redis = create_redis(settings)
    metrics_server = (
        await serve_metrics(settings.worker_metrics_port, "worker")
        if settings.metrics_enabled
        else None
    )
    worker_id = settings.worker_id or default_worker_id()
    worker = Worker(
        settings=settings,
        session_factory=create_session_factory(engine),
        consumer=StreamConsumer(
            redis, settings.redis_stream_prefix, settings.redis_consumer_group, worker_id
        ),
        registry=load_builtin_handlers(),
        worker_id=worker_id,
        dispatcher=StreamProducer(redis, settings.redis_stream_prefix, settings.stream_maxlen),
        limiter=TokenBucket(redis, settings.redis_stream_prefix),
    )
    stop = asyncio.Event()
    install_signal_handlers(stop)
    try:
        await worker.run(stop)
    finally:
        if metrics_server is not None:
            metrics_server.close()
        await redis.aclose()
        await engine.dispose()
        shutdown_tracing()


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    asyncio.run(run_worker(settings))
