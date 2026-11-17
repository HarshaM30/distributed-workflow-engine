"""Worker entrypoint: `python -m workflow_engine.worker`."""

from __future__ import annotations

import asyncio
import os
import socket

from workflow_engine.config import Settings, get_settings
from workflow_engine.db.session import create_engine, create_session_factory
from workflow_engine.dispatch.streams import StreamConsumer, create_redis
from workflow_engine.handlers.registry import load_builtin_handlers
from workflow_engine.logging import configure_logging
from workflow_engine.worker.runner import Worker
from workflow_engine.worker.shutdown import install_signal_handlers


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


async def run_worker(settings: Settings) -> None:
    engine = create_engine(settings)
    redis = create_redis(settings)
    worker_id = settings.worker_id or default_worker_id()
    worker = Worker(
        settings=settings,
        session_factory=create_session_factory(engine),
        consumer=StreamConsumer(
            redis, settings.redis_stream_prefix, settings.redis_consumer_group, worker_id
        ),
        registry=load_builtin_handlers(),
        worker_id=worker_id,
    )
    stop = asyncio.Event()
    install_signal_handlers(stop)
    try:
        await worker.run(stop)
    finally:
        await redis.aclose()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    asyncio.run(run_worker(settings))
