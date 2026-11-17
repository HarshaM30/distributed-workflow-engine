"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from workflow_engine import __version__
from workflow_engine.api.routes import health, jobs
from workflow_engine.config import Settings, get_settings
from workflow_engine.core.idempotency import IdempotencyConflictError
from workflow_engine.db.session import create_engine, create_session_factory
from workflow_engine.dispatch.streams import StreamProducer, create_redis
from workflow_engine.handlers.registry import HandlerRegistry, load_builtin_handlers
from workflow_engine.logging import configure_logging


def create_app(
    settings: Settings | None = None, registry: HandlerRegistry | None = None
) -> FastAPI:
    settings = settings or get_settings()
    registry = registry if registry is not None else load_builtin_handlers()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Nothing here may require Redis/Postgres to be up: /health must still answer 503.
        engine = create_engine(settings)
        redis = create_redis(settings)
        app.state.engine = engine
        app.state.session_factory = create_session_factory(engine)
        app.state.redis = redis
        app.state.producer = StreamProducer(
            redis, settings.redis_stream_prefix, settings.stream_maxlen
        )
        try:
            yield
        finally:
            await redis.aclose()
            await engine.dispose()

    app = FastAPI(title="Distributed Workflow Engine", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.registry = registry
    app.include_router(health.router)
    app.include_router(jobs.router)

    @app.exception_handler(IdempotencyConflictError)
    async def _idempotency_conflict(
        request: Request, exc: IdempotencyConflictError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"detail": "Idempotency-Key was already used with a different request body"},
        )

    return app


def main() -> None:
    import uvicorn

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,
        access_log=False,
    )
