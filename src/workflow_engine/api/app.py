"""FastAPI application factory."""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from workflow_engine import __version__
from workflow_engine.api.routes import dlq, embeddings, health, jobs, metrics
from workflow_engine.config import Settings, get_settings
from workflow_engine.core.idempotency import IdempotencyConflictError
from workflow_engine.db.session import create_engine, create_session_factory
from workflow_engine.dispatch.rate_limit import TokenBucket
from workflow_engine.dispatch.streams import StreamProducer, create_redis
from workflow_engine.handlers.registry import HandlerRegistry, load_builtin_handlers
from workflow_engine.logging import configure_logging, get_logger
from workflow_engine.telemetry import (
    configure_tracing,
    instrument_app,
    instrument_clients,
    shutdown_tracing,
)

log = get_logger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
# A caller-supplied request ID is echoed only if it's short and plain (no log injection).
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_QUIET_PATHS = frozenset({"/health", "/metrics"})


def request_id_from(request: Request) -> str:
    supplied = request.headers.get(REQUEST_ID_HEADER)
    if supplied and _REQUEST_ID_RE.match(supplied):
        return supplied
    return uuid.uuid4().hex


def create_app(
    settings: Settings | None = None, registry: HandlerRegistry | None = None
) -> FastAPI:
    settings = settings or get_settings()
    registry = registry if registry is not None else load_builtin_handlers()
    configure_tracing(settings, "api")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Nothing here may require Redis/Postgres to be up: /health must still answer 503.
        engine = create_engine(settings)
        instrument_clients(settings, engine)
        redis = create_redis(settings)
        app.state.engine = engine
        app.state.session_factory = create_session_factory(engine)
        app.state.redis = redis
        app.state.producer = StreamProducer(
            redis, settings.redis_stream_prefix, settings.stream_maxlen
        )
        app.state.limiter = TokenBucket(redis, settings.redis_stream_prefix)
        try:
            yield
        finally:
            await redis.aclose()
            await engine.dispose()

    app = FastAPI(title="Distributed Workflow Engine", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.registry = registry
    app.include_router(health.router)
    app.include_router(metrics.router)
    app.include_router(jobs.router)
    app.include_router(dlq.router)
    app.include_router(embeddings.router)

    @app.middleware("http")
    async def request_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request_id_from(request)
        started = time.perf_counter()
        with structlog.contextvars.bound_contextvars(request_id=request_id):
            response = await call_next(request)
            fields = {
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }
            if request.url.path in _QUIET_PATHS:
                log.debug("http_request", **fields)
            else:
                log.info("http_request", **fields)
        response.headers[REQUEST_ID_HEADER] = request_id
        return response

    @app.exception_handler(IdempotencyConflictError)
    async def _idempotency_conflict(
        request: Request, exc: IdempotencyConflictError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"detail": "Idempotency-Key was already used with a different request body"},
        )

    # Added last, so its server span wraps the middleware above (trace_id in the access log).
    instrument_app(app)
    return app


def main() -> None:
    import uvicorn

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    try:
        uvicorn.run(
            create_app(settings),
            host=settings.api_host,
            port=settings.api_port,
            log_config=None,
            access_log=False,
        )
    finally:
        shutdown_tracing()
