"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from workflow_engine import __version__
from workflow_engine.api.routes import health
from workflow_engine.config import Settings, get_settings
from workflow_engine.db.session import create_engine, create_session_factory
from workflow_engine.dispatch.streams import create_redis
from workflow_engine.logging import configure_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Nothing here may require Redis/Postgres to be up: /health must still answer 503.
        engine = create_engine(settings)
        redis = create_redis(settings)
        app.state.engine = engine
        app.state.session_factory = create_session_factory(engine)
        app.state.redis = redis
        try:
            yield
        finally:
            await redis.aclose()
            await engine.dispose()

    app = FastAPI(title="Distributed Workflow Engine", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.include_router(health.router)
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
