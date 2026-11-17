"""Integration fixtures: a dedicated `<db>_test` database and Redis DB 15, reset per test."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import psycopg
import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from psycopg import sql
from pydantic import BaseModel, ConfigDict, SecretStr
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import make_url

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.db.session import create_engine, create_session_factory
from workflow_engine.dispatch.streams import StreamConsumer, create_redis
from workflow_engine.handlers.registry import HandlerRegistry, JobContext, load_builtin_handlers
from workflow_engine.worker.runner import Worker

REPO_ROOT = Path(__file__).resolve().parents[2]
API_KEY_A = "test-key-a"
API_KEY_B = "test-key-b"
TABLES = "jobs, job_attempts, dead_letters, idempotency_keys, embeddings"


def alembic_config(url: str) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.attributes["sqlalchemy.url"] = url
    cfg.attributes["configure_logger"] = False
    return cfg


def _ensure_database(admin_url: str, name: str) -> None:
    with psycopg.connect(admin_url, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def _redis_test_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "/15", parts.query, parts.fragment))


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    base = Settings()
    url = make_url(base.sqlalchemy_url)
    test_db = f"{url.database}_test"
    admin = url.set(drivername="postgresql", database="postgres")
    _ensure_database(admin.render_as_string(hide_password=False), test_db)
    test_url = url.set(database=test_db).render_as_string(hide_password=False)
    command.upgrade(alembic_config(test_url), "head")
    return base.model_copy(
        update={
            "database_url": SecretStr(test_url),
            "redis_url": SecretStr(_redis_test_url(base.redis_url.get_secret_value())),
            "redis_stream_prefix": "test-jobs",
            "api_keys": [SecretStr(API_KEY_A), SecretStr(API_KEY_B)],
            "worker_block_ms": 100,
            "db_pool_size": 20,
        }
    )


@pytest.fixture(autouse=True)
async def clean_state(test_settings: Settings) -> None:
    engine = create_engine(test_settings)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
    await engine.dispose()
    redis = Redis.from_url(test_settings.redis_url.get_secret_value())
    await redis.flushdb()
    await redis.aclose()


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY_A}
    ) as c:
        yield c


# --------------------------------------------------------------------------- worker helpers


class ExecutionCounter:
    def __init__(self) -> None:
        self.calls: list[uuid.UUID] = []


class CountPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seconds: float = 0
    fail: bool = False


@pytest.fixture
def counter() -> ExecutionCounter:
    return ExecutionCounter()


@pytest.fixture
def registry(counter: ExecutionCounter) -> HandlerRegistry:
    """Built-in handlers plus `count`, which records each execution."""
    reg = load_builtin_handlers().copy()

    @reg.register("count", payload_model=CountPayload, timeout_seconds=2)
    async def count(payload: CountPayload, ctx: JobContext) -> dict[str, int]:
        counter.calls.append(ctx.job_id)
        await asyncio.sleep(payload.seconds)
        if payload.fail:
            raise RuntimeError("asked to fail")
        return {"executions": len(counter.calls)}

    return reg


@pytest.fixture
async def app(test_settings: Settings, registry: HandlerRegistry) -> AsyncIterator[FastAPI]:
    application = create_app(test_settings, registry)
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture
async def make_worker(
    test_settings: Settings, registry: HandlerRegistry
) -> AsyncIterator[Callable[..., Worker]]:
    engines = []
    redises: list[Redis] = []

    def factory(worker_id: str = "test-worker") -> Worker:
        engine = create_engine(test_settings)
        redis = create_redis(test_settings)
        engines.append(engine)
        redises.append(redis)
        return Worker(
            settings=test_settings,
            session_factory=create_session_factory(engine),
            consumer=StreamConsumer(
                redis,
                test_settings.redis_stream_prefix,
                test_settings.redis_consumer_group,
                worker_id,
            ),
            registry=registry,
            worker_id=worker_id,
        )

    yield factory
    for redis in redises:
        await redis.aclose()
    for engine in engines:
        await engine.dispose()


@pytest.fixture
async def running_worker(make_worker: Callable[..., Worker]) -> AsyncIterator[Worker]:
    worker = make_worker()
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    yield worker
    stop.set()
    await asyncio.wait_for(task, timeout=10)


async def wait_for_status(
    client: httpx.AsyncClient, job_id: str, statuses: set[str], within: float = 10
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + within
    while True:
        body: dict[str, Any] = (await client.get(f"/jobs/{job_id}")).json()
        if body["status"] in statuses:
            return body
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"job {job_id} stuck in {body['status']}")
        await asyncio.sleep(0.05)
