"""Integration fixtures: a dedicated `<db>_test` database and Redis DB 15, reset per test."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx
import psycopg
import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from psycopg import sql
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import make_url

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings
from workflow_engine.db.session import create_engine

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
async def app(test_settings: Settings) -> AsyncIterator[FastAPI]:
    application = create_app(test_settings)
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY_A}
    ) as c:
        yield c
