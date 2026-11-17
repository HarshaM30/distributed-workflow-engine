from __future__ import annotations

import psycopg
from alembic import command
from sqlalchemy.engine import make_url

from tests.integration.conftest import alembic_config
from workflow_engine.config import Settings

EXPECTED_TABLES = {
    "jobs",
    "job_attempts",
    "dead_letters",
    "idempotency_keys",
    "embeddings",
    "job_executions",
}


def _tables(url: str) -> set[str]:
    plain = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    with psycopg.connect(plain) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ).fetchall()
    return {r[0] for r in rows} - {"alembic_version"}


def test_downgrade_base_then_upgrade_head(test_settings: Settings) -> None:
    url = test_settings.sqlalchemy_url
    cfg = alembic_config(url)
    assert _tables(url) == EXPECTED_TABLES
    command.downgrade(cfg, "base")
    assert _tables(url) == set()
    command.upgrade(cfg, "head")
    assert _tables(url) == EXPECTED_TABLES
