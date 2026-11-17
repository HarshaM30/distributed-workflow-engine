from __future__ import annotations

import pytest
from pydantic import SecretStr

from workflow_engine.config import Settings


def make(**kwargs: object) -> Settings:
    return Settings(_env_file=None, **kwargs)  # type: ignore[call-arg]


def test_api_keys_are_split_on_commas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEYS", " a , b,,c ")
    assert make().api_key_values == ["a", "b", "c"]


def test_api_keys_are_hidden_in_repr() -> None:
    s = make(api_keys="super-secret")
    assert "super-secret" not in repr(s)


def test_url_built_from_parts() -> None:
    s = make(
        postgres_user="u",
        postgres_password=SecretStr("p@ss"),
        postgres_host="db",
        postgres_port=6543,
        postgres_db="d",
    )
    assert s.sqlalchemy_url == "postgresql+psycopg://u:p%40ss@db:6543/d"


@pytest.mark.parametrize(
    "raw",
    ["postgresql://u:p@h:1/d", "postgres://u:p@h:1/d", "postgresql+psycopg://u:p@h:1/d"],
)
def test_database_url_takes_precedence_and_is_normalised(raw: str) -> None:
    s = make(database_url=raw, postgres_host="ignored")
    assert s.sqlalchemy_url == "postgresql+psycopg://u:p@h:1/d"


def test_empty_database_url_falls_back_to_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("POSTGRES_HOST", "fallback")
    assert "@fallback:" in make().sqlalchemy_url


def test_rate_limits_parse_from_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RATE_LIMITS", '{"cpu": {"rate": 50, "burst": 100}}')
    s = make()
    assert s.rate_limits["cpu"].rate == 50
    assert s.rate_limits["cpu"].burst == 100


def test_blank_worker_id_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_ID", "")
    assert make().worker_id is None


def test_env_example_covers_every_setting() -> None:
    from pathlib import Path

    example = Path(__file__).resolve().parents[2] / ".env.example"
    keys = {
        line.split("=", 1)[0].strip().lstrip("# ").lower()
        for line in example.read_text().splitlines()
        if "=" in line and not line.startswith("# ---")
    }
    missing = set(Settings.model_fields) - keys
    assert not missing, f"settings missing from .env.example: {sorted(missing)}"
