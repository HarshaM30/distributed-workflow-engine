from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from fastapi.exceptions import RequestValidationError
from hypothesis import given
from hypothesis import strategies as st

from workflow_engine.api.auth import key_matches
from workflow_engine.api.schemas import JobCreate, validate_job_payload
from workflow_engine.core.idempotency import api_key_id, request_hash
from workflow_engine.core.jobs import (
    InvalidCursorError,
    decode_cursor,
    encode_cursor,
    resolve_max_attempts,
)
from workflow_engine.handlers.registry import load_builtin_handlers

REGISTRY = load_builtin_handlers()


def test_key_matches() -> None:
    assert key_matches("b", ["a", "b"])
    assert not key_matches("c", ["a", "b"])
    assert not key_matches("a", [])


def test_api_key_id_is_stable_and_not_the_key() -> None:
    assert api_key_id("k") == api_key_id("k")
    assert api_key_id("k") != api_key_id("j")
    assert api_key_id("k") != "k"
    assert len(api_key_id("k")) == 64


def test_request_hash_ignores_key_order() -> None:
    assert request_hash({"a": 1, "b": {"x": 1, "y": 2}}) == request_hash(
        {"b": {"y": 2, "x": 1}, "a": 1}
    )
    assert request_hash({"a": 1}) != request_hash({"a": 2})


def test_resolve_max_attempts_fallback_order() -> None:
    assert resolve_max_attempts(3, 7, 5) == 3
    assert resolve_max_attempts(None, 7, 5) == 7
    assert resolve_max_attempts(None, None, 5) == 5


@given(
    st.datetimes(timezones=st.just(UTC)),
    st.uuids(),
)
def test_cursor_round_trip(created_at: datetime, job_id: uuid.UUID) -> None:
    assert decode_cursor(encode_cursor(created_at, job_id)) == (created_at, job_id)


@pytest.mark.parametrize("bad", ["", "not-base64!", "e30", "WyJ4IiwgInkiXQ"])
def test_invalid_cursor(bad: str) -> None:
    with pytest.raises(InvalidCursorError):
        decode_cursor(bad)


def test_payload_is_normalised_with_defaults() -> None:
    body = JobCreate(type="sleep", payload={})
    assert validate_job_payload(body, REGISTRY, 1000) == {"seconds": 1.0}


def test_unknown_type_is_422() -> None:
    with pytest.raises(RequestValidationError) as exc:
        validate_job_payload(JobCreate(type="nope"), REGISTRY, 1000)
    (err,) = exc.value.errors()
    assert err["loc"] == ("body", "type")
    assert "unknown job type" in err["msg"]


def test_bad_payload_errors_point_into_payload() -> None:
    with pytest.raises(RequestValidationError) as exc:
        validate_job_payload(JobCreate(type="sleep", payload={"seconds": -5}), REGISTRY, 1000)
    assert exc.value.errors()[0]["loc"] == ("body", "payload", "seconds")


def test_oversized_payload_is_422() -> None:
    body = JobCreate(type="cpu", payload={"seed": "x" * 200})
    with pytest.raises(RequestValidationError) as exc:
        validate_job_payload(body, REGISTRY, 100)
    assert "limit is 100" in exc.value.errors()[0]["msg"]
