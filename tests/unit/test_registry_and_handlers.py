from __future__ import annotations

import uuid
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from workflow_engine.handlers import cpu, flaky, sleep
from workflow_engine.handlers.registry import (
    HandlerRegistry,
    JobContext,
    UnknownJobTypeError,
    load_builtin_handlers,
)

CTX = JobContext(job_id=uuid.uuid4(), attempt=1, worker_id="w")


class Empty(BaseModel):
    pass


def test_register_and_lookup() -> None:
    reg = HandlerRegistry()

    @reg.register("noop", payload_model=Empty, max_attempts=2, timeout_seconds=5)
    async def noop(payload: Empty, ctx: JobContext) -> None:
        return None

    spec = reg.require("noop")
    assert spec.func is noop
    assert spec.max_attempts == 2
    assert spec.timeout_seconds == 5
    assert "noop" in reg
    assert reg.names() == ["noop"]


def test_duplicate_registration_is_rejected() -> None:
    reg = HandlerRegistry()

    async def fn(payload: Any, ctx: JobContext) -> None:
        return None

    reg.register("x", payload_model=Empty)(fn)
    with pytest.raises(ValueError, match="already registered"):
        reg.register("x", payload_model=Empty)(fn)


def test_invalid_config_is_rejected() -> None:
    reg = HandlerRegistry()
    with pytest.raises(ValueError, match="max_attempts"):
        reg.register("x", payload_model=Empty, max_attempts=0)
    with pytest.raises(ValueError, match="timeout"):
        reg.register("x", payload_model=Empty, timeout_seconds=0)


def test_unknown_type() -> None:
    assert HandlerRegistry().get("nope") is None
    with pytest.raises(UnknownJobTypeError):
        HandlerRegistry().require("nope")


def test_copy_is_independent() -> None:
    base = load_builtin_handlers()
    clone = base.copy()

    @clone.register("only-in-clone", payload_model=Empty)
    async def fn(payload: Empty, ctx: JobContext) -> None:
        return None

    assert "only-in-clone" in clone
    assert "only-in-clone" not in base


def test_builtins_registered() -> None:
    assert {"sleep", "cpu", "flaky"} <= set(load_builtin_handlers().names())


async def test_sleep_handler() -> None:
    assert await sleep.run(sleep.SleepPayload(seconds=0), CTX) == {"slept_seconds": 0}


def test_sleep_payload_bounds() -> None:
    with pytest.raises(ValidationError):
        sleep.SleepPayload(seconds=-1)
    with pytest.raises(ValidationError):
        sleep.SleepPayload.model_validate({"seconds": 1, "extra": True})


async def test_cpu_handler_is_deterministic() -> None:
    a = await cpu.run(cpu.CpuPayload(iterations=1000, seed="s"), CTX)
    b = await cpu.run(cpu.CpuPayload(iterations=1000, seed="s"), CTX)
    assert a == b
    assert a["digest"] != cpu.hash_chain("other", 1000)


async def test_flaky_handler_rates() -> None:
    assert await flaky.run(flaky.FlakyPayload(failure_rate=0), CTX) == {"ok": True}
    with pytest.raises(flaky.FlakyError):
        await flaky.run(flaky.FlakyPayload(failure_rate=1), CTX)
