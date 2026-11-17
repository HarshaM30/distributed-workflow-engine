"""Handler registry: maps a job type to its coroutine, payload model, and per-type config."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel

from workflow_engine.db.session import SessionFactory

DEFAULT_TIMEOUT_SECONDS = 300.0


@dataclass(frozen=True)
class JobContext:
    job_id: uuid.UUID
    attempt: int
    worker_id: str
    # For handlers that write their own results (e.g. embed_texts upserts vectors).
    session_factory: SessionFactory | None = None


HandlerFunc = Callable[[Any, JobContext], Awaitable[Any]]
F = TypeVar("F", bound=HandlerFunc)


@dataclass(frozen=True)
class HandlerSpec:
    name: str
    func: HandlerFunc
    payload_model: type[BaseModel]
    max_attempts: int | None
    timeout_seconds: float


class UnknownJobTypeError(LookupError):
    pass


class HandlerRegistry:
    def __init__(self, specs: dict[str, HandlerSpec] | None = None) -> None:
        self._specs: dict[str, HandlerSpec] = dict(specs or {})

    def register(
        self,
        name: str,
        *,
        payload_model: type[BaseModel],
        max_attempts: int | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> Callable[[F], F]:
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")

        def decorator(func: F) -> F:
            if name in self._specs:
                raise ValueError(f"handler {name!r} is already registered")
            self._specs[name] = HandlerSpec(
                name, func, payload_model, max_attempts, timeout_seconds
            )
            return func

        return decorator

    def get(self, name: str) -> HandlerSpec | None:
        return self._specs.get(name)

    def require(self, name: str) -> HandlerSpec:
        spec = self._specs.get(name)
        if spec is None:
            raise UnknownJobTypeError(name)
        return spec

    def names(self) -> list[str]:
        return sorted(self._specs)

    def copy(self) -> HandlerRegistry:
        return HandlerRegistry(self._specs)

    def __contains__(self, name: object) -> bool:
        return name in self._specs


default_registry = HandlerRegistry()
handler = default_registry.register


def load_builtin_handlers() -> HandlerRegistry:
    """Import the built-in handler modules (registration happens on import)."""
    from workflow_engine.handlers import cpu, embed_texts, flaky, ledger, sleep  # noqa: F401

    return default_registry
