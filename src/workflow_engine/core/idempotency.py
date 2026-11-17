"""Idempotency helpers: API-key identity, canonical request hashing, and the key record."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IdempotencyRequest:
    api_key_id: str
    key: str
    request_hash: str


class IdempotencyConflictError(Exception):
    """The Idempotency-Key was already used with a different request body."""


def api_key_id(api_key: str) -> str:
    """Stable identifier for an API key. The raw key is never stored."""
    return hashlib.sha256(api_key.encode()).hexdigest()


def canonical_json(body: Any) -> str:
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_hash(body: Any) -> str:
    return hashlib.sha256(canonical_json(body).encode()).hexdigest()
