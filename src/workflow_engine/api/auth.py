"""`X-API-Key` authentication. Keys come from API_KEYS and are compared in constant time."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Annotated

from fastapi import Header, HTTPException, Request, status

from workflow_engine.core.idempotency import api_key_id


@dataclass(frozen=True)
class Principal:
    key_id: str  # sha256 of the key; the raw key is never stored or logged


def key_matches(candidate: str, keys: list[str]) -> bool:
    # Check every key (no early exit) so timing doesn't reveal which one matched.
    matched = False
    for key in keys:
        matched |= hmac.compare_digest(candidate.encode(), key.encode())
    return matched


async def require_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> Principal:
    keys: list[str] = request.app.state.settings.api_key_values
    if not x_api_key or not key_matches(x_api_key, keys):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return Principal(key_id=api_key_id(x_api_key))
