"""Token-bucket rate limiting in Redis, shared by both limiters.

- Execution throttle, per job type: a worker that finds the bucket empty defers the job.
- Submission limit, per API key: `POST /jobs` answers 429 with `Retry-After`.

Both fail open: if Redis is unreachable the request is allowed and a warning is logged.
Postgres stays the source of truth, so a limiter outage never loses jobs.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from workflow_engine.config import RateLimit
from workflow_engine.logging import get_logger

log = get_logger(__name__)

_SCRIPT = resources.files("workflow_engine.dispatch").joinpath("lua/token_bucket.lua").read_text()


@dataclass(frozen=True)
class Decision:
    allowed: bool
    retry_after: float  # seconds until enough tokens exist; 0 when allowed
    failed_open: bool = False


class TokenBucket:
    def __init__(self, redis: Redis, prefix: str) -> None:
        self._prefix = prefix
        self._script: Any = redis.register_script(_SCRIPT)

    def key(self, scope: str, name: str) -> str:
        return f"{self._prefix}:ratelimit:{scope}:{name}"

    async def acquire(self, scope: str, name: str, limit: RateLimit, cost: int = 1) -> Decision:
        try:
            allowed, retry_after, _ = await self._script(
                keys=[self.key(scope, name)], args=[limit.rate, limit.burst, cost]
            )
        except (RedisError, OSError) as exc:
            log.warning("rate_limiter_unavailable", scope=scope, error=type(exc).__name__)
            return Decision(allowed=True, retry_after=0, failed_open=True)
        return Decision(allowed=bool(int(allowed)), retry_after=float(retry_after))
