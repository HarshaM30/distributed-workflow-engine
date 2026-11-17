"""Redis connection helpers and Streams producer/consumer."""

from __future__ import annotations

from redis.asyncio import Redis

from workflow_engine.config import Settings


def create_redis(settings: Settings) -> Redis:
    return Redis.from_url(
        settings.redis_url.get_secret_value(),
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=5,
        health_check_interval=30,
    )
