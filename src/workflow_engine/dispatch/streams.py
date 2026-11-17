"""Redis connection helpers and the Streams producer/consumer.

Stream entries are hints only: a worker must still win the CAS claim in Postgres.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, cast

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from workflow_engine.config import Settings
from workflow_engine.core.states import PRIORITY_ORDER, Priority


def create_redis(settings: Settings) -> Redis:
    return Redis.from_url(
        settings.redis_url.get_secret_value(),
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=10,
        health_check_interval=30,
    )


def stream_name(prefix: str, priority: Priority) -> str:
    return f"{prefix}:{priority.value}"


@dataclass(frozen=True)
class StreamMessage:
    stream: str
    message_id: str
    job_id: uuid.UUID | None  # None when the entry is malformed


class StreamProducer:
    def __init__(self, redis: Redis, prefix: str, maxlen: int) -> None:
        self._redis = redis
        self._prefix = prefix
        self._maxlen = maxlen

    async def enqueue(self, job_id: uuid.UUID, priority: Priority) -> str:
        message_id = await self._redis.xadd(
            stream_name(self._prefix, priority),
            {"job_id": str(job_id)},
            maxlen=self._maxlen,
            approximate=True,
        )
        return str(message_id)


def _parse_job_id(fields: dict[str, Any]) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(fields["job_id"]))
    except (KeyError, ValueError):
        return None


class StreamConsumer:
    """Consumer-group reader that polls streams in priority order (high, normal, low)."""

    def __init__(self, redis: Redis, prefix: str, group: str, consumer: str) -> None:
        self._redis = redis
        self.group = group
        self.consumer = consumer
        self.streams = [stream_name(prefix, p) for p in PRIORITY_ORDER]

    async def ensure_groups(self) -> None:
        for stream in self.streams:
            try:
                # id="0": entries added before the group existed are still delivered.
                await self._redis.xgroup_create(stream, self.group, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

    async def _xreadgroup(
        self, streams: dict[str, str], block_ms: int | None
    ) -> list[StreamMessage]:
        try:
            raw = await self._redis.xreadgroup(
                self.group,
                self.consumer,
                cast(Any, streams),
                count=1,
                block=block_ms,
            )
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            # Redis lost the stream/group (restart, FLUSHALL): recreate and read next time.
            await self.ensure_groups()
            return []
        # Shape: [[stream, [(message_id, {field: value}), ...]], ...]
        response = cast(list[tuple[str, list[tuple[str, dict[str, Any]]]]], raw or [])
        messages: list[StreamMessage] = []
        for stream, entries in response:
            for message_id, fields in entries:
                messages.append(StreamMessage(stream, message_id, _parse_job_id(fields)))
        messages.sort(key=lambda m: self.streams.index(m.stream))
        return messages

    async def read(self, block_ms: int) -> list[StreamMessage]:
        # Non-blocking pass in strict priority order, then block on all three.
        for stream in self.streams:
            messages = await self._xreadgroup({stream: ">"}, None)
            if messages:
                return messages
        return await self._xreadgroup(dict.fromkeys(self.streams, ">"), block_ms)

    async def ack(self, message: StreamMessage) -> None:
        await self._redis.xack(message.stream, self.group, message.message_id)


async def reclaim_idle(
    redis: Redis,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int,
    *,
    count: int = 100,
    max_rounds: int = 10,
) -> list[StreamMessage]:
    """XAUTOCLAIM pending-entry-list entries idle for `min_idle_ms`, then XACK them.

    They belong to consumers that died (or are still running a long job). Either way,
    Postgres knows the job's real state; the caller re-sends any job that is still `queued`.
    """
    reclaimed: list[StreamMessage] = []
    start = "0-0"
    for _ in range(max_rounds):
        try:
            raw = await redis.xautoclaim(
                stream, group, consumer, min_idle_ms, start_id=start, count=count
            )
        except ResponseError as exc:
            if "NOGROUP" in str(exc):
                return reclaimed  # stream or group is gone (FLUSHALL); nothing to reclaim
            raise
        # Shape (Redis 7): [next_start, [(message_id, {field: value}), ...], [deleted_ids]]
        next_start, entries = cast(tuple[str, list[tuple[str, dict[str, Any] | None]]], raw[:2])
        batch = [
            StreamMessage(stream, message_id, _parse_job_id(fields or {}))
            for message_id, fields in entries
        ]
        if batch:
            await redis.xack(stream, group, *[m.message_id for m in batch])
            reclaimed.extend(batch)
        if next_start == "0-0":
            break
        start = next_start
    return reclaimed
