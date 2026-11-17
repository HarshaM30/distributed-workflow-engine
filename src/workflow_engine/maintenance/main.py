"""Maintenance process: `python -m workflow_engine.maintenance`.

Runs the scheduler, reaper, and sweeper loops, but only while it holds a Postgres
session-level advisory lock (leader election). Any number of copies can run; one is active,
and a standby takes over within MAINTENANCE_LEADER_RETRY_SECONDS once the leader's
connection goes away.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from workflow_engine.config import Settings, get_settings
from workflow_engine.core import jobs
from workflow_engine.core.states import PRIORITY_ORDER
from workflow_engine.db.session import SessionFactory, create_engine, create_session_factory
from workflow_engine.dispatch.streams import StreamProducer, create_redis, stream_name
from workflow_engine.logging import configure_logging, get_logger
from workflow_engine.maintenance.reaper import reap_expired
from workflow_engine.maintenance.scheduler import promote_due_jobs
from workflow_engine.maintenance.sweeper import sweep
from workflow_engine.metrics import serve as serve_metrics
from workflow_engine.telemetry import configure_tracing, instrument_clients, shutdown_tracing
from workflow_engine.worker.shutdown import install_signal_handlers

log = get_logger(__name__)

# Arbitrary but fixed: every maintenance process contends for this one advisory lock.
LEADER_LOCK_KEY = 0x5746_4D41_494E  # "WFMAIN"
LOCK_CHECK_SECONDS = 2.0


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


class Maintenance:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: SessionFactory,
        redis: Redis,
        dispatcher: jobs.Dispatcher,
        name: str = "maintenance",
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.redis = redis
        self.dispatcher = dispatcher
        self.name = name
        self.is_leader = False
        self.streams = [stream_name(settings.redis_stream_prefix, p) for p in PRIORITY_ORDER]

    # ------------------------------------------------------------------ one pass of each loop

    async def scheduler_tick(self) -> int:
        return await promote_due_jobs(
            self.session_factory, self.dispatcher, self.settings.maintenance_batch_size
        )

    async def reaper_tick(self) -> int:
        counts = await reap_expired(
            self.session_factory, self.dispatcher, self.settings.maintenance_batch_size
        )
        return counts.total()

    async def sweeper_tick(self) -> int:
        result = await sweep(
            self.session_factory,
            self.dispatcher,
            self.redis,
            streams=self.streams,
            group=self.settings.redis_consumer_group,
            stale_seconds=self.settings.sweeper_stale_seconds,
            batch_size=self.settings.maintenance_batch_size,
        )
        return result.requeued

    # ------------------------------------------------------------------ leader election

    async def run(self, stop: asyncio.Event) -> None:
        # A dedicated, unpooled connection holds the lock: closing it always releases the
        # lock, and a pooled connection can never carry it to unrelated work.
        lock_engine = create_async_engine(self.settings.sqlalchemy_url, poolclass=NullPool)
        try:
            while not stop.is_set():
                try:
                    async with lock_engine.connect() as raw:
                        conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
                        acquired = await conn.scalar(
                            select(func.pg_try_advisory_lock(LEADER_LOCK_KEY))
                        )
                        if acquired:
                            await self._lead(conn, stop)
                except (SQLAlchemyError, OSError) as exc:
                    log.warning("leader_lock_error", error=type(exc).__name__)
                finally:
                    if self.is_leader:
                        log.info("leadership_released", name=self.name)
                    self.is_leader = False
                await _sleep_or_stop(stop, self.settings.maintenance_leader_retry_seconds)
        finally:
            await lock_engine.dispose()

    async def _lead(self, conn: AsyncConnection, stop: asyncio.Event) -> None:
        self.is_leader = True
        log.info("leadership_acquired", name=self.name)
        stepdown = asyncio.Event()
        s = self.settings
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._watch_lock(conn, stop, stepdown))
            tg.create_task(
                self._loop("scheduler", s.scheduler_interval_seconds, self.scheduler_tick, stepdown)
            )
            tg.create_task(
                self._loop("reaper", s.reaper_interval_seconds, self.reaper_tick, stepdown)
            )
            tg.create_task(
                self._loop("sweeper", s.sweeper_interval_seconds, self.sweeper_tick, stepdown)
            )

    async def _watch_lock(
        self, conn: AsyncConnection, stop: asyncio.Event, stepdown: asyncio.Event
    ) -> None:
        """Step down on shutdown, or as soon as the lock connection stops answering."""
        while not stepdown.is_set():
            await _sleep_or_stop(stop, LOCK_CHECK_SECONDS)
            if stop.is_set():
                break
            try:
                await conn.execute(select(1))
            except (SQLAlchemyError, OSError) as exc:
                log.warning("leader_lock_lost", error=type(exc).__name__)
                break
        stepdown.set()

    @staticmethod
    async def _loop(
        name: str, interval: float, tick: Callable[[], Awaitable[int]], stepdown: asyncio.Event
    ) -> None:
        while not stepdown.is_set():
            try:
                await tick()
            except Exception:
                # One bad pass (Postgres/Redis blip) must not end the loop.
                log.exception("maintenance_tick_failed", loop=name)
            await _sleep_or_stop(stepdown, interval)


async def run_maintenance(settings: Settings) -> None:
    configure_tracing(settings, "maintenance")
    engine = create_engine(settings)
    instrument_clients(settings, engine)
    redis = create_redis(settings)
    metrics_server = (
        await serve_metrics(settings.maintenance_metrics_port, "maintenance")
        if settings.metrics_enabled
        else None
    )
    maintenance = Maintenance(
        settings=settings,
        session_factory=create_session_factory(engine),
        redis=redis,
        dispatcher=StreamProducer(redis, settings.redis_stream_prefix, settings.stream_maxlen),
    )
    stop = asyncio.Event()
    install_signal_handlers(stop)
    try:
        await maintenance.run(stop)
    finally:
        if metrics_server is not None:
            metrics_server.close()
        await redis.aclose()
        await engine.dispose()
        shutdown_tracing()


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    asyncio.run(run_maintenance(settings))
