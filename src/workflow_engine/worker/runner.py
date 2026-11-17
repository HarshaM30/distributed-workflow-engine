"""Worker loop: read a stream entry, CAS-claim the job in Postgres, execute, finalize, XACK."""

from __future__ import annotations

import asyncio
import contextlib
import random
from typing import Any

import structlog
from pydantic import BaseModel, ValidationError
from pydantic_core import to_jsonable_python
from redis.exceptions import RedisError

from workflow_engine.config import Settings
from workflow_engine.core import jobs
from workflow_engine.core.backoff import backoff_delay
from workflow_engine.core.states import JobStatus
from workflow_engine.db.session import SessionFactory
from workflow_engine.dispatch.rate_limit import TokenBucket
from workflow_engine.dispatch.streams import StreamConsumer, StreamMessage
from workflow_engine.handlers.registry import HandlerRegistry, HandlerSpec, JobContext
from workflow_engine.logging import get_logger
from workflow_engine.worker.heartbeat import Interrupt, LeaseKeeper
from workflow_engine.worker.shutdown import drain_deadline

log = get_logger(__name__)

READ_ERROR_BACKOFF_SECONDS = 1.0
EXECUTION_SCOPE = "exec"


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


class Worker:
    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: SessionFactory,
        consumer: StreamConsumer,
        registry: HandlerRegistry,
        worker_id: str,
        dispatcher: jobs.Dispatcher | None = None,
        limiter: TokenBucket | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.consumer = consumer
        self.registry = registry
        self.worker_id = worker_id
        # Re-sends released jobs right away; without it the sweeper picks them up later.
        self.dispatcher = dispatcher
        # Execution throttle; None disables it.
        self.limiter = limiter
        self._stop = asyncio.Event()

    async def start(self) -> None:
        """Create the consumer groups, retrying until Redis is reachable."""
        while True:
            try:
                await self.consumer.ensure_groups()
                return
            except (RedisError, OSError) as exc:
                log.warning("redis_unavailable", error=type(exc).__name__)
                await asyncio.sleep(READ_ERROR_BACKOFF_SECONDS)

    async def run(self, stop: asyncio.Event) -> None:
        self._stop = stop
        await self.start()
        log.info("worker_started", worker_id=self.worker_id)
        async with asyncio.TaskGroup() as tg:
            for _ in range(self.settings.worker_concurrency):
                tg.create_task(self._loop(stop))
        log.info("worker_stopped", worker_id=self.worker_id)

    async def _loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                messages = await self.consumer.read(self.settings.worker_block_ms)
            except (RedisError, OSError) as exc:
                log.warning("stream_read_failed", error=type(exc).__name__)
                await _sleep_or_stop(stop, READ_ERROR_BACKOFF_SECONDS)
                continue
            for message in messages:
                # Messages already read are processed even if stop was requested meanwhile;
                # the drain deadline still bounds how long that takes.
                try:
                    await self.process_message(message)
                except Exception:
                    # Not ACKed: the entry stays in the PEL and the job stays in Postgres,
                    # so maintenance can recover it.
                    log.exception("message_processing_failed", message_id=message.message_id)

    async def process_message(self, message: StreamMessage) -> None:
        if message.job_id is None:
            log.warning("malformed_stream_entry", message_id=message.message_id)
            await self.consumer.ack(message)
            return
        claim = await jobs.claim_job(
            self.session_factory,
            message.job_id,
            self.worker_id,
            self.settings.worker_lease_seconds,
        )
        if claim is None:
            # Stale or duplicate hint: Postgres says the job isn't queued.
            log.debug("stale_message_dropped", job_id=str(message.job_id))
            await self.consumer.ack(message)
            return
        with structlog.contextvars.bound_contextvars(
            job_id=str(claim.job_id),
            job_type=claim.type,
            attempt=claim.attempt,
            worker_id=self.worker_id,
        ):
            await self.execute(claim)
        await self.consumer.ack(message)

    # ------------------------------------------------------------------ execution

    async def execute(self, claim: jobs.Claim) -> None:
        spec = self.registry.get(claim.type)
        if spec is None:
            await self._fail(claim, f"no handler registered for {claim.type!r}", retryable=False)
            return
        if await self._throttled(claim):
            return
        try:
            payload = spec.payload_model.model_validate(claim.payload)
        except ValidationError as exc:
            error = f"invalid payload: {exc.error_count()} error(s)"
            await self._fail(claim, error, retryable=False)
            return

        ctx = JobContext(job_id=claim.job_id, attempt=claim.attempt, worker_id=self.worker_id)
        log.info("job_started")
        handler: asyncio.Task[Any] = asyncio.create_task(self._invoke(spec, payload, ctx))
        keeper = LeaseKeeper(
            self.session_factory,
            claim,
            lease_seconds=self.settings.worker_lease_seconds,
            interval=self.settings.worker_heartbeat_seconds,
        )
        keeper_task = asyncio.create_task(keeper.run(handler))
        deadline = asyncio.create_task(
            drain_deadline(self._stop, self.settings.worker_shutdown_grace_seconds)
        )
        try:
            await asyncio.wait({handler, deadline}, return_when=asyncio.FIRST_COMPLETED)
            if not handler.done():
                log.warning("drain_deadline_reached")
                keeper.interrupt_handler(handler, Interrupt.SHUTDOWN)
            await asyncio.wait({handler})
        finally:
            deadline.cancel()
            keeper.stop()
            await keeper_task
        await self._finalize(claim, handler, keeper.interrupt, spec)

    @staticmethod
    async def _invoke(spec: HandlerSpec, payload: BaseModel, ctx: JobContext) -> Any:
        return await asyncio.wait_for(spec.func(payload, ctx), timeout=spec.timeout_seconds)

    async def _finalize(
        self,
        claim: jobs.Claim,
        handler: asyncio.Task[Any],
        interrupt: Interrupt | None,
        spec: HandlerSpec,
    ) -> None:
        if handler.cancelled():
            await self._interrupted(claim, interrupt)
            return
        exc = handler.exception()
        if exc is None:
            if await jobs.complete_job(
                self.session_factory, claim, to_jsonable_python(handler.result())
            ):
                log.info("job_succeeded")
            else:
                log.warning("completion_fenced_out")
            return
        if isinstance(exc, TimeoutError):
            error = f"timed out after {spec.timeout_seconds:g}s"
        else:
            error = f"{type(exc).__name__}: {exc}"
        await self._fail(claim, error, retryable=True)

    async def _interrupted(self, claim: jobs.Claim, interrupt: Interrupt | None) -> None:
        if interrupt == Interrupt.CANCEL_REQUESTED:
            if await jobs.cancel_running_job(self.session_factory, claim):
                log.info("job_cancelled")
            return
        if interrupt == Interrupt.LEASE_LOST:
            return  # someone else owns the job now; there's nothing left to write
        # Shutdown (or an unexpected cancellation): hand the job back rather than abandon it.
        if await jobs.release_job(self.session_factory, claim):
            log.info("job_released")
            if self.dispatcher is not None:
                await jobs.enqueue_safely(self.dispatcher, claim.job_id, claim.priority)

    async def _fail(self, claim: jobs.Claim, error: str, *, retryable: bool) -> None:
        delay = backoff_delay(
            claim.attempt,
            self.settings.retry_base_delay_seconds,
            self.settings.retry_max_delay_seconds,
        )
        status = await jobs.fail_job(
            self.session_factory, claim, error, retry_delay=delay, retryable=retryable
        )
        if status is None:
            log.warning("failure_fenced_out", error=error)
        elif status == JobStatus.PENDING:
            log.warning("job_retry_scheduled", error=error, retry_in_seconds=round(delay, 3))
        else:
            log.warning("job_dead", error=error, retryable=retryable)

    async def _throttled(self, claim: jobs.Claim) -> bool:
        """Execution throttle: an empty bucket defers the job to `pending`, never rejects it."""
        limit = self.settings.rate_limits.get(claim.type)
        if limit is None or self.limiter is None:
            return False
        decision = await self.limiter.acquire(EXECUTION_SCOPE, claim.type, limit)
        if decision.allowed:
            return False
        # Jitter spreads the deferred jobs so they don't all come back on the same tick.
        delay = decision.retry_after * (1 + random.random())  # noqa: S311
        if await jobs.defer_job(self.session_factory, claim, delay):
            log.info("job_throttled", retry_in_seconds=round(delay, 3))
        return True
