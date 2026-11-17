"""Worker loop: read a stream entry, CAS-claim the job in Postgres, execute, finalize, XACK."""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from datetime import UTC, datetime
from typing import Any

import structlog
from opentelemetry.trace import SpanKind, Status, StatusCode
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
from workflow_engine.metrics import (
    JOB_DURATION,
    JOB_E2E_LATENCY,
    JOBS_COMPLETED,
    RATE_LIMITED,
    RETRIES,
)
from workflow_engine.telemetry import extract_trace_context, tracer
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
        # The sender's trace (API request, scheduler, sweeper) becomes this span's parent.
        with tracer().start_as_current_span(
            "job.process",
            context=extract_trace_context(message.trace_context),
            kind=SpanKind.CONSUMER,
            attributes={"job.id": str(message.job_id), "stream": message.stream},
        ) as span:
            with tracer().start_as_current_span("job.claim") as claim_span:
                claim = await jobs.claim_job(
                    self.session_factory,
                    message.job_id,
                    self.worker_id,
                    self.settings.worker_lease_seconds,
                )
                claim_span.set_attribute("job.claimed", claim is not None)
            if claim is None:
                # Stale or duplicate hint: Postgres says the job isn't queued.
                log.debug("stale_message_dropped", job_id=str(message.job_id))
                await self.consumer.ack(message)
                return
            span.set_attributes(
                {"job.type": claim.type, "job.attempt": claim.attempt, "worker.id": self.worker_id}
            )
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

        ctx = JobContext(
            job_id=claim.job_id,
            attempt=claim.attempt,
            worker_id=self.worker_id,
            session_factory=self.session_factory,
        )
        log.info("job_started")
        handler: asyncio.Task[Any] = asyncio.create_task(self._invoke(spec, payload, ctx))
        keeper = LeaseKeeper(
            self.session_factory,
            claim,
            lease_seconds=self.settings.worker_lease_seconds,
            interval=self.settings.worker_heartbeat_seconds,
        )
        started = time.perf_counter()
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
        outcome = await self._finalize(claim, handler, keeper.interrupt, spec)
        JOB_DURATION.labels(claim.type, outcome).observe(time.perf_counter() - started)

    @staticmethod
    async def _invoke(spec: HandlerSpec, payload: BaseModel, ctx: JobContext) -> Any:
        # Runs in its own task, which inherits the job.process span as its parent.
        with tracer().start_as_current_span(
            "job.handler", attributes={"job.type": spec.name}
        ) as span:
            try:
                return await asyncio.wait_for(spec.func(payload, ctx), timeout=spec.timeout_seconds)
            except Exception as exc:
                span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
                raise

    async def _finalize(
        self,
        claim: jobs.Claim,
        handler: asyncio.Task[Any],
        interrupt: Interrupt | None,
        spec: HandlerSpec,
    ) -> str:
        """Write the attempt's outcome; returns a label for the duration histogram."""
        if handler.cancelled():
            return await self._interrupted(claim, interrupt)
        exc = handler.exception()
        if exc is None:
            if await jobs.complete_job(
                self.session_factory, claim, to_jsonable_python(handler.result())
            ):
                log.info("job_succeeded")
                JOBS_COMPLETED.labels(claim.type, JobStatus.SUCCEEDED.value).inc()
                if claim.created_at is not None:
                    e2e = (datetime.now(UTC) - claim.created_at).total_seconds()
                    JOB_E2E_LATENCY.labels(claim.type).observe(max(e2e, 0.0))
                return "succeeded"
            log.warning("completion_fenced_out")
            return "fenced_out"
        if isinstance(exc, TimeoutError):
            error = f"timed out after {spec.timeout_seconds:g}s"
        else:
            error = f"{type(exc).__name__}: {exc}"
        await self._fail(claim, error, retryable=True)
        return "failed"

    async def _interrupted(self, claim: jobs.Claim, interrupt: Interrupt | None) -> str:
        if interrupt == Interrupt.CANCEL_REQUESTED:
            if await jobs.cancel_running_job(self.session_factory, claim):
                log.info("job_cancelled")
                JOBS_COMPLETED.labels(claim.type, JobStatus.CANCELLED.value).inc()
            return "cancelled"
        if interrupt == Interrupt.LEASE_LOST:
            return "lease_lost"  # someone else owns the job now; there's nothing left to write
        # Shutdown (or an unexpected cancellation): hand the job back rather than abandon it.
        if await jobs.release_job(self.session_factory, claim):
            log.info("job_released")
            if self.dispatcher is not None:
                await jobs.enqueue_safely(self.dispatcher, claim.job_id, claim.priority)
        return "released"

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
            RETRIES.labels(claim.type).inc()
        else:
            log.warning("job_dead", error=error, retryable=retryable)
            JOBS_COMPLETED.labels(claim.type, JobStatus.DEAD.value).inc()

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
            RATE_LIMITED.labels("execution", claim.type).inc()
        return True
