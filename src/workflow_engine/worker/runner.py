"""Worker loop: read a stream entry, CAS-claim the job in Postgres, execute, finalize, XACK."""

from __future__ import annotations

import asyncio
import contextlib

import structlog
from pydantic import ValidationError
from pydantic_core import to_jsonable_python
from redis.exceptions import RedisError

from workflow_engine.config import Settings
from workflow_engine.core import jobs
from workflow_engine.db.session import SessionFactory
from workflow_engine.dispatch.streams import StreamConsumer, StreamMessage
from workflow_engine.handlers.registry import HandlerRegistry, JobContext
from workflow_engine.logging import get_logger

log = get_logger(__name__)

READ_ERROR_BACKOFF_SECONDS = 1.0


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
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.consumer = consumer
        self.registry = registry
        self.worker_id = worker_id

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
                # Messages already read are finished even if stop was requested meanwhile.
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

    async def execute(self, claim: jobs.Claim) -> None:
        spec = self.registry.get(claim.type)
        if spec is None:
            await jobs.fail_job(
                self.session_factory, claim, f"no handler registered for {claim.type!r}"
            )
            log.error("job_failed", reason="unknown_type")
            return
        ctx = JobContext(job_id=claim.job_id, attempt=claim.attempt, worker_id=self.worker_id)
        log.info("job_started")
        try:
            payload = spec.payload_model.model_validate(claim.payload)
            raw = await asyncio.wait_for(spec.func(payload, ctx), timeout=spec.timeout_seconds)
            result = to_jsonable_python(raw)
        except TimeoutError:
            error = f"timed out after {spec.timeout_seconds:g}s"
        except ValidationError as exc:
            error = f"invalid payload: {exc.error_count()} error(s)"
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            if await jobs.complete_job(self.session_factory, claim, result):
                log.info("job_succeeded")
            else:
                log.warning("completion_fenced_out")
            return
        if await jobs.fail_job(self.session_factory, claim, error):
            log.warning("job_failed", error=error)
        else:
            log.warning("failure_fenced_out", error=error)
