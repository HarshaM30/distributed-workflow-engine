"""Lease renewal while a handler runs. Also the path by which a running job learns it was
cancelled, or that its lease was lost to the reaper."""

from __future__ import annotations

import asyncio
import contextlib
from enum import StrEnum

from sqlalchemy.exc import SQLAlchemyError

from workflow_engine.core import jobs
from workflow_engine.db.session import SessionFactory
from workflow_engine.logging import get_logger

log = get_logger(__name__)


class Interrupt(StrEnum):
    CANCEL_REQUESTED = "cancel_requested"
    LEASE_LOST = "lease_lost"
    SHUTDOWN = "shutdown"


class LeaseKeeper:
    """Renews the lease every `interval` seconds until `stop()`; can interrupt the handler.

    It is stopped with an event rather than task cancellation, so an in-flight heartbeat
    query always runs to completion.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        claim: jobs.Claim,
        *,
        lease_seconds: float,
        interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._claim = claim
        self._lease_seconds = lease_seconds
        self._interval = interval
        self._stopped = asyncio.Event()
        self.interrupt: Interrupt | None = None

    def stop(self) -> None:
        self._stopped.set()

    def interrupt_handler(self, handler: asyncio.Task[object], reason: Interrupt) -> None:
        if self.interrupt is None:
            self.interrupt = reason
        handler.cancel()

    async def run(self, handler: asyncio.Task[object]) -> None:
        while not self._stopped.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._interval)
            if self._stopped.is_set() or handler.done():
                return
            try:
                state = await jobs.heartbeat(
                    self._session_factory, self._claim, self._lease_seconds
                )
            except (SQLAlchemyError, OSError) as exc:
                # Keep the handler going; if Postgres stays down the reaper takes over.
                log.warning("heartbeat_failed", error=type(exc).__name__)
                continue
            if state is None:
                log.warning("lease_lost")
                self.interrupt_handler(handler, Interrupt.LEASE_LOST)
                return
            if state.cancel_requested:
                log.info("cancel_observed")
                self.interrupt_handler(handler, Interrupt.CANCEL_REQUESTED)
                return
