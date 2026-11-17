"""SIGTERM/SIGINT handling and the drain deadline.

On a signal the worker stops reading new messages. In-flight jobs get
`WORKER_SHUTDOWN_GRACE_SECONDS` to finish; after that their leases are released (back to
`queued`) instead of being abandoned until the reaper notices.
"""

from __future__ import annotations

import asyncio
import signal

from workflow_engine.logging import get_logger

log = get_logger(__name__)


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _request_stop(sig: signal.Signals) -> None:
        if not stop.is_set():
            log.info("shutdown_requested", signal=sig.name)
            stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_stop, sig)


async def drain_deadline(stop: asyncio.Event, grace_seconds: float) -> None:
    """Returns `grace_seconds` after `stop` is set (never, if it isn't)."""
    await stop.wait()
    await asyncio.sleep(grace_seconds)
