"""SIGTERM/SIGINT handling: stop reading new messages and let in-flight jobs finish."""

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
