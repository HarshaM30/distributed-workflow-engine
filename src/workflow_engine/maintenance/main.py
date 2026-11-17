"""Maintenance process entrypoint. The scheduler, reaper, and sweeper land in Milestone 3."""

from __future__ import annotations

from workflow_engine.config import get_settings
from workflow_engine.logging import configure_logging, get_logger


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    get_logger(__name__).warning("maintenance_loops_not_implemented")
