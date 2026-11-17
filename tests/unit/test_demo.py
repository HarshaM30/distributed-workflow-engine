"""The demo script's pure helpers: worker-id parsing and the board's notes."""

from __future__ import annotations

from typing import Any

import pytest
from demo.run import Board, container_of

pytestmark = pytest.mark.unit


def _job(job_id: str, status: str, *worker_ids: str) -> dict[str, Any]:
    return {
        "id": job_id,
        "status": status,
        "attempt_history": [{"worker_id": w} for w in worker_ids],
    }


def test_container_of_strips_pid_and_truncates_to_short_id() -> None:
    assert container_of("0123456789abcdef-1") == "0123456789ab"
    assert container_of("abc123-42") == "abc123"


def test_board_names_workers_by_container() -> None:
    board = Board(names={"0123456789ab": "worker-2"})
    assert board.worker_name("0123456789abcdef-1") == "worker-2"
    assert board.worker_name("unknown-7") == "unknown-7"


def test_board_notes_follow_the_victim_through_recovery() -> None:
    board = Board(names={"aaa": "worker-1", "bbb": "worker-2"}, victim="j1", victim_attempt=1)
    assert board.note(_job("j2", "running", "aaa-1")) == ""
    assert "dead" in board.note(_job("j1", "running", "aaa-1"))
    assert "queued" in board.note(_job("j1", "queued", "aaa-1"))
    assert board.note(_job("j1", "succeeded", "aaa-1", "bbb-1")) == "recovered by worker-2"
