from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from workflow_engine.core.states import (
    ACTIVE_STATUSES,
    PRIORITY_ORDER,
    TERMINAL_STATUSES,
    TRANSITIONS,
    JobStatus,
    Priority,
    can_transition,
    is_terminal,
)

statuses = st.sampled_from(list(JobStatus))


def test_every_status_has_a_transition_entry() -> None:
    assert set(TRANSITIONS) == set(JobStatus)


def test_active_and_terminal_partition_all_statuses() -> None:
    assert set(JobStatus) == ACTIVE_STATUSES | TERMINAL_STATUSES
    assert not ACTIVE_STATUSES & TERMINAL_STATUSES


def test_only_dead_can_leave_a_terminal_state() -> None:
    for status in TERMINAL_STATUSES - {JobStatus.DEAD}:
        assert TRANSITIONS[status] == frozenset()
    assert TRANSITIONS[JobStatus.DEAD] == {JobStatus.QUEUED}


def test_only_queued_can_be_claimed() -> None:
    assert [s for s in JobStatus if can_transition(s, JobStatus.RUNNING)] == [JobStatus.QUEUED]


@given(statuses, statuses)
def test_can_transition_matches_table(a: JobStatus, b: JobStatus) -> None:
    assert can_transition(a, b) == (b in TRANSITIONS[a])
    assert not can_transition(a, a)


@given(st.lists(st.integers(min_value=0, max_value=10), max_size=50))
def test_random_walks_stay_inside_the_machine(choices: list[int]) -> None:
    state = JobStatus.PENDING
    for choice in choices:
        options = sorted(TRANSITIONS[state])
        if not options:
            assert is_terminal(state)
            break
        state = options[choice % len(options)]
        assert state in JobStatus


def test_priority_order() -> None:
    assert PRIORITY_ORDER == (Priority.HIGH, Priority.NORMAL, Priority.LOW)
