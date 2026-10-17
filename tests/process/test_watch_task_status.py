from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
from watch_task_status import observe  # noqa: E402


def run(states, *, timeout=10):
    sequence = iter(states)
    events = []
    now = [0]

    def read():
        value = next(sequence)
        if isinstance(value, Exception):
            raise value
        return value

    def sleep(seconds):
        now[0] += seconds

    result = observe(
        read,
        fields=["status", "conclusion", "active_step"],
        terminal={"completed", "success", "failure"},
        success={"success"},
        interval=1,
        timeout=timeout,
        emit=events.append,
        clock=lambda: now[0],
        sleep=sleep,
    )
    return result, events


def test_unchanged_heartbeat_is_silent_and_step_change_emits():
    result, events = run(
        [
            {"status": "started", "active_step": "a", "heartbeat": 1},
            {"status": "started", "active_step": "a", "heartbeat": 2},
            {"status": "started", "active_step": "b"},
            {"status": "completed", "conclusion": "success"},
        ]
    )
    assert result == 0
    assert len(events) == 3


@pytest.mark.parametrize(
    "state,code",
    [
        ({"status": "failure"}, 1),
        ({"status": "completed", "conclusion": "failure"}, 1),
        ({"status": "completed"}, 2),
        ({"wrong": "field"}, 2),
        (ValueError("invalid JSON"), 2),
    ],
)
def test_failure_and_invalid_status_never_report_success(state, code):
    result, _ = run([state])
    assert result == code


def test_missing_file_waits_and_timeout_has_distinct_exit_code():
    result, events = run([FileNotFoundError(), FileNotFoundError()], timeout=1)
    assert result == 3
    assert [event["event"] for event in events] == ["status_changed", "observer_timeout"]
