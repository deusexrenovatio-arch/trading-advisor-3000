from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from agent_process_telemetry import compute_process_rollup, render_rollup_markdown  # noqa: E402


def test_compute_process_rollup_metrics() -> None:
    payload = {
        "version": 1,
        "items": [
            {
                "task_id": "A",
                "closed_at": "2026-03-15T10:00:00Z",
                "route_match": "matched",
                "decision_quality": "correct_first_time",
                "incident_signature": "none",
                "same_path_attempts": 1,
                "outcome_status": "completed",
            },
            {
                "task_id": "B",
                "closed_at": "2026-03-16T10:00:00Z",
                "route_match": "expanded",
                "decision_quality": "correct_after_replan",
                "incident_signature": "X",
                "same_path_attempts": 2,
                "outcome_status": "partial",
            },
            {
                "task_id": "C",
                "closed_at": "2026-03-16T11:00:00Z",
                "route_match": "matched",
                "decision_quality": "environment_blocked",
                "incident_signature": "X",
                "same_path_attempts": 1,
                "outcome_status": "blocked",
            },
        ],
    }
    rollup = compute_process_rollup(payload, window_size=20)
    assert rollup["completed_tasks_count"] == 3
    assert rollup["current_metrics"]["correct_first_time_pct"] == 1 / 3
    assert rollup["current_metrics"]["repeat_error_rate"] > 0


def test_render_rollup_markdown_has_table() -> None:
    rollup = {
        "completed_tasks_count": 1,
        "window_tasks_count": 1,
        "burn_in_complete": False,
        "current_metrics": {
            "correct_first_time_pct": 1.0,
            "start_match_pct": 1.0,
            "context_expansion_rate": 0.0,
            "repeat_error_rate": 0.0,
            "environment_blocker_rate": 0.0,
        },
    }
    rendered = render_rollup_markdown(rollup)
    assert "| Metric | Value |" in rendered
