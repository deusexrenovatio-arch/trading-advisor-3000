from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from measure_dev_loop import compute_baseline  # noqa: E402


def test_compute_baseline_median_and_p90() -> None:
    payload = {
        "version": 1,
        "items": [
            {"outcome_status": "completed", "time_to_first_patch_sec": 60, "same_path_attempts": 1, "decision_quality": "correct_first_time", "route_match": "matched"},
            {"outcome_status": "partial", "time_to_first_patch_sec": 120, "same_path_attempts": 2, "decision_quality": "partial_outcome", "route_match": "expanded"},
            {"outcome_status": "blocked", "time_to_first_patch_sec": 180, "same_path_attempts": 1, "decision_quality": "environment_blocked", "route_match": "matched"},
        ],
    }
    metrics = compute_baseline(payload)
    assert metrics["terminal_outcomes_total"] == 3
    assert metrics["median_time_to_first_patch_sec"] == 120
    assert metrics["p90_time_to_first_patch_sec"] == 180


def test_measure_dev_loop_cli_writes_output(tmp_path: Path) -> None:
    output = tmp_path / "dev-loop.json"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/measure_dev_loop.py",
            "--format",
            "json",
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert "terminal_outcomes_total" in payload
