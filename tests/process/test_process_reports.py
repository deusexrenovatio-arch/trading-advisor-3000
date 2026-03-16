from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_harness_baseline_metrics_report(tmp_path: Path) -> None:
    output = tmp_path / "harness-baseline.json"
    result = _run(
        [
            sys.executable,
            "scripts/harness_baseline_metrics.py",
            "--output",
            str(output),
            "--format",
            "json",
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert "plans" in payload
    assert "dev_loop_baseline" in payload


def test_governance_dashboard_report(tmp_path: Path) -> None:
    output_json = tmp_path / "dashboard.json"
    output_md = tmp_path / "dashboard.md"
    result = _run(
        [
            sys.executable,
            "scripts/build_governance_dashboard.py",
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["overall_status"] in {"green", "yellow"}
    assert output_md.exists()


def test_process_and_kpi_reports(tmp_path: Path) -> None:
    process_output = tmp_path / "process-report.md"
    process_output_json = tmp_path / "process-report.json"
    kpi_output = tmp_path / "kpi-report.md"
    result_process = _run(
        [
            sys.executable,
            "scripts/process_improvement_report.py",
            "--output",
            str(process_output),
        ]
    )
    result_process_json = _run(
        [
            sys.executable,
            "scripts/process_improvement_report.py",
            "--output",
            str(process_output_json),
            "--format",
            "json",
        ]
    )
    result_kpi = _run(
        [
            sys.executable,
            "scripts/autonomy_kpi_report.py",
            "--output",
            str(kpi_output),
        ]
    )
    assert result_process.returncode == 0, result_process.stdout + "\n" + result_process.stderr
    assert result_process_json.returncode == 0, result_process_json.stdout + "\n" + result_process_json.stderr
    assert result_kpi.returncode == 0, result_kpi.stdout + "\n" + result_kpi.stderr
    assert process_output.exists()
    assert process_output_json.exists()
    assert kpi_output.exists()

    process_text = process_output.read_text(encoding="utf-8")
    assert "# Process Improvement Report" in process_text
    assert "## Action Items" in process_text
    assert "| ID | Priority | Trigger | Action | Owner | Due |" in process_text
    assert any(line.startswith("| PROC-") for line in process_text.splitlines())

    process_payload = json.loads(process_output_json.read_text(encoding="utf-8"))
    action_items = process_payload.get("action_items", [])
    assert action_items, "process report must contain actionable items"
    for item in action_items:
        assert item.get("id")
        assert item.get("priority") in {"P1", "P2", "P3"}
        assert item.get("action")
