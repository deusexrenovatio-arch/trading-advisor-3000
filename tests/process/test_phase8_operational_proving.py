from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from run_phase8_operational_proving import (  # noqa: E402
    Phase8Step,
    build_phase8_plan,
    run_phase8_plan,
)


def test_phase8_plan_has_expected_lanes_and_session_policy() -> None:
    plan = build_phase8_plan(
        mapping="configs/change_surface_mapping.yaml",
        scope_args=["--changed-files", "docs/README.md"],
        enforce_session_check=False,
        include_nightly_lane=True,
        include_dashboard_refresh=True,
        python_executable="python",
    )

    assert plan[0].lane == "loop-lane"
    assert plan[0].step_id == "loop-gate"
    assert "--skip-session-check" in plan[0].command

    assert plan[1].lane == "pr-lane"
    assert plan[1].step_id == "pr-gate"
    assert "--skip-session-check" in plan[1].command

    nightly_steps = [item for item in plan if item.lane == "nightly-lane"]
    assert len(nightly_steps) == 1
    assert "--skip-session-check" not in nightly_steps[0].command

    dashboard_steps = [item for item in plan if item.lane == "dashboard-refresh"]
    assert len(dashboard_steps) == 5


def test_phase8_run_stops_on_first_failed_step(tmp_path: Path) -> None:
    steps = [
        Phase8Step(lane="loop-lane", step_id="step-1", command=("python", "-c", "print('ok1')")),
        Phase8Step(lane="pr-lane", step_id="step-2", command=("python", "-c", "print('ok2')")),
        Phase8Step(lane="nightly-lane", step_id="step-3", command=("python", "-c", "print('ok3')")),
    ]
    calls: list[tuple[str, ...]] = []

    def _runner(command: list[str] | tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(command))
        is_failure = len(calls) == 2
        return subprocess.CompletedProcess(
            args=list(command),
            returncode=1 if is_failure else 0,
            stdout="stdout",
            stderr="boom" if is_failure else "",
        )

    exit_code, report = run_phase8_plan(
        plan=steps,
        report_path=tmp_path / "phase8-run.json",
        dry_run=False,
        runner=_runner,
        dashboard_artifact_paths=(),
        require_dashboard_artifacts=False,
    )

    assert exit_code == 1
    assert report["status"] == "failed"
    assert len(calls) == 2
    assert len(report["steps"]) == 2
    assert report["failure"]["step_id"] == "step-2"


def test_phase8_run_fails_on_missing_dashboard_artifacts(tmp_path: Path) -> None:
    exit_code, report = run_phase8_plan(
        plan=[],
        report_path=tmp_path / "phase8-artifacts.json",
        dry_run=False,
        dashboard_artifact_paths=(tmp_path / "missing-dashboard-artifact.md",),
        require_dashboard_artifacts=True,
    )

    assert exit_code == 1
    assert report["status"] == "failed"
    assert report["failure"]["step_id"] == "artifact-validation"


def test_phase8_run_fails_on_stale_dashboard_artifacts(tmp_path: Path) -> None:
    stale_artifact = tmp_path / "dashboard-stale.json"
    stale_artifact.write_text('{"stale": true}\n', encoding="utf-8")
    exit_code, report = run_phase8_plan(
        plan=[],
        report_path=tmp_path / "phase8-stale.json",
        dry_run=False,
        dashboard_artifact_paths=(stale_artifact,),
        require_dashboard_artifacts=True,
    )

    assert exit_code == 1
    assert report["status"] == "failed"
    assert report["failure"]["step_id"] == "artifact-validation"
    assert stale_artifact.as_posix() in report["failure"]["stale_artifacts"]


def test_phase8_run_dry_run_can_skip_report_write(tmp_path: Path) -> None:
    report_path = tmp_path / "dry-run-report.json"
    exit_code, report = run_phase8_plan(
        plan=[Phase8Step(lane="loop-lane", step_id="dry-step", command=("python", "-c", "print('x')"))],
        report_path=report_path,
        dry_run=True,
        require_dashboard_artifacts=False,
        write_report=False,
    )

    assert exit_code == 0
    assert report["status"] == "dry_run"
    assert report_path.exists() is False


def test_phase8_cli_dry_run_rejects_output_without_opt_in(tmp_path: Path) -> None:
    output = tmp_path / "phase8-dry-run.json"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase8_operational_proving.py",
            "--dry-run",
            "--changed-files",
            "docs/README.md",
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2, result.stdout + "\n" + result.stderr
    assert output.exists() is False


def test_phase8_cli_dry_run_writes_report_when_opted_in(tmp_path: Path) -> None:
    output = tmp_path / "phase8-dry-run.json"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase8_operational_proving.py",
            "--dry-run",
            "--write-dry-run-report",
            "--changed-files",
            "docs/README.md",
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
    assert payload["status"] == "dry_run"
    assert len(payload["steps"]) == 8
    assert payload["steps"][0]["step_id"] == "loop-gate"
