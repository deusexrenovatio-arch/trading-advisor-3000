from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _write_task_note(
    path: Path,
    *,
    updated: date | None,
    solution_class: str,
    critical_contour: str,
    outcome_status: str,
    note_text: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    updated_line = f"Updated: {updated.isoformat()}" if updated is not None else ""
    path.write_text(
        "\n".join(
            [
                "# Task Note",
                "",
                updated_line,
                "",
                "## Objective",
                "- Scope: test pilot observation counters",
                "",
                "## Solution Intent",
                f"- Solution Class: {solution_class}",
                f"- Critical Contour: {critical_contour}",
                "- Forbidden Shortcuts: none",
                "- Closure Evidence: test fixture",
                "- Shortcut Waiver: none",
                "",
                "## Task Outcome",
                f"- Outcome Status: {outcome_status}",
                "",
                "## Notes",
                f"- {note_text}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_orchestration_run(
    artifact_root: Path,
    *,
    run_id: str,
    updated_at: str,
    attempts: list[dict],
    final_status: str,
) -> None:
    _write_json(
        artifact_root / run_id / "state.json",
        {
            "run_id": run_id,
            "updated_at": updated_at,
            "backend": "simulate",
            "route_mode": "governed-phase-orchestration",
            "phase_brief": "docs/codex/modules/demo.phase-01.md",
            "phase_name": "Phase 01",
            "phase_status": "completed" if final_status == "accepted" else "blocked",
            "attempts": attempts,
            "attempts_total": len(attempts),
            "final_status": final_status,
            "next_phase": "docs/codex/modules/demo.phase-02.md",
        },
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
    orchestration = payload["sections"]["orchestration_quality"]
    assert "average_orchestration_score" in orchestration
    assert "average_worker_acceptor_absolute_delta" in orchestration
    assert "top_blocker_categories" in orchestration
    assert "quality_expansion_points" in orchestration
    observation = payload["sections"]["pilot_observation"]
    assert "critical_tasks_with_explicit_solution_class" in observation
    assert "blocked_shortcut_claims" in observation
    assert "staged_vs_target_declarations" in observation
    assert "observation_window_start" in observation
    assert "observation_window_status" in observation
    assert {"staged", "target"} <= set(observation["staged_vs_target_declarations"].keys())
    assert output_md.exists()
    markdown = output_md.read_text(encoding="utf-8")
    assert "| orchestration_quality |" in markdown
    assert "avg_score=" in markdown
    assert "| pilot_observation |" in markdown
    assert "critical_tasks_with_explicit_solution_class=" in markdown
    assert "observation_window_status=" in markdown


def test_governance_dashboard_includes_orchestration_quality_rollup(tmp_path: Path) -> None:
    artifact_root = tmp_path / "orchestration"

    pass_acceptance = artifact_root / "run-a/attempt-01/acceptance.json"
    _write_json(
        pass_acceptance,
        {
            "verdict": "PASS",
            "summary": "Accepted cleanly.",
            "route_signal": "acceptance:attempt-01",
            "used_skills": ["phase-acceptance-governor"],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
            "policy_blockers": [],
        },
    )
    _write_orchestration_run(
        artifact_root,
        run_id="run-a",
        updated_at="2026-04-10T10:00:00Z",
        attempts=[
            {
                "attempt": 1,
                "kind": "worker",
                "worker_summary": "worker pass",
                "worker_route_signal": "worker:attempt-01",
                "worker_report_path": (pass_acceptance.parent / "worker-report.json").as_posix(),
                "changed_files_path": (pass_acceptance.parent / "changed-files.txt").as_posix(),
                "acceptance_json_path": pass_acceptance.as_posix(),
                "acceptance_md_path": pass_acceptance.with_suffix(".md").as_posix(),
                "acceptor_route_signal": "acceptance:attempt-01",
                "acceptor_used_skills": ["phase-acceptance-governor"],
                "verdict": "PASS",
                "blockers_total": 0,
                "policy_blockers_total": 0,
            }
        ],
        final_status="accepted",
    )

    blocked_acceptance = artifact_root / "run-b/attempt-01/acceptance.json"
    blocker = {
        "id": "P-EVIDENCE_GAP-1",
        "title": "Required evidence is missing",
        "why": "Integration proof is missing.",
        "remediation": "Run the missing checks.",
    }
    _write_json(
        blocked_acceptance,
        {
            "verdict": "BLOCKED",
            "summary": "Blocked pending evidence.",
            "route_signal": "acceptance:attempt-01",
            "used_skills": ["phase-acceptance-governor"],
            "blockers": [blocker],
            "rerun_checks": [],
            "evidence_gaps": ["Integration proof is missing."],
            "prohibited_findings": [],
            "policy_blockers": [blocker],
        },
    )
    _write_orchestration_run(
        artifact_root,
        run_id="run-b",
        updated_at="2026-04-10T10:05:00Z",
        attempts=[
            {
                "attempt": 1,
                "kind": "worker",
                "worker_summary": "worker blocked",
                "worker_route_signal": "worker:attempt-01",
                "worker_report_path": (blocked_acceptance.parent / "worker-report.json").as_posix(),
                "changed_files_path": (blocked_acceptance.parent / "changed-files.txt").as_posix(),
                "acceptance_json_path": blocked_acceptance.as_posix(),
                "acceptance_md_path": blocked_acceptance.with_suffix(".md").as_posix(),
                "acceptor_route_signal": "acceptance:attempt-01",
                "acceptor_used_skills": ["phase-acceptance-governor"],
                "verdict": "BLOCKED",
                "blockers_total": 1,
                "policy_blockers_total": 1,
            }
        ],
        final_status="blocked",
    )

    output_json = tmp_path / "dashboard-orchestration.json"
    output_md = tmp_path / "dashboard-orchestration.md"
    result = _run(
        [
            sys.executable,
            "scripts/build_governance_dashboard.py",
            "--task-notes-root",
            str(tmp_path / "no-notes"),
            "--orchestration-artifact-root",
            str(artifact_root),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    orchestration = payload["sections"]["orchestration_quality"]
    assert orchestration["runs_total"] == 2
    assert orchestration["window_runs_count"] == 2
    assert orchestration["blocked_runs"] == 1
    assert orchestration["average_orchestration_score"] < 100.0
    assert any(item["category"] == "evidence_gap" for item in orchestration["top_blocker_categories"])
    assert orchestration["quality_expansion_points"]


def test_governance_dashboard_pilot_observation_semantics(tmp_path: Path) -> None:
    task_notes_root = tmp_path / "task-notes"
    pilot_start = date(2026, 3, 25)
    contour = "runtime-publication-closure"

    _write_task_note(
        task_notes_root / "pre-pilot-blocked-shortcut.md",
        updated=date(2026, 3, 20),
        solution_class="staged",
        critical_contour=contour,
        outcome_status="blocked",
        note_text="shortcut was blocked before pilot start",
    )
    _write_task_note(
        task_notes_root / "pilot-staged-blocked-shortcut.md",
        updated=date(2026, 3, 26),
        solution_class="staged",
        critical_contour=contour,
        outcome_status="blocked",
        note_text="shortcut attempt blocked by validator",
    )
    _write_task_note(
        task_notes_root / "pilot-target-done.md",
        updated=date(2026, 3, 27),
        solution_class="target",
        critical_contour=contour,
        outcome_status="done",
        note_text="target closure evidence is attached",
    )
    _write_task_note(
        task_notes_root / "pilot-fallback-blocked-no-shortcut.md",
        updated=date(2026, 3, 28),
        solution_class="fallback",
        critical_contour=contour,
        outcome_status="blocked",
        note_text="blocked by unrelated validation rule",
    )
    _write_task_note(
        task_notes_root / "pilot-invalid-class.md",
        updated=date(2026, 3, 28),
        solution_class="prototype",
        critical_contour=contour,
        outcome_status="blocked",
        note_text="shortcut word exists but invalid class must be ignored",
    )
    _write_task_note(
        task_notes_root / "pilot-none-contour.md",
        updated=date(2026, 3, 28),
        solution_class="target",
        critical_contour="none",
        outcome_status="done",
        note_text="contour none should be excluded from counters",
    )
    _write_task_note(
        task_notes_root / "pilot-missing-updated.md",
        updated=None,
        solution_class="target",
        critical_contour=contour,
        outcome_status="blocked",
        note_text="shortcut should be ignored without an explicit note date",
    )

    output_json = tmp_path / "dashboard.json"
    output_md = tmp_path / "dashboard.md"
    result = _run(
        [
            sys.executable,
            "scripts/build_governance_dashboard.py",
            "--task-notes-root",
            str(task_notes_root),
            "--pilot-observation-start",
            pilot_start.isoformat(),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    observation = payload["sections"]["pilot_observation"]
    assert observation["observation_window_start"] == pilot_start.isoformat()
    assert observation["critical_tasks_with_explicit_solution_class"] == 3
    assert observation["blocked_shortcut_claims"] == 1
    assert observation["staged_vs_target_declarations"]["staged"] == 1
    assert observation["staged_vs_target_declarations"]["target"] == 1


@pytest.mark.parametrize(
    ("days_since_start", "expected_status"),
    [
        (6, "collecting"),
        (7, "review_ready"),
        (15, "review_due"),
    ],
)
def test_governance_dashboard_pilot_window_thresholds(
    tmp_path: Path,
    days_since_start: int,
    expected_status: str,
) -> None:
    pilot_start = date.today() - timedelta(days=days_since_start)
    output_json = tmp_path / f"dashboard-{days_since_start}.json"
    output_md = tmp_path / f"dashboard-{days_since_start}.md"
    result = _run(
        [
            sys.executable,
            "scripts/build_governance_dashboard.py",
            "--task-notes-root",
            str(tmp_path / "no-notes"),
            "--pilot-observation-start",
            pilot_start.isoformat(),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ]
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    observation = payload["sections"]["pilot_observation"]
    assert observation["observation_window_start"] == pilot_start.isoformat()
    assert observation["observation_window_days"] == days_since_start
    assert observation["observation_window_status"] == expected_status


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
