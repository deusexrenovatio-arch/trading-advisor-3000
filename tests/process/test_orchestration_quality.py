from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from orchestration_quality import attach_quality_summaries, compute_orchestration_rollup  # noqa: E402
from orchestration_quality import render_rollup_markdown  # noqa: E402
from orchestration_quality import summarize_orchestration_run  # noqa: E402


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _acceptance_payload(
    *,
    verdict: str,
    blockers: list[dict] | None = None,
    evidence_gaps: list[str] | None = None,
    policy_blockers: list[dict] | None = None,
    result_quality: dict | None = None,
) -> dict:
    return {
        "verdict": verdict,
        "summary": f"Acceptance {verdict.lower()} summary.",
        "route_signal": "acceptance:attempt-01",
        "used_skills": ["phase-acceptance-governor", "testing-suite"],
        "blockers": blockers or [],
        "rerun_checks": [],
        "evidence_gaps": evidence_gaps or [],
        "prohibited_findings": [],
        "policy_blockers": policy_blockers or [],
        "result_quality": result_quality
        or {
            "requirements_alignment": {"score": 90, "summary": "Result matches the scoped request."},
            "documentation_quality": {"score": 88, "summary": "Docs match the delivered phase output."},
            "implementation_quality": {"score": 87, "summary": "Implementation quality is strong for this phase."},
            "testing_quality": {"score": 84, "summary": "Executed test evidence is credible for this phase."},
            "strengths": ["Good fit to the scoped outcome."],
            "gaps": [],
        },
    }


def _worker_report_payload(*, worker_self_quality: dict | None = None) -> dict:
    return {
        "status": "DONE",
        "summary": "Worker report summary.",
        "route_signal": "worker:attempt-01",
        "files_touched": ["src/example.py"],
        "checks_run": ["python -m pytest tests/process/test_orchestration_quality.py -q"],
        "remaining_risks": [],
        "assumptions": [],
        "skips": [],
        "fallbacks": [],
        "deferred_work": [],
        "worker_self_quality": worker_self_quality
        or {
            "requirements_alignment": {"score": 85, "summary": "Worker sees a strong requirements fit."},
            "documentation_quality": {"score": 82, "summary": "Handoff is clear enough for downstream review."},
            "implementation_quality": {"score": 84, "summary": "Implementation looks solid from the worker lens."},
            "testing_quality": {"score": 80, "summary": "Executed tests look good enough from the worker lens."},
            "strengths": ["Worker sees the phase as coherent."],
            "gaps": [],
        },
        "evidence_contract": {
            "surfaces": ["demo_surface"],
            "proof_class": "integration",
            "artifact_paths": ["artifacts/demo-proof.json"],
            "checks": ["python -m pytest tests/process/test_orchestration_quality.py -q"],
            "real_bindings": [],
        },
    }


def _attempt_record(attempt: int, *, kind: str, verdict: str, acceptance_json_path: Path) -> dict:
    return {
        "attempt": attempt,
        "kind": kind,
        "worker_summary": f"{kind} attempt {attempt}",
        "worker_route_signal": f"{kind}:attempt-{attempt:02d}",
        "worker_report_path": (acceptance_json_path.parent / "worker-report.json").as_posix(),
        "changed_files_path": (acceptance_json_path.parent / "changed-files.txt").as_posix(),
        "acceptance_json_path": acceptance_json_path.as_posix(),
        "acceptance_md_path": acceptance_json_path.with_suffix(".md").as_posix(),
        "acceptor_route_signal": f"acceptance:attempt-{attempt:02d}",
        "acceptor_used_skills": ["phase-acceptance-governor"],
        "verdict": verdict,
        "blockers_total": 0,
        "policy_blockers_total": 0,
    }


def _state_payload(*, run_id: str, updated_at: str, attempts: list[dict], final_status: str) -> dict:
    return {
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
        "next_phase": "docs/codex/modules/demo.phase-02.md" if final_status == "accepted" else "docs/codex/modules/demo.phase-01.md",
    }


def test_summarize_orchestration_run_scores_clean_first_pass(tmp_path: Path) -> None:
    acceptance_path = tmp_path / "artifacts/codex/orchestration/run-clean/attempt-01/acceptance.json"
    _write_json(acceptance_path, _acceptance_payload(verdict="PASS"))

    state = _state_payload(
        run_id="run-clean",
        updated_at="2026-04-10T10:00:00Z",
        attempts=[_attempt_record(1, kind="worker", verdict="PASS", acceptance_json_path=acceptance_path)],
        final_status="accepted",
    )

    summary = summarize_orchestration_run(state, repo_root=tmp_path)
    assert summary["orchestration_score"] == 100
    assert summary["score_label"] == "excellent"
    assert summary["first_pass_passed"] is True
    assert summary["quality_expansion_points"] == []

    worker_report_path = acceptance_path.parent / "worker-report.json"
    _write_json(worker_report_path, _worker_report_payload())
    enriched = attach_quality_summaries(state, repo_root=tmp_path)
    assert enriched["result_quality_summary"]["status"] == "scored"
    assert enriched["result_quality_summary"]["overall_score"] == 87
    assert enriched["worker_self_quality_summary"]["overall_score"] == 83
    assert enriched["worker_acceptor_delta_summary"]["calibration"] == "aligned"
    assert enriched["worker_acceptor_delta_summary"]["signed_delta"] == -4
    assert enriched["orchestration_quality_summary"]["orchestration_score"] == 100


def test_summarize_orchestration_run_penalizes_retries_and_policy_gaps(tmp_path: Path) -> None:
    blocked_path = tmp_path / "artifacts/codex/orchestration/run-hard/attempt-01/acceptance.json"
    pass_path = tmp_path / "artifacts/codex/orchestration/run-hard/attempt-02/acceptance.json"
    fallback_blocker = {
        "id": "P-FALLBACK-1",
        "title": "Silent or unapproved fallback path",
        "why": "Fallback path was used.",
        "remediation": "Remove the fallback.",
    }
    evidence_blocker = {
        "id": "P-EVIDENCE_GAP-2",
        "title": "Required evidence is missing",
        "why": "Executed integration evidence is missing.",
        "remediation": "Run the missing checks.",
    }
    _write_json(
        blocked_path,
        _acceptance_payload(
            verdict="BLOCKED",
            blockers=[
                {"id": "B1", "title": "Acceptance blocker", "why": "Needs more evidence.", "remediation": "Add proof."},
                fallback_blocker,
                evidence_blocker,
            ],
            evidence_gaps=["Missing integration proof."],
            policy_blockers=[fallback_blocker, evidence_blocker],
        ),
    )
    _write_json(pass_path, _acceptance_payload(verdict="PASS"))
    _write_json(blocked_path.parent / "worker-report.json", _worker_report_payload())
    _write_json(pass_path.parent / "worker-report.json", _worker_report_payload())

    state = _state_payload(
        run_id="run-hard",
        updated_at="2026-04-10T10:05:00Z",
        attempts=[
            _attempt_record(1, kind="worker", verdict="BLOCKED", acceptance_json_path=blocked_path),
            _attempt_record(2, kind="remediation", verdict="PASS", acceptance_json_path=pass_path),
        ],
        final_status="accepted",
    )

    summary = summarize_orchestration_run(state, repo_root=tmp_path)
    assert summary["orchestration_score"] < 100
    assert summary["orchestration_score"] < 80
    assert summary["remediation_attempts"] == 1
    assert summary["policy_blockers_total"] == 2
    categories = {item["category"] for item in summary["top_blocker_categories"]}
    assert {"fallback", "evidence_gap"} <= categories
    assert summary["quality_expansion_points"]


def test_compute_orchestration_rollup_aggregates_scores_and_categories(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts/codex/orchestration"

    clean_acceptance = artifact_root / "run-a/attempt-01/acceptance.json"
    _write_json(clean_acceptance, _acceptance_payload(verdict="PASS"))
    _write_json(clean_acceptance.parent / "worker-report.json", _worker_report_payload())
    _write_json(
        artifact_root / "run-a/state.json",
        _state_payload(
            run_id="run-a",
            updated_at="2026-04-10T10:00:00Z",
            attempts=[_attempt_record(1, kind="worker", verdict="PASS", acceptance_json_path=clean_acceptance)],
            final_status="accepted",
        ),
    )

    evidence_blocker = {
        "id": "P-EVIDENCE_GAP-1",
        "title": "Required evidence is missing",
        "why": "Executed checks are incomplete.",
        "remediation": "Add the missing checks.",
    }
    blocked_acceptance = artifact_root / "run-b/attempt-01/acceptance.json"
    pass_acceptance = artifact_root / "run-b/attempt-02/acceptance.json"
    _write_json(
        blocked_acceptance,
        _acceptance_payload(
            verdict="BLOCKED",
            blockers=[evidence_blocker],
            evidence_gaps=["Executed checks are incomplete."],
            policy_blockers=[evidence_blocker],
        ),
    )
    _write_json(pass_acceptance, _acceptance_payload(verdict="PASS"))
    _write_json(blocked_acceptance.parent / "worker-report.json", _worker_report_payload())
    _write_json(pass_acceptance.parent / "worker-report.json", _worker_report_payload())
    _write_json(
        artifact_root / "run-b/state.json",
        _state_payload(
            run_id="run-b",
            updated_at="2026-04-10T10:05:00Z",
            attempts=[
                _attempt_record(1, kind="worker", verdict="BLOCKED", acceptance_json_path=blocked_acceptance),
                _attempt_record(2, kind="remediation", verdict="PASS", acceptance_json_path=pass_acceptance),
            ],
            final_status="accepted",
        ),
    )

    fallback_blocker = {
        "id": "P-FALLBACK-1",
        "title": "Silent or unapproved fallback path",
        "why": "Fallback path was used.",
        "remediation": "Remove the fallback.",
    }
    hard_blocked_acceptance = artifact_root / "run-c/attempt-01/acceptance.json"
    _write_json(
        hard_blocked_acceptance,
        _acceptance_payload(
            verdict="BLOCKED",
            blockers=[fallback_blocker],
            policy_blockers=[fallback_blocker],
        ),
    )
    _write_json(hard_blocked_acceptance.parent / "worker-report.json", _worker_report_payload())
    _write_json(
        artifact_root / "run-c/state.json",
        _state_payload(
            run_id="run-c",
            updated_at="2026-04-10T10:10:00Z",
            attempts=[_attempt_record(1, kind="worker", verdict="BLOCKED", acceptance_json_path=hard_blocked_acceptance)],
            final_status="blocked",
        ),
    )

    rollup = compute_orchestration_rollup(artifact_root=artifact_root, repo_root=tmp_path, window_size=20)
    assert rollup["runs_total"] == 3
    assert rollup["window_runs_count"] == 3
    assert rollup["accepted_runs"] == 2
    assert rollup["blocked_runs"] == 1
    assert rollup["average_orchestration_score"] < 100.0
    assert rollup["average_worker_acceptor_absolute_delta"] > 0.0
    assert rollup["first_pass_acceptance_rate"] < 1.0
    top_categories = {item["category"] for item in rollup["top_blocker_categories"]}
    assert {"evidence_gap", "fallback"} <= top_categories

    markdown = render_rollup_markdown(rollup)
    assert "| Metric | Value |" in markdown
    assert "## Quality Expansion Points" in markdown
