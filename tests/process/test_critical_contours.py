from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from context_router import route_files  # noqa: E402
from critical_contours import load_critical_contours  # noqa: E402
from run_loop_gate import _apply_non_trivial_loop_policy  # noqa: E402
from validate_critical_contour_closure import run as run_closure_validation  # noqa: E402
from validate_solution_intent import run as run_solution_intent  # noqa: E402


def _task_note(solution_intent_block: str = "") -> str:
    extra_section = f"{solution_intent_block}\n\n" if solution_intent_block else ""
    return (
        "# Task Note\n"
        "Updated: 2026-03-25 13:30 UTC\n\n"
        "## Goal\n"
        "- Deliver: validate critical contour pilot governance.\n\n"
        "## Task Request Contract\n"
        "- Objective: prove critical contour validation behavior.\n"
        "- In Scope: temporary task note and pointer shim.\n"
        "- Out of Scope: product trading logic.\n"
        "- Constraints: deterministic validation only.\n"
        "- Done Evidence: validators return the expected code.\n"
        "- Priority Rule: quality and safety over speed.\n\n"
        f"{extra_section}"
        "## Current Delta\n"
        "- Synthetic task note for contour validation tests.\n\n"
        "## First-Time-Right Report\n"
        "1. Confirmed coverage: validation paths are explicit.\n"
        "2. Missing or risky scenarios: none.\n"
        "3. Resource/time risks and chosen controls: keep scope local.\n"
        "4. Highest-priority fixes or follow-ups: none.\n\n"
        "## Repetition Control\n"
        "- Max Same-Path Attempts: 2\n"
        "- Stop Trigger: repeated failure twice.\n"
        "- Reset Action: isolate the failing validator.\n"
        "- New Search Space: contour config, note fields, gate wiring.\n"
        "- Next Probe: rerun the smallest validator.\n\n"
        "## Task Outcome\n"
        "- Outcome Status: in_progress\n"
        "- Decision Quality: pending\n"
        "- Final Contexts: pending\n"
        "- Route Match: pending\n"
        "- Primary Rework Cause: none\n"
        "- Incident Signature: none\n"
        "- Improvement Action: pending\n"
        "- Improvement Artifact: pending\n"
    )


def _handoff(task_path: Path) -> str:
    return (
        "# Session Handoff\n"
        "Updated: 2026-03-25 13:31 UTC\n\n"
        "## Active Task Note\n"
        f"- Path: {task_path.as_posix()}\n"
        "- Mode: full\n"
        "- Status: in_progress\n\n"
        "## Validation\n"
        "- `python scripts/validate_task_request_contract.py`\n"
        "- `python scripts/validate_session_handoff.py`\n"
        "- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`\n"
    )


def _write_handoff_pair(tmp_path: Path, *, solution_intent_block: str = "") -> Path:
    task_path = tmp_path / "TASK.md"
    handoff_path = tmp_path / "session_handoff.md"
    task_path.write_text(_task_note(solution_intent_block), encoding="utf-8")
    handoff_path.write_text(_handoff(task_path), encoding="utf-8")
    return handoff_path


def test_critical_contour_config_has_two_pilot_entries() -> None:
    contours = load_critical_contours(ROOT / "configs" / "critical_contours.yaml")
    contour_ids = [item.contour_id for item in contours]
    assert contour_ids == ["data-integration-closure", "runtime-publication-closure"]
    assert "tests/app/fixtures/data_plane/" in contours[0].trigger_paths


def test_validate_solution_intent_requires_fields_for_critical_contour(tmp_path: Path) -> None:
    handoff_path = _write_handoff_pair(tmp_path)
    code = run_solution_intent(
        handoff_path,
        config_path=ROOT / "configs" / "critical_contours.yaml",
        changed_files_override=["src/trading_advisor_3000/app/data_plane/pipeline.py"],
    )
    assert code == 1


def test_validate_solution_intent_allows_docs_only_change_without_addendum(tmp_path: Path) -> None:
    handoff_path = _write_handoff_pair(tmp_path)
    code = run_solution_intent(
        handoff_path,
        config_path=ROOT / "configs" / "critical_contours.yaml",
        changed_files_override=["docs/README.md"],
    )
    assert code == 0


def test_validate_critical_contour_closure_blocks_target_with_fixture_evidence(tmp_path: Path) -> None:
    handoff_path = _write_handoff_pair(
        tmp_path,
        solution_intent_block=(
            "## Solution Intent\n"
            "- Solution Class: target\n"
            "- Critical Contour: data-integration-closure\n"
            "- Forbidden Shortcuts: fixture path, sample artifact\n"
            "- Closure Evidence: integration test uses tests/app/fixtures/data_plane/mock.json as canonical dataset proof.\n"
            "- Shortcut Waiver: none\n"
            "- Design Checkpoint: chosen path=real contour closure; why_not_shortcut=future shape preserved; future_shape=downstream research handoff.\n"
        ),
    )
    code = run_closure_validation(
        handoff_path,
        config_path=ROOT / "configs" / "critical_contours.yaml",
        changed_files_override=["src/trading_advisor_3000/app/data_plane/pipeline.py"],
    )
    assert code == 1


def test_validate_critical_contour_closure_allows_staged_claim_with_explicit_wording(tmp_path: Path) -> None:
    handoff_path = _write_handoff_pair(
        tmp_path,
        solution_intent_block=(
            "## Solution Intent\n"
            "- Solution Class: staged\n"
            "- Critical Contour: runtime-publication-closure\n"
            "- Forbidden Shortcuts: synthetic publication, smoke only\n"
            "- Closure Evidence: staged runtime output reaches the publication contour with end-to-end publication evidence, but this is not full target closure for the durable store.\n"
            "- Shortcut Waiver: none\n"
            "- Design Checkpoint: chosen path=partial publication; why_not_shortcut=future shape preserved; future_shape=durable store publication contour.\n"
        ),
    )
    code = run_closure_validation(
        handoff_path,
        config_path=ROOT / "configs" / "critical_contours.yaml",
        changed_files_override=["src/trading_advisor_3000/app/runtime/runtime_service.py"],
    )
    assert code == 0


def test_context_router_adds_critical_contour_policy_signals() -> None:
    result = route_files(["src/trading_advisor_3000/app/data_plane/pipeline.py"])
    context_ids = [entry["id"] for entry in result["contexts"]]
    assert "data-integration-closure" in result["critical_contours"]
    assert "CTX-ARCHITECTURE" in context_ids
    assert "qa-test-engineer" in result["required_review_lenses"]


def test_loop_gate_non_trivial_policy_includes_critical_contour_validators() -> None:
    commands, require_contract_validation = _apply_non_trivial_loop_policy(
        ["python scripts/validate_docs_links.py --roots AGENTS.md docs"],
        changed_files=["src/trading_advisor_3000/app/runtime/runtime_service.py"],
        docs_only=False,
    )
    rendered = "\n".join(commands)
    assert require_contract_validation is True
    assert "validate_solution_intent.py" in rendered
    assert "validate_critical_contour_closure.py" in rendered
