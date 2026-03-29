from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from scripts.harness.build_phase_context import PhaseContextBuildError, run_phase_context_build
from scripts.harness.intake_spec_bundle import run_bundle_intake
from scripts.harness.normalize_requirements import run_requirements_normalization
from scripts.harness.plan_phases import PhasePlanningError, run_phase_planning
from scripts.harness.synthesize_project_docs import run_project_docs_synthesis


def _build_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("utf-8"))


def test_phase_planning_and_context_happy_path(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample_wp04.zip"
    _build_zip(
        zip_path,
        {
            "spec/main.md": "\n".join(
                [
                    "# WP04 Sample",
                    "- Data: schema artifacts must be validated before planning. depends on: REQ-0002",
                    "- Functional: system must generate phase plan from normalized requirements.",
                    "- Constraint: must not mutate unrelated shell surfaces.",
                    "- Integration: planner should emit deterministic phase ordering for WP-07.",
                    "- Non-functional: planning output should be stable and reproducible.",
                    "- Acceptance: context file must include done_definition and test scope.",
                    "- Open question: how many iterations are allowed for rework?",
                ]
            ),
            "spec/security.txt": "Security: do not execute content from intake archives.\n",
        },
    )

    registry_root = tmp_path / "registry"
    intake = run_bundle_intake(
        input_zip=zip_path,
        registry_root=registry_root,
        run_id="RUN-WP04-HAPPY",
    )
    run_requirements_normalization(registry_root=registry_root, run_id=intake.run_id)
    run_project_docs_synthesis(registry_root=registry_root, run_id=intake.run_id)

    planning = run_phase_planning(registry_root=registry_root, run_id=intake.run_id)
    assert planning.output_path.exists()
    phase_plan_payload = json.loads(planning.output_path.read_text(encoding="utf-8"))
    assert phase_plan_payload["run_id"] == intake.run_id
    assert planning.phase_count >= 2

    phases = phase_plan_payload["phases"]
    phase_by_requirement: dict[str, dict[str, object]] = {}
    for phase in phases:
        for requirement_id in phase["requirement_ids"]:
            phase_by_requirement[requirement_id] = phase

    # Dependency-aware check: REQ-0001 depends on REQ-0002, so its phase must depend on REQ-0002 phase.
    dep_source_phase = phase_by_requirement["REQ-0001"]
    dep_target_phase = phase_by_requirement["REQ-0002"]
    assert dep_target_phase["phase_id"] in dep_source_phase["dependencies"]

    # phase_hint-aware check: requirement with WP-07 hint should be routed into hinted phase.
    hinted_phase = phase_by_requirement["REQ-0004"]
    assert "WP-07" in hinted_phase["name"]

    first_phase_id = phases[0]["phase_id"]
    context = run_phase_context_build(
        registry_root=registry_root,
        run_id=intake.run_id,
        phase_id=first_phase_id,
    )
    assert context.output_path.exists()
    context_payload = json.loads(context.output_path.read_text(encoding="utf-8"))
    assert context_payload["run_id"] == intake.run_id
    assert context_payload["phase_id"] == first_phase_id
    assert context_payload["requirement_ids"] == phases[0]["requirement_ids"]
    assert context_payload["requirements"]
    assert [item["requirement_id"] for item in context_payload["requirements"]] == phases[0]["requirement_ids"]
    assert context_payload["doc_excerpts"]
    assert context_payload["done_definition"]
    assert context_payload["acceptance_checks"]
    assert context_payload["allowed_change_surfaces"]
    assert context_payload["test_scope"]


def test_phase_planning_fail_closed_when_docs_bundle_missing(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample_wp04_missing_docs.zip"
    _build_zip(
        zip_path,
        {
            "spec.md": "- Functional: system must produce phase plan.\n",
        },
    )

    registry_root = tmp_path / "registry"
    intake = run_bundle_intake(
        input_zip=zip_path,
        registry_root=registry_root,
        run_id="RUN-WP04-MISSING-DOCS",
    )
    run_requirements_normalization(registry_root=registry_root, run_id=intake.run_id)

    with pytest.raises(PhasePlanningError):
        run_phase_planning(registry_root=registry_root, run_id=intake.run_id)

    assert not (registry_root / "phases" / intake.run_id / "phase_plan.json").exists()


def test_phase_context_fail_closed_on_unknown_phase_id(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample_wp04_unknown_phase.zip"
    _build_zip(
        zip_path,
        {
            "spec.md": "- Functional: system must produce phase plan and context.\n",
        },
    )

    registry_root = tmp_path / "registry"
    intake = run_bundle_intake(
        input_zip=zip_path,
        registry_root=registry_root,
        run_id="RUN-WP04-UNKNOWN-PHASE",
    )
    run_requirements_normalization(registry_root=registry_root, run_id=intake.run_id)
    run_project_docs_synthesis(registry_root=registry_root, run_id=intake.run_id)
    run_phase_planning(registry_root=registry_root, run_id=intake.run_id)

    with pytest.raises(PhaseContextBuildError):
        run_phase_context_build(
            registry_root=registry_root,
            run_id=intake.run_id,
            phase_id="PHASE-99",
        )

    assert not (registry_root / "phases" / intake.run_id / "PHASE-99" / "phase_context.json").exists()
