from __future__ import annotations

import json
import zipfile
from pathlib import Path

from scripts.harness.models import parse_phase_acceptance_report, parse_phase_plan, parse_run_state
from scripts.harness.run_harness import run_current_phase, run_plan, run_to_completion
from scripts.harness.intake_spec_bundle import run_bundle_intake
from scripts.harness.run_phase_acceptance import run_phase_acceptance_stage
from scripts.harness.run_phase_review import run_phase_review_stage


def _build_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("utf-8"))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _materialize_surface(surface: str, token: str) -> str:
    normalized = surface.replace("\\", "/")
    if "*" in normalized:
        return normalized.replace("*", f"{token}.py")
    return normalized


def _force_traceability_covered(
    *,
    registry_root: Path,
    run_id: str,
    phase_id: str,
    requirement_ids: list[str],
) -> None:
    traceability_path = registry_root / "traceability" / run_id / "traceability_matrix.json"
    payload = _read_json(traceability_path)
    mappings = payload.get("mappings")
    if not isinstance(mappings, list):
        raise AssertionError("traceability_matrix.json has invalid `mappings` payload")

    mapping_by_req: dict[str, dict[str, object]] = {}
    for item in mappings:
        if not isinstance(item, dict):
            continue
        requirement_id = item.get("requirement_id")
        if isinstance(requirement_id, str) and requirement_id:
            mapping_by_req[requirement_id] = item

    for requirement_id in requirement_ids:
        entry = mapping_by_req.get(requirement_id)
        if entry is None:
            entry = {
                "requirement_id": requirement_id,
                "phase_ids": [phase_id],
                "artifact_refs": [f"phases/{run_id}/{phase_id}/implementation_summary.json"],
                "status": "covered",
                "notes": "forced covered for deterministic evidence contract regression test",
            }
            mappings.append(entry)
            continue
        phase_ids = entry.get("phase_ids")
        if not isinstance(phase_ids, list):
            phase_ids = []
            entry["phase_ids"] = phase_ids
        if phase_id not in phase_ids:
            phase_ids.append(phase_id)
        entry["status"] = "covered"

    traceability_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _bootstrap_planned_run(tmp_path: Path, run_id: str) -> tuple[Path, Path, str]:
    zip_path = tmp_path / f"{run_id}.zip"
    _build_zip(
        zip_path,
        {
            "spec/main.md": "\n".join(
                [
                    "# Harness Runtime Sample",
                    "- Data: schemas must be canonical before phase execution.",
                    "- Functional: implementation stage should produce structured summary.",
                    "- Acceptance: next phase must not start until current phase is accepted.",
                    "- Integration: run_harness should support WP-07 mode orchestration.",
                    "- Open question: should retry ceiling be configurable per phase?",
                ]
            ),
            "spec/security.txt": "Security: do not execute input bundle content.\n",
        },
    )
    registry_root = tmp_path / "registry"
    docs_root = tmp_path / "docs" / "generated"
    run_bundle_intake(input_zip=zip_path, registry_root=registry_root, run_id=run_id)
    run_plan(registry_root=registry_root, run_id=run_id, docs_root=docs_root, auto_render_docs=True)
    return registry_root, docs_root, run_id


def test_rejected_phase_creates_rework_request_and_blocks_advance(tmp_path: Path) -> None:
    registry_root, docs_root, run_id = _bootstrap_planned_run(tmp_path, "RUN-WP05-REJECT")

    result = run_current_phase(
        registry_root=registry_root,
        run_id=run_id,
        phase_id=None,
        retry_ceiling=0,
        backend="simulate",
        quality_profile="always-fail",
        docs_root=docs_root,
        auto_render_docs=True,
        prompt_file=Path(__file__).resolve().parents[2] / "configs/harness/prompts/implementer.prompt.md",
        codex_bin=None,
    )

    assert result.final_verdict == "rejected"
    assert result.retry_limit_reached is True
    acceptance_path = registry_root / "acceptance" / run_id / result.phase_id / "phase_acceptance_report.json"
    acceptance = parse_phase_acceptance_report(_read_json(acceptance_path))
    assert acceptance.verdict == "rejected"
    rework_path = registry_root / "phases" / run_id / result.phase_id / "phase_rework_request.json"
    assert rework_path.exists()

    run_state = parse_run_state(_read_json(registry_root / "runs" / run_id / "run_state.json"))
    assert run_state.status == "failed"
    assert run_state.current_phase_id == result.phase_id
    assert result.phase_id not in run_state.accepted_phase_ids


def test_rework_loop_retries_and_accepts_phase_then_advances(tmp_path: Path) -> None:
    registry_root, docs_root, run_id = _bootstrap_planned_run(tmp_path, "RUN-WP05-REWORK")

    result = run_current_phase(
        registry_root=registry_root,
        run_id=run_id,
        phase_id=None,
        retry_ceiling=2,
        backend="simulate",
        quality_profile="improve-on-rework",
        docs_root=docs_root,
        auto_render_docs=True,
        prompt_file=Path(__file__).resolve().parents[2] / "configs/harness/prompts/implementer.prompt.md",
        codex_bin=None,
    )

    assert result.final_verdict == "accepted"
    assert result.attempts >= 2
    run_state = parse_run_state(_read_json(registry_root / "runs" / run_id / "run_state.json"))
    assert result.phase_id in run_state.accepted_phase_ids
    if run_state.status != "completed":
        assert run_state.current_phase_id != result.phase_id
    assert (registry_root / "phases" / run_id / result.phase_id / "phase_rework_request.json").exists()


def test_run_to_completion_e2e_from_intake_to_completed(tmp_path: Path) -> None:
    registry_root, docs_root, run_id = _bootstrap_planned_run(tmp_path, "RUN-WP07-E2E")

    summary = run_to_completion(
        registry_root=registry_root,
        run_id=run_id,
        retry_ceiling=1,
        backend="simulate",
        quality_profile="always-pass",
        docs_root=docs_root,
        auto_render_docs=True,
        prompt_file=Path(__file__).resolve().parents[2] / "configs/harness/prompts/implementer.prompt.md",
        codex_bin=None,
    )

    assert summary["final_status"] == "completed"
    phase_plan = parse_phase_plan(_read_json(registry_root / "phases" / run_id / "phase_plan.json"))
    for phase in phase_plan.phases:
        acceptance_path = registry_root / "acceptance" / run_id / phase.phase_id / "phase_acceptance_report.json"
        acceptance = parse_phase_acceptance_report(_read_json(acceptance_path))
        assert acceptance.verdict == "accepted"

    assert (registry_root / "intake" / run_id / "spec_manifest.json").exists()
    assert (registry_root / "intake" / run_id / "normalized_requirements.json").exists()
    assert (registry_root / "phases" / run_id / "phase_plan.json").exists()
    assert (registry_root / "traceability" / run_id / "traceability_matrix.json").exists()
    assert (registry_root / "runs" / run_id / "run_state.json").exists()
    assert (registry_root / "runs" / run_id / "events.jsonl").read_text(encoding="utf-8").strip()

    current_phase_doc = (docs_root / "current_phase.md").read_text(encoding="utf-8")
    assert f"Run ID: `{run_id}`" in current_phase_doc


def test_review_and_acceptance_do_not_raise_false_missing_tests_for_codex_style_evidence(tmp_path: Path) -> None:
    registry_root, _docs_root, run_id = _bootstrap_planned_run(tmp_path, "RUN-WP05-EVIDENCE-FALSE-NEGATIVE")
    run_state = parse_run_state(_read_json(registry_root / "runs" / run_id / "run_state.json"))
    phase_id = run_state.current_phase_id
    assert phase_id

    phase_root = registry_root / "phases" / run_id / phase_id
    phase_context = _read_json(phase_root / "phase_context.json")
    required_tests = list(phase_context["test_scope"])
    requirement_ids = list(phase_context["requirement_ids"])
    changed_file = _materialize_surface(str(phase_context["allowed_change_surfaces"][0]), "evidence_contract")
    _force_traceability_covered(
        registry_root=registry_root,
        run_id=run_id,
        phase_id=phase_id,
        requirement_ids=requirement_ids,
    )

    implementation_payload = {
        "run_id": run_id,
        "phase_id": phase_id,
        "iteration": 1,
        "generated_at": "2026-03-27T00:00:00Z",
        "backend": "codex-cli",
        "prompt_template": "configs/harness/prompts/implementer.prompt.md",
        "phase_context_ref": f"phases/{run_id}/{phase_id}/phase_context.json",
        "summary": "Codex-style test output was captured successfully.",
        "changed_files": [changed_file],
        "checks_run": [f"PASSED {item}::test_contract_proof [100%]" for item in required_tests],
        "required_tests": required_tests,
        "passed_tests": [f"passed: {item}" for item in required_tests],
        "failed_tests": [],
        "covered_requirements": requirement_ids,
        "unresolved_risks": [],
    }
    (phase_root / "implementation_summary.json").write_text(
        json.dumps(implementation_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    review_result = run_phase_review_stage(registry_root=registry_root, run_id=run_id, phase_id=phase_id)
    assert review_result.verdict in {"pass", "pass_with_notes"}
    assert review_result.missing_tests_count == 0

    review_payload = _read_json(phase_root / "phase_review_report.json")
    assert review_payload["missing_tests"] == []

    acceptance_result = run_phase_acceptance_stage(registry_root=registry_root, run_id=run_id, phase_id=phase_id)
    acceptance = parse_phase_acceptance_report(_read_json(acceptance_result.output_path))
    assert acceptance.verdict == "accepted"


def test_review_and_acceptance_remain_fail_closed_for_truly_missing_required_tests(tmp_path: Path) -> None:
    registry_root, _docs_root, run_id = _bootstrap_planned_run(tmp_path, "RUN-WP05-EVIDENCE-MISSING")
    run_state = parse_run_state(_read_json(registry_root / "runs" / run_id / "run_state.json"))
    phase_id = run_state.current_phase_id
    assert phase_id

    phase_root = registry_root / "phases" / run_id / phase_id
    phase_context = _read_json(phase_root / "phase_context.json")
    required_tests = list(phase_context["test_scope"])
    requirement_ids = list(phase_context["requirement_ids"])
    changed_file = _materialize_surface(str(phase_context["allowed_change_surfaces"][0]), "evidence_contract")
    _force_traceability_covered(
        registry_root=registry_root,
        run_id=run_id,
        phase_id=phase_id,
        requirement_ids=requirement_ids,
    )

    implementation_payload = {
        "run_id": run_id,
        "phase_id": phase_id,
        "iteration": 1,
        "generated_at": "2026-03-27T00:00:00Z",
        "backend": "codex-cli",
        "prompt_template": "configs/harness/prompts/implementer.prompt.md",
        "phase_context_ref": f"phases/{run_id}/{phase_id}/phase_context.json",
        "summary": "Implementation evidence omitted required tests.",
        "changed_files": [changed_file],
        "checks_run": ["PASSED tests/process/test_unrelated.py::test_example [100%]"],
        "required_tests": required_tests,
        "passed_tests": ["tests/process/test_unrelated.py::test_example"],
        "failed_tests": [],
        "covered_requirements": requirement_ids,
        "unresolved_risks": [],
    }
    (phase_root / "implementation_summary.json").write_text(
        json.dumps(implementation_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    review_result = run_phase_review_stage(registry_root=registry_root, run_id=run_id, phase_id=phase_id)
    assert review_result.verdict == "fail"
    assert review_result.missing_tests_count >= 1

    acceptance_result = run_phase_acceptance_stage(registry_root=registry_root, run_id=run_id, phase_id=phase_id)
    acceptance = parse_phase_acceptance_report(_read_json(acceptance_result.output_path))
    assert acceptance.verdict == "rejected"
    assert any("Missing required test execution:" in item for item in acceptance.failed_checks)
