from __future__ import annotations

import json
import zipfile
from pathlib import Path

from scripts.harness.models import parse_phase_acceptance_report, parse_phase_plan, parse_run_state
from scripts.harness.run_harness import run_current_phase, run_plan, run_to_completion
from scripts.harness.intake_spec_bundle import run_bundle_intake


def _build_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("utf-8"))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


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
