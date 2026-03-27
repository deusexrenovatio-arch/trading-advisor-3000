from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from scripts.harness.intake_spec_bundle import run_bundle_intake
from scripts.harness.render_docs_from_registry import run_docs_render
from scripts.harness.run_harness import run_plan
from scripts.harness.validate_registry_consistency import (
    RegistryConsistencyError,
    validate_registry_consistency,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "harness_docs"


def _build_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("utf-8"))


def _bootstrap_run(tmp_path: Path, run_id: str) -> tuple[Path, Path]:
    zip_path = tmp_path / f"{run_id}.zip"
    _build_zip(
        zip_path,
        {
            "spec/main.md": "\n".join(
                [
                    "# WP06 Docs Sample",
                    "- Functional: generated docs should be deterministic and canonical-derived.",
                    "- Constraint: generated docs are read-model only and not source of truth.",
                    "- Acceptance: drift validator should fail closed on mismatched docs.",
                    "- Open question: what additional docs are required for operators?",
                ]
            )
        },
    )
    registry_root = tmp_path / "registry"
    docs_root = tmp_path / "docs" / "generated"
    run_bundle_intake(input_zip=zip_path, registry_root=registry_root, run_id=run_id)
    run_plan(registry_root=registry_root, run_id=run_id, docs_root=docs_root, auto_render_docs=True)
    run_docs_render(registry_root=registry_root, run_id=run_id, docs_root=docs_root)
    return registry_root, docs_root


def _write_session_handoff(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# Session Handoff",
                "Updated: 2026-03-27 12:00 UTC",
                "",
                "## Active Task Note",
                "- Path: docs/tasks/active/TASK-demo.md",
                "- Mode: full",
                "- Status: in_progress",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _golden(name: str, run_id: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8").replace("{RUN_ID}", run_id)


def test_render_docs_matches_golden_and_validator_passes(tmp_path: Path) -> None:
    run_id = "RUN-WP06-GOLDEN"
    registry_root, docs_root = _bootstrap_run(tmp_path, run_id)
    session_handoff = tmp_path / "docs" / "session_handoff.md"
    _write_session_handoff(session_handoff)

    current_project_brief = (docs_root / "current_project_brief.md").read_text(encoding="utf-8")
    open_questions = (docs_root / "open_questions.md").read_text(encoding="utf-8")
    assert current_project_brief == _golden("current_project_brief.golden.md", run_id)
    assert open_questions == _golden("open_questions.golden.md", run_id)

    result = validate_registry_consistency(
        registry_root=registry_root,
        run_id=run_id,
        docs_root=docs_root,
        session_handoff_path=session_handoff,
    )
    assert result.status == "ok"
    assert result.checked_files == 5


def test_validator_fail_closed_on_docs_drift(tmp_path: Path) -> None:
    run_id = "RUN-WP06-DRIFT"
    registry_root, docs_root = _bootstrap_run(tmp_path, run_id)
    session_handoff = tmp_path / "docs" / "session_handoff.md"
    _write_session_handoff(session_handoff)

    target = docs_root / "current_phase.md"
    target.write_text(target.read_text(encoding="utf-8") + "\nDRIFT\n", encoding="utf-8")

    with pytest.raises(RegistryConsistencyError):
        validate_registry_consistency(
            registry_root=registry_root,
            run_id=run_id,
            docs_root=docs_root,
            session_handoff_path=session_handoff,
        )
