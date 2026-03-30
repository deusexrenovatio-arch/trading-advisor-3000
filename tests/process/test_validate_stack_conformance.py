from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import validate_stack_conformance  # noqa: E402


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _seed_common(
    tmp_path: Path,
    *,
    status_claim: str,
    readme_text: str,
    dependencies: list[str],
    include_fastapi_entrypoint: bool,
    include_fastapi_test: bool,
    checklist_text: str = "Checklist wording only.\n",
    acceptance_doc_text: str = "Acceptance verdict wording only.\n",
    reacceptance_report_text: str = "Report wording only.\n",
    red_team_text: str = "Red-team wording only.\n",
    evidence_pack_text: str = "{\"status\":\"deny_release_readiness\"}\n",
    module_brief_text: str = (
        "## Disprover\n"
        "- Reinsert an unsupported phrase such as `aiogram removed by ADR` "
        "and confirm validation fails.\n"
    ),
) -> None:
    status_md = """# Status

| Surface | Status | Real | Not real |
| --- | --- | --- | --- |
| API surface | {status_claim} | slice exists | closure pending |
"""
    _write(
        tmp_path / "docs/architecture/app/STATUS.md",
        status_md.format(status_claim=status_claim),
    )
    _write(tmp_path / "docs/architecture/app/README.md", readme_text)
    _write(
        tmp_path / "docs/architecture/app/product-plane-spec-v2/07_Tech_Stack_and_Open_Source.md",
        "| FastAPI | service/API layer | chosen | OSS | runtime/admin APIs |\n",
    )
    _write(
        tmp_path / "docs/architecture/app/product-plane-spec-v2/04_ADRs.md",
        "ADR placeholder for stack replacement notes.\n",
    )
    _write(
        tmp_path / "docs/checklists/app/phase2-acceptance-checklist.md",
        checklist_text,
    )
    _write(
        tmp_path / "docs/architecture/app/phase0-acceptance-verdict.md",
        acceptance_doc_text,
    )
    _write(
        tmp_path / "docs/architecture/app/phase10-stack-conformance-reacceptance-report.md",
        reacceptance_report_text,
    )
    _write(
        tmp_path / "artifacts/acceptance/f1/red-team-review-result.md",
        red_team_text,
    )
    _write(
        tmp_path / "artifacts/acceptance/f1/reacceptance-evidence-pack.json",
        evidence_pack_text,
    )
    _write(
        tmp_path / "docs/codex/modules/f1-full-closure.phase-01.md",
        module_brief_text,
    )

    quoted = ", ".join(f'"{item}"' for item in dependencies)
    pyproject = f"""[project]
name = "tmp-stack"
version = "0.1.0"
dependencies = [{quoted}]
"""
    _write(tmp_path / "pyproject.toml", pyproject)

    if include_fastapi_entrypoint:
        _write(
            tmp_path / "src/trading_advisor_3000/app/interfaces/asgi.py",
            "app = object()\n",
        )
    if include_fastapi_test:
        _write(
            tmp_path / "tests/app/unit/test_fastapi_smoke.py",
            "def test_fastapi_smoke() -> None:\n    assert True\n",
        )


def _write_registry(tmp_path: Path, payload: dict[str, Any]) -> Path:
    registry_path = tmp_path / "registry/stack_conformance.yaml"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return registry_path


def _base_registry(*, surface_claim: str, technology_claim: str) -> dict[str, Any]:
    return {
        "version": 1,
        "truth_sources": {
            "status_doc": "docs/architecture/app/STATUS.md",
            "spec_doc": "docs/architecture/app/product-plane-spec-v2/07_Tech_Stack_and_Open_Source.md",
            "pyproject": "pyproject.toml",
        },
        "closure_claim_guard": {
            "documents": [
                "docs/architecture/app/README.md",
                "docs/architecture/app/phase10-stack-conformance-reacceptance-report.md",
                "artifacts/acceptance/f1/red-team-review-result.md",
                "artifacts/acceptance/f1/reacceptance-evidence-pack.json",
                "docs/codex/modules/f1-full-closure.phase-01.md",
            ],
            "document_globs": [
                "docs/checklists/app/*acceptance*.md",
                "docs/architecture/app/phase*-acceptance-*.md",
            ],
            "forbidden_terms": ["production ready", "full acceptance"],
        },
        "removed_claim_guard": {
            "documents": [
                "docs/architecture/app/phase10-stack-conformance-reacceptance-report.md",
                "artifacts/acceptance/f1/red-team-review-result.md",
                "artifacts/acceptance/f1/reacceptance-evidence-pack.json",
                "docs/codex/modules/f1-full-closure.phase-01.md",
            ],
            "phrase_markers_any": [
                "removed by ADR",
                "ADR-backed removal",
                "removed through ADR",
            ],
            "skip_markers_any": [
                "unsupported phrase",
                "confirm validation fails",
                "confirm the phase fails",
            ],
        },
        "surface_claims": [
            {
                "id": "api_surface",
                "status_label": "API surface",
                "claim": surface_claim,
                "runtime_proof": {
                    "entrypoints_any": ["src/trading_advisor_3000/app/interfaces/asgi.py"],
                    "tests_any": ["tests/app/unit/test_fastapi_smoke.py"],
                },
            }
        ],
        "technology_claims": [
            {
                "id": "fastapi",
                "display_name": "FastAPI",
                "claim": technology_claim,
                "spec_markers_any": ["| FastAPI | service/API layer | chosen"],
                "runtime_proof": {
                    "dependencies_any": ["fastapi"],
                    "entrypoints_any": ["src/trading_advisor_3000/app/interfaces/asgi.py"],
                    "tests_any": ["tests/app/unit/test_fastapi_smoke.py"],
                },
                "replacement": {
                    "adr_required_when_removed": True,
                    "adr_paths_any": ["docs/architecture/app/product-plane-spec-v2/04_ADRs.md"],
                    "adr_markers_any": ["fastapi"],
                },
            },
            {
                "id": "aiogram",
                "display_name": "aiogram",
                "claim": "planned",
            },
        ],
    }


def test_validate_stack_conformance_passes_with_matching_proof(tmp_path: Path) -> None:
    _seed_common(
        tmp_path,
        status_claim="implemented",
        readme_text="Baseline documentation with constrained wording.\n",
        dependencies=["fastapi>=0.110"],
        include_fastapi_entrypoint=True,
        include_fastapi_test=True,
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="implemented", technology_claim="implemented"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    assert code == 0


def test_validate_stack_conformance_fails_when_surface_is_implemented_without_runtime_proof(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="implemented",
        readme_text="Baseline wording only.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
    )
    registry = _base_registry(surface_claim="implemented", technology_claim="planned")
    registry["surface_claims"][0].pop("runtime_proof")
    registry_path = _write_registry(tmp_path, registry)

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "surface `api_surface` is marked `implemented` but runtime_proof is empty" in captured.out


def test_validate_stack_conformance_fails_when_fastapi_is_implemented_without_runtime_proof(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="Baseline wording only.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="partial", technology_claim="implemented"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "technology `FastAPI` runtime proof missing dependency evidence" in captured.out
    assert "technology `FastAPI` runtime proof missing entrypoint evidence" in captured.out


def test_validate_stack_conformance_fails_on_full_closure_term_with_non_implemented_surface(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="This slice is production ready right now.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="partial", technology_claim="planned"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "forbidden closure term `production ready`" in captured.out


def test_validate_stack_conformance_fails_on_checklist_overclaim_with_non_implemented_surface(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="Baseline wording only.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
        checklist_text="This checklist now claims full acceptance.\n",
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="partial", technology_claim="planned"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "forbidden closure term `full acceptance`" in captured.out
    assert "docs/checklists/app/phase2-acceptance-checklist.md" in captured.out


def test_validate_stack_conformance_fails_when_closure_glob_has_no_matches(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="Baseline wording only.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
    )
    registry = _base_registry(surface_claim="partial", technology_claim="planned")
    registry["closure_claim_guard"]["document_globs"] = ["docs/checklists/app/not-found-*.md"]
    registry_path = _write_registry(tmp_path, registry)

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "closure_claim_guard.document_globs pattern matched no files" in captured.out


def test_validate_stack_conformance_fails_when_removed_technology_is_still_chosen(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="Constrained wording.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
    )
    _write(
        tmp_path / "docs/architecture/app/product-plane-spec-v2/04_ADRs.md",
        "ADR remove fastapi from runtime path.\n",
    )
    registry = _base_registry(surface_claim="partial", technology_claim="removed")
    registry_path = _write_registry(tmp_path, registry)

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "technology `FastAPI` has claim `removed` but is still declared as chosen in the stack spec" in captured.out


def test_validate_stack_conformance_fails_when_report_claims_removed_without_registry_removed_state(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="Constrained wording.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
        reacceptance_report_text="aiogram removed by ADR in this report.\n",
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="partial", technology_claim="planned"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "technology `aiogram` has claim `planned` but ADR-removal wording appears" in captured.out
    assert "phase10-stack-conformance-reacceptance-report.md:1" in captured.out


def test_validate_stack_conformance_fails_when_red_team_claims_removed_without_registry_removed_state(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="Constrained wording.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
        red_team_text="aiogram removed by ADR in this red-team note.\n",
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="partial", technology_claim="planned"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "technology `aiogram` has claim `planned` but ADR-removal wording appears" in captured.out
    assert "artifacts/acceptance/f1/red-team-review-result.md:1" in captured.out


def test_validate_stack_conformance_fails_when_evidence_pack_claims_removed_without_registry_removed_state(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="partial",
        readme_text="Constrained wording.\n",
        dependencies=[],
        include_fastapi_entrypoint=False,
        include_fastapi_test=False,
        evidence_pack_text='{"finding":"aiogram removed by ADR in this evidence pack"}\n',
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="partial", technology_claim="planned"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "technology `aiogram` has claim `planned` but ADR-removal wording appears" in captured.out
    assert "artifacts/acceptance/f1/reacceptance-evidence-pack.json:1" in captured.out


def test_validate_stack_conformance_fails_when_module_brief_claims_removed_without_registry_removed_state(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="implemented",
        readme_text="Constrained wording.\n",
        dependencies=["fastapi>=0.110"],
        include_fastapi_entrypoint=True,
        include_fastapi_test=True,
        module_brief_text="aiogram removed by ADR in this module brief.\n",
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="implemented", technology_claim="implemented"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "technology `aiogram` has claim `planned` but ADR-removal wording appears" in captured.out
    assert "docs/codex/modules/f1-full-closure.phase-01.md:1" in captured.out


def test_validate_stack_conformance_allows_module_brief_disprover_skip_marker(
    tmp_path: Path,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="implemented",
        readme_text="Constrained wording.\n",
        dependencies=["fastapi>=0.110"],
        include_fastapi_entrypoint=True,
        include_fastapi_test=True,
        module_brief_text=(
            "## Disprover\n"
            "- Reinsert an unsupported phrase such as aiogram removed by ADR "
            "and confirm validation fails.\n"
        ),
    )
    registry_path = _write_registry(
        tmp_path,
        _base_registry(surface_claim="implemented", technology_claim="implemented"),
    )

    code = validate_stack_conformance.run(tmp_path, registry_path)
    assert code == 0


def test_validate_stack_conformance_fails_when_terminal_guard_keeps_planned_claim(
    tmp_path: Path,
    capsys,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="implemented",
        readme_text="Constrained wording.\n",
        dependencies=["fastapi>=0.110"],
        include_fastapi_entrypoint=True,
        include_fastapi_test=True,
    )
    registry = _base_registry(surface_claim="implemented", technology_claim="implemented")
    registry["terminal_technology_guard"] = {
        "technology_ids": ["aiogram"],
    }
    registry_path = _write_registry(tmp_path, registry)

    code = validate_stack_conformance.run(tmp_path, registry_path)
    captured = capsys.readouterr()
    assert code == 1
    assert "technology `aiogram` has non-terminal claim `planned` under terminal_technology_guard" in captured.out


def test_validate_stack_conformance_allows_replaced_by_claim_under_terminal_guard(
    tmp_path: Path,
) -> None:
    _seed_common(
        tmp_path,
        status_claim="implemented",
        readme_text="Constrained wording.\n",
        dependencies=["fastapi>=0.110"],
        include_fastapi_entrypoint=True,
        include_fastapi_test=True,
    )
    _write(
        tmp_path / "docs/architecture/app/product-plane-spec-v2/04_ADRs.md",
        "ADR-011: aiogram removed by ADR and replaced by custom_bot_api_engine.\n",
    )
    registry = _base_registry(surface_claim="implemented", technology_claim="implemented")
    registry["terminal_technology_guard"] = {
        "technology_ids": ["aiogram"],
    }
    registry["technology_claims"][1]["claim"] = "replaced_by:custom_bot_api_engine"
    registry["technology_claims"][1]["replacement"] = {
        "adr_required_when_removed": True,
        "adr_paths_any": ["docs/architecture/app/product-plane-spec-v2/04_ADRs.md"],
        "adr_markers_any": ["custom_bot_api_engine"],
    }
    registry_path = _write_registry(tmp_path, registry)

    code = validate_stack_conformance.run(tmp_path, registry_path)
    assert code == 0
