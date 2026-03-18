from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SPEC_DIR = ROOT / "docs" / "architecture" / "app" / "product-plane-spec-v2"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_phase0_docs_package_is_complete() -> None:
    expected = {
        "README.md",
        "TECHNICAL_REQUIREMENTS.md",
        "00_AI_Shell_Alignment.md",
        "01_Architecture_Overview.md",
        "02_Repository_Structure.md",
        "03_Data_Model_and_Flows.md",
        "04_ADRs.md",
        "05_Modules_DoD_and_Parallelization.md",
        "06_Phases_and_Acceptance_Gates.md",
        "07_Tech_Stack_and_Open_Source.md",
        "08_Codex_AI_Shell_Integration.md",
        "09_Constraints_Risks_and_NFR.md",
        "10_MCP_Deployment_Request.md",
    }
    present = {path.name for path in SPEC_DIR.glob("*.md")}
    assert expected <= present


def test_overlay_references_root_shell_hot_docs() -> None:
    overlay = _read(ROOT / "src" / "trading_advisor_3000" / "AGENTS.md")
    required_refs = (
        "AGENTS.md",
        "docs/agent/entrypoint.md",
        "docs/agent/domains.md",
        "docs/agent/checks.md",
        "docs/agent/runtime.md",
        "docs/DEV_WORKFLOW.md",
        "TECHNICAL_REQUIREMENTS.md",
        "00_AI_Shell_Alignment.md",
        "06_Phases_and_Acceptance_Gates.md",
    )
    for ref in required_refs:
        assert ref in overlay


def test_phase0_plan_contains_pr1_baseline_decision() -> None:
    plan = _read(ROOT / "docs" / "architecture" / "app" / "phase0-plan.md")
    assert "PR #1 merged first" in plan
    assert "shell hardening" in plan


def test_product_status_doc_separates_shell_and_product_phase_namespaces() -> None:
    status_doc = _read(ROOT / "docs" / "architecture" / "app" / "STATUS.md")
    assert "S0-S8" in status_doc
    assert "P0-P7" in status_doc
    assert "not accepted as live/prod ready" in status_doc


def test_app_docs_index_points_to_status_and_contract_truth_sources() -> None:
    index_doc = _read(ROOT / "docs" / "architecture" / "app" / "README.md")
    assert "STATUS.md" in index_doc
    assert "CONTRACT_SURFACES.md" in index_doc


def test_bootstrap_runbook_is_listed_in_app_runbooks_index() -> None:
    runbooks_index = _read(ROOT / "docs" / "runbooks" / "app" / "README.md")
    assert "bootstrap.md" in runbooks_index


def test_phase0_checklist_contains_acceptance_evidence_commands() -> None:
    checklist = _read(
        ROOT / "docs" / "checklists" / "app" / "phase0-acceptance-checklist.md"
    )
    required_commands = (
        "python -m pytest tests/app -q",
        "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
        "python scripts/run_pr_gate.py --from-git --git-ref HEAD",
    )
    for command in required_commands:
        assert command in checklist


def test_product_phase0_skeleton_paths_exist() -> None:
    expected_dirs = (
        ROOT / "src" / "trading_advisor_3000" / "app" / "data_plane",
        ROOT / "src" / "trading_advisor_3000" / "app" / "research",
        ROOT / "src" / "trading_advisor_3000" / "app" / "runtime",
        ROOT / "src" / "trading_advisor_3000" / "app" / "execution",
        ROOT / "src" / "trading_advisor_3000" / "app" / "interfaces",
        ROOT / "tests" / "app" / "contracts",
        ROOT / "docs" / "architecture" / "app",
        ROOT / "docs" / "runbooks" / "app",
        ROOT / "deployment" / "stocksharp-sidecar",
    )
    for path in expected_dirs:
        assert path.is_dir()
