from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ACTIVE_SPEC_DIR = ROOT / "docs" / "architecture" / "product-plane" / "product-plane-spec-v2"
ARCHIVED_SPEC_DIR = ROOT / "docs" / "archive" / "product-plane-spec-v2" / "2026-05-06"
ARCHIVED_CHECKLIST_DIR = ROOT / "docs" / "archive" / "product-plane-acceptance-checklists" / "2026-05-06"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_product_plane_spec_v2_is_archived_and_complete() -> None:
    expected = {
        "README.md",
        "PACKAGE-README.md",
        "TECHNICAL_REQUIREMENTS.md",
        "00_AI_Shell_Alignment.md",
        "01_Architecture_Overview.md",
        "02_Repository_Structure.md",
        "03_Data_Model_and_Flows.md",
        "04_ADRs.md",
        "05_Modules_DoD_and_Parallelization.md",
        "06_Capability_Slices_and_Acceptance_Gates.md",
        "07_Tech_Stack_and_Open_Source.md",
        "08_Codex_AI_Shell_Integration.md",
        "09_Constraints_Risks_and_NFR.md",
        "10_MCP_Deployment_Request.md",
    }
    present = {path.name for path in ARCHIVED_SPEC_DIR.glob("*.md")}

    assert not ACTIVE_SPEC_DIR.exists()
    assert expected <= present


def test_overlay_references_current_product_truth_sources() -> None:
    overlay = _read(ROOT / "src" / "trading_advisor_3000" / "AGENTS.md")
    required_refs = (
        "AGENTS.md",
        "docs/agent/entrypoint.md",
        "docs/agent/domains.md",
        "docs/agent/checks.md",
        "docs/agent/runtime.md",
        "docs/DEV_WORKFLOW.md",
        "docs/project-map/current-truth-map-2026-05-05.md",
        "docs/architecture/product-plane/STATUS.md",
        "docs/architecture/product-plane/CONTRACT_SURFACES.md",
        "docs/architecture/product-plane/stack-conformance-baseline.md",
    )
    for ref in required_refs:
        assert ref in overlay

    assert "docs/architecture/product-plane/product-plane-spec-v2" not in overlay


def test_product_plane_bootstrap_plan_is_historical_and_points_to_archive() -> None:
    plan = _read(ROOT / "docs" / "architecture" / "product-plane" / "product-plane-bootstrap-plan.md")
    assert "PR #1 merged first" in plan
    assert "historical evidence" in plan
    assert "docs/archive/product-plane-spec-v2/2026-05-06/README.md" in plan


def test_product_status_doc_uses_capability_naming_rule() -> None:
    status_doc = _read(ROOT / "docs" / "architecture" / "product-plane" / "STATUS.md")
    assert "Active surfaces use capability names, not phase labels" in status_doc
    assert "not accepted as live/prod ready" in status_doc


def test_app_docs_index_points_to_status_and_contract_truth_sources() -> None:
    index_doc = _read(ROOT / "docs" / "architecture" / "product-plane" / "README.md")
    assert "STATUS.md" in index_doc
    assert "CONTRACT_SURFACES.md" in index_doc
    assert "docs/archive/product-plane-spec-v2/2026-05-06/README.md" in index_doc


def test_bootstrap_runbook_is_listed_in_app_runbooks_index() -> None:
    runbooks_index = _read(ROOT / "docs" / "runbooks" / "app" / "README.md")
    assert "bootstrap.md" in runbooks_index


def test_product_plane_bootstrap_checklist_is_archived_with_acceptance_evidence_commands() -> None:
    checklist = _read(
        ARCHIVED_CHECKLIST_DIR / "product-plane-bootstrap-acceptance-checklist.md"
    )
    required_commands = (
        "python -m pytest tests/product-plane -q",
        "python scripts/run_loop_gate.py --from-git --git-ref HEAD",
        "python scripts/run_pr_gate.py --from-git --git-ref HEAD",
    )
    for command in required_commands:
        assert command in checklist


def test_product_plane_skeleton_paths_exist() -> None:
    expected_dirs = (
        ROOT / "src" / "trading_advisor_3000" / "product_plane" / "data_plane",
        ROOT / "src" / "trading_advisor_3000" / "product_plane" / "research",
        ROOT / "src" / "trading_advisor_3000" / "product_plane" / "runtime",
        ROOT / "src" / "trading_advisor_3000" / "product_plane" / "execution",
        ROOT / "src" / "trading_advisor_3000" / "product_plane" / "interfaces",
        ROOT / "tests" / "product-plane" / "contracts",
        ROOT / "docs" / "architecture" / "product-plane",
        ROOT / "docs" / "runbooks" / "app",
        ROOT / "deployment" / "stocksharp-sidecar",
    )
    for path in expected_dirs:
        assert path.is_dir()
