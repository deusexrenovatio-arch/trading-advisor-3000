from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from compute_change_surface import compute_surface  # noqa: E402


def test_docs_only_surface_detection() -> None:
    result = compute_surface(
        ["docs/README.md"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "docs-only"
    assert result["docs_only"] is True


def test_contracts_surface_detection() -> None:
    result = compute_surface(
        ["plans/items/index.yaml", "memory/task_outcomes.yaml"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "contracts"
    assert "contracts" in result["surfaces"]
    assert "governance" not in result["surfaces"]


def test_mixed_surface_detection() -> None:
    result = compute_surface(
        ["docs/architecture/layers.md", "src/trading_advisor_3000/app.py"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert "architecture" in result["surfaces"]
    assert "app" in result["surfaces"]
    assert "mixed" in result["surfaces"]


def test_registry_changes_route_to_contracts_and_run_stack_conformance_validator() -> None:
    result = compute_surface(
        ["registry/stack_conformance.yaml"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "contracts"
    assert any(
        "scripts/validate_stack_conformance.py" in command
        for command in result["commands"]["loop"]
    )


def test_stack_validator_wiring_for_runtime_script_changes() -> None:
    result = compute_surface(
        ["scripts/validate_stack_conformance.py"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "contracts"
    assert "governance" in result["surfaces"]
    assert any(
        "scripts/validate_stack_conformance.py" in command
        for command in result["commands"]["loop"]
    )


def test_stack_validator_wiring_for_governance_docs_changes() -> None:
    result = compute_surface(
        ["docs/agent/checks.md"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "governance"
    assert result["docs_only"] is True
    assert any(
        "scripts/validate_stack_conformance.py" in command
        for command in result["commands"]["loop"]
    )


def test_stack_validator_wiring_for_architecture_docs_changes() -> None:
    result = compute_surface(
        ["docs/architecture/app/stack-conformance-baseline.md"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "architecture"
    assert any(
        "scripts/validate_stack_conformance.py" in command
        for command in result["commands"]["loop"]
    )


def test_runtime_surface_routes_to_surface_pr_matrix_runner() -> None:
    result = compute_surface(
        ["src/trading_advisor_3000/product_plane/runtime/bootstrap.py"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "app-runtime"
    assert "app-runtime" in result["surfaces"]
    assert any(
        "scripts/run_surface_pr_matrix.py" in command
        for command in result["commands"]["pr"]
    )


def test_data_surface_routes_to_surface_pr_matrix_runner() -> None:
    result = compute_surface(
        ["src/trading_advisor_3000/product_plane/data_plane/pipeline.py"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "app-data"
    assert "app-data" in result["surfaces"]
    assert any(
        "scripts/run_surface_pr_matrix.py" in command
        for command in result["commands"]["pr"]
    )
