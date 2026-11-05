from __future__ import annotations

import sys
from pathlib import Path

import pytest

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
        "scripts/validate_stack_conformance.py" in command for command in result["commands"]["loop"]
    )


def test_stack_validator_wiring_for_runtime_script_changes() -> None:
    result = compute_surface(
        ["scripts/validate_stack_conformance.py"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "contracts"
    assert "governance" in result["surfaces"]
    assert any(
        "scripts/validate_stack_conformance.py" in command for command in result["commands"]["loop"]
    )


def test_stack_validator_wiring_for_governance_docs_changes() -> None:
    result = compute_surface(
        ["docs/agent/checks.md"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "governance"
    assert result["docs_only"] is True
    assert any(
        "scripts/validate_stack_conformance.py" in command for command in result["commands"]["loop"]
    )


def test_stack_validator_wiring_for_architecture_docs_changes() -> None:
    result = compute_surface(
        ["docs/architecture/product-plane/stack-conformance-baseline.md"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "architecture"
    assert any(
        "scripts/validate_stack_conformance.py" in command for command in result["commands"]["loop"]
    )


def test_runtime_surface_routes_to_surface_pr_matrix_runner() -> None:
    result = compute_surface(
        ["src/trading_advisor_3000/product_plane/runtime/bootstrap.py"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "app-runtime"
    assert "app-runtime" in result["surfaces"]
    assert any(
        "scripts/run_surface_pr_matrix.py" in command for command in result["commands"]["pr"]
    )


def test_data_surface_routes_to_surface_pr_matrix_runner() -> None:
    result = compute_surface(
        ["src/trading_advisor_3000/product_plane/data_plane/pipeline.py"],
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )
    assert result["primary_surface"] == "app-data"
    assert "app-data" in result["surfaces"]
    assert any(
        "scripts/run_surface_pr_matrix.py" in command for command in result["commands"]["pr"]
    )


@pytest.mark.parametrize(
    "paths,level",
    [
        (["configs/change_surface_mapping.yaml"], "code"),
        (["scripts/run_pr_gate.py"], "code"),
        (["scripts/run_staging_control.py"], "code"),
        (["scripts/collect_staging_acceptance.py"], "code"),
        (["deployment/docker/dagster-staging/docker-compose.proof.yml"], "code"),
        (["src/trading_advisor_3000/product_plane/common/staging_execution.py"], "code"),
        (
            ["src/trading_advisor_3000/product_plane/data_plane/moex/staging_test_fixture.py"],
            "code",
        ),
        (["docs/README.md"], "code"),
        (["src/trading_advisor_3000/product_plane/interfaces/api.py"], "code"),
        (["tests/product-plane/unit/test_research_frames.py"], "code"),
        (["src/trading_advisor_3000/product_plane/research/README.md"], "code"),
        (["src/trading_advisor_3000/product_plane/research/io/loaders.py"], "native"),
        (["src/trading_advisor_3000/product_plane/data_plane/delta_runtime.py"], "native"),
        (["src/trading_advisor_3000/product_plane/common/data_snapshots.py"], "native"),
        (["configs/product-plane/continuous_front_indicators.yaml"], "native"),
        (["deployment/docker/dagster-staging/Dockerfile"], "native"),
        (["deployment/runtime-instances/moex-runtime-instances.v1.yaml"], "native"),
        (["pyproject.toml"], "native"),
        (
            [
                "configs/change_surface_mapping.yaml",
                "src/trading_advisor_3000/spark_jobs/example.py",
            ],
            "native",
        ),
    ],
)
def test_evidence_level_uses_contours_and_strongest_match(paths, level):
    result = compute_surface(paths, mapping_path=ROOT / "configs/change_surface_mapping.yaml")
    assert result["evidence_level"] == level


def test_evidence_level_is_configured_and_unknown_level_blocks(tmp_path):
    path = tmp_path / "mapping.yaml"
    path.write_text("surfaces:\n  engine:\n    prefixes: [engine/]\n    evidence_level: native\n")
    assert compute_surface(["engine/code.py"], mapping_path=path)["evidence_level"] == "native"
    path.write_text(path.read_text().replace("native", "unknown"))
    with pytest.raises(ValueError, match="evidence"):
        compute_surface(["engine/code.py"], mapping_path=path)


@pytest.mark.parametrize(
    "source",
    [
        "surfaces: []",
        (
            "surfaces: {engine: {prefixes: [engine/], evidence_level: native, "
            "exclude_prefixes: tests/}}"
        ),
        "surfaces: {engine: {prefixes: [engine/], evidence_level: native, exclude_prefixes: ['']}}",
        "surfaces: {engine: {prefixes: [''], evidence_level: native}}",
        (
            "surfaces: {engine: {prefixes: [engine/], evidence_level: native, "
            "evidence_exclude_prefixes: tests/}}"
        ),
        (
            "surfaces: {engine: {prefixes: [engine/], evidence_level: native}}\n"
            "docs_only: {suffixes: ['']}"
        ),
    ],
)
def test_broken_evidence_mapping_cannot_downgrade_to_code(tmp_path, source):
    path = tmp_path / "mapping.yaml"
    path.write_text(source)
    with pytest.raises(ValueError, match="[Ee]vidence"):
        compute_surface(["engine/code.py"], mapping_path=path)


def test_acceptance_support_does_not_select_product_data_matrix():
    from run_surface_pr_matrix import build_pr_surface_matrix_plan

    paths = [
        "src/trading_advisor_3000/product_plane/common/staging_acceptance.py",
        "src/trading_advisor_3000/product_plane/common/staging_execution.py",
        "src/trading_advisor_3000/product_plane/common/staging_test_fixture.py",
        "src/trading_advisor_3000/product_plane/data_plane/moex/staging_test_fixture.py",
        "tests/product-plane/unit/test_staging_test_fixture.py",
        "deployment/docker/dagster-staging/docker-compose.proof.yml",
    ]
    surface = compute_surface(paths, mapping_path=ROOT / "configs/change_surface_mapping.yaml")
    plan = build_pr_surface_matrix_plan(surface)
    assert surface["evidence_level"] == "code"
    assert plan.contour == "governance-only" and not plan.checks
    paths.append("src/trading_advisor_3000/product_plane/research/io.py")
    surface = compute_surface(paths, mapping_path=ROOT / "configs/change_surface_mapping.yaml")
    assert surface["evidence_level"] == "native"
    assert build_pr_surface_matrix_plan(surface).contour == "data-proof"
