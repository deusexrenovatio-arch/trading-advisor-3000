from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from compute_change_surface import compute_surface  # noqa: E402
from run_surface_pr_matrix import build_pr_surface_matrix_plan  # noqa: E402


def _surface(changed_files: list[str]) -> dict[str, object]:
    return compute_surface(
        changed_files,
        mapping_path=ROOT / "configs" / "change_surface_mapping.yaml",
    )


def test_runtime_contour_plan_uses_runtime_profile_without_data_stack() -> None:
    plan = build_pr_surface_matrix_plan(
        _surface(["src/trading_advisor_3000/product_plane/runtime/bootstrap.py"])
    )

    assert plan.contour == "runtime-publication"
    assert plan.gate_profile == "runtime-api"
    assert "runtime-api" in plan.dependency_profiles
    assert "data-proof" not in plan.dependency_profiles
    assert any("test_runtime_api_smoke.py" in command for command in plan.checks)
    assert all("test_historical_data_spark_execution.py" not in command for command in plan.checks)


def test_data_contour_plan_uses_data_proof_profile() -> None:
    plan = build_pr_surface_matrix_plan(
        _surface(["src/trading_advisor_3000/product_plane/data_plane/pipeline.py"])
    )

    assert plan.contour == "data-proof"
    assert plan.gate_profile == "data-proof"
    assert "data-proof" in plan.dependency_profiles
    assert any("test_historical_data_spark_execution.py" in command for command in plan.checks)


def test_mixed_contour_plan_uses_integration_profile() -> None:
    plan = build_pr_surface_matrix_plan(
        _surface(
            [
                "src/trading_advisor_3000/product_plane/runtime/bootstrap.py",
                "src/trading_advisor_3000/product_plane/data_plane/pipeline.py",
            ]
        )
    )

    assert plan.contour == "mixed-integration"
    assert plan.gate_profile == "integration"
    assert "data-proof" in plan.dependency_profiles
    assert any("test_runtime_api_smoke.py" in command for command in plan.checks)
    assert any("test_historical_data_spark_execution.py" in command for command in plan.checks)


def test_governance_only_plan_requires_no_app_matrix_checks() -> None:
    plan = build_pr_surface_matrix_plan(
        _surface(["docs/README.md"])
    )

    assert plan.contour == "governance-only"
    assert plan.gate_profile == "governance"
    assert plan.checks == tuple()


def test_plan_only_cli_emits_github_outputs(tmp_path: Path) -> None:
    output_json = tmp_path / "surface-plan.json"
    github_output = tmp_path / "github-output.txt"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_surface_pr_matrix.py",
            "--plan-only",
            "--changed-files",
            "src/trading_advisor_3000/product_plane/runtime/bootstrap.py",
            "--output-json",
            output_json.as_posix(),
            "--emit-github-output",
            github_output.as_posix(),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["contour"] == "runtime-publication"
    output_lines = github_output.read_text(encoding="utf-8")
    assert "ci_contour=runtime-publication" in output_lines
    assert "install_extras=runtime-api,dev-test" in output_lines
