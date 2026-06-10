from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_no_tracked_secrets import validate as validate_no_tracked_secrets  # noqa: E402

REQUIRED_HARNESS_FILES = (
    "scripts/measure_dev_loop.py",
    "scripts/harness_baseline_metrics.py",
    "scripts/build_governance_dashboard.py",
    "scripts/run_shell_delivery_operational_proving.py",
    "scripts/skill_update_decision.py",
    "scripts/skill_precommit_gate.py",
    "scripts/sync_architecture_map.py",
    "scripts/nightly_root_hygiene.py",
    "scripts/validate_no_tracked_secrets.py",
    "scripts/run_staging_real_execution_rollout.py",
    "scripts/run_surface_pr_matrix.py",
)


def test_required_harness_files_exist() -> None:
    for rel in REQUIRED_HARNESS_FILES:
        assert (ROOT / rel).exists(), f"missing harness file: {rel}"
    assert (ROOT / "scripts" / "validate_pr_size.py").exists()


def test_shell_delivery_dashboard_lane_scripts_are_registered_in_mapping() -> None:
    mapping_text = (ROOT / "configs/change_surface_mapping.yaml").read_text(encoding="utf-8")
    assert "build_governance_dashboard.py" in mapping_text
    assert "harness_baseline_metrics.py" in mapping_text
    assert "measure_dev_loop.py" in mapping_text
    assert "validate_no_tracked_secrets.py" in mapping_text


def test_ci_workflow_has_hosted_ci_opt_in_guard() -> None:
    workflow_text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "AI_SHELL_ENABLE_HOSTED_CI" in workflow_text
    assert "vars.AI_SHELL_ENABLE_HOSTED_CI == '1'" in workflow_text
    assert "Tracked Secret Scan" in workflow_text
    assert "MCP Governance Contract" not in workflow_text


def test_ci_workflow_resolves_explicit_diff_ranges_for_hosted_gates() -> None:
    workflow_text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    branch_text = (ROOT / ".github/workflows/branch-diagnostic.yml").read_text(encoding="utf-8")
    assert "Resolve Diff Range" in workflow_text
    assert "github.event.pull_request.base.sha" in workflow_text
    assert "github.event.pull_request.head.sha" in workflow_text
    assert "github.event.before" in branch_text
    assert "--base-ref" in workflow_text
    assert "--head-ref" in workflow_text


def test_ci_workflow_keeps_pr_required_lanes_pr_only() -> None:
    workflow_text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    branch_text = (ROOT / ".github/workflows/branch-diagnostic.yml").read_text(encoding="utf-8")

    assert "branch-lane:" not in workflow_text
    assert "loop-lane:" not in workflow_text
    assert "github.event_name == 'pull_request' || github.event_name == 'push'" not in workflow_text
    assert "pr-lane:" in workflow_text
    assert "needs: loop-lane" not in workflow_text
    assert "branch-lane:" in branch_text
    assert "branch-lane-surface-summary" in branch_text


def test_ci_workflow_uses_surface_aware_profile_planning() -> None:
    workflow_text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "scripts/run_surface_pr_matrix.py" in workflow_text
    assert "--plan-only" in workflow_text
    assert "install_extras" in workflow_text
    assert "--summary-file artifacts/ci/pr-gate-summary.md" in workflow_text


def test_skill_update_decision_allows_content_only_contract() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/skill_update_decision.py",
            "--changed-files",
            ".codex/skills/ta3000-data-plane-proof/SKILL.md",
            "--format",
            "json",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "ready"
    assert payload["missing_required_updates"] == []


def test_skill_precommit_gate_passes_when_docs_are_synced() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/skill_precommit_gate.py",
            "--changed-files",
            ".codex/skills/ta3000-data-plane-proof/SKILL.md",
            "docs/agent/skills-catalog.md",
            "docs/agent/skills-routing.md",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr


def test_tracked_secret_validator_detects_hardcoded_token_pattern(tmp_path: Path) -> None:
    suspicious = tmp_path / "suspicious.env"
    token = "ghp_" + "ABCDEFGHIJKLMNO" + "PQRSTUVWXYZ12345"
    suspicious.write_text(f"GITHUB_TOKEN={token}\n", encoding="utf-8")

    findings, report = validate_no_tracked_secrets(root=tmp_path, paths=[suspicious.name])

    assert report["status"] == "failed"
    assert report["findings_total"] == 1
    assert findings[0]["pattern"] == "github_classic_pat"
