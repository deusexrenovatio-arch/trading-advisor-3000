from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


REQUIRED_HARNESS_FILES = (
    "scripts/measure_dev_loop.py",
    "scripts/harness_baseline_metrics.py",
    "scripts/build_governance_dashboard.py",
    "scripts/skill_update_decision.py",
    "scripts/skill_precommit_gate.py",
    "scripts/sync_architecture_map.py",
    "scripts/nightly_root_hygiene.py",
)


def test_required_harness_files_exist() -> None:
    for rel in REQUIRED_HARNESS_FILES:
        assert (ROOT / rel).exists(), f"missing harness file: {rel}"


def test_phase8_dashboard_lane_scripts_are_registered_in_mapping() -> None:
    mapping_text = (ROOT / "configs/change_surface_mapping.yaml").read_text(encoding="utf-8")
    assert "build_governance_dashboard.py" in mapping_text
    assert "harness_baseline_metrics.py" in mapping_text
    assert "measure_dev_loop.py" in mapping_text


def test_skill_update_decision_contract() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/skill_update_decision.py",
            "--changed-files",
            ".cursor/skills/architecture-review/SKILL.md",
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
    assert payload["status"] == "update_required"


def test_skill_precommit_gate_passes_when_docs_are_synced() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/skill_precommit_gate.py",
            "--changed-files",
            ".cursor/skills/architecture-review/SKILL.md",
            "docs/agent/skills-catalog.md",
            "docs/agent/skills-routing.md",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
