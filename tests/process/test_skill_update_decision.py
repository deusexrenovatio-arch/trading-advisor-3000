from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from skill_update_decision import _build_decision, _parse_git_skill_operations  # noqa: E402


def test_parse_git_skill_operations_handles_add_remove_rename() -> None:
    payload = _parse_git_skill_operations(
        [
            "A\t.codex/skills/new-skill/SKILL.md",
            "D\t.codex/skills/old-skill/SKILL.md",
            "D\t.cursor/skills/legacy-skill/SKILL.md",
            "R100\t.codex/skills/rename-from/SKILL.md\t.codex/skills/rename-to/SKILL.md",
            "M\t.codex/skills/existing/SKILL.md",
        ]
    )
    assert payload["added"] == ["new-skill"]
    assert payload["removed"] == ["legacy-skill", "old-skill"]
    assert payload["updated"] == ["existing"]
    assert payload["renamed"] == [{"from": "rename-from", "to": "rename-to"}]


def test_build_decision_requires_catalog_for_runtime_change(monkeypatch) -> None:
    monkeypatch.setattr("skill_update_decision._detect_catalog_drift", lambda: False)
    decision = _build_decision(
        changed_files=[".codex/skills/product-data-quality/SKILL.md"],
        git_operations={"added": [], "removed": [], "updated": ["product-data-quality"], "renamed": []},
        metadata_drift={},
        routing_trigger_drift_skills=[],
        forbidden_non_baseline=[],
        strict=True,
    )
    assert decision["status"] == "update_required"
    assert "docs/agent/skills-catalog.md" in decision["missing_required_updates"]


def test_build_decision_requires_routing_doc_for_routing_metadata_drift(monkeypatch) -> None:
    monkeypatch.setattr("skill_update_decision._detect_catalog_drift", lambda: False)
    decision = _build_decision(
        changed_files=[".codex/skills/product-data-quality/SKILL.md", "docs/agent/skills-catalog.md"],
        git_operations={"added": [], "removed": [], "updated": ["product-data-quality"], "renamed": []},
        metadata_drift={"product-data-quality": {"routing_triggers": {"old": ["a"], "new": ["b"]}}},
        routing_trigger_drift_skills=["product-data-quality"],
        forbidden_non_baseline=[],
        strict=True,
    )
    assert decision["status"] == "update_required"
    assert "docs/agent/skills-routing.md" in decision["missing_required_updates"]


def test_build_decision_blocks_forbidden_class(monkeypatch) -> None:
    monkeypatch.setattr("skill_update_decision._detect_catalog_drift", lambda: False)
    decision = _build_decision(
        changed_files=[
            ".codex/skills/product-data-quality/SKILL.md",
            "docs/agent/skills-catalog.md",
            "docs/agent/skills-routing.md",
            "docs/workflows/skill-governance-sync.md",
        ],
        git_operations={"added": [], "removed": [], "updated": ["product-data-quality"], "renamed": []},
        metadata_drift={},
        routing_trigger_drift_skills=[],
        forbidden_non_baseline=["product-data-quality"],
        strict=True,
    )
    assert decision["status"] == "blocked"
    assert decision["forbidden_non_baseline_skills"] == ["product-data-quality"]
