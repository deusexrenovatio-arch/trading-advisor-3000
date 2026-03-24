from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_critical_contours_config_has_expected_pilot_shape() -> None:
    path = ROOT / "configs" / "critical_contours.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert payload["version"] == 1
    assert sorted(payload["solution_classes"]) == ["fallback", "staged", "target"]

    shortcut_ids = [item["id"] for item in payload["forbidden_shortcuts"]]
    assert shortcut_ids == [
        "synthetic-upstream-boundary",
        "scaffold-only-closure",
        "undeclared-runtime-substitution",
        "hidden-fallback-path",
        "sample-evidence-instead-of-contour-evidence",
    ]

    contours = {item["id"]: item for item in payload["contours"]}
    assert sorted(contours) == ["data-integration-closure", "runtime-publication-closure"]

    for contour in contours.values():
        assert contour["status"] == "pilot"
        assert contour["default_required_class"] == "target"
        assert contour["required_task_fields"] == [
            "Solution Class",
            "Critical Contour",
            "Forbidden Shortcuts",
            "Closure Evidence",
            "Shortcut Waiver",
        ]
        assert contour["forbidden_shortcut_markers"] == shortcut_ids
        assert sorted(contour["required_evidence_markers"]) == ["fallback", "staged", "target"]
        assert contour["allowed_staged_wording"]
        assert contour["reacceptance_trigger_markers"]
