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
