from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from context_router import route_files  # noqa: E402


def test_contract_context_selected_for_plans_memory() -> None:
    result = route_files(["plans/PLANS.yaml", "memory/task_outcomes.yaml"], include_cold_paths=True)
    assert result["primary_context"] == "CTX-CONTRACTS"
    context_ids = [entry["id"] for entry in result["contexts"]]
    assert "CTX-CONTRACTS" in context_ids


def test_cold_context_stays_cold_by_default() -> None:
    result = route_files(["memory/task_outcomes.yaml"])
    assert "memory/task_outcomes.yaml" in result["cold_context_files"]
    assert result["contexts"] == []


def test_multi_context_change_is_marked_with_recommendation() -> None:
    result = route_files(["docs/architecture/README.md", "src/trading_advisor_3000/app.py"])
    notes = " ".join(result["recommendations"]).lower()
    assert "multiple contexts" in notes
