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
    result = route_files(
        [
            "docs/architecture/README.md",
            "src/trading_advisor_3000/product_plane/data_plane/pipeline.py",
        ]
    )
    notes = " ".join(result["recommendations"]).lower()
    assert "multiple contexts" in notes
    assert result["navigation_order"][0] == result["primary_context"]


def test_data_research_orchestration_api_contexts_are_resolved() -> None:
    result = route_files(
        [
            "src/trading_advisor_3000/product_plane/data_plane/pipeline.py",
            "src/trading_advisor_3000/product_plane/research/pipeline.py",
            "src/trading_advisor_3000/product_plane/runtime/pipeline.py",
            "src/trading_advisor_3000/product_plane/interfaces/api/runtime_api.py",
        ]
    )
    context_ids = [entry["id"] for entry in result["contexts"]]
    assert "CTX-DATA" in context_ids
    assert "CTX-RESEARCH" in context_ids
    assert "CTX-ORCHESTRATION" in context_ids
    assert "CTX-API-UI" in context_ids


def test_product_data_context_is_not_swallowed_by_orchestration() -> None:
    result = route_files(["src/trading_advisor_3000/product_plane/data_plane/pipeline.py"])
    context_ids = [entry["id"] for entry in result["contexts"]]
    assert result["primary_context"] == "CTX-DATA"
    assert "CTX-DATA" in context_ids
    assert "CTX-ORCHESTRATION" not in context_ids


def test_file_prefix_patterns_route_owned_tests() -> None:
    result = route_files(["tests/product-plane/unit/test_delta_runtime.py"])
    assert result["primary_context"] == "CTX-DATA"
    assert result["unmapped_files"] == []


def test_ops_bookkeeping_does_not_override_product_navigation() -> None:
    result = route_files(
        [
            "docs/session_handoff.md",
            "docs/tasks/active/TASK-2026-04-26-example.md",
            "src/trading_advisor_3000/product_plane/data_plane/pipeline.py",
        ]
    )
    assert result["primary_context"] == "CTX-DATA"
    assert result["navigation_order"][0] == "CTX-DATA"


def test_context_entries_include_search_seeds() -> None:
    result = route_files(["src/trading_advisor_3000/product_plane/research/pipeline.py"])
    research = next(entry for entry in result["contexts"] if entry["id"] == "CTX-RESEARCH")
    assert "materialization" in research["facets"]
    assert "src/trading_advisor_3000/product_plane/research/" in research["search_seeds"]


def test_generated_artifacts_stay_cold_by_default() -> None:
    result = route_files(["artifacts/codex/run/stdout.log"])
    assert "artifacts/codex/run/stdout.log" in result["cold_context_files"]
    assert result["contexts"] == []


def test_cold_only_changes_ignore_implicit_handoff_intent() -> None:
    result = route_files(
        ["artifacts/codex/run/stdout.log", ".serena/project.yml"],
        session_handoff_text="domain runtime data",
    )
    assert result["primary_context"] is None
    assert result["contexts"] == []
