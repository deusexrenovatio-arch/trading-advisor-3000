from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.product_plane.research.registry_store import (
    append_run_stats_index,
    read_registry_table,
    research_registry_root,
    write_strategy_note,
)


def test_research_registry_root_lives_under_research_namespace(tmp_path: Path) -> None:
    data_root = tmp_path / "trading-advisor-3000-nightly"
    canonical_root = data_root / "canonical" / "moex" / "baseline-4y-current"
    gold_root = data_root / "research" / "gold" / "current"

    assert research_registry_root(canonical_output_dir=canonical_root) == (
        data_root / "research" / "registry" / "current"
    ).resolve()
    assert research_registry_root(materialized_root=gold_root) == (
        data_root / "research" / "registry" / "current"
    ).resolve()


def test_registry_indices_are_logically_append_only_for_existing_keys(tmp_path: Path) -> None:
    registry_root = tmp_path / "research-registry"
    original = {
        "campaign_run_id": "crun_001",
        "backtest_run_id": "run_001",
        "strategy_instance_id": "sinst_001",
        "strategy_template_id": "stpl_001",
        "family_id": "sfam_001",
        "family_key": "ma_cross",
        "dataset_version": "dataset-v1",
        "instrument_id": "BR",
        "contract_id": "BR-6.26",
        "timeframe": "15m",
        "window_id": "wf-01",
        "total_return": 0.10,
        "sharpe": 1.1,
        "sortino": 1.2,
        "calmar": 1.3,
        "max_drawdown": 0.05,
        "profit_factor": 1.4,
        "win_rate": 0.55,
        "trade_count": 4,
        "turnover": 1000.0,
        "commission_total": 2.0,
        "slippage_total": 1.0,
        "status": "completed",
        "created_at": "2026-04-22T00:00:00Z",
    }
    duplicate = {**original, "total_return": 0.99}

    append_run_stats_index(registry_root=registry_root, rows=[original])
    append_run_stats_index(registry_root=registry_root, rows=[duplicate])

    rows = read_registry_table(registry_root=registry_root, table_name="research_run_stats_index")
    assert len(rows) == 1
    assert rows[0]["total_return"] == original["total_return"]


def test_registry_notes_append_without_rewriting_existing_note_ids(tmp_path: Path) -> None:
    registry_root = tmp_path / "research-registry"

    write_strategy_note(
        registry_root=registry_root,
        entity_type="campaign_run",
        entity_id="crun_001",
        note_kind="finding",
        summary="first finding",
        created_at="2026-04-22T00:00:00Z",
    )
    write_strategy_note(
        registry_root=registry_root,
        entity_type="campaign_run",
        entity_id="crun_001",
        note_kind="finding",
        summary="first finding",
        severity="critical",
        created_at="2026-04-22T00:00:00Z",
    )

    rows = read_registry_table(registry_root=registry_root, table_name="research_strategy_notes")
    assert len(rows) == 2
    assert {row["severity"] for row in rows} == {None, "critical"}
