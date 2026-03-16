from __future__ import annotations

from trading_advisor_3000.dagster_defs import phase2b_asset_specs
from trading_advisor_3000.spark_jobs import build_research_sql_plan


def test_phase2b_dagster_asset_specs_declared() -> None:
    keys = {spec.key for spec in phase2b_asset_specs()}
    assert keys == {"feature_snapshots", "research_backtest_runs", "research_signal_candidates"}


def test_phase2b_spark_sql_plan_contains_scoring_logic() -> None:
    sql = build_research_sql_plan()
    assert "LEAST(1.0" in sql
    assert "CASE" in sql
