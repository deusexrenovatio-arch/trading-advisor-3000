from __future__ import annotations

from trading_advisor_3000.product_plane.research.ids import candidate_id
from trading_advisor_3000.product_plane.research.features import research_feature_store_contract
from trading_advisor_3000.dagster_defs import research_governed_asset_specs
from trading_advisor_3000.spark_jobs import (
    build_research_sql_plan,
    default_research_spec,
    spark_candidate_id_expr,
)


def test_research_dagster_asset_specs_declared() -> None:
    specs = {spec.key: spec for spec in research_governed_asset_specs()}
    keys = set(specs)
    assert keys == {
        "technical_indicator_snapshot",
        "gold_feature_snapshot",
        "research_runtime_candidate_projection",
        "strategy_scorecard",
        "strategy_promotion_decision",
        "promoted_strategy_registry",
    }
    assert set(specs["technical_indicator_snapshot"].inputs) == {
        "canonical_bars_delta",
    }
    assert set(specs["gold_feature_snapshot"].inputs) == {
        "technical_indicator_snapshot_delta",
    }
    assert set(specs["research_runtime_candidate_projection"].inputs) == {
        "gold_feature_snapshot_delta",
    }


def test_research_contract_lineage_is_consistent_across_manifest_and_spark() -> None:
    manifest = research_feature_store_contract()
    indicator_columns = set(manifest["technical_indicator_snapshot"]["columns"])
    assert {
        "indicator_snapshot_id",
        "indicator_set_version",
        "rsi_14",
        "macd_hist",
        "adx_14",
        "rvol_20",
    } <= indicator_columns
    projection_columns = set(manifest["research_runtime_candidate_projection"]["columns"])
    required_columns = {
        "candidate_projection_id",
        "candidate_id",
        "backtest_run_id",
        "strategy_version_id",
        "strategy_family",
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts_signal",
        "side",
        "entry_ref",
        "stop_ref",
        "target_ref",
        "score",
        "window_id",
        "estimated_commission",
        "estimated_slippage",
        "capital_rub",
        "reproducibility_fingerprint",
    }
    assert required_columns <= projection_columns
    assert "strategy_scorecard" in manifest
    assert "strategy_promotion_decision" in manifest

    spec = default_research_spec()
    sql = build_research_sql_plan(spec)
    assert "LEAST(1.0" in sql
    assert "CASE" in sql
    assert spec.feature_source_table in sql
    assert spec.backtest_runs_source_table in sql
    assert "candidate_projection_id" in sql
    assert "strategy_family" in sql
    assert "capital_rub" in sql
    assert "reproducibility_fingerprint" in sql


def test_research_spark_candidate_id_formula_uses_python_contract_seed() -> None:
    sql = build_research_sql_plan(default_research_spec())
    expected_expr = spark_candidate_id_expr(
        version_id_column="run.strategy_version_id",
        contract_id_column="sf.contract_id",
        timeframe_column="sf.timeframe",
        ts_signal_column="sf.ts_signal",
    )
    assert f"{expected_expr} AS candidate_id" in sql
    assert "sha2(concat(contract_id, timeframe, ts" not in sql

    value = candidate_id(
        strategy_version_id="trend-follow-v1",
        contract_id="BR-6.26",
        timeframe="15m",
        ts_signal="2026-03-16T10:15:00Z",
    )
    assert value == "CAND-C82902612C14"
