from __future__ import annotations

from trading_advisor_3000.app.research.ids import candidate_id
from trading_advisor_3000.app.research.features import phase2b_feature_store_contract
from trading_advisor_3000.dagster_defs import phase2b_asset_specs
from trading_advisor_3000.spark_jobs import (
    build_research_sql_plan,
    default_research_spec,
    spark_candidate_id_expr,
)


def test_phase2b_dagster_asset_specs_declared() -> None:
    specs = {spec.key: spec for spec in phase2b_asset_specs()}
    keys = set(specs)
    assert keys == {"feature_snapshots", "research_backtest_runs", "research_signal_candidates"}
    assert set(specs["research_signal_candidates"].inputs) == {
        "feature_snapshots_delta",
        "research_backtest_runs_delta",
    }


def test_phase2b_contract_lineage_is_consistent_across_manifest_and_spark() -> None:
    manifest = phase2b_feature_store_contract()
    candidate_columns = set(manifest["research_signal_candidates"]["columns"])
    required_columns = {
        "candidate_id",
        "backtest_run_id",
        "strategy_version_id",
        "contract_id",
        "timeframe",
        "ts_signal",
        "side",
        "entry_ref",
        "stop_ref",
        "target_ref",
        "score",
    }
    assert required_columns <= candidate_columns

    spec = default_research_spec()
    sql = build_research_sql_plan(spec)
    assert "LEAST(1.0" in sql
    assert "CASE" in sql
    assert spec.feature_source_table in sql
    assert spec.backtest_runs_source_table in sql
    assert "backtest_run_id" in sql
    assert "strategy_version_id" in sql
    assert "entry_ref" in sql
    assert "stop_ref" in sql
    assert "target_ref" in sql


def test_phase2b_spark_candidate_id_formula_uses_python_contract_seed() -> None:
    sql = build_research_sql_plan(default_research_spec())
    expected_expr = spark_candidate_id_expr(
        strategy_version_column="run.strategy_version_id",
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
