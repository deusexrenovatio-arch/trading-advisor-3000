from __future__ import annotations

from trading_advisor_3000.product_plane.research.ids import candidate_id
from trading_advisor_3000.spark_jobs import (
    build_research_sql_plan,
    default_research_spec,
    spark_candidate_id_expr,
)


def test_research_spark_plan_reads_derived_indicator_source() -> None:
    spec = default_research_spec()
    sql = build_research_sql_plan(spec)
    assert "LEAST(1.0" in sql
    assert "CASE" in sql
    assert "scored_indicators" in sql
    assert spec.derived_indicator_source_table in sql
    assert spec.backtest_runs_source_table in sql
    assert "candidate_projection_id" in sql
    assert "strategy_family" in sql
    assert "capital_rub" in sql
    assert "reproducibility_fingerprint" in sql
    assert "gold_feature_snapshot" not in sql
    assert "scored_features" not in sql


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
