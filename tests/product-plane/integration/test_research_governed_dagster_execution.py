from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.dagster_defs import materialize_research_governed_assets
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "research" / "canonical_bars_sample.jsonl"


def _canonical_columns() -> dict[str, str]:
    return {
        "contract_id": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "ts": "timestamp",
        "open": "double",
        "high": "double",
        "low": "double",
        "close": "double",
        "volume": "bigint",
        "open_interest": "bigint",
    }


def _load_canonical_rows(path: Path) -> list[dict[str, object]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows


def test_research_governed_dagster_materialization_executes_end_to_end(tmp_path: Path) -> None:
    canonical_path = tmp_path / "canonical_bars.delta"
    output_dir = tmp_path / "research-governed"
    indicator_output_dir = tmp_path / "technical-indicators"

    write_delta_table_rows(
        table_path=canonical_path,
        rows=_load_canonical_rows(SOURCE_FIXTURE),
        columns=_canonical_columns(),
    )

    report = materialize_research_governed_assets(
        canonical_bars_path=canonical_path,
        output_dir=output_dir,
        indicator_output_dir=indicator_output_dir,
        strategy_ids=["trend-follow-v1", "mean-revert-v1", "breakout-volatility-v1"],
        feature_set_version="gold",
        canonical_timeframes=["5m", "15m", "1h"],
    )

    assert report["success"] is True
    assert set(report["materialized_assets"]) == {
        "technical_indicator_snapshot",
        "gold_feature_snapshot",
        "research_runtime_candidate_projection",
        "strategy_scorecard",
        "strategy_promotion_decision",
        "promoted_strategy_registry",
    }

    paths = {key: Path(str(value)) for key, value in dict(report["output_paths"]).items()}
    for key in report["materialized_assets"]:
        assert paths[key].exists()
        assert (paths[key] / "_delta_log").exists()
    assert paths["indicator_feature_qc_report"].exists()
    assert paths["technical_indicator_snapshot"].parent == indicator_output_dir.resolve()

    indicator_rows = read_delta_table_rows(paths["technical_indicator_snapshot"])
    feature_rows = read_delta_table_rows(paths["feature_snapshots"])
    projection_rows = read_delta_table_rows(paths["research_runtime_candidate_projection"])
    scorecard_rows = read_delta_table_rows(paths["strategy_scorecard"])
    decision_rows = read_delta_table_rows(paths["strategy_promotion_decision"])

    assert indicator_rows
    assert feature_rows
    assert projection_rows
    assert scorecard_rows
    assert decision_rows
    assert all("strategy_version_id" in row for row in scorecard_rows)
    assert all("verdict" in row for row in decision_rows)
    assert all("rsi_14" in row for row in indicator_rows)
    assert all(row["feature_set_version"] == "gold" for row in feature_rows)
    assert all("trend_score" in row for row in feature_rows)
    assert all("breakout_state" in row for row in feature_rows)
    assert all("trend_score" in str(row.get("features_json", "")) for row in feature_rows)
