from __future__ import annotations

from trading_advisor_3000.dagster_defs import phase2b_asset_specs
from trading_advisor_3000.product_plane.research.datasets import phase2_research_dataset_store_contract
from trading_advisor_3000.product_plane.research.features import phase2b_feature_store_contract
from trading_advisor_3000.product_plane.research.ids import candidate_id
from trading_advisor_3000.product_plane.research.indicators import build_indicator_profile_registry, phase3_indicator_store_contract


def test_phase2b_dagster_asset_specs_declared() -> None:
    specs = {spec.key: spec for spec in phase2b_asset_specs()}
    keys = set(specs)
    assert {
        "research_datasets",
        "research_bar_views",
        "research_indicator_frames",
        "research_feature_frames",
        "research_backtest_batches",
        "research_backtest_runs",
        "research_strategy_stats",
        "research_trade_records",
        "research_order_records",
        "research_drawdown_records",
        "research_strategy_rankings",
        "research_signal_candidates",
    } == keys
    assert set(specs["research_datasets"].inputs) == {
        "canonical_bars_delta",
        "canonical_session_calendar_delta",
        "canonical_roll_map_delta",
    }
    assert set(specs["research_indicator_frames"].inputs) == {
        "research_datasets_delta",
        "research_bar_views_delta",
    }
    assert set(specs["research_feature_frames"].inputs) == {
        "research_datasets_delta",
        "research_bar_views_delta",
        "research_indicator_frames_delta",
    }
    assert set(specs["research_backtest_batches"].inputs) == {
        "research_datasets_delta",
        "research_indicator_frames_delta",
        "research_feature_frames_delta",
    }
    assert set(specs["research_strategy_rankings"].inputs) == {
        "research_backtest_batches_delta",
        "research_strategy_stats_delta",
        "research_trade_records_delta",
    }
    assert set(specs["research_signal_candidates"].inputs) == {
        "research_datasets_delta",
        "research_feature_frames_delta",
        "research_strategy_rankings_delta",
    }


def test_phase2b_contract_lineage_is_consistent_across_dataset_indicator_and_feature_layers() -> None:
    dataset_manifest = phase2_research_dataset_store_contract()
    indicator_manifest = phase3_indicator_store_contract()
    feature_manifest = phase2b_feature_store_contract()

    assert {"research_datasets", "research_bar_views"} == set(dataset_manifest)
    assert "research_indicator_frames" in indicator_manifest
    assert "research_feature_frames" in feature_manifest
    indicator_columns = set(indicator_manifest["research_indicator_frames"]["columns"])
    feature_columns = set(feature_manifest["research_feature_frames"]["columns"])
    assert {
        "dataset_version",
        "indicator_set_version",
        "profile_version",
        "source_bars_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
    } <= indicator_columns

    registry = build_indicator_profile_registry()
    assert registry.versions() == ("core_v1", "core_intraday_v1", "core_swing_v1")
    assert set(registry.get("core_v1").expected_output_columns()) <= indicator_columns
    assert {
        "dataset_version",
        "indicator_set_version",
        "feature_set_version",
        "profile_version",
        "source_bars_hash",
        "source_indicators_hash",
        "breakout_ready_flag",
        "reversion_ready_flag",
        "atr_stop_ref_1x",
        "atr_target_ref_2x",
    } <= feature_columns
    assert "research_strategy_metrics" in feature_manifest


def test_phase2b_candidate_id_formula_is_stable() -> None:
    value = candidate_id(
        strategy_version_id="trend-follow-v1",
        contract_id="BR-6.26",
        timeframe="15m",
        ts_signal="2026-03-16T10:15:00Z",
    )
    assert value == "CAND-C82902612C14"
