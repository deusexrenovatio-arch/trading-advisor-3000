from __future__ import annotations

from trading_advisor_3000.dagster_defs import research_asset_specs
from trading_advisor_3000.dagster_defs import research_assets
from trading_advisor_3000.product_plane.research.datasets import research_dataset_store_contract
from trading_advisor_3000.product_plane.research.derived_indicators import research_derived_indicator_store_contract
from trading_advisor_3000.product_plane.research.ids import candidate_id
from trading_advisor_3000.product_plane.research.indicators import build_indicator_profile_registry, indicator_store_contract
from trading_advisor_3000.product_plane.research.strategies.families import phase_stg02_family_adapters


def test_research_dagster_asset_specs_declared() -> None:
    specs = {spec.key: spec for spec in research_asset_specs()}
    keys = set(specs)
    assert {
        "research_datasets",
        "research_instrument_tree",
        "research_bar_views",
        "research_indicator_frames",
        "research_derived_indicator_frames",
        "research_strategy_families",
        "research_strategy_templates",
        "research_strategy_template_modules",
        "research_strategy_instances",
        "research_strategy_instance_modules",
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
    assert set(specs["research_instrument_tree"].inputs) == {"research_datasets_delta"}
    assert set(specs["research_indicator_frames"].inputs) == {
        "research_datasets_delta",
        "research_bar_views_delta",
    }
    assert set(specs["research_derived_indicator_frames"].inputs) == {
        "research_datasets_delta",
        "research_bar_views_delta",
        "research_indicator_frames_delta",
    }
    assert set(specs["research_strategy_families"].inputs) == {"research_datasets_delta"}
    assert set(specs["research_strategy_templates"].inputs) == {"research_strategy_families_delta"}
    assert set(specs["research_strategy_template_modules"].inputs) == {"research_strategy_templates_delta"}
    assert set(specs["research_strategy_instances"].inputs) == {"research_strategy_template_modules_delta"}
    assert set(specs["research_strategy_instance_modules"].inputs) == {"research_strategy_instances_delta"}
    assert set(specs["research_backtest_batches"].inputs) == {
        "research_datasets_delta",
        "research_indicator_frames_delta",
        "research_derived_indicator_frames_delta",
        "research_strategy_instance_modules_delta",
    }
    assert set(specs["research_strategy_rankings"].inputs) == {
        "research_backtest_batches_delta",
        "research_strategy_stats_delta",
        "research_trade_records_delta",
    }
    assert set(specs["research_signal_candidates"].inputs) == {
        "research_datasets_delta",
        "research_derived_indicator_frames_delta",
        "research_strategy_rankings_delta",
    }


def test_research_contract_lineage_is_consistent_across_dataset_indicator_and_derived_layers() -> None:
    dataset_manifest = research_dataset_store_contract()
    indicator_manifest = indicator_store_contract()
    derived_manifest = research_derived_indicator_store_contract()

    assert {"research_datasets", "research_instrument_tree", "research_bar_views"} == set(dataset_manifest)
    assert "research_indicator_frames" in indicator_manifest
    assert "research_derived_indicator_frames" in derived_manifest
    indicator_columns = set(indicator_manifest["research_indicator_frames"]["columns"])
    derived_columns = set(derived_manifest["research_derived_indicator_frames"]["columns"])
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
        "derived_indicator_set_version",
        "profile_version",
        "source_bars_hash",
        "source_indicators_hash",
        "distance_to_ema_20_atr",
        "donchian_position_20",
        "divergence_price_rsi_14_score",
        "mtf_1h_to_15m_ema_20",
        "mtf_1h_to_15m_ema_50",
    } <= derived_columns
    assert {
        "volatility_regime_code",
        "oscillator_pressure_code",
        "breakout_ready_flag",
        "reversion_ready_flag",
        "atr_stop_ref_1x",
        "atr_target_ref_2x",
    }.isdisjoint(derived_columns)


def test_research_candidate_id_formula_is_stable() -> None:
    value = candidate_id(
        strategy_instance_id="sinst_trend_follow",
        contract_id="BR-6.26",
        timeframe="15m",
        ts_signal="2026-03-16T10:15:00Z",
    )
    assert value == candidate_id(
        strategy_instance_id="sinst_trend_follow",
        contract_id="BR-6.26",
        timeframe="15m",
        ts_signal="2026-03-16T10:15:00Z",
    )
    assert value.startswith("CAND-")


def test_research_default_strategy_space_follows_frozen_stg02_adapter_inventory() -> None:
    default_strategy_space = research_assets._default_strategy_space()
    assert tuple(default_strategy_space["family_keys"]) == tuple(
        adapter.family_manifest.family_key for adapter in phase_stg02_family_adapters()
    )
    assert default_strategy_space["materialize_instances"] is True

