from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.research import MissingResearchDependencyError, ensure_research_dependencies
from trading_advisor_3000.product_plane.research.backtests import BacktestBatchRequest, build_ephemeral_strategy_space
from trading_advisor_3000.product_plane.research.backtests.engine import strategy_spec_to_search_spec
from trading_advisor_3000.product_plane.research.datasets import (
    ContinuousFrontPolicy,
    HoldoutSplitConfig,
    ResearchDatasetManifest,
    ResearchDatasetPartitionKey,
    WalkForwardSplitConfig,
    build_holdout_window,
    build_walk_forward_windows,
)
from trading_advisor_3000.product_plane.research.dependencies import PANDAS_TA_REQUIREMENT, resolve_research_dependency
from trading_advisor_3000.product_plane.research.indicators import (
    build_indicator_profile_registry,
    indicator_column_name,
    core_v1_indicator_profile,
)
from trading_advisor_3000.product_plane.research.strategies import build_strategy_registry, default_strategy_catalog


ROOT = Path(__file__).resolve().parents[3]


def test_research_base_dependencies_declare_vectorized_stack() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]
    assert "vectorbt>=0.28.5,<0.29" in dependencies
    assert "pandas-ta-classic>=0.4.47,<0.5" in dependencies


def test_indicator_naming_normalizes_decimal_parameters() -> None:
    assert indicator_column_name("kc_upper", 20, 1.5) == "kc_upper_20_1_5"
    assert indicator_column_name("macd_signal", 12, 26, 9) == "macd_signal_12_26_9"


def test_vectorized_research_indicator_profile_covers_core_indicator_groups() -> None:
    profile = core_v1_indicator_profile()
    assert profile.version == "core_v1"
    grouped = profile.by_category()
    assert set(grouped) == {"momentum", "open_interest", "oscillator", "trend", "volatility", "volume"}

    all_columns = {column for spec in profile.indicators for column in spec.output_columns}
    assert {
        "ema_10",
        "rsi_14",
        "macd_12_26_9",
        "atr_14",
        "obv",
        "oi_change_1",
        "kc_upper_20_1_5",
    } <= all_columns

    registry = build_indicator_profile_registry()
    assert registry.versions() == ("core_v1", "core_intraday_v1", "core_swing_v1")


def test_vectorized_research_strategy_catalog_declares_required_families_and_modes() -> None:
    catalog = default_strategy_catalog()
    assert catalog.version == "research-strategy-catalog-v1"

    strategies = {spec.family: spec for spec in catalog.strategies}
    assert set(strategies) == {
        "breakout",
        "ma_cross",
        "mean_reversion",
        "trend_mtf_pullback_v1",
        "volatility_squeeze_release_v1",
        "trend_movement_cross_v1",
        "channel_breakout_continuation_v1",
        "range_vwap_band_reversion_v1",
        "failed_breakout_reversal_v1",
        "divergence_reversal_v1",
    }
    assert strategies["volatility_squeeze_release_v1"].execution_mode == "signals"
    assert strategies["trend_movement_cross_v1"].signal_builder_key == "trend_movement_cross"
    assert strategies["channel_breakout_continuation_v1"].signal_builder_key == "channel_breakout_continuation"
    assert strategies["trend_mtf_pullback_v1"].signal_builder_key == "trend_mtf_pullback"
    assert strategies["ma_cross"].execution_mode == "signals"
    assert not any(column.startswith("mtf_") for column in strategies["trend_mtf_pullback_v1"].required_columns)
    assert "mtf_1h_to_15m_ema_20" not in strategies["trend_movement_cross_v1"].required_columns
    assert "mtf_1d_to_15m_ema_20" not in strategies["trend_movement_cross_v1"].required_columns
    assert "mtf_1d_to_4h_ema_20" not in strategies["trend_movement_cross_v1"].required_columns
    assert strategies["trend_movement_cross_v1"].clock_profile is not None
    assert strategies["trend_movement_cross_v1"].clock_profile.regime_tf == "1d"
    assert strategies["trend_movement_cross_v1"].clock_profile.signal_tf == "4h"
    assert strategies["trend_movement_cross_v1"].clock_profile.trigger_tf == "1h"
    assert strategies["trend_movement_cross_v1"].clock_profile.execution_tf == "15m"
    assert "close_slope_20" in strategies["trend_movement_cross_v1"].required_columns
    assert "ema_20_slope_5" in strategies["trend_movement_cross_v1"].required_columns
    assert "macd_signal_cross_code" in strategies["trend_movement_cross_v1"].required_columns
    assert "ppo_signal_cross_code" in strategies["trend_movement_cross_v1"].required_columns
    assert "sma_20_slope_5" not in strategies["trend_movement_cross_v1"].required_columns
    assert "trix_signal_cross_code" not in strategies["trend_movement_cross_v1"].required_columns
    assert "kst_signal_cross_code" not in strategies["trend_movement_cross_v1"].required_columns
    assert "mtf_4h_to_15m_ema_20" not in strategies["channel_breakout_continuation_v1"].required_columns
    assert "mtf_4h_to_15m_donchian_high_55" not in strategies["channel_breakout_continuation_v1"].required_columns
    assert "mtf_1d_to_15m_ema_20" not in strategies["channel_breakout_continuation_v1"].required_columns
    assert "donchian_high_55" in strategies["channel_breakout_continuation_v1"].required_columns
    assert "mtf_1d_to_4h_ema_20" not in strategies["channel_breakout_continuation_v1"].required_columns
    assert set(strategies["channel_breakout_continuation_v1"].required_columns_for_role("decision")) >= {
        "adx_14",
        "donchian_high_20",
        "donchian_high_55",
        "donchian_position_55",
    }
    assert set(strategies["channel_breakout_continuation_v1"].required_columns_for_role("trigger")) >= {
        "close_slope_20",
        "roc_10_change_1",
        "cross_close_rolling_high_20_code",
    }
    assert set(strategies["trend_movement_cross_v1"].required_columns_for_role("decision")) >= {
        "ema_20",
        "adx_14",
        "rsi_14",
    }
    mean_reversion = strategies["mean_reversion"]
    assert mean_reversion.allowed_clock_profiles == ("intraday_15m_v1",)
    assert mean_reversion.required_columns_for_role("decision") == ("rsi_14",)
    assert set(mean_reversion.required_columns_for_timeframe("15m")) == {
        "close",
        "rsi_14",
        "distance_to_session_vwap",
        "atr_14",
    }
    channel_params = {
        parameter.name: parameter
        for parameter in strategies["channel_breakout_continuation_v1"].parameter_grid
    }
    assert channel_params["channel_variant"].values == ("20", "55")
    assert channel_params["channel_variant"].timeframe == "4h"
    assert channel_params["breakout_confirm_bars"].timeframe == "1h"
    assert channel_params["stop_atr_mult"].timeframe == "15m"
    assert "trail_atr_mult" in channel_params
    assert {"range_vwap_band_reversion_v1", "failed_breakout_reversal_v1", "divergence_reversal_v1"} <= set(strategies)

    registry = build_strategy_registry()
    assert registry.get("ma-cross-v1").family == "ma_cross"
    assert registry.strategy_versions() == tuple(spec.version for spec in catalog.strategies)


def test_strategy_family_search_specs_match_tz_indicator_and_derived_scope() -> None:
    expected = {
        "trend-mtf-pullback-v1": {
            "indicators": {"ema_20", "ema_50", "adx_14", "rsi_14", "atr_14"},
            "derived": {"close_slope_20", "ema_20_slope_5", "distance_to_ema_20_atr", "distance_to_ema_50_atr"},
        },
        "trend-movement-cross-v1": {
            "indicators": {"ema_20", "ema_50", "macd_hist_12_26_9", "adx_14", "atr_14", "rsi_14"},
            "derived": {
                "close_change_1",
                "close_slope_20",
                "ema_20_slope_5",
                "roc_10_change_1",
                "mom_10_change_1",
                "cross_close_ema_20_code",
                "macd_signal_cross_code",
                "ppo_signal_cross_code",
                "distance_to_ema_20_atr",
            },
        },
        "channel-breakout-continuation-v1": {
            "indicators": {
                "donchian_high_20",
                "donchian_low_20",
                "donchian_high_55",
                "donchian_low_55",
                "atr_14",
                "adx_14",
            },
            "derived": {
                "donchian_position_20",
                "donchian_position_55",
                "cross_close_rolling_high_20_code",
                "cross_close_rolling_low_20_code",
                "distance_to_donchian_high_20_atr",
                "distance_to_donchian_low_20_atr",
                "distance_to_donchian_high_55_atr",
                "distance_to_donchian_low_55_atr",
                "close_slope_20",
                "roc_10_change_1",
            },
        },
        "volatility-squeeze-release-v1": {
            "indicators": {
                "bb_width_20_2",
                "bb_percent_b_20_2",
                "kc_upper_20_1_5",
                "kc_lower_20_1_5",
                "atr_14",
                "natr_14",
                "adx_14",
            },
            "derived": {
                "bb_position_20_2",
                "kc_position_20_1_5",
                "distance_to_bb_upper_20_2_atr",
                "distance_to_bb_lower_20_2_atr",
                "distance_to_kc_upper_20_1_5_atr",
                "distance_to_kc_lower_20_1_5_atr",
                "cross_close_rolling_high_20_code",
                "cross_close_rolling_low_20_code",
                "close_slope_20",
                "roc_10_change_1",
                "rvol_20",
                "volume_zscore_20",
            },
        },
        "range-vwap-band-reversion-v1": {
            "indicators": {
                "rsi_14",
                "stoch_k_14_3_3",
                "stoch_d_14_3_3",
                "cci_20",
                "willr_14",
                "atr_14",
                "adx_14",
                "chop_14",
                "bb_upper_20_2",
                "bb_lower_20_2",
                "bb_mid_20_2",
                "kc_upper_20_1_5",
                "kc_lower_20_1_5",
            },
            "derived": {
                "session_vwap",
                "distance_to_session_vwap",
                "bb_position_20_2",
                "kc_position_20_1_5",
                "rolling_position_20",
                "session_position",
                "distance_to_bb_upper_20_2_atr",
                "distance_to_bb_lower_20_2_atr",
                "distance_to_kc_upper_20_1_5_atr",
                "distance_to_kc_lower_20_1_5_atr",
                "close_slope_20",
            },
        },
        "failed-breakout-reversal-v1": {
            "indicators": {"donchian_high_20", "donchian_low_20", "atr_14", "adx_14", "rsi_14"},
            "derived": {
                "cross_close_rolling_high_20_code",
                "cross_close_rolling_low_20_code",
                "rolling_position_20",
                "donchian_position_20",
                "distance_to_rolling_high_20",
                "distance_to_rolling_low_20",
                "distance_to_donchian_high_20_atr",
                "distance_to_donchian_low_20_atr",
                "close_change_1",
                "close_slope_20",
            },
        },
        "divergence-reversal-v1": {
            "indicators": {"rsi_14", "macd_hist_12_26_9", "ppo_hist_12_26_9", "tsi_25_13", "mfi_14", "cmf_20", "atr_14"},
            "derived": {
                "divergence_price_rsi_14_score",
                "divergence_price_stoch_k_14_3_3_score",
                "divergence_price_cci_20_score",
                "divergence_price_willr_14_score",
                "divergence_price_macd_hist_12_26_9_score",
                "divergence_price_ppo_hist_12_26_9_score",
                "divergence_price_tsi_25_13_score",
                "divergence_price_mfi_14_score",
                "divergence_price_cmf_20_score",
                "divergence_price_obv_score",
                "divergence_price_oi_change_1_score",
                "rolling_position_20",
                "bb_position_20_2",
                "close_slope_20",
            },
        },
    }
    registry = build_strategy_registry()
    for version, expected_scope in expected.items():
        spec = strategy_spec_to_search_spec(registry.get(version))
        assert set(spec.required_materialized_indicators) == expected_scope["indicators"]
        assert set(spec.required_materialized_derived) == expected_scope["derived"]
        assert not any(column.startswith("mtf_") for column in spec.required_materialized_derived)

    pullback = strategy_spec_to_search_spec(registry.get("trend-mtf-pullback-v1"))
    assert set(pullback.required_inputs_by_clock["regime"]["materialized_indicators"]) == {"ema_20", "ema_50", "adx_14", "rsi_14"}
    assert set(pullback.required_inputs_by_clock["signal"]["materialized_indicators"]) == {"ema_20", "ema_50", "adx_14", "rsi_14"}
    assert set(pullback.required_inputs_by_clock["trigger"]["materialized_indicators"]) == {"ema_20", "ema_50", "adx_14", "rsi_14"}


def test_dataset_and_backtest_keys_are_deterministic() -> None:
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-v1",
        universe_id="moex-futures",
        timeframes=("15m", "1h"),
        series_mode="continuous_front",
        split_method="walk_forward",
        continuous_front_policy=ContinuousFrontPolicy(),
    )
    assert manifest.lineage_key() == manifest.lineage_key()

    partition = ResearchDatasetPartitionKey(
        dataset_version="dataset-v1",
        timeframe="15m",
        instrument_id="BR",
        contract_id=None,
    )
    assert partition.partition_path().endswith("contract_id=continuous-front/timeframe=15m")

    strategy_space = build_ephemeral_strategy_space(
        strategy_registry=build_strategy_registry(),
        strategy_version_labels=("ma-cross-v1", "breakout-v1"),
        instances_per_strategy=24,
    )
    batch = BacktestBatchRequest(
        campaign_run_id="crun_foundation_test",
        strategy_space_id=strategy_space.strategy_space_id,
        dataset_version="dataset-v1",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        search_specs=strategy_space.search_specs,
        combination_count=sum(len(spec.parameter_space.get("rows", ())) or 1 for spec in strategy_space.search_specs),
    )
    assert batch.batch_id() == batch.batch_id()


def test_splitter_helpers_produce_predictable_windows() -> None:
    holdout = build_holdout_window(100, HoldoutSplitConfig(holdout_ratio=0.25))
    assert holdout.train_stop == 75
    assert holdout.test_stop == 100

    windows = build_walk_forward_windows(
        120,
        WalkForwardSplitConfig(train_size=40, test_size=20, step_size=20),
    )
    assert tuple(window.window_id for window in windows) == ("wf-01", "wf-02", "wf-03", "wf-04")


def test_required_dependency_error_names_base_environment_requirement(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import(name: str) -> object:
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(
        "trading_advisor_3000.product_plane.research.dependencies.import_module",
        fake_import,
    )

    with pytest.raises(MissingResearchDependencyError) as exc_info:
        resolve_research_dependency(PANDAS_TA_REQUIREMENT)

    message = str(exc_info.value)
    assert "mandatory research contour" in message
    assert "base environment" in message
    assert "pandas-ta-classic" in message


def test_research_dependencies_import_in_real_environment() -> None:
    resolved = ensure_research_dependencies()
    assert resolved["vectorbt"].import_name == "vectorbt"
    assert resolved["pandas-ta"].import_name == "pandas_ta_classic"


def test_research_namespace_import_remains_stable_with_mandatory_stack() -> None:
    env = {"PYTHONPATH": str(ROOT / "src")}
    snippet = r"""
import importlib
importlib.import_module("trading_advisor_3000.product_plane.research")
importlib.import_module("trading_advisor_3000.product_plane.research.strategies")
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=env,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
