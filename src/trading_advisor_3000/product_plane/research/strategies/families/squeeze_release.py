from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyIndicatorRequirement,
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)
from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter
from trading_advisor_3000.product_plane.research.strategies.template_compiler import build_strategy_family_adapter


def squeeze_release_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="volatility-squeeze-release-v1",
        family="volatility_squeeze_release_v1",
        description="Volatility compression to release continuation search executed through vectorbt signal surfaces.",
        required_columns=(
            "close",
            "bb_width_20_2",
            "bb_percent_b_20_2",
            "kc_upper_20_1_5",
            "kc_lower_20_1_5",
            "atr_14",
            "natr_14",
            "adx_14",
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
        ),
        parameter_grid=(
            StrategyParameter("bb_width_percentile_max", (10, 20, 30), role="decision", timeframe="1h"),
            StrategyParameter(
                "release_direction_mode",
                ("range_break", "band_break", "movement_confirmed"),
                role="trigger",
                timeframe="15m",
            ),
            StrategyParameter("volume_z_min", (None, 0.5, 1.0), role="trigger", timeframe="15m"),
            StrategyParameter("slope_min", (0.0, 0.0005, 0.001), role="trigger", timeframe="15m"),
            StrategyParameter("stop_atr_mult", (1.5, 2.0, 2.5), role="risk", timeframe="15m"),
            StrategyParameter("trail_atr_mult", (2.0, 3.0, 4.0), role="risk", timeframe="15m"),
            StrategyParameter("max_holding_bars", (24, 48, 96), role="risk", timeframe="15m"),
        ),
        parameter_constraints=("stop_atr_mult <= trail_atr_mult", "max_holding_bars >= 24"),
        optional_indicator_plan=(
            {
                "alias": "bbands_20_40_std_grid",
                "source": "vbt_builtin",
                "missing_policy": "compute_ephemeral_allowed",
                "params": {"window": [20, 40], "std": [1.5, 2.0, 2.5]},
            },
        ),
        signal_builder_key="squeeze_release",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.5, target_atr_multiple=3.0),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("profit_factor", "calmar", "max_drawdown"),
            tags=("volatility_squeeze", "breakout_release", "signals"),
        ),
        intent=(
            "Find volatility-compression releases from native band/channel state on the signal clock, "
            "then execute after the 15m trigger confirms breakout direction."
        ),
        allowed_clock_profiles=("short_swing_1h_v1",),
        market_regimes=("volatility_squeeze", "breakout_release"),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("bb_width_20_2", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("bb_percent_b_20_2", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("kc_upper_20_1_5", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("kc_lower_20_1_5", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
            StrategyIndicatorRequirement("natr_14", "risk", "15m", "indicator"),
            StrategyIndicatorRequirement("adx_14", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("bb_position_20_2", "decision", "1h", "derived"),
            StrategyIndicatorRequirement("kc_position_20_1_5", "decision", "1h", "derived"),
            StrategyIndicatorRequirement("distance_to_bb_upper_20_2_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_bb_lower_20_2_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_kc_upper_20_1_5_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_kc_lower_20_1_5_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("cross_close_rolling_high_20_code", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("cross_close_rolling_low_20_code", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("close_slope_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("roc_10_change_1", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("rvol_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("volume_zscore_20", "trigger", "15m", "derived"),
        ),
        entry_logic=(
            "Use native Bollinger/Keltner width and position on the signal clock to define compression context.",
            "Use 15m cross, slope, ROC, and volume confirmation for release entry.",
        ),
        exit_logic=("Use ATR stop/trailing-target parameters on the 15m execution index.",),
        verification_questions=(
            "Do squeeze decision fields use native BB/KC materialized columns, not mtf_4h_to_15m band aliases?",
            "Are 15m trigger crosses shifted by one execution bar before simulation?",
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="volatility_squeeze_release_v1",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families.squeeze_release."
            "squeeze_release_family_adapter"
        ),
        hypothesis_family="volatility_release",
        strategy_spec=strategy_spec,
        template_key="volatility_squeeze_release",
        template_title="Volatility Squeeze Release",
        regime_module_key="derived.volatility_channel_position",
        signal_tf="1h",
        trigger_tf="15m",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def squeeze_release_strategy_spec() -> StrategySpec:
    return squeeze_release_family_adapter().strategy_spec
