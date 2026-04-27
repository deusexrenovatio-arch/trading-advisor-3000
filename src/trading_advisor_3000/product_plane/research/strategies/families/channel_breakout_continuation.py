from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter
from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyIndicatorRequirement,
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)
from trading_advisor_3000.product_plane.research.strategies.template_compiler import build_strategy_family_adapter


def channel_breakout_continuation_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="channel-breakout-continuation-v1",
        family="channel_breakout_continuation_v1",
        description=(
            "Parametric continuation search after Donchian or rolling-range breakout, "
            "filtered by higher-timeframe trend strength and movement confirmation."
        ),
        required_columns=(
            "close",
            "atr_14",
            "adx_14",
            "donchian_high_20",
            "donchian_low_20",
            "donchian_high_55",
            "donchian_low_55",
            "donchian_position_20",
            "donchian_position_55",
            "distance_to_donchian_high_20_atr",
            "distance_to_donchian_low_20_atr",
            "distance_to_donchian_high_55_atr",
            "distance_to_donchian_low_55_atr",
            "close_slope_20",
            "roc_10_change_1",
            "cross_close_rolling_high_20_code",
            "cross_close_rolling_low_20_code",
        ),
        parameter_grid=(
            StrategyParameter("channel_variant", ("20", "55"), role="decision", timeframe="4h"),
            StrategyParameter("adx_min", (18, 22, 28), role="decision", timeframe="4h"),
            StrategyParameter("breakout_confirm_bars", (1, 2), role="trigger", timeframe="1h"),
            StrategyParameter("slope_min", (0.0, 0.0005, 0.001), role="trigger", timeframe="1h"),
            StrategyParameter("max_distance_after_breakout_atr", (0.5, 1.0, 1.5), role="trigger", timeframe="1h"),
            StrategyParameter("stop_atr_mult", (1.5, 2.0, 2.5), role="risk", timeframe="15m"),
            StrategyParameter("trail_atr_mult", (2.0, 3.0, 4.0), role="risk", timeframe="15m"),
            StrategyParameter("max_holding_bars", (24, 48, 96), role="risk", timeframe="15m"),
        ),
        parameter_constraints=("stop_atr_mult <= trail_atr_mult", "max_holding_bars >= 24"),
        signal_builder_key="channel_breakout_continuation",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=2.0, target_atr_multiple=3.0),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "profit_factor", "max_drawdown"),
            tags=("trend_up", "trend_down", "breakout_release"),
        ),
        intent=(
            "Find continuation moves after a native 4h Donchian 20/55 breakout, "
            "only when native daily regime strength and the 4h decision clock agree."
        ),
        allowed_clock_profiles=("swing_4h_v1",),
        market_regimes=("trend_up", "trend_down", "breakout_release"),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
            StrategyIndicatorRequirement("adx_14", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("adx_14", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("donchian_high_20", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("donchian_low_20", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("donchian_high_55", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("donchian_low_55", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("donchian_position_20", "decision", "4h", "derived"),
            StrategyIndicatorRequirement("donchian_position_55", "decision", "4h", "derived"),
            StrategyIndicatorRequirement("distance_to_donchian_high_20_atr", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("distance_to_donchian_low_20_atr", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("distance_to_donchian_high_55_atr", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("distance_to_donchian_low_55_atr", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("close_slope_20", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("roc_10_change_1", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("cross_close_rolling_high_20_code", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("cross_close_rolling_low_20_code", "trigger", "1h", "derived"),
        ),
        entry_logic=(
            "Use native daily ADX as the regime guard.",
            "Use native 4h Donchian 20/55 channel, ADX, slope, and ROC as the decision surface.",
            "Use native 1h breakout confirmation before 15m execution.",
        ),
        exit_logic=("Use ATR stop/target on the 15m execution index.",),
        verification_questions=(
            "Do channel_variant=20 and channel_variant=55 both produce distinct parameter rows?",
            "Does the active grid use native 1d/4h/1h layers instead of MTF Donchian or trend aliases?",
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="channel_breakout_continuation_v1",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families."
            "channel_breakout_continuation.channel_breakout_continuation_family_adapter"
        ),
        hypothesis_family="channel_breakout_continuation",
        strategy_spec=strategy_spec,
        template_key="channel_breakout_continuation",
        template_title="Channel Breakout Continuation",
        regime_module_key="derived.channel_breakout_context",
        signal_tf="4h",
        trigger_tf="1h",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def channel_breakout_continuation_strategy_spec() -> StrategySpec:
    return channel_breakout_continuation_family_adapter().strategy_spec
