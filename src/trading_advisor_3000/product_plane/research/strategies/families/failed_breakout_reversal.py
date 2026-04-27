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


def failed_breakout_reversal_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="failed-breakout-reversal-v1",
        family="failed_breakout_reversal_v1",
        description="Failed breakout reversal search after price returns inside a range or Donchian channel.",
        required_columns=(
            "close",
            "donchian_high_20",
            "donchian_low_20",
            "atr_14",
            "adx_14",
            "rsi_14",
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
        ),
        parameter_grid=(
            StrategyParameter("failure_window_bars", (2, 4, 8), role="trigger", timeframe="15m"),
            StrategyParameter(
                "reentry_confirm_mode",
                ("close_back_inside", "close_back_plus_momentum"),
                role="trigger",
                timeframe="15m",
            ),
            StrategyParameter("adx_max", (18, 22, 26), role="decision", timeframe="1h"),
            StrategyParameter("stop_buffer_atr", (0.3, 0.5, 1.0), role="risk", timeframe="15m"),
            StrategyParameter("target_mode", ("range_mid", "session_vwap", "opposite_band"), role="exit", timeframe="15m"),
            StrategyParameter("max_holding_bars", (8, 16, 24), role="risk", timeframe="15m"),
        ),
        signal_builder_key="failed_breakout_reversal",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("profit_factor", "calmar", "max_drawdown"),
            tags=("range_chop", "failed_breakout"),
        ),
        intent="Find false breakouts where price quickly returns inside the prior range/channel.",
        allowed_clock_profiles=("short_swing_1h_v1",),
        market_regimes=("range_chop", "mixed_unknown"),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("adx_14", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("rsi_14", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("donchian_high_20", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("donchian_low_20", "decision", "1h", "indicator"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
            StrategyIndicatorRequirement("cross_close_rolling_high_20_code", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("cross_close_rolling_low_20_code", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("rolling_position_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("donchian_position_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_rolling_high_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_rolling_low_20", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_donchian_high_20_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("distance_to_donchian_low_20_atr", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("close_change_1", "trigger", "15m", "derived"),
            StrategyIndicatorRequirement("close_slope_20", "trigger", "15m", "derived"),
        ),
        entry_logic=(
            "Use native 1h weak-trend/range context.",
            "Use native 15m breakout cross followed by return-inside and momentum confirmation.",
        ),
        exit_logic=("Use target mode plus ATR stop on the 15m execution index.",),
        verification_questions=(
            "Does the failure window use only already-closed 15m trigger bars?",
            "Does the family avoid higher-timeframe projection aliases?",
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="failed_breakout_reversal_v1",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families."
            "failed_breakout_reversal.failed_breakout_reversal_family_adapter"
        ),
        hypothesis_family="failed_breakout_reversal",
        strategy_spec=strategy_spec,
        template_key="failed_breakout_reversal",
        template_title="Failed Breakout Reversal",
        regime_module_key="native.range_failure_context",
        signal_tf="1h",
        trigger_tf="15m",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def failed_breakout_reversal_strategy_spec() -> StrategySpec:
    return failed_breakout_reversal_family_adapter().strategy_spec
