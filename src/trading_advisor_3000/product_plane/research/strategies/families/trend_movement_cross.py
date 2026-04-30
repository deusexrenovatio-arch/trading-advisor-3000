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


def trend_movement_cross_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="trend-movement-cross-v1",
        family="trend_movement_cross_v1",
        description=(
            "Parametric trend continuation search using trend context, movement acceleration, "
            "and close/EMA cross recovery over materialized indicator and derived frames."
        ),
        required_columns=(
            "close",
            "ema_20",
            "ema_50",
            "macd_hist_12_26_9",
            "adx_14",
            "atr_14",
            "rsi_14",
            "close_change_1",
            "close_slope_20",
            "ema_20_slope_5",
            "roc_10_change_1",
            "mom_10_change_1",
            "cross_close_ema_20_code",
            "macd_signal_cross_code",
            "ppo_signal_cross_code",
            "distance_to_ema_20_atr",
        ),
        parameter_grid=(
            StrategyParameter("adx_min", (18, 22, 28), role="decision", timeframe="4h"),
            StrategyParameter("close_slope_min", (0.0, 0.0005, 0.001), role="trigger", timeframe="1h"),
            StrategyParameter("ema_slope_min", (0.0, 0.0005, 0.001), role="trigger", timeframe="1h"),
            StrategyParameter("roc_change_min", (0.0, 0.001, 0.003), role="trigger", timeframe="1h"),
            StrategyParameter("mom_change_min", (0.0, 0.001, 0.003), role="trigger", timeframe="1h"),
            StrategyParameter("rsi_min_long", (45, 50, 55), role="decision", timeframe="4h"),
            StrategyParameter("rsi_max_short", (45, 50, 55), role="decision", timeframe="4h"),
            StrategyParameter("require_cross_code", (False, True), role="trigger", timeframe="1h"),
            StrategyParameter("stop_atr_mult", (1.5, 2.0, 2.5), role="risk", timeframe="15m"),
            StrategyParameter("trail_atr_mult", (2.0, 2.5, 3.0), role="risk", timeframe="15m"),
            StrategyParameter("max_holding_bars", (24, 48, 96), role="risk", timeframe="15m"),
        ),
        parameter_constraints=("stop_atr_mult <= trail_atr_mult", "max_holding_bars >= 24"),
        signal_builder_key="trend_movement_cross",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=2.0, target_atr_multiple=2.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "profit_factor", "max_drawdown"),
            tags=("trend_up", "trend_down", "movement"),
        ),
        intent=(
            "Find continuation moves when native 4h trend/momentum context and daily trend regime agree, "
            "then execute on the next 15m bar after the trigger clock confirms movement."
        ),
        allowed_clock_profiles=("swing_4h_v1",),
        market_regimes=("trend_up", "trend_down", "movement_acceleration"),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("adx_14", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("ema_20", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("ema_50", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("ema_20", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("ema_50", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("macd_hist_12_26_9", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("adx_14", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
            StrategyIndicatorRequirement("rsi_14", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("close_change_1", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("close_slope_20", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("ema_20_slope_5", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("roc_10_change_1", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("mom_10_change_1", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("cross_close_ema_20_code", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("macd_signal_cross_code", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("ppo_signal_cross_code", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("distance_to_ema_20_atr", "trigger", "1h", "derived"),
        ),
        entry_logic=(
            "Use native daily EMA/ADX as the regime guard.",
            "Use native 4h EMA/ADX/RSI/MACD context as directional decision state.",
            "Use native 1h movement acceleration and cross-derived fields as trigger confirmation before 15m execution.",
        ),
        exit_logic=("Use ATR stop/target on the 15m execution index.",),
        verification_questions=(
            "Do regime and decision fields use native 1d/4h materialized columns rather than MTF indicator aliases?",
            "Are close-derived trigger signals shifted by one 15m execution bar?",
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="trend_movement_cross_v1",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families."
            "trend_movement_cross.trend_movement_cross_family_adapter"
        ),
        hypothesis_family="trend_continuation",
        strategy_spec=strategy_spec,
        template_key="trend_movement_cross",
        template_title="Trend Movement Cross",
        regime_module_key="derived.mtf_trend_context",
        signal_tf="4h",
        trigger_tf="1h",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def trend_movement_cross_strategy_spec() -> StrategySpec:
    return trend_movement_cross_family_adapter().strategy_spec
