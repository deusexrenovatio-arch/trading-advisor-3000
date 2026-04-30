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


def mtf_pullback_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="trend-mtf-pullback-v1",
        family="trend_mtf_pullback_v1",
        description=(
            "Native-clock trend pullback continuation search: daily regime, 4h trend decision, "
            "1h pullback/reclaim trigger, and 15m execution."
        ),
        required_columns=(
            "open",
            "high",
            "low",
            "close",
            "ema_20",
            "ema_50",
            "adx_14",
            "rsi_14",
            "atr_14",
            "close_slope_20",
            "ema_20_slope_5",
            "distance_to_ema_20_atr",
            "distance_to_ema_50_atr",
        ),
        parameter_grid=(
            StrategyParameter("adx_min", (18, 22, 28), role="decision", timeframe="4h"),
            StrategyParameter("slope_min", (0.0, 0.0005, 0.001), role="trigger", timeframe="1h"),
            StrategyParameter("pullback_atr_min", (0.0, 0.3), role="trigger", timeframe="1h"),
            StrategyParameter("pullback_atr_max", (0.8, 1.2, 1.8), role="trigger", timeframe="1h"),
            StrategyParameter("rsi_reclaim_long", (45, 50, 55), role="trigger", timeframe="1h"),
            StrategyParameter("rsi_reclaim_short", (45, 50, 55), role="trigger", timeframe="1h"),
            StrategyParameter("stop_atr_mult", (1.5, 2.0, 2.5), role="risk", timeframe="15m"),
            StrategyParameter("trail_atr_mult", (2.0, 2.5, 3.0), role="risk", timeframe="15m"),
            StrategyParameter("max_holding_bars", (24, 48, 96), role="risk", timeframe="15m"),
        ),
        parameter_constraints=(
            "pullback_atr_min < pullback_atr_max",
            "stop_atr_mult <= trail_atr_mult",
        ),
        signal_builder_key="trend_mtf_pullback",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.5, target_atr_multiple=2.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "calmar", "profit_factor"),
            tags=("trend_up", "trend_down", "pullback"),
        ),
        intent=(
            "Find continuation trades after a pullback inside a higher-timeframe trend, "
            "confirmed by movement recovery on the trigger clock."
        ),
        allowed_clock_profiles=("swing_4h_v1",),
        market_regimes=("trend_up", "trend_down", "pullback"),
        indicator_requirements=(
            StrategyIndicatorRequirement("open", "entry", "15m", "price"),
            StrategyIndicatorRequirement("high", "entry", "15m", "price"),
            StrategyIndicatorRequirement("low", "entry", "15m", "price"),
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("ema_20", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("ema_50", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("adx_14", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("rsi_14", "regime", "1d", "indicator"),
            StrategyIndicatorRequirement("ema_20", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("ema_50", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("adx_14", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("rsi_14", "decision", "4h", "indicator"),
            StrategyIndicatorRequirement("ema_20", "trigger", "1h", "indicator"),
            StrategyIndicatorRequirement("ema_50", "trigger", "1h", "indicator"),
            StrategyIndicatorRequirement("adx_14", "trigger", "1h", "indicator"),
            StrategyIndicatorRequirement("rsi_14", "trigger", "1h", "indicator"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
            StrategyIndicatorRequirement("close_slope_20", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("ema_20_slope_5", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("distance_to_ema_20_atr", "trigger", "1h", "derived"),
            StrategyIndicatorRequirement("distance_to_ema_50_atr", "trigger", "1h", "derived"),
        ),
        entry_logic=(
            "Use native daily EMA/ADX/RSI as the regime guard.",
            "Use native 4h EMA/ADX/RSI as trend continuation decision context.",
            "Use native 1h EMA-distance pullback and slope reclaim as the trigger before 15m execution.",
        ),
        exit_logic=("Use ATR stop/trailing-target parameters on the 15m execution index.",),
        verification_questions=(
            "Does the strategy avoid mtf_* aliases and consume native 1d/4h/1h/15m frames instead?",
            "Do pullback_atr_min and pullback_atr_max gate trigger distance before vectorbt execution?",
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="trend_mtf_pullback_v1",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families.mtf_pullback."
            "mtf_pullback_family_adapter"
        ),
        hypothesis_family="trend_continuation",
        strategy_spec=strategy_spec,
        template_key="trend_mtf_pullback",
        template_title="Trend MTF Pullback",
        regime_module_key="native.trend_regime_context",
        signal_tf="4h",
        trigger_tf="1h",
        module_versions={"entry": "v2", "regime_filter": "v1", "risk_exit": "v1"},
    )


def mtf_pullback_strategy_spec() -> StrategySpec:
    return mtf_pullback_family_adapter().strategy_spec
