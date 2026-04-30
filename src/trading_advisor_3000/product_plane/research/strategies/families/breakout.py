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


def breakout_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="breakout-v1",
        family="breakout",
        description="Breakout continuation strategy over rolling price ranges and ADX strength.",
        required_columns=(
            "close",
            "high",
            "low",
            "adx_14",
            "atr_14",
        ),
        parameter_grid=(
            StrategyParameter("breakout_window", (20, 30, 55)),
            StrategyParameter("min_adx", (18, 22, 25)),
            StrategyParameter("entry_buffer_atr", (0.0, 0.25)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.25, target_atr_multiple=2.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "profit_factor", "max_drawdown"),
            tags=("breakout", "signals"),
        ),
        intent="Baseline 15m rolling-range breakout with local ADX and ATR controls.",
        allowed_clock_profiles=("intraday_15m_v1",),
        market_regimes=("breakout",),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("high", "trigger", "15m", "price"),
            StrategyIndicatorRequirement("low", "trigger", "15m", "price"),
            StrategyIndicatorRequirement("adx_14", "decision", "15m", "indicator"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
        ),
        entry_logic=("Enter when 15m close breaks the rolling range with ADX above threshold.",),
        exit_logic=("Use ATR stop/target on the 15m execution index.",),
        verification_questions=("Do breakout_window and entry_buffer_atr change the signal timing?",),
    )
    return build_strategy_family_adapter(
        adapter_key="breakout",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families.breakout.breakout_family_adapter"
        ),
        hypothesis_family="breakout_continuation",
        strategy_spec=strategy_spec,
        template_key="breakout_core",
        template_title="Breakout Core",
        regime_module_key="derived.rolling_range_breakout",
        signal_tf="15m",
        trigger_tf="15m",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def breakout_strategy_spec() -> StrategySpec:
    return breakout_family_adapter().strategy_spec
