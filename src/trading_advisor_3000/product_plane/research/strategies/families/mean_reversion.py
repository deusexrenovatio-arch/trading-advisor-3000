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


def mean_reversion_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="mean-reversion-v1",
        family="mean_reversion",
        description="Mean-reversion strategy around session VWAP and RSI extremes.",
        required_columns=("close", "rsi_14", "atr_14", "distance_to_session_vwap"),
        parameter_grid=(
            StrategyParameter("entry_rsi", (20, 25, 30)),
            StrategyParameter("exit_rsi", (45, 50, 55)),
            StrategyParameter("entry_distance_atr", (0.5, 0.75, 1.0)),
        ),
        signal_builder_key="mean_reversion",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=0.9, target_atr_multiple=1.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("sortino", "profit_factor", "win_rate"),
            tags=("reversion", "signals"),
        ),
        intent="Baseline 15m mean-reversion around session VWAP using local RSI extremes.",
        allowed_clock_profiles=("intraday_15m_v1",),
        market_regimes=("reversion", "range"),
        indicator_requirements=(
            StrategyIndicatorRequirement("close", "entry", "15m", "price"),
            StrategyIndicatorRequirement("rsi_14", "decision", "15m", "indicator"),
            StrategyIndicatorRequirement("distance_to_session_vwap", "entry", "15m", "derived"),
            StrategyIndicatorRequirement("atr_14", "risk", "15m", "indicator"),
        ),
        entry_logic=("Enter when 15m RSI is extreme and price is stretched from session VWAP.",),
        exit_logic=("Exit on RSI normalization or ATR stop/target.",),
        verification_questions=("Do RSI and VWAP distance thresholds change the reversion entries?",),
    )
    return build_strategy_family_adapter(
        adapter_key="mean_reversion",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families.mean_reversion."
            "mean_reversion_family_adapter"
        ),
        hypothesis_family="mean_reversion",
        strategy_spec=strategy_spec,
        template_key="mean_reversion_core",
        template_title="Mean Reversion Core",
        regime_module_key="derived.session_vwap_distance",
        signal_tf="15m",
        trigger_tf="15m",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def mean_reversion_strategy_spec() -> StrategySpec:
    return mean_reversion_family_adapter().strategy_spec
