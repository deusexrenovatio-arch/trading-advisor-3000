from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)
from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter
from trading_advisor_3000.product_plane.research.strategies.template_compiler import build_strategy_family_adapter


def mtf_pullback_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="mtf-pullback-v1",
        family="mtf_pullback",
        description="Higher-timeframe pullback continuation strategy using MTF alignment overlays.",
        required_columns=(
            "close",
            "atr_14",
            "ema_20",
            "ema_50",
            "distance_to_session_vwap",
            "mtf_1h_to_15m_ema_20",
            "mtf_1h_to_15m_ema_50",
            "mtf_1h_to_15m_rsi_14",
        ),
        parameter_grid=(
            StrategyParameter("pullback_depth", (0.25, 0.5, 0.75)),
            StrategyParameter("confirmation_bars", (1, 2, 3)),
            StrategyParameter("min_htf_rsi", (50, 55)),
        ),
        signal_builder_key="mtf_pullback",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.1, target_atr_multiple=2.2),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "calmar", "sharpe"),
            tags=("mtf", "signals"),
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="mtf_pullback",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families.mtf_pullback."
            "mtf_pullback_family_adapter"
        ),
        hypothesis_family="trend_continuation",
        strategy_spec=strategy_spec,
        template_key="mtf_pullback_core",
        template_title="MTF Pullback Core",
        regime_module_key="derived.mtf_ema_alignment",
        module_versions={"entry": "v2", "regime_filter": "v1", "risk_exit": "v1"},
    )


def mtf_pullback_strategy_spec() -> StrategySpec:
    return mtf_pullback_family_adapter().strategy_spec
