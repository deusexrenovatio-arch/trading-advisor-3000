from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)
from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter
from trading_advisor_3000.product_plane.research.strategies.template_compiler import build_strategy_family_adapter


def ma_cross_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="ma-cross-v1",
        family="ma_cross",
        description="Trend-following moving-average crossover on materialized indicators and trend states.",
        required_columns=("close", "ema_10", "ema_20", "ema_50", "atr_14", "trend_state_fast_slow_code", "ma_stack_state_code"),
        parameter_grid=(
            StrategyParameter("fast_window", (10, 20)),
            StrategyParameter("slow_window", (20, 50)),
        ),
        signal_builder_key="ma_cross",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.5),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("total_return", "sharpe", "max_drawdown"),
            tags=("trend", "signals"),
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="ma_cross",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families.ma_cross.ma_cross_family_adapter"
        ),
        hypothesis_family="trend_following",
        strategy_spec=strategy_spec,
        template_key="ma_cross_core",
        template_title="MA Cross Core",
        regime_module_key="feature.trend_state_fast_slow_code",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def ma_cross_strategy_spec() -> StrategySpec:
    return ma_cross_family_adapter().strategy_spec
