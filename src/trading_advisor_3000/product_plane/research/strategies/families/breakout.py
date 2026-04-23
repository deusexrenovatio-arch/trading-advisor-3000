from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
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
        description="Breakout continuation strategy over prebuilt range and breakout readiness features.",
        required_columns=(
            "close",
            "high",
            "low",
            "rolling_high_20",
            "rolling_low_20",
            "adx_14",
            "atr_14",
            "breakout_ready_state_code",
            "trend_state_fast_slow_code",
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
        regime_module_key="feature.breakout_ready_state_code",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def breakout_strategy_spec() -> StrategySpec:
    return breakout_family_adapter().strategy_spec
