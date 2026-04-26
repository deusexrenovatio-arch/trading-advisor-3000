from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.spec import (
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRiskPolicy,
    StrategySpec,
)
from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter
from trading_advisor_3000.product_plane.research.strategies.template_compiler import build_strategy_family_adapter


def squeeze_release_family_adapter() -> StrategyFamilyAdapter:
    strategy_spec = StrategySpec(
        version="squeeze-release-v1",
        family="squeeze_release",
        description="Event-driven volatility release strategy executed through vectorbt order callbacks.",
        required_columns=(
            "close",
            "atr_14",
            "ema_20",
            "ema_50",
            "bb_position_20_2",
            "kc_position_20_1_5",
            "cross_close_rolling_high_20_code",
            "cross_close_rolling_low_20_code",
        ),
        parameter_grid=(
            StrategyParameter("min_squeeze_bars", (3, 5, 8)),
            StrategyParameter("atr_target_multiple", (1.5, 2.0, 2.5)),
            StrategyParameter("release_confirmation", (1, 2)),
        ),
        signal_builder_key="squeeze_release",
        execution_mode="order_func",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(
            preferred_metrics=("profit_factor", "calmar", "max_drawdown"),
            tags=("volatility", "order-func"),
        ),
    )
    return build_strategy_family_adapter(
        adapter_key="squeeze_release",
        adapter_version="v1",
        source_ref=(
            "python_adapter:"
            "trading_advisor_3000.product_plane.research.strategies.families.squeeze_release."
            "squeeze_release_family_adapter"
        ),
        hypothesis_family="volatility_release",
        strategy_spec=strategy_spec,
        template_key="squeeze_release_core",
        template_title="Squeeze Release Core",
        regime_module_key="derived.volatility_channel_position",
        module_versions={"entry": "v1", "regime_filter": "v1", "risk_exit": "v1"},
    )


def squeeze_release_strategy_spec() -> StrategySpec:
    return squeeze_release_family_adapter().strategy_spec
