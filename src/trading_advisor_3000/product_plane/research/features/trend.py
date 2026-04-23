from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TrendFeatureSpec:
    column_name: str
    description: str


def phase1_trend_specs() -> tuple[TrendFeatureSpec, ...]:
    return (
        TrendFeatureSpec("trend_state_code", "Primary trend direction state derived from stacked moving averages."),
        TrendFeatureSpec("trend_strength", "Continuous trend-strength score for ranking and gating."),
        TrendFeatureSpec("ma_stack_state_code", "Categorical encoding of the MA ordering state."),
        TrendFeatureSpec("htf_trend_state_code", "Higher-timeframe alignment flag projected into the trading frame."),
        TrendFeatureSpec("htf_rsi_14", "Higher-timeframe RSI value carried with point-in-time lag."),
    )

