from __future__ import annotations

from trading_advisor_3000.product_plane.research.strategies.adapter_contracts import StrategyFamilyAdapter

from .breakout import breakout_family_adapter, breakout_strategy_spec
from .channel_breakout_continuation import channel_breakout_continuation_family_adapter, channel_breakout_continuation_strategy_spec
from .divergence_reversal import divergence_reversal_family_adapter, divergence_reversal_strategy_spec
from .failed_breakout_reversal import failed_breakout_reversal_family_adapter, failed_breakout_reversal_strategy_spec
from .ma_cross import ma_cross_family_adapter, ma_cross_strategy_spec
from .mean_reversion import mean_reversion_family_adapter, mean_reversion_strategy_spec
from .mtf_pullback import mtf_pullback_family_adapter, mtf_pullback_strategy_spec
from .range_vwap_band_reversion import range_vwap_band_reversion_family_adapter, range_vwap_band_reversion_strategy_spec
from .squeeze_release import squeeze_release_family_adapter, squeeze_release_strategy_spec
from .trend_movement_cross import trend_movement_cross_family_adapter, trend_movement_cross_strategy_spec


def phase_stg02_family_adapters() -> tuple[StrategyFamilyAdapter, ...]:
    return (
        ma_cross_family_adapter(),
        breakout_family_adapter(),
        mean_reversion_family_adapter(),
        mtf_pullback_family_adapter(),
        squeeze_release_family_adapter(),
        trend_movement_cross_family_adapter(),
        channel_breakout_continuation_family_adapter(),
        range_vwap_band_reversion_family_adapter(),
        failed_breakout_reversal_family_adapter(),
        divergence_reversal_family_adapter(),
    )

__all__ = [
    "breakout_family_adapter",
    "breakout_strategy_spec",
    "channel_breakout_continuation_family_adapter",
    "channel_breakout_continuation_strategy_spec",
    "divergence_reversal_family_adapter",
    "divergence_reversal_strategy_spec",
    "failed_breakout_reversal_family_adapter",
    "failed_breakout_reversal_strategy_spec",
    "ma_cross_family_adapter",
    "ma_cross_strategy_spec",
    "mean_reversion_family_adapter",
    "mean_reversion_strategy_spec",
    "mtf_pullback_family_adapter",
    "mtf_pullback_strategy_spec",
    "phase_stg02_family_adapters",
    "range_vwap_band_reversion_family_adapter",
    "range_vwap_band_reversion_strategy_spec",
    "squeeze_release_family_adapter",
    "squeeze_release_strategy_spec",
    "trend_movement_cross_family_adapter",
    "trend_movement_cross_strategy_spec",
]
