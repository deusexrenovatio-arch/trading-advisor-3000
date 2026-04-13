from __future__ import annotations

from .breakout import breakout_strategy_spec
from .ma_cross import ma_cross_strategy_spec
from .mean_reversion import mean_reversion_strategy_spec
from .mtf_pullback import mtf_pullback_strategy_spec
from .squeeze_release import squeeze_release_strategy_spec

__all__ = [
    "breakout_strategy_spec",
    "ma_cross_strategy_spec",
    "mean_reversion_strategy_spec",
    "mtf_pullback_strategy_spec",
    "squeeze_release_strategy_spec",
]
