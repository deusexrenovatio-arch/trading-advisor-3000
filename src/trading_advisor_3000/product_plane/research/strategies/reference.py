from __future__ import annotations

from collections.abc import Callable

from trading_advisor_3000.product_plane.contracts import TradeSide

from trading_advisor_3000.product_plane.research.features.snapshot import FeatureSnapshot


def _momentum_breakout(snapshot: FeatureSnapshot) -> TradeSide:
    if snapshot.ema_fast > snapshot.ema_slow and snapshot.rvol >= 1.0:
        return TradeSide.LONG
    if snapshot.ema_fast < snapshot.ema_slow and snapshot.rvol >= 1.0:
        return TradeSide.SHORT
    return TradeSide.FLAT


def _mean_reversion(snapshot: FeatureSnapshot) -> TradeSide:
    atr = snapshot.atr if snapshot.atr > 0 else 1.0
    midpoint = (snapshot.donchian_high + snapshot.donchian_low) / 2.0
    last_close_raw = snapshot.features_json.get("last_close", midpoint)
    if isinstance(last_close_raw, str):
        last_close = midpoint
    else:
        last_close = float(last_close_raw)
    z_score = (last_close - midpoint) / atr
    if z_score >= 0.8:
        return TradeSide.SHORT
    if z_score <= -0.8:
        return TradeSide.LONG
    return TradeSide.FLAT


def _breakout_volatility(snapshot: FeatureSnapshot) -> TradeSide:
    breakout_state = str(snapshot.features_json.get("breakout_state", "inside"))
    volume_pressure = float(snapshot.features_json.get("volume_pressure", snapshot.rvol))
    supertrend_direction = float(snapshot.features_json.get("supertrend_direction", 0.0))
    if breakout_state == "upper" and volume_pressure >= 1.0 and supertrend_direction >= 0.0:
        return TradeSide.LONG
    if breakout_state == "lower" and volume_pressure >= 1.0 and supertrend_direction <= 0.0:
        return TradeSide.SHORT
    return TradeSide.FLAT


_REFERENCE_STRATEGIES: dict[str, Callable[[FeatureSnapshot], TradeSide]] = {
    "breakout-volatility-v1": _breakout_volatility,
    "ma-cross-v1": _momentum_breakout,
    "mean-revert-v1": _mean_reversion,
    "mean-reversion-v1": _mean_reversion,
    "trend-follow-v1": _momentum_breakout,
}

_STRATEGY_FAMILY: dict[str, str] = {
    "breakout-volatility-v1": "breakout-volatility",
    "ma-cross-v1": "trend-following",
    "mean-revert-v1": "mean-reversion",
    "mean-reversion-v1": "mean-reversion",
    "trend-follow-v1": "trend-following",
}

_SAMPLE_STRATEGY_IDS: tuple[str, ...] = (
    "breakout-volatility-v1",
    "mean-revert-v1",
    "trend-follow-v1",
)


def supported_strategy_ids() -> tuple[str, ...]:
    return tuple(sorted(_REFERENCE_STRATEGIES))


def sample_strategy_ids() -> tuple[str, ...]:
    return _SAMPLE_STRATEGY_IDS


def strategy_family_of(strategy_version_id: str) -> str:
    family = _STRATEGY_FAMILY.get(strategy_version_id)
    if family is None:
        raise ValueError(f"unsupported strategy_version_id: {strategy_version_id}")
    return family


def evaluate_strategy(*, strategy_version_id: str, snapshot: FeatureSnapshot) -> TradeSide:
    strategy = _REFERENCE_STRATEGIES.get(strategy_version_id)
    if strategy is None:
        raise ValueError(f"unsupported strategy_version_id: {strategy_version_id}")
    return strategy(snapshot)
