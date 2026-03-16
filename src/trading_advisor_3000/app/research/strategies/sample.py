from __future__ import annotations

from collections.abc import Callable

from trading_advisor_3000.app.contracts import TradeSide

from trading_advisor_3000.app.research.features.snapshot import FeatureSnapshot


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


_SAMPLE_STRATEGIES: dict[str, Callable[[FeatureSnapshot], TradeSide]] = {
    "trend-follow-v1": _momentum_breakout,
    "mean-revert-v1": _mean_reversion,
}


def sample_strategy_ids() -> tuple[str, ...]:
    return tuple(sorted(_SAMPLE_STRATEGIES))


def evaluate_strategy(*, strategy_version_id: str, snapshot: FeatureSnapshot) -> TradeSide:
    strategy = _SAMPLE_STRATEGIES.get(strategy_version_id)
    if strategy is None:
        raise ValueError(f"unsupported strategy_version_id: {strategy_version_id}")
    return strategy(snapshot)
