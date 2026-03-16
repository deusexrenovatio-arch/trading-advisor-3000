from __future__ import annotations

import hashlib
from collections import defaultdict

from trading_advisor_3000.app.contracts import CanonicalBar, Timeframe

from .snapshot import FeatureSnapshot


def _avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _ema(previous: float | None, *, value: float, span: int) -> float:
    alpha = 2.0 / (span + 1.0)
    if previous is None:
        return value
    return alpha * value + (1.0 - alpha) * previous


def _snapshot_id(
    *,
    contract_id: str,
    timeframe: Timeframe,
    ts: str,
    feature_set_version: str,
) -> str:
    raw = f"{contract_id}|{timeframe.value}|{ts}|{feature_set_version}"
    return "FS-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12].upper()


def build_feature_snapshots(
    bars: list[CanonicalBar],
    *,
    feature_set_version: str,
    instrument_by_contract: dict[str, str],
    lookback: int = 5,
) -> list[FeatureSnapshot]:
    if lookback <= 0:
        raise ValueError("lookback must be positive")

    grouped: dict[tuple[str, Timeframe], list[CanonicalBar]] = defaultdict(list)
    for bar in sorted(bars, key=lambda row: (row.contract_id, row.timeframe.value, row.ts)):
        grouped[(bar.contract_id, bar.timeframe)].append(bar)

    snapshots: list[FeatureSnapshot] = []
    for (contract_id, timeframe), series in grouped.items():
        ema_fast_prev: float | None = None
        ema_slow_prev: float | None = None
        instrument_id = instrument_by_contract.get(contract_id, series[0].instrument_id)

        for index, bar in enumerate(series):
            window = series[max(0, index - lookback + 1) : index + 1]
            atr = _avg([item.high - item.low for item in window])
            volume_avg = _avg([float(item.volume) for item in window])
            donchian_high = max(item.high for item in window)
            donchian_low = min(item.low for item in window)

            ema_fast_prev = _ema(ema_fast_prev, value=bar.close, span=3)
            ema_slow_prev = _ema(ema_slow_prev, value=bar.close, span=5)
            rvol = 0.0 if volume_avg == 0 else float(bar.volume) / volume_avg
            regime = "trend" if ema_fast_prev >= ema_slow_prev else "mean_revert"

            features_json: dict[str, float | str] = {
                "atr": atr,
                "ema_fast": ema_fast_prev,
                "ema_slow": ema_slow_prev,
                "donchian_high": donchian_high,
                "donchian_low": donchian_low,
                "rvol": rvol,
                "last_close": bar.close,
                "last_volume": float(bar.volume),
            }
            snapshots.append(
                FeatureSnapshot(
                    snapshot_id=_snapshot_id(
                        contract_id=contract_id,
                        timeframe=timeframe,
                        ts=bar.ts,
                        feature_set_version=feature_set_version,
                    ),
                    contract_id=contract_id,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    ts=bar.ts,
                    feature_set_version=feature_set_version,
                    regime=regime,
                    atr=atr,
                    ema_fast=ema_fast_prev,
                    ema_slow=ema_slow_prev,
                    donchian_high=donchian_high,
                    donchian_low=donchian_low,
                    rvol=rvol,
                    features_json=features_json,
                )
            )

    return sorted(snapshots, key=lambda item: (item.contract_id, item.timeframe.value, item.ts))
