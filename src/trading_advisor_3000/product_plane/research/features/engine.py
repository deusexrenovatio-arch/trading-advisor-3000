from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
import hashlib
from io import StringIO
import math
from typing import TypeVar

import pandas as pd
import pandas_ta_classic as ta

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe

from .snapshot import FeatureSnapshot, TechnicalIndicatorSnapshot

T = TypeVar("T")

FEATURE_LAYER_TIMEFRAMES: tuple[Timeframe, ...] = (
    Timeframe.M5,
    Timeframe.M15,
    Timeframe.H1,
    Timeframe.H4,
    Timeframe.D1,
    Timeframe.W1,
)

RESEARCH_MEDIUM_TIMEFRAMES: tuple[Timeframe, ...] = (
    Timeframe.M15,
    Timeframe.H1,
    Timeframe.H4,
    Timeframe.D1,
    Timeframe.W1,
)


def medium_term_timeframes() -> tuple[Timeframe, ...]:
    return RESEARCH_MEDIUM_TIMEFRAMES


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _snapshot_id(
    *,
    contract_id: str,
    timeframe: Timeframe,
    ts: str,
    feature_set_version: str,
) -> str:
    raw = f"{contract_id}|{timeframe.value}|{ts}|{feature_set_version}"
    return "FS-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12].upper()


def _indicator_snapshot_id(
    *,
    contract_id: str,
    timeframe: Timeframe,
    ts: str,
    indicator_set_version: str,
) -> str:
    raw = f"{contract_id}|{timeframe.value}|{ts}|{indicator_set_version}"
    return "IND-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12].upper()


def _bar_fingerprint(bar: CanonicalBar) -> str:
    raw = (
        f"{bar.contract_id}|{bar.instrument_id}|{bar.timeframe.value}|{bar.ts}|"
        f"{bar.open}|{bar.high}|{bar.low}|{bar.close}|{bar.volume}|{bar.open_interest}"
    )
    return "BAR-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16].upper()


def _safe_float(value: object, *, default: float = 0.0) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(normalized) or math.isinf(normalized):
        return default
    return normalized


def _quiet_indicator_call(func: Callable[..., T], *args: object, **kwargs: object) -> T:
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        return func(*args, **kwargs)


def _series_or_default(result: object, *, index: pd.Index, default: float) -> pd.Series:
    if isinstance(result, pd.Series):
        return result.reindex(index).fillna(default)
    if isinstance(result, pd.DataFrame):
        if len(result.columns) == 1:
            return result.iloc[:, 0].reindex(index).fillna(default)
        numeric_columns = list(result.select_dtypes(include=["number"]).columns)
        if numeric_columns:
            return result[numeric_columns[0]].reindex(index).fillna(default)
    return pd.Series([default] * len(index), index=index, dtype="float64")


def _series_or_fallback(result: object, *, index: pd.Index, fallback: pd.Series) -> pd.Series:
    fallback = fallback.reindex(index)
    if isinstance(result, pd.Series):
        return result.reindex(index).combine_first(fallback)
    if isinstance(result, pd.DataFrame):
        if len(result.columns) == 1:
            return result.iloc[:, 0].reindex(index).combine_first(fallback)
        numeric_columns = list(result.select_dtypes(include=["number"]).columns)
        if numeric_columns:
            return result[numeric_columns[0]].reindex(index).combine_first(fallback)
    return fallback


def _indicator_column(
    result: object,
    *,
    index: pd.Index,
    prefixes: tuple[str, ...],
    default: float,
) -> pd.Series:
    if not isinstance(result, pd.DataFrame):
        return _series_or_default(result, index=index, default=default)
    for prefix in prefixes:
        column = next((name for name in result.columns if str(name).startswith(prefix)), None)
        if column is not None:
            return result[column].reindex(index).fillna(default)
    return pd.Series([default] * len(index), index=index, dtype="float64")


def _base_frame(series: list[CanonicalBar]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts": [bar.ts for bar in series],
            "open": [bar.open for bar in series],
            "high": [bar.high for bar in series],
            "low": [bar.low for bar in series],
            "close": [bar.close for bar in series],
            "volume": [float(bar.volume) for bar in series],
        }
    )


def _indicator_frame(series: list[CanonicalBar]) -> pd.DataFrame:
    frame = _base_frame(series)
    if frame.empty:
        return frame

    high = frame["high"]
    low = frame["low"]
    close = frame["close"]
    volume = frame["volume"]
    fallback_range = (high - low).abs()
    safe_close = close.where(close.abs() > 0, 1.0)
    ta_call = _quiet_indicator_call

    atr_fallback = fallback_range.rolling(14, min_periods=1).mean()
    frame["atr_14"] = _series_or_fallback(
        ta_call(ta.atr, high, low, close, length=14),
        index=frame.index,
        fallback=atr_fallback,
    )
    frame["atr_14"] = frame["atr_14"].where(frame["atr_14"] >= 0, atr_fallback).fillna(atr_fallback)
    frame["natr_14"] = ((frame["atr_14"] / safe_close.abs()) * 100.0).fillna(0.0)
    frame["ema_12"] = _series_or_fallback(
        ta_call(ta.ema, close, length=12),
        index=frame.index,
        fallback=close.ewm(span=12, adjust=False).mean(),
    )
    frame["ema_26"] = _series_or_fallback(
        ta_call(ta.ema, close, length=26),
        index=frame.index,
        fallback=close.ewm(span=26, adjust=False).mean(),
    )
    frame["sma_20"] = _series_or_fallback(
        ta_call(ta.sma, close, length=20),
        index=frame.index,
        fallback=close.rolling(20, min_periods=1).mean(),
    )
    frame["sma_50"] = _series_or_fallback(
        ta_call(ta.sma, close, length=50),
        index=frame.index,
        fallback=close.rolling(50, min_periods=1).mean(),
    )
    frame["rsi_14"] = _series_or_default(ta_call(ta.rsi, close, length=14), index=frame.index, default=50.0)

    macd = ta_call(ta.macd, close, fast=12, slow=26, signal=9)
    frame["macd"] = _indicator_column(macd, index=frame.index, prefixes=("MACD_",), default=0.0)
    frame["macd_hist"] = _indicator_column(macd, index=frame.index, prefixes=("MACDh_",), default=0.0)
    frame["macd_signal"] = _indicator_column(macd, index=frame.index, prefixes=("MACDs_",), default=0.0)

    bbands = ta_call(ta.bbands, close, length=20, std=2)
    bb_mid_fallback = close.rolling(20, min_periods=1).mean()
    bb_std_fallback = close.rolling(20, min_periods=1).std().fillna(0.0)
    bb_lower_fallback = bb_mid_fallback - (2.0 * bb_std_fallback)
    bb_upper_fallback = bb_mid_fallback + (2.0 * bb_std_fallback)
    frame["bb_lower"] = _indicator_column(
        bbands,
        index=frame.index,
        prefixes=("BBL_",),
        default=float("nan"),
    ).combine_first(bb_lower_fallback)
    frame["bb_mid"] = _indicator_column(
        bbands,
        index=frame.index,
        prefixes=("BBM_",),
        default=float("nan"),
    ).combine_first(bb_mid_fallback)
    frame["bb_upper"] = _indicator_column(
        bbands,
        index=frame.index,
        prefixes=("BBU_",),
        default=float("nan"),
    ).combine_first(bb_upper_fallback)
    frame["bb_width"] = _indicator_column(
        bbands,
        index=frame.index,
        prefixes=("BBB_",),
        default=float("nan"),
    ).combine_first(((frame["bb_upper"] - frame["bb_lower"]) / safe_close.abs()) * 100.0).clip(lower=0.0)
    frame["bb_percent_b"] = _indicator_column(
        bbands,
        index=frame.index,
        prefixes=("BBP_",),
        default=float("nan"),
    ).combine_first((close - frame["bb_lower"]) / (frame["bb_upper"] - frame["bb_lower"]).replace(0, math.nan)).fillna(0.5)

    adx = ta_call(ta.adx, high, low, close, length=14)
    frame["adx_14"] = _indicator_column(adx, index=frame.index, prefixes=("ADX_",), default=0.0)
    frame["dmp_14"] = _indicator_column(adx, index=frame.index, prefixes=("DMP_",), default=0.0)
    frame["dmn_14"] = _indicator_column(adx, index=frame.index, prefixes=("DMN_",), default=0.0)

    stoch = ta_call(ta.stoch, high, low, close, k=14, d=3, smooth_k=3)
    frame["stoch_k"] = _indicator_column(stoch, index=frame.index, prefixes=("STOCHk_",), default=50.0)
    frame["stoch_d"] = _indicator_column(stoch, index=frame.index, prefixes=("STOCHd_",), default=50.0)
    frame["cci_20"] = _series_or_default(ta_call(ta.cci, high, low, close, length=20), index=frame.index, default=0.0)
    frame["willr_14"] = _series_or_default(ta_call(ta.willr, high, low, close, length=14), index=frame.index, default=0.0)
    frame["mom_10"] = _series_or_default(ta_call(ta.mom, close, length=10), index=frame.index, default=0.0)
    frame["roc_10"] = _series_or_default(ta_call(ta.roc, close, length=10), index=frame.index, default=0.0)
    frame["obv"] = _series_or_default(ta_call(ta.obv, close, volume), index=frame.index, default=0.0)
    frame["mfi_14"] = _series_or_default(
        ta_call(ta.mfi, high, low, close, volume, length=14),
        index=frame.index,
        default=50.0,
    )

    donchian = ta_call(ta.donchian, high, low, lower_length=20, upper_length=20)
    donchian_low_fallback = low.rolling(20, min_periods=1).min()
    donchian_high_fallback = high.rolling(20, min_periods=1).max()
    donchian_mid_fallback = (donchian_low_fallback + donchian_high_fallback) / 2.0
    frame["donchian_low_20"] = _indicator_column(
        donchian,
        index=frame.index,
        prefixes=("DCL_",),
        default=float("nan"),
    ).combine_first(donchian_low_fallback)
    frame["donchian_mid_20"] = _indicator_column(
        donchian,
        index=frame.index,
        prefixes=("DCM_",),
        default=float("nan"),
    ).combine_first(donchian_mid_fallback)
    frame["donchian_high_20"] = _indicator_column(
        donchian,
        index=frame.index,
        prefixes=("DCU_",),
        default=float("nan"),
    ).combine_first(donchian_high_fallback)

    supertrend = ta_call(ta.supertrend, high, low, close, length=7, multiplier=3.0)
    frame["supertrend"] = _indicator_column(supertrend, index=frame.index, prefixes=("SUPERT_",), default=0.0).fillna(close)
    frame["supertrend_direction"] = _indicator_column(
        supertrend,
        index=frame.index,
        prefixes=("SUPERTd_",),
        default=0.0,
    )

    rolling_volume = volume.rolling(20, min_periods=1).mean()
    safe_rolling_volume = rolling_volume.where(rolling_volume > 0, 1.0)
    frame["rvol_20"] = (volume / safe_rolling_volume).fillna(0.0).clip(lower=0.0)
    return frame


def build_indicator_snapshots(
    bars: list[CanonicalBar],
    *,
    indicator_set_version: str,
    allowed_timeframes: set[Timeframe] | None = None,
    computed_at_utc: str | None = None,
) -> list[TechnicalIndicatorSnapshot]:
    accepted_timeframes = set(allowed_timeframes or set(FEATURE_LAYER_TIMEFRAMES))
    if not accepted_timeframes:
        raise ValueError("allowed_timeframes must contain at least one timeframe")

    computed_at = computed_at_utc or _utc_now_iso()
    grouped: dict[tuple[str, Timeframe], list[CanonicalBar]] = defaultdict(list)
    for bar in sorted(bars, key=lambda row: (row.contract_id, row.timeframe.value, row.ts)):
        if bar.timeframe not in accepted_timeframes:
            continue
        grouped[(bar.contract_id, bar.timeframe)].append(bar)

    snapshots: list[TechnicalIndicatorSnapshot] = []
    for (contract_id, timeframe), series in grouped.items():
        frame = _indicator_frame(series)
        instrument_id = series[0].instrument_id

        for index, bar in enumerate(series):
            row = frame.iloc[index]
            donchian_low = _safe_float(row["donchian_low_20"], default=bar.low)
            donchian_high = _safe_float(row["donchian_high_20"], default=bar.high)
            if donchian_low > donchian_high:
                donchian_low, donchian_high = donchian_high, donchian_low
            donchian_mid = _safe_float(row["donchian_mid_20"], default=(donchian_low + donchian_high) / 2.0)

            indicator_values: dict[str, float | str] = {
                "source": "pandas-ta-classic",
                "atr_14": _safe_float(row["atr_14"], default=max(0.0, bar.high - bar.low)),
                "natr_14": max(0.0, _safe_float(row["natr_14"], default=0.0)),
                "ema_12": _safe_float(row["ema_12"], default=bar.close),
                "ema_26": _safe_float(row["ema_26"], default=bar.close),
                "sma_20": _safe_float(row["sma_20"], default=bar.close),
                "sma_50": _safe_float(row["sma_50"], default=bar.close),
                "rsi_14": _safe_float(row["rsi_14"], default=50.0),
                "macd": _safe_float(row["macd"], default=0.0),
                "macd_signal": _safe_float(row["macd_signal"], default=0.0),
                "macd_hist": _safe_float(row["macd_hist"], default=0.0),
                "bb_percent_b": _safe_float(row["bb_percent_b"], default=0.5),
                "adx_14": _safe_float(row["adx_14"], default=0.0),
                "stoch_k": _safe_float(row["stoch_k"], default=50.0),
                "stoch_d": _safe_float(row["stoch_d"], default=50.0),
                "cci_20": _safe_float(row["cci_20"], default=0.0),
                "willr_14": _safe_float(row["willr_14"], default=0.0),
                "mom_10": _safe_float(row["mom_10"], default=0.0),
                "roc_10": _safe_float(row["roc_10"], default=0.0),
                "mfi_14": _safe_float(row["mfi_14"], default=50.0),
                "rvol_20": max(0.0, _safe_float(row["rvol_20"], default=0.0)),
            }

            snapshots.append(
                TechnicalIndicatorSnapshot(
                    indicator_snapshot_id=_indicator_snapshot_id(
                        contract_id=contract_id,
                        timeframe=timeframe,
                        ts=bar.ts,
                        indicator_set_version=indicator_set_version,
                    ),
                    contract_id=contract_id,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    ts=bar.ts,
                    indicator_set_version=indicator_set_version,
                    source_bar_fingerprint=_bar_fingerprint(bar),
                    close=bar.close,
                    volume=float(bar.volume),
                    atr_14=max(0.0, _safe_float(row["atr_14"], default=max(0.0, bar.high - bar.low))),
                    natr_14=max(0.0, _safe_float(row["natr_14"], default=0.0)),
                    ema_12=_safe_float(row["ema_12"], default=bar.close),
                    ema_26=_safe_float(row["ema_26"], default=bar.close),
                    sma_20=_safe_float(row["sma_20"], default=bar.close),
                    sma_50=_safe_float(row["sma_50"], default=bar.close),
                    rsi_14=_safe_float(row["rsi_14"], default=50.0),
                    macd=_safe_float(row["macd"], default=0.0),
                    macd_signal=_safe_float(row["macd_signal"], default=0.0),
                    macd_hist=_safe_float(row["macd_hist"], default=0.0),
                    bb_lower=_safe_float(row["bb_lower"], default=bar.close),
                    bb_mid=_safe_float(row["bb_mid"], default=bar.close),
                    bb_upper=_safe_float(row["bb_upper"], default=bar.close),
                    bb_width=max(0.0, _safe_float(row["bb_width"], default=0.0)),
                    bb_percent_b=_safe_float(row["bb_percent_b"], default=0.5),
                    adx_14=_safe_float(row["adx_14"], default=0.0),
                    dmp_14=_safe_float(row["dmp_14"], default=0.0),
                    dmn_14=_safe_float(row["dmn_14"], default=0.0),
                    stoch_k=_safe_float(row["stoch_k"], default=50.0),
                    stoch_d=_safe_float(row["stoch_d"], default=50.0),
                    cci_20=_safe_float(row["cci_20"], default=0.0),
                    willr_14=_safe_float(row["willr_14"], default=0.0),
                    mom_10=_safe_float(row["mom_10"], default=0.0),
                    roc_10=_safe_float(row["roc_10"], default=0.0),
                    obv=_safe_float(row["obv"], default=0.0),
                    mfi_14=_safe_float(row["mfi_14"], default=50.0),
                    donchian_low_20=donchian_low,
                    donchian_mid_20=donchian_mid,
                    donchian_high_20=donchian_high,
                    supertrend=_safe_float(row["supertrend"], default=bar.close),
                    supertrend_direction=_safe_float(row["supertrend_direction"], default=0.0),
                    rvol_20=max(0.0, _safe_float(row["rvol_20"], default=0.0)),
                    computed_at_utc=computed_at,
                    indicator_values_json=indicator_values,
                )
            )

    return sorted(snapshots, key=lambda item: (item.contract_id, item.timeframe.value, item.ts))


def _ratio(numerator: float, denominator: float, *, default: float = 0.0) -> float:
    if denominator == 0:
        return default
    return numerator / denominator


def _feature_regime(indicator: TechnicalIndicatorSnapshot) -> str:
    if indicator.adx_14 >= 18.0:
        return "trend" if indicator.ema_12 >= indicator.ema_26 else "mean_revert"
    if indicator.bb_width >= 8.0 or indicator.natr_14 >= 2.5:
        return "volatile"
    return "mean_revert"


def _feature_json(indicator: TechnicalIndicatorSnapshot) -> dict[str, float | str]:
    donchian_range = indicator.donchian_high_20 - indicator.donchian_low_20
    donchian_position = _ratio(indicator.close - indicator.donchian_low_20, donchian_range, default=0.5)
    trend_score = _ratio(indicator.ema_12 - indicator.ema_26, indicator.atr_14 if indicator.atr_14 > 0 else 1.0)
    momentum_score = ((indicator.rsi_14 - 50.0) / 50.0) + _ratio(indicator.macd_hist, indicator.atr_14 or 1.0)
    volatility_score = indicator.natr_14 + indicator.bb_width
    volume_pressure = indicator.rvol_20 * (1.0 if indicator.close >= indicator.ema_12 else -1.0)
    breakout_state = "upper" if indicator.close >= indicator.donchian_high_20 else "lower"
    if indicator.donchian_low_20 < indicator.close < indicator.donchian_high_20:
        breakout_state = "inside"

    return {
        "last_close": indicator.close,
        "last_volume": indicator.volume,
        "atr_14": indicator.atr_14,
        "natr_14": indicator.natr_14,
        "ema_12": indicator.ema_12,
        "ema_26": indicator.ema_26,
        "sma_20": indicator.sma_20,
        "sma_50": indicator.sma_50,
        "rsi_14": indicator.rsi_14,
        "macd": indicator.macd,
        "macd_signal": indicator.macd_signal,
        "macd_hist": indicator.macd_hist,
        "bb_width": indicator.bb_width,
        "bb_percent_b": indicator.bb_percent_b,
        "adx_14": indicator.adx_14,
        "stoch_k": indicator.stoch_k,
        "stoch_d": indicator.stoch_d,
        "cci_20": indicator.cci_20,
        "willr_14": indicator.willr_14,
        "mom_10": indicator.mom_10,
        "roc_10": indicator.roc_10,
        "mfi_14": indicator.mfi_14,
        "donchian_position": donchian_position,
        "trend_score": trend_score,
        "momentum_score": momentum_score,
        "volatility_score": volatility_score,
        "volume_pressure": volume_pressure,
        "supertrend_direction": indicator.supertrend_direction,
        "breakout_state": breakout_state,
    }


def build_feature_snapshots_from_indicators(
    indicators: list[TechnicalIndicatorSnapshot],
    *,
    feature_set_version: str,
    allowed_timeframes: set[Timeframe] | None = None,
) -> list[FeatureSnapshot]:
    accepted_timeframes = set(allowed_timeframes or set(FEATURE_LAYER_TIMEFRAMES))
    if not accepted_timeframes:
        raise ValueError("allowed_timeframes must contain at least one timeframe")

    snapshots: list[FeatureSnapshot] = []
    for indicator in sorted(indicators, key=lambda row: (row.contract_id, row.timeframe.value, row.ts)):
        if indicator.timeframe not in accepted_timeframes:
            continue
        snapshots.append(
            FeatureSnapshot(
                snapshot_id=_snapshot_id(
                    contract_id=indicator.contract_id,
                    timeframe=indicator.timeframe,
                    ts=indicator.ts,
                    feature_set_version=feature_set_version,
                ),
                contract_id=indicator.contract_id,
                instrument_id=indicator.instrument_id,
                timeframe=indicator.timeframe,
                ts=indicator.ts,
                feature_set_version=feature_set_version,
                regime=_feature_regime(indicator),
                atr=indicator.atr_14,
                ema_fast=indicator.ema_12,
                ema_slow=indicator.ema_26,
                donchian_high=indicator.donchian_high_20,
                donchian_low=indicator.donchian_low_20,
                rvol=indicator.rvol_20,
                features_json=_feature_json(indicator),
            )
        )
    return snapshots


def build_feature_snapshots(
    bars: list[CanonicalBar],
    *,
    feature_set_version: str,
    instrument_by_contract: dict[str, str],
    lookback: int = 5,
    allowed_timeframes: set[Timeframe] | None = None,
    indicator_set_version: str = "pandas-ta-v1",
    computed_at_utc: str | None = None,
) -> list[FeatureSnapshot]:
    if lookback <= 0:
        raise ValueError("lookback must be positive")

    normalized_bars = [
        CanonicalBar(
            contract_id=bar.contract_id,
            instrument_id=instrument_by_contract.get(bar.contract_id, bar.instrument_id),
            timeframe=bar.timeframe,
            ts=bar.ts,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            open_interest=bar.open_interest,
        )
        for bar in bars
    ]
    indicators = build_indicator_snapshots(
        normalized_bars,
        indicator_set_version=indicator_set_version,
        allowed_timeframes=allowed_timeframes,
        computed_at_utc=computed_at_utc,
    )
    return build_feature_snapshots_from_indicators(
        indicators,
        feature_set_version=feature_set_version,
        allowed_timeframes=allowed_timeframes,
    )


def run_indicator_feature_quality_gates(
    *,
    indicators: list[TechnicalIndicatorSnapshot],
    features: list[FeatureSnapshot],
) -> dict[str, object]:
    checks: list[dict[str, object]] = []

    def add_check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    indicator_keys = [
        (item.contract_id, item.timeframe.value, item.ts, item.indicator_set_version)
        for item in indicators
    ]
    feature_keys = [
        (item.contract_id, item.timeframe.value, item.ts, item.feature_set_version)
        for item in features
    ]
    indicator_base_keys = {(item.contract_id, item.timeframe.value, item.ts) for item in indicators}
    feature_base_keys = {(item.contract_id, item.timeframe.value, item.ts) for item in features}
    negative_indicator_rows = [
        item.indicator_snapshot_id
        for item in indicators
        if item.atr_14 < 0 or item.natr_14 < 0 or item.rvol_20 < 0 or item.bb_width < 0
    ]
    invalid_feature_rows = [
        item.snapshot_id
        for item in features
        if item.atr < 0 or item.rvol < 0 or item.donchian_low > item.donchian_high
    ]
    missing_feature_keys = sorted(indicator_base_keys - feature_base_keys)

    add_check("indicator_rows_present", bool(indicators), f"indicator_rows={len(indicators)}")
    add_check("feature_rows_present", bool(features), f"feature_rows={len(features)}")
    add_check(
        "indicator_uniqueness",
        len(indicator_keys) == len(set(indicator_keys)),
        f"unique={len(set(indicator_keys))} total={len(indicator_keys)}",
    )
    add_check(
        "feature_uniqueness",
        len(feature_keys) == len(set(feature_keys)),
        f"unique={len(set(feature_keys))} total={len(feature_keys)}",
    )
    add_check(
        "feature_coverage",
        not missing_feature_keys,
        f"missing_feature_keys={len(missing_feature_keys)}",
    )
    add_check(
        "indicator_non_negative_core",
        not negative_indicator_rows,
        f"invalid_rows={len(negative_indicator_rows)}",
    )
    add_check(
        "feature_non_negative_core",
        not invalid_feature_rows,
        f"invalid_rows={len(invalid_feature_rows)}",
    )

    failed = [item for item in checks if item["status"] != "PASS"]
    return {
        "status": "PASS" if not failed else "FAIL",
        "indicator_rows": len(indicators),
        "feature_rows": len(features),
        "checks": checks,
        "sample_missing_feature_keys": ["|".join(item) for item in missing_feature_keys[:10]],
        "sample_invalid_indicator_ids": negative_indicator_rows[:10],
        "sample_invalid_feature_ids": invalid_feature_rows[:10],
    }
