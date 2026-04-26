from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView, load_materialized_research_dataset
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    DerivedIndicatorProfile,
    DerivedIndicatorProfileRegistry,
    build_derived_indicator_profile_registry,
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.indicators import IndicatorFrameRow, load_indicator_frames

from .store import (
    DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    DerivedIndicatorFramePartitionKey,
    DerivedIndicatorFrameRow,
    load_derived_indicator_frames,
    research_derived_indicator_store_contract,
    write_derived_indicator_frames,
)


@dataclass(frozen=True)
class DerivedSeriesKey:
    instrument_id: str
    contract_id: str | None


MTF_MAPPINGS: tuple[tuple[str, str], ...] = (
    ("1h", "15m"),
    ("4h", "15m"),
    ("4h", "1h"),
    ("1d", "15m"),
    ("1d", "1h"),
    ("1d", "4h"),
)


def _timeframe_delta(timeframe: str) -> pd.Timedelta:
    if timeframe.endswith("m"):
        return pd.Timedelta(minutes=int(timeframe[:-1]))
    if timeframe.endswith("h"):
        return pd.Timedelta(hours=int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return pd.Timedelta(days=int(timeframe[:-1]))
    raise ValueError(f"unsupported timeframe token: {timeframe}")


def _series_key_from_bar(row: ResearchBarView, *, series_mode: str) -> DerivedSeriesKey:
    return DerivedSeriesKey(
        instrument_id=row.instrument_id,
        contract_id=None if series_mode == "continuous_front" else row.contract_id,
    )


def _series_key_from_indicator(row: IndicatorFrameRow, *, series_mode: str) -> DerivedSeriesKey:
    return DerivedSeriesKey(
        instrument_id=row.instrument_id,
        contract_id=None if series_mode == "continuous_front" else row.contract_id,
    )


def _group_bar_views(
    *,
    bar_views: list[ResearchBarView],
    series_mode: str,
) -> dict[DerivedSeriesKey, dict[str, list[ResearchBarView]]]:
    grouped: dict[DerivedSeriesKey, dict[str, list[ResearchBarView]]] = {}
    for row in sorted(bar_views, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe, item.ts)):
        series_key = _series_key_from_bar(row, series_mode=series_mode)
        grouped.setdefault(series_key, {}).setdefault(row.timeframe, []).append(row)
    return grouped


def _group_indicator_rows(
    *,
    rows: list[IndicatorFrameRow],
    series_mode: str,
) -> dict[DerivedSeriesKey, dict[str, list[IndicatorFrameRow]]]:
    grouped: dict[DerivedSeriesKey, dict[str, list[IndicatorFrameRow]]] = {}
    for row in sorted(rows, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe, item.ts)):
        series_key = _series_key_from_indicator(row, series_mode=series_mode)
        grouped.setdefault(series_key, {}).setdefault(row.timeframe, []).append(row)
    return grouped


def _bars_hash(rows: list[ResearchBarView]) -> str:
    payload = [row.to_dict() for row in rows]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _indicator_hash(rows: list[IndicatorFrameRow]) -> str:
    payload = [row.to_dict() for row in rows]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _prepare_base_frame(
    *,
    bars: list[ResearchBarView],
    indicators: list[IndicatorFrameRow],
) -> pd.DataFrame:
    if not indicators:
        raise ValueError("derived indicator materialization requires base indicators for every series/timeframe partition")

    bar_frame = pd.DataFrame([row.to_dict() for row in bars])
    indicator_frame = pd.DataFrame([row.to_dict() for row in indicators])
    keep_columns = [
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        *sorted(
            column
            for column in indicator_frame.columns
            if column
            not in {
                "contract_id",
                "instrument_id",
                "timeframe",
                "ts",
                "dataset_version",
                "indicator_set_version",
                "profile_version",
                "source_bars_hash",
                "row_count",
                "warmup_span",
                "null_warmup_span",
                "created_at",
            }
            and column not in bar_frame.columns
        ),
    ]
    merged = bar_frame.merge(
        indicator_frame[keep_columns],
        on=["contract_id", "instrument_id", "timeframe", "ts"],
        how="left",
        validate="one_to_one",
    )
    return merged.sort_values("ts").reset_index(drop=True)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")
    return pd.to_numeric(frame[column], errors="coerce")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace({0.0: pd.NA})


def _append_columns(frame: pd.DataFrame, columns: dict[str, pd.Series]) -> pd.DataFrame:
    if not columns:
        return frame
    return pd.concat([frame, pd.DataFrame(columns, index=frame.index)], axis=1)


def _int_code_series(values: list[int | None], *, index: pd.Index) -> pd.Series:
    return pd.Series(values, index=index, dtype="object")


def _distance_to(frame: pd.DataFrame, column: str, *, denominator: pd.Series) -> pd.Series:
    return _safe_divide(_numeric(frame, "close") - _numeric(frame, column), denominator)


def _position_between(close: pd.Series, lower: pd.Series, upper: pd.Series) -> pd.Series:
    return _safe_divide(close - lower, upper - lower)


def _change_code(current: pd.Series, reference: pd.Series) -> pd.Series:
    diff = current - reference
    previous = diff.shift(1)
    values: list[int | None] = []
    for prev, now in zip(previous.tolist(), diff.tolist(), strict=True):
        if pd.isna(prev) or pd.isna(now):
            values.append(None)
        elif float(prev) <= 0.0 < float(now):
            values.append(1)
        elif float(prev) >= 0.0 > float(now):
            values.append(-1)
        else:
            values.append(0)
    return _int_code_series(values, index=current.index)


def _slope(series: pd.Series, *, length: int) -> pd.Series:
    return (series - series.shift(length)) / float(length)


def _divergence_score(price: pd.Series, indicator: pd.Series, *, length: int) -> pd.Series:
    price = pd.to_numeric(price, errors="coerce")
    indicator = pd.to_numeric(indicator, errors="coerce")
    prior_price_high = price.shift(1).rolling(window=length, min_periods=length).max()
    prior_price_low = price.shift(1).rolling(window=length, min_periods=length).min()
    prior_indicator_high = indicator.shift(1).rolling(window=length, min_periods=length).max()
    prior_indicator_low = indicator.shift(1).rolling(window=length, min_periods=length).min()
    price_position = _safe_divide(price - prior_price_low, prior_price_high - prior_price_low)
    indicator_position = _safe_divide(indicator - prior_indicator_low, prior_indicator_high - prior_indicator_low)
    return price_position - indicator_position


def _compute_opening_range(frame: pd.DataFrame, *, opening_bars: int) -> dict[str, pd.Series]:
    highs: list[float | None] = []
    lows: list[float | None] = []
    for _, group in frame.groupby("session_date", sort=False):
        session_highs = group["high"].tolist()
        session_lows = group["low"].tolist()
        fixed_high = max(session_highs[:opening_bars]) if len(session_highs) >= opening_bars else None
        fixed_low = min(session_lows[:opening_bars]) if len(session_lows) >= opening_bars else None
        for idx in range(len(group)):
            if idx < opening_bars - 1 or fixed_high is None or fixed_low is None:
                highs.append(None)
                lows.append(None)
            else:
                highs.append(float(fixed_high))
                lows.append(float(fixed_low))
    return {
        "opening_range_high": pd.Series(highs, index=frame.index, dtype="object"),
        "opening_range_low": pd.Series(lows, index=frame.index, dtype="object"),
    }


def _compute_swing_levels(frame: pd.DataFrame, *, left_bars: int, right_bars: int) -> dict[str, pd.Series]:
    highs = frame["high"].tolist()
    lows = frame["low"].tolist()
    confirmed_highs: list[float | None] = []
    confirmed_lows: list[float | None] = []
    last_high: float | None = None
    last_low: float | None = None
    for current_index in range(len(frame)):
        candidate_index = current_index - right_bars
        if candidate_index >= left_bars:
            high_window = highs[candidate_index - left_bars : candidate_index + right_bars + 1]
            low_window = lows[candidate_index - left_bars : candidate_index + right_bars + 1]
            if highs[candidate_index] == max(high_window):
                last_high = float(highs[candidate_index])
            if lows[candidate_index] == min(low_window):
                last_low = float(lows[candidate_index])
        confirmed_highs.append(last_high)
        confirmed_lows.append(last_low)
    return {
        "swing_high_10": pd.Series(confirmed_highs, index=frame.index, dtype="object"),
        "swing_low_10": pd.Series(confirmed_lows, index=frame.index, dtype="object"),
    }


def _compute_session_vwap(frame: pd.DataFrame) -> pd.Series:
    typical_price = (_numeric(frame, "high") + _numeric(frame, "low") + _numeric(frame, "close")) / 3.0
    weighted = typical_price * _numeric(frame, "volume")
    session_cum_weight = weighted.groupby(frame["session_date"], sort=False).cumsum()
    session_cum_volume = _numeric(frame, "volume").groupby(frame["session_date"], sort=False).cumsum()
    return _safe_divide(session_cum_weight, session_cum_volume)


def _bar_close_times(frame: pd.DataFrame, timeframe: str) -> pd.Series:
    if timeframe.endswith("d") and "session_close_ts" in frame.columns:
        return pd.to_datetime(frame["session_close_ts"], utc=True)
    return pd.to_datetime(frame["ts"], utc=True) + _timeframe_delta(timeframe)


def _compute_mtf_overlay(
    *,
    frame: pd.DataFrame,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
) -> dict[str, pd.Series]:
    output: dict[str, pd.Series] = {}
    for source_timeframe, target_timeframe in MTF_MAPPINGS:
        prefix = f"mtf_{source_timeframe}_to_{target_timeframe}"
        columns = (
            f"{prefix}_ema_20",
            f"{prefix}_ema_50",
            f"{prefix}_adx_14",
            f"{prefix}_rsi_14",
        )
        if current_timeframe != target_timeframe or source_timeframe not in source_frames:
            for column in columns:
                output[column] = pd.Series([None] * len(frame), index=frame.index, dtype="object")
            continue
        source_frame = source_frames[source_timeframe]
        required = {"ts", "ema_20", "ema_50", "adx_14", "rsi_14"}
        if not required <= set(source_frame.columns):
            for column in columns:
                output[column] = pd.Series([None] * len(frame), index=frame.index, dtype="object")
            continue
        current_close_ts = _bar_close_times(frame, current_timeframe)
        source_close_ts = _bar_close_times(source_frame, source_timeframe)
        left = pd.DataFrame({"_row_index": frame.index, "current_close_ts": current_close_ts}).sort_values("current_close_ts")
        right = pd.DataFrame(
            {
                "source_close_ts": source_close_ts,
                columns[0]: _numeric(source_frame, "ema_20"),
                columns[1]: _numeric(source_frame, "ema_50"),
                columns[2]: _numeric(source_frame, "adx_14"),
                columns[3]: _numeric(source_frame, "rsi_14"),
            }
        ).sort_values("source_close_ts")
        merged = pd.merge_asof(left, right, left_on="current_close_ts", right_on="source_close_ts", direction="backward")
        merged = merged.set_index("_row_index").reindex(frame.index)
        for column in columns:
            output[column] = merged[column].astype("object")
    return output


def _compute_derived_frame(
    *,
    base_frame: pd.DataFrame,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
    profile: DerivedIndicatorProfile,
) -> pd.DataFrame:
    result = base_frame.copy()
    close = _numeric(result, "close")
    high = _numeric(result, "high")
    low = _numeric(result, "low")
    atr = _numeric(result, "atr_14").replace({0.0: pd.NA})

    result["rolling_high_20"] = high.rolling(window=20, min_periods=20).max()
    result["rolling_low_20"] = low.rolling(window=20, min_periods=20).min()
    result["session_high"] = high.groupby(result["session_date"], sort=False).cummax()
    result["session_low"] = low.groupby(result["session_date"], sort=False).cummin()
    week_key = pd.to_datetime(result["ts"], utc=True).dt.strftime("%G-%V")
    result["week_high"] = high.groupby(week_key, sort=False).cummax()
    result["week_low"] = low.groupby(week_key, sort=False).cummin()
    result = _append_columns(result, _compute_opening_range(result, opening_bars=4))
    result = _append_columns(result, _compute_swing_levels(result, left_bars=5, right_bars=5))
    result["session_vwap"] = _compute_session_vwap(result)

    distance_targets = {
        "session_vwap": "session_vwap",
        "session_high": "session_high",
        "session_low": "session_low",
        "week_high": "week_high",
        "week_low": "week_low",
        "rolling_high_20": "rolling_high_20",
        "rolling_low_20": "rolling_low_20",
        "sma_20_atr": "sma_20",
        "sma_50_atr": "sma_50",
        "sma_100_atr": "sma_100",
        "sma_200_atr": "sma_200",
        "ema_20_atr": "ema_20",
        "ema_50_atr": "ema_50",
        "ema_100_atr": "ema_100",
        "ema_200_atr": "ema_200",
        "hma_20_atr": "hma_20",
        "hma_100_atr": "hma_100",
        "hma_200_atr": "hma_200",
        "vwma_20_atr": "vwma_20",
        "vwma_100_atr": "vwma_100",
        "vwma_200_atr": "vwma_200",
        "bb_upper_20_2_atr": "bb_upper_20_2",
        "bb_lower_20_2_atr": "bb_lower_20_2",
        "kc_upper_20_1_5_atr": "kc_upper_20_1_5",
        "kc_lower_20_1_5_atr": "kc_lower_20_1_5",
        "donchian_high_20_atr": "donchian_high_20",
        "donchian_low_20_atr": "donchian_low_20",
        "donchian_high_55_atr": "donchian_high_55",
        "donchian_low_55_atr": "donchian_low_55",
    }
    for output_suffix, target_column in distance_targets.items():
        denominator = atr if output_suffix.endswith("_atr") else atr
        result[f"distance_to_{output_suffix}"] = _distance_to(result, target_column, denominator=denominator)

    result["rolling_position_20"] = _position_between(close, _numeric(result, "rolling_low_20"), _numeric(result, "rolling_high_20"))
    result["session_position"] = _position_between(close, _numeric(result, "session_low"), _numeric(result, "session_high"))
    result["week_position"] = _position_between(close, _numeric(result, "week_low"), _numeric(result, "week_high"))
    result["bb_position_20_2"] = _position_between(close, _numeric(result, "bb_lower_20_2"), _numeric(result, "bb_upper_20_2"))
    result["kc_position_20_1_5"] = _position_between(close, _numeric(result, "kc_lower_20_1_5"), _numeric(result, "kc_upper_20_1_5"))
    result["donchian_position_20"] = _position_between(close, _numeric(result, "donchian_low_20"), _numeric(result, "donchian_high_20"))
    result["donchian_position_55"] = _position_between(close, _numeric(result, "donchian_low_55"), _numeric(result, "donchian_high_55"))

    result["cross_close_sma_20_code"] = _change_code(close, _numeric(result, "sma_20"))
    result["cross_close_ema_20_code"] = _change_code(close, _numeric(result, "ema_20"))
    result["cross_close_session_vwap_code"] = _change_code(close, _numeric(result, "session_vwap"))
    result["cross_close_rolling_high_20_code"] = _change_code(close, _numeric(result, "rolling_high_20").shift(1))
    result["cross_close_rolling_low_20_code"] = _change_code(close, _numeric(result, "rolling_low_20").shift(1))
    result["macd_signal_cross_code"] = _change_code(_numeric(result, "macd_12_26_9"), _numeric(result, "macd_signal_12_26_9"))
    result["ppo_signal_cross_code"] = _change_code(_numeric(result, "ppo_12_26_9"), _numeric(result, "ppo_signal_12_26_9"))
    result["trix_signal_cross_code"] = _change_code(_numeric(result, "trix_30_9"), _numeric(result, "trix_signal_30_9"))
    result["kst_signal_cross_code"] = _change_code(_numeric(result, "kst_10_15_20_30"), _numeric(result, "kst_signal_9"))

    result["close_change_1"] = close.diff(1)
    result["close_slope_20"] = _numeric(result, "close_slope_20")
    result["sma_20_slope_5"] = _slope(_numeric(result, "sma_20"), length=5)
    result["ema_20_slope_5"] = _slope(_numeric(result, "ema_20"), length=5)
    result["roc_10_change_1"] = _numeric(result, "roc_10").diff(1)
    result["mom_10_change_1"] = _numeric(result, "mom_10").diff(1)
    result["volume_change_1"] = _numeric(result, "volume").diff(1)
    result["oi_change_1"] = _numeric(result, "oi_change_1")

    result["rvol_20"] = _numeric(result, "rvol_20")
    result["volume_zscore_20"] = _numeric(result, "volume_z_20")
    result["price_volume_corr_20"] = close.diff(1).rolling(window=20, min_periods=20).corr(_numeric(result, "volume").diff(1))
    result["price_oi_corr_20"] = close.diff(1).rolling(window=20, min_periods=20).corr(_numeric(result, "oi_change_1"))
    result["volume_oi_corr_20"] = _numeric(result, "volume").diff(1).rolling(window=20, min_periods=20).corr(_numeric(result, "oi_change_1"))

    divergence_sources = {
        "rsi_14": "rsi_14",
        "stoch_k_14_3_3": "stoch_k_14_3_3",
        "stoch_d_14_3_3": "stoch_d_14_3_3",
        "cci_20": "cci_20",
        "willr_14": "willr_14",
        "stochrsi_k_14_14_3_3": "stochrsi_k_14_14_3_3",
        "stochrsi_d_14_14_3_3": "stochrsi_d_14_14_3_3",
        "ultimate_oscillator_7_14_28": "ultimate_oscillator_7_14_28",
        "macd_hist_12_26_9": "macd_hist_12_26_9",
        "ppo_hist_12_26_9": "ppo_hist_12_26_9",
        "tsi_25_13": "tsi_25_13",
        "roc_10": "roc_10",
        "obv": "obv",
        "mfi_14": "mfi_14",
        "cmf_20": "cmf_20",
        "ad": "ad",
        "adosc_3_10": "adosc_3_10",
        "force_index_13": "force_index_13",
        "pvt": "pvt",
        "pvo_12_26_9": "pvo_12_26_9",
        "pvo_hist_12_26_9": "pvo_hist_12_26_9",
        "oi_change_1": "oi_change_1",
        "oi_roc_10": "oi_roc_10",
        "oi_z_20": "oi_z_20",
        "oi_relative_activity_20": "oi_relative_activity_20",
    }
    divergence_columns: dict[str, pd.Series] = {}
    for output_suffix, column in divergence_sources.items():
        divergence_columns[f"divergence_price_{output_suffix}_score"] = _divergence_score(close, _numeric(result, column), length=20)
    result = _append_columns(result, divergence_columns)

    result = _append_columns(
        result,
        _compute_mtf_overlay(
            frame=result,
            current_timeframe=current_timeframe,
            source_frames=source_frames,
        ),
    )

    missing_columns: dict[str, pd.Series] = {}
    for column in profile.output_columns:
        if column not in result.columns:
            missing_columns[column] = pd.Series([None] * len(result), index=result.index, dtype="object")
    result = _append_columns(result, missing_columns)
    return result


def _null_warmup_span(payload_rows: list[dict[str, object]], output_columns: tuple[str, ...]) -> int:
    if not output_columns:
        return 0
    active_columns = tuple(
        column
        for column in output_columns
        if any(row.get(column) is not None and not pd.isna(row.get(column)) for row in payload_rows)
    )
    if not active_columns:
        return 0
    count = 0
    for row in payload_rows:
        if any(row.get(column) is None or pd.isna(row.get(column)) for column in active_columns):
            count += 1
        else:
            break
    return count


def _derived_value(column: str, value: object) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    value_type = research_derived_indicator_store_contract()["research_derived_indicator_frames"]["columns"][column]
    if value_type == "int":
        return int(value)
    return float(value)


def _build_partition_rows(
    *,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    profile: DerivedIndicatorProfile,
    local_series: list[ResearchBarView],
    local_indicator_rows: list[IndicatorFrameRow],
    source_frames: dict[str, pd.DataFrame],
) -> list[DerivedIndicatorFrameRow]:
    base_frame = _prepare_base_frame(bars=local_series, indicators=local_indicator_rows)
    current_timeframe = local_series[0].timeframe
    computed = _compute_derived_frame(
        base_frame=base_frame,
        current_timeframe=current_timeframe,
        source_frames=source_frames,
        profile=profile,
    )
    payload_rows = computed.to_dict("records")
    output_columns = profile.output_columns
    source_bars_hash = _bars_hash(local_series)
    source_indicators_hash = _indicator_hash(local_indicator_rows)
    null_warmup_span = _null_warmup_span(payload_rows, output_columns)
    created_at = pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")

    rows: list[DerivedIndicatorFrameRow] = []
    for original, payload in zip(local_series, payload_rows, strict=True):
        rows.append(
            DerivedIndicatorFrameRow(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                derived_indicator_set_version=derived_indicator_set_version,
                profile_version=profile.version,
                contract_id=original.contract_id,
                instrument_id=original.instrument_id,
                timeframe=original.timeframe,
                ts=original.ts,
                values={column: _derived_value(column, payload.get(column)) for column in output_columns},
                source_bars_hash=source_bars_hash,
                source_indicators_hash=source_indicators_hash,
                row_count=len(local_series),
                warmup_span=profile.warmup_bars,
                null_warmup_span=null_warmup_span,
                created_at=created_at,
            )
        )
    return rows


def build_derived_indicator_frames(
    *,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    bar_views: list[ResearchBarView],
    indicator_rows: list[IndicatorFrameRow],
    series_mode: str = "contract",
    profile: DerivedIndicatorProfile | None = None,
) -> list[DerivedIndicatorFrameRow]:
    profile = profile or current_derived_indicator_profile()
    grouped_bars = _group_bar_views(bar_views=bar_views, series_mode=series_mode)
    grouped_indicators = _group_indicator_rows(rows=indicator_rows, series_mode=series_mode)

    rows: list[DerivedIndicatorFrameRow] = []
    for series_key, bars_by_timeframe in sorted(grouped_bars.items(), key=lambda item: (item[0].instrument_id, item[0].contract_id or "")):
        indicators_by_timeframe = grouped_indicators.get(series_key, {})
        source_frames = {
            timeframe: _prepare_base_frame(
                bars=bars_by_timeframe[timeframe],
                indicators=indicators_by_timeframe.get(timeframe, []),
            )
            for timeframe in bars_by_timeframe
        }
        for timeframe, local_series in sorted(bars_by_timeframe.items(), key=lambda item: item[0]):
            rows.extend(
                _build_partition_rows(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    derived_indicator_set_version=derived_indicator_set_version,
                    profile=profile,
                    local_series=local_series,
                    local_indicator_rows=indicators_by_timeframe.get(timeframe, []),
                    source_frames=source_frames,
                )
            )
    return rows


def materialize_derived_indicator_frames(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    derived_indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    profile: DerivedIndicatorProfile | None = None,
    profile_version: str | None = None,
) -> dict[str, object]:
    loaded = load_materialized_research_dataset(output_dir=dataset_output_dir, dataset_version=dataset_version)
    series_mode = str(loaded["dataset_manifest"].get("series_mode", "contract"))
    indicator_rows = load_indicator_frames(
        output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    )
    registry: DerivedIndicatorProfileRegistry = build_derived_indicator_profile_registry()
    resolved_profile = profile or registry.get(profile_version or "core_v1")
    rows = build_derived_indicator_frames(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        bar_views=loaded["bar_views"],
        indicator_rows=indicator_rows,
        series_mode=series_mode,
        profile=resolved_profile,
    )
    replace_partitions: list[DerivedIndicatorFramePartitionKey] = []
    for row in rows:
        partition_key = row.partition_key(series_mode=series_mode)
        if partition_key not in replace_partitions:
            replace_partitions.append(partition_key)
    output_paths = write_derived_indicator_frames(
        output_dir=derived_indicator_output_dir,
        rows=rows,
        replace_partitions=tuple(replace_partitions),
    )
    return {
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "derived_indicator_set_version": derived_indicator_set_version,
        "profile_version": resolved_profile.version,
        "derived_indicator_row_count": len(rows),
        "refreshed_partition_count": len(replace_partitions),
        "output_paths": output_paths,
        "delta_manifest": research_derived_indicator_store_contract(),
    }


def reload_derived_indicator_frames(
    *,
    derived_indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
) -> list[DerivedIndicatorFrameRow]:
    return load_derived_indicator_frames(
        output_dir=derived_indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
    )
