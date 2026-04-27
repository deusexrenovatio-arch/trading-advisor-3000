from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_advisor_3000.product_plane.data_plane.delta_runtime import iter_delta_table_row_batches, read_delta_table_rows
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    DerivedIndicatorProfile,
    DerivedIndicatorProfileRegistry,
    build_derived_indicator_profile_registry,
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.indicators import IndicatorFramePartitionKey, IndicatorFrameRow
from trading_advisor_3000.product_plane.research.indicators.store import (
    existing_indicator_value_columns,
    load_indicator_partition_metadata,
    load_indicator_partition_rows,
)

from .store import (
    DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    DerivedIndicatorFramePartitionKey,
    DerivedIndicatorFrameRow,
    derived_indicator_output_columns_hash,
    existing_derived_indicator_value_columns,
    load_derived_indicator_frames,
    load_derived_indicator_partition_metadata,
    load_derived_indicator_partition_rows,
    research_derived_indicator_store_contract,
    write_derived_indicator_frame_batches,
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


DERIVED_SOURCE_INDICATOR_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        (
            "atr_14",
            "sma_20",
            "sma_50",
            "sma_100",
            "sma_200",
            "ema_20",
            "ema_50",
            "ema_100",
            "ema_200",
            "hma_20",
            "hma_100",
            "hma_200",
            "vwma_20",
            "vwma_100",
            "vwma_200",
            "bb_upper_20_2",
            "bb_lower_20_2",
            "kc_upper_20_1_5",
            "kc_lower_20_1_5",
            "donchian_high_20",
            "donchian_low_20",
            "donchian_high_55",
            "donchian_low_55",
            "macd_12_26_9",
            "macd_signal_12_26_9",
            "macd_hist_12_26_9",
            "ppo_12_26_9",
            "ppo_signal_12_26_9",
            "ppo_hist_12_26_9",
            "trix_30_9",
            "trix_signal_30_9",
            "kst_10_15_20_30",
            "kst_signal_9",
            "close_slope_20",
            "roc_10",
            "mom_10",
            "rvol_20",
            "volume_z_20",
            "rsi_14",
            "stoch_k_14_3_3",
            "stoch_d_14_3_3",
            "cci_20",
            "willr_14",
            "stochrsi_k_14_14_3_3",
            "stochrsi_d_14_14_3_3",
            "ultimate_oscillator_7_14_28",
            "tsi_25_13",
            "obv",
            "mfi_14",
            "cmf_20",
            "ad",
            "adosc_3_10",
            "force_index_13",
            "pvt",
            "pvo_12_26_9",
            "pvo_hist_12_26_9",
            "oi_change_1",
            "oi_roc_10",
            "oi_z_20",
            "oi_relative_activity_20",
        )
    )
)


DERIVED_DYNAMIC_DISTANCE_NON_INDICATOR_TARGETS: frozenset[str] = frozenset(
    {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
        "true_range",
        "session_vwap",
        "session_high",
        "session_low",
        "week_high",
        "week_low",
        "rolling_high_20",
        "rolling_low_20",
        "opening_range_high",
        "opening_range_low",
        "swing_high_10",
        "swing_low_10",
    }
)


def _dynamic_distance_target(column: str) -> str | None:
    if not column.startswith("distance_to_") or not column.endswith("_atr"):
        return None
    return column.removeprefix("distance_to_").removesuffix("_atr")


def _source_indicator_columns_for_profile(profile: DerivedIndicatorProfile) -> tuple[str, ...]:
    columns = list(DERIVED_SOURCE_INDICATOR_COLUMNS)
    for column in profile.output_columns:
        target = _dynamic_distance_target(column)
        if target and target not in DERIVED_DYNAMIC_DISTANCE_NON_INDICATOR_TARGETS:
            columns.append(target)
    return tuple(dict.fromkeys(columns))


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


def _load_dataset_manifest(*, output_dir: Path, dataset_version: str) -> dict[str, object]:
    rows = read_delta_table_rows(
        output_dir / "research_datasets.delta",
        filters=[("dataset_version", "=", dataset_version)],
    )
    if not rows:
        raise KeyError(f"dataset_version not found: {dataset_version}")
    return rows[0]


def _series_key_from_bar_row(row: dict[str, object], *, series_mode: str) -> DerivedSeriesKey:
    return DerivedSeriesKey(
        instrument_id=str(row["instrument_id"]),
        contract_id=None if series_mode == "continuous_front" else str(row["contract_id"]),
    )


def _load_bar_partition_counts(
    *,
    dataset_output_dir: Path,
    dataset_version: str,
    series_mode: str,
) -> dict[DerivedSeriesKey, dict[str, int]]:
    counts: dict[DerivedSeriesKey, dict[str, int]] = {}
    for batch in iter_delta_table_row_batches(
        dataset_output_dir / "research_bar_views.delta",
        columns=["dataset_version", "contract_id", "instrument_id", "timeframe"],
    ):
        for row in batch:
            if row.get("dataset_version") != dataset_version:
                continue
            series_key = _series_key_from_bar_row(row, series_mode=series_mode)
            timeframe = str(row["timeframe"])
            counts.setdefault(series_key, {})[timeframe] = counts.setdefault(series_key, {}).get(timeframe, 0) + 1
    return counts


def _load_bar_partition_rows(
    *,
    dataset_output_dir: Path,
    dataset_version: str,
    series_key: DerivedSeriesKey,
    timeframe: str,
) -> list[ResearchBarView]:
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("instrument_id", "=", series_key.instrument_id),
        ("timeframe", "=", timeframe),
    ]
    if series_key.contract_id is not None:
        filters.append(("contract_id", "=", series_key.contract_id))
    rows = read_delta_table_rows(dataset_output_dir / "research_bar_views.delta", filters=filters)
    return [
        ResearchBarView.from_dict(row)
        for row in sorted(rows, key=lambda item: str(item["ts"]))
    ]


def _latest_delta_commit_timestamp(table_path: Path) -> int | None:
    log_dir = table_path / "_delta_log"
    if not log_dir.exists():
        return None
    log_files = sorted(log_dir.glob("*.json"))
    if not log_files:
        return None
    for line in reversed(log_files[-1].read_text(encoding="utf-8").splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        commit_info = payload.get("commitInfo") if isinstance(payload, dict) else None
        if isinstance(commit_info, dict) and commit_info.get("timestamp") is not None:
            return int(commit_info["timestamp"])
    return None


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


def _indicator_metadata_key(row: dict[str, object], *, series_mode: str) -> tuple[DerivedSeriesKey, str]:
    series_key = DerivedSeriesKey(
        instrument_id=str(row["instrument_id"]),
        contract_id=None if series_mode == "continuous_front" else str(row["contract_id"]),
    )
    return series_key, str(row["timeframe"])


def _group_indicator_partition_metadata(
    *,
    rows: list[dict[str, object]],
    series_mode: str,
) -> dict[tuple[DerivedSeriesKey, str], dict[str, object]]:
    grouped: dict[tuple[DerivedSeriesKey, str], dict[str, object]] = {}
    for row in rows:
        grouped.setdefault(_indicator_metadata_key(row, series_mode=series_mode), row)
    return grouped


def _indicator_partition_key(
    *,
    dataset_version: str,
    indicator_set_version: str,
    series_key: DerivedSeriesKey,
    timeframe: str,
) -> IndicatorFramePartitionKey:
    return IndicatorFramePartitionKey(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        timeframe=timeframe,
        instrument_id=series_key.instrument_id,
        contract_id=series_key.contract_id,
    )


def _metadata_partition_key(row: dict[str, object], *, series_mode: str) -> DerivedIndicatorFramePartitionKey:
    return DerivedIndicatorFramePartitionKey(
        dataset_version=str(row["dataset_version"]),
        indicator_set_version=str(row["indicator_set_version"]),
        derived_indicator_set_version=str(row["derived_indicator_set_version"]),
        timeframe=str(row["timeframe"]),
        instrument_id=str(row["instrument_id"]),
        contract_id=None if series_mode == "continuous_front" else str(row["contract_id"]),
    )


def _group_existing_partition_metadata(
    *,
    rows: list[dict[str, object]],
    series_mode: str,
) -> dict[DerivedIndicatorFramePartitionKey, list[dict[str, object]]]:
    grouped: dict[DerivedIndicatorFramePartitionKey, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(_metadata_partition_key(row, series_mode=series_mode), []).append(row)
    return grouped


def _derived_partition_key(
    *,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    series_key: DerivedSeriesKey,
    timeframe: str,
) -> DerivedIndicatorFramePartitionKey:
    return DerivedIndicatorFramePartitionKey(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        timeframe=timeframe,
        instrument_id=series_key.instrument_id,
        contract_id=series_key.contract_id,
    )


def _bars_hash(rows: list[ResearchBarView]) -> str:
    payload = [row.to_dict() for row in rows]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _indicator_source_bars_hash(rows: list[ResearchBarView]) -> str:
    payload = [
        {
            "contract_id": row.contract_id,
            "instrument_id": row.instrument_id,
            "timeframe": row.timeframe,
            "ts": row.ts,
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume,
            "open_interest": row.open_interest,
        }
        for row in rows
    ]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _indicator_hash(rows: list[IndicatorFrameRow], *, value_columns: tuple[str, ...] = DERIVED_SOURCE_INDICATOR_COLUMNS) -> str:
    payload = [
        {
            "contract_id": row.contract_id,
            "instrument_id": row.instrument_id,
            "timeframe": row.timeframe,
            "ts": row.ts,
            "source_bars_hash": row.source_bars_hash,
            "row_count": row.row_count,
            "values": {column: row.values.get(column) for column in value_columns},
        }
        for row in rows
    ]
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
                "output_columns_hash",
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
    for column in profile.output_columns:
        if column in result.columns or not column.startswith("distance_to_") or not column.endswith("_atr"):
            continue
        target_column = column.removeprefix("distance_to_").removesuffix("_atr")
        if target_column in result.columns:
            result[column] = _distance_to(result, target_column, denominator=atr)

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


def _profile_for_output_columns(profile: DerivedIndicatorProfile, columns: set[str]) -> DerivedIndicatorProfile:
    return DerivedIndicatorProfile(
        version=profile.version,
        description=profile.description,
        output_columns=tuple(column for column in profile.output_columns if column in columns),
        warmup_bars=profile.warmup_bars,
    )


def _existing_partition_matches(
    existing_rows: list[dict[str, object]],
    *,
    source_bars_hash: str,
    source_indicators_hash: str,
    profile_version: str,
    row_count: int,
    output_columns_hash: str,
    legacy_output_columns_match: bool = False,
) -> bool:
    if not existing_rows:
        return False
    head = existing_rows[0]
    existing_output_columns_hash = str(head.get("output_columns_hash") or "")
    output_hash_matches = (
        existing_output_columns_hash == output_columns_hash
        or (not existing_output_columns_hash and legacy_output_columns_match)
    )
    return (
        head.get("source_bars_hash") == source_bars_hash
        and head.get("source_indicators_hash") == source_indicators_hash
        and head.get("profile_version") == profile_version
        and int(head.get("row_count", -1) or -1) == row_count
        and output_hash_matches
    )


def _derived_value(column: str, value: object) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    value_type = "int" if column.endswith("_code") or column.endswith("_flag") else "double"
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
    source_dataset_bars_hash: str = "",
    output_columns_hash: str | None = None,
    compute_profile: DerivedIndicatorProfile | None = None,
    existing_rows: list[DerivedIndicatorFrameRow] | None = None,
) -> list[DerivedIndicatorFrameRow]:
    base_frame = _prepare_base_frame(bars=local_series, indicators=local_indicator_rows)
    current_timeframe = local_series[0].timeframe
    compute_profile = compute_profile or profile
    computed = _compute_derived_frame(
        base_frame=base_frame,
        current_timeframe=current_timeframe,
        source_frames=source_frames,
        profile=compute_profile,
    )
    payload_rows = computed.to_dict("records")
    output_columns = profile.output_columns
    computed_columns = set(compute_profile.output_columns)
    existing_by_ts = {row.ts: row for row in existing_rows or []}
    source_bars_hash = _bars_hash(local_series)
    source_indicators_hash = _indicator_hash(
        local_indicator_rows,
        value_columns=_source_indicator_columns_for_profile(profile),
    )
    source_indicator_profile_version = local_indicator_rows[0].profile_version if local_indicator_rows else ""
    source_indicator_output_columns_hash = local_indicator_rows[0].output_columns_hash if local_indicator_rows else ""
    output_columns_hash = output_columns_hash or derived_indicator_output_columns_hash(output_columns)
    prepared_values: list[tuple[ResearchBarView, dict[str, float | int | None], DerivedIndicatorFrameRow | None]] = []
    merged_payload_rows: list[dict[str, object]] = []

    for original, payload in zip(local_series, payload_rows, strict=True):
        existing = existing_by_ts.get(original.ts)
        values: dict[str, float | int | None] = {}
        for column in output_columns:
            if column in computed_columns:
                value = payload.get(column)
            elif existing is not None and column in existing.values:
                value = existing.values[column]
            else:
                value = None
            values[column] = _derived_value(column, value)
        prepared_values.append((original, values, existing))
        merged_payload_rows.append(values)

    null_warmup_span = _null_warmup_span(merged_payload_rows, output_columns)
    materialized_at = pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")

    rows: list[DerivedIndicatorFrameRow] = []
    for original, values, existing in prepared_values:
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
                values=values,
                source_bars_hash=source_bars_hash,
                source_dataset_bars_hash=source_dataset_bars_hash,
                source_indicators_hash=source_indicators_hash,
                source_indicator_profile_version=source_indicator_profile_version,
                source_indicator_output_columns_hash=source_indicator_output_columns_hash,
                row_count=len(local_series),
                warmup_span=profile.warmup_bars,
                null_warmup_span=null_warmup_span,
                created_at=existing.created_at if existing is not None else materialized_at,
                output_columns_hash=output_columns_hash,
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
    dataset_manifest = _load_dataset_manifest(output_dir=dataset_output_dir, dataset_version=dataset_version)
    series_mode = str(dataset_manifest.get("series_mode", "contract"))
    current_dataset_bars_hash = str(dataset_manifest.get("bars_hash") or "")
    registry: DerivedIndicatorProfileRegistry = build_derived_indicator_profile_registry()
    resolved_profile = profile or registry.get(profile_version or "core_v1")
    partition_counts = _load_bar_partition_counts(
        dataset_output_dir=dataset_output_dir,
        dataset_version=dataset_version,
        series_mode=series_mode,
    )
    bar_rows_cache: dict[tuple[DerivedSeriesKey, str], list[ResearchBarView]] = {}

    def _load_bars(series_key: DerivedSeriesKey, timeframe: str) -> list[ResearchBarView]:
        cache_key = (series_key, timeframe)
        if cache_key not in bar_rows_cache:
            bar_rows_cache[cache_key] = _load_bar_partition_rows(
                dataset_output_dir=dataset_output_dir,
                dataset_version=dataset_version,
                series_key=series_key,
                timeframe=timeframe,
            )
        return bar_rows_cache[cache_key]

    indicator_table_exists = (indicator_output_dir / "research_indicator_frames.delta" / "_delta_log").exists()
    indicator_metadata = load_indicator_partition_metadata(
        output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    ) if indicator_table_exists else []
    indicator_by_partition = _group_indicator_partition_metadata(rows=indicator_metadata, series_mode=series_mode)
    available_indicator_columns = set(
        existing_indicator_value_columns(output_dir=indicator_output_dir)
        ) if indicator_table_exists else set()
    required_source_indicator_columns = _source_indicator_columns_for_profile(resolved_profile)
    required_indicator_columns_available = set(required_source_indicator_columns) <= available_indicator_columns
    indicator_rows_cache: dict[tuple[DerivedSeriesKey, str], list[IndicatorFrameRow]] = {}

    def _load_indicator_rows(series_key: DerivedSeriesKey, timeframe: str) -> list[IndicatorFrameRow]:
        cache_key = (series_key, timeframe)
        if cache_key not in indicator_rows_cache:
            indicator_rows_cache[cache_key] = load_indicator_partition_rows(
                output_dir=indicator_output_dir,
                partition=_indicator_partition_key(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    series_key=series_key,
                    timeframe=timeframe,
                ),
                value_columns=required_source_indicator_columns,
            )
        return indicator_rows_cache[cache_key]

    table_exists = (derived_indicator_output_dir / "research_derived_indicator_frames.delta" / "_delta_log").exists()
    source_table_commit_ts = _latest_delta_commit_timestamp(dataset_output_dir / "research_bar_views.delta")
    derived_table_commit_ts = (
        _latest_delta_commit_timestamp(derived_indicator_output_dir / "research_derived_indicator_frames.delta")
        if table_exists else None
    )
    legacy_table_covers_source_commit = bool(
        source_table_commit_ts is not None
        and derived_table_commit_ts is not None
        and derived_table_commit_ts >= source_table_commit_ts
    )
    existing_metadata = load_derived_indicator_partition_metadata(
        output_dir=derived_indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
    ) if table_exists else []
    existing_by_partition = _group_existing_partition_metadata(rows=existing_metadata, series_mode=series_mode)
    target_output_columns = resolved_profile.output_columns
    target_output_columns_hash = derived_indicator_output_columns_hash(target_output_columns)
    existing_output_columns = set(
        existing_derived_indicator_value_columns(output_dir=derived_indicator_output_dir)
    ) if table_exists else set()
    reusable_existing_columns = tuple(
        column for column in target_output_columns if column in existing_output_columns
    )
    legacy_output_columns_match = set(target_output_columns).issubset(existing_output_columns)

    current_partitions: set[DerivedIndicatorFramePartitionKey] = set()
    replace_partitions: list[DerivedIndicatorFramePartitionKey] = []
    refresh_plan: list[
        tuple[
            DerivedSeriesKey,
            dict[str, list[ResearchBarView]],
            list[tuple[str, list[ResearchBarView], DerivedIndicatorProfile, list[DerivedIndicatorFrameRow] | None]],
        ]
    ] = []
    reused_partitions = 0
    refreshed_partitions = 0
    extended_partitions = 0
    recomputed_partitions = 0

    for series_key, counts_by_timeframe in sorted(
        partition_counts.items(),
        key=lambda item: (item[0].instrument_id, item[0].contract_id or ""),
    ):
        refreshed_timeframes: list[
            tuple[str, list[ResearchBarView], DerivedIndicatorProfile, list[DerivedIndicatorFrameRow] | None]
        ] = []
        for timeframe, row_count in sorted(counts_by_timeframe.items(), key=lambda item: item[0]):
            partition_key = _derived_partition_key(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                derived_indicator_set_version=derived_indicator_set_version,
                series_key=series_key,
                timeframe=timeframe,
            )
            current_partitions.add(partition_key)
            existing_partition_metadata = existing_by_partition.get(partition_key, [])
            head = existing_partition_metadata[0] if existing_partition_metadata else None
            existing_row_count = int(head.get("row_count", -1) or -1) if head else -1
            existing_source_dataset_bars_hash = str(head.get("source_dataset_bars_hash") or "") if head else ""
            local_series: list[ResearchBarView] | None = None
            source_bars_hash = str(head.get("source_bars_hash") or "") if head else ""
            source_bars_may_have_changed = (
                not source_bars_hash
                or existing_row_count != row_count
                or bool(
                    existing_source_dataset_bars_hash
                    and current_dataset_bars_hash
                    and existing_source_dataset_bars_hash != current_dataset_bars_hash
                )
                or bool(
                    not existing_source_dataset_bars_hash
                    and table_exists
                    and not legacy_table_covers_source_commit
                    and existing_row_count != row_count
                )
            )
            if source_bars_may_have_changed:
                local_series = _load_bars(series_key, timeframe)
                source_bars_hash = _bars_hash(local_series)
            indicator_partition_metadata = indicator_by_partition.get((series_key, timeframe))
            indicator_source_unchanged = bool(indicator_partition_metadata) and (
                int(indicator_partition_metadata.get("row_count", -1) or -1) == row_count
                and required_indicator_columns_available
            )
            source_indicator_profile_version = str(
                indicator_partition_metadata.get("profile_version") or ""
            ) if indicator_partition_metadata else ""
            source_indicator_output_columns_hash = str(
                indicator_partition_metadata.get("output_columns_hash") or ""
            ) if indicator_partition_metadata else ""
            stored_indicator_metadata_matches = bool(head) and (
                head.get("source_indicator_profile_version") == source_indicator_profile_version
                and head.get("source_indicator_output_columns_hash") == source_indicator_output_columns_hash
            )
            legacy_indicator_hash_reusable = bool(
                indicator_source_unchanged
                and head
                and head.get("source_indicators_hash")
                and not head.get("source_indicator_profile_version")
                and not head.get("source_indicator_output_columns_hash")
            )
            if indicator_source_unchanged and head and (stored_indicator_metadata_matches or legacy_indicator_hash_reusable):
                source_indicators_hash = str(head.get("source_indicators_hash"))
            else:
                source_indicators_hash = _indicator_hash(
                    _load_indicator_rows(series_key, timeframe),
                    value_columns=required_source_indicator_columns,
                )
            if _existing_partition_matches(
                existing_partition_metadata,
                source_bars_hash=source_bars_hash,
                source_indicators_hash=source_indicators_hash,
                profile_version=resolved_profile.version,
                row_count=row_count,
                output_columns_hash=target_output_columns_hash,
                legacy_output_columns_match=legacy_output_columns_match,
            ):
                reused_partitions += 1
                continue

            source_unchanged = bool(head) and (
                head.get("source_bars_hash") == source_bars_hash
                and head.get("source_indicators_hash") == source_indicators_hash
                and int(head.get("row_count", -1) or -1) == row_count
            )
            missing_columns = set(target_output_columns) - existing_output_columns
            can_extend_from_existing = bool(source_unchanged and missing_columns and reusable_existing_columns)
            if local_series is None:
                local_series = _load_bars(series_key, timeframe)
            if can_extend_from_existing:
                compute_profile = _profile_for_output_columns(resolved_profile, missing_columns)
                existing_rows = load_derived_indicator_partition_rows(
                    output_dir=derived_indicator_output_dir,
                    partition=partition_key,
                    value_columns=reusable_existing_columns,
                )
                extended_partitions += 1
            else:
                compute_profile = resolved_profile
                existing_rows = None
                recomputed_partitions += 1

            replace_partitions.append(partition_key)
            refreshed_timeframes.append((timeframe, local_series, compute_profile, existing_rows))
            refreshed_partitions += 1
        if refreshed_timeframes:
            bars_by_timeframe = {
                timeframe: _load_bars(series_key, timeframe)
                for timeframe in counts_by_timeframe
            }
            refresh_plan.append((series_key, bars_by_timeframe, refreshed_timeframes))

    deleted_partitions = tuple(
        partition
        for partition in existing_by_partition
        if partition not in current_partitions
    )
    replace_partitions.extend(deleted_partitions)

    current_total_rows = sum(
        row_count
        for counts_by_timeframe in partition_counts.values()
        for row_count in counts_by_timeframe.values()
    )
    output_paths = {"research_derived_indicator_frames": (derived_indicator_output_dir / "research_derived_indicator_frames.delta").as_posix()}
    refreshed_row_count = 0
    batch_count = 0
    if replace_partitions or not table_exists:
        def _row_batches() -> Iterator[list[DerivedIndicatorFrameRow]]:
            for series_key, bars_by_timeframe, refreshed_timeframes in refresh_plan:
                source_frames = {
                    timeframe: _prepare_base_frame(
                        bars=bars_by_timeframe[timeframe],
                        indicators=_load_indicator_rows(series_key, timeframe),
                    )
                    for timeframe in bars_by_timeframe
                }
                for timeframe, local_series, compute_profile, existing_rows in refreshed_timeframes:
                    yield _build_partition_rows(
                        dataset_version=dataset_version,
                        indicator_set_version=indicator_set_version,
                        derived_indicator_set_version=derived_indicator_set_version,
                        profile=resolved_profile,
                        local_series=local_series,
                        local_indicator_rows=_load_indicator_rows(series_key, timeframe),
                        source_frames=source_frames,
                        source_dataset_bars_hash=current_dataset_bars_hash,
                        output_columns_hash=target_output_columns_hash,
                        compute_profile=compute_profile,
                        existing_rows=existing_rows,
                    )

        output_paths, refreshed_row_count, batch_count = write_derived_indicator_frame_batches(
            output_dir=derived_indicator_output_dir,
            row_batches=_row_batches(),
            replace_partitions=tuple(replace_partitions),
            profile=resolved_profile,
        )
    return {
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "derived_indicator_set_version": derived_indicator_set_version,
        "profile_version": resolved_profile.version,
        "derived_indicator_row_count": current_total_rows,
        "refreshed_row_count": refreshed_row_count,
        "refreshed_partition_count": refreshed_partitions,
        "reused_partition_count": reused_partitions,
        "extended_partition_count": extended_partitions,
        "recomputed_partition_count": recomputed_partitions,
        "deleted_partition_count": len(deleted_partitions),
        "write_batch_count": batch_count,
        "output_columns_hash": target_output_columns_hash,
        "loaded_indicator_partition_count": len(indicator_rows_cache),
        "output_paths": output_paths,
        "delta_manifest": research_derived_indicator_store_contract(profile=resolved_profile),
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
