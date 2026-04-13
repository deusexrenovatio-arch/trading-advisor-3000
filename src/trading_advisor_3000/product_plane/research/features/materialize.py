from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView, load_materialized_research_dataset
from trading_advisor_3000.product_plane.research.indicators import IndicatorFrameRow, load_indicator_frames

from .derived import FeatureProfile, FeatureProfileRegistry, FeatureSpec, build_feature_profile_registry, phase1_feature_profile
from .store import (
    FeatureFramePartitionKey,
    FeatureFrameRow,
    load_feature_frames,
    phase2b_feature_store_contract,
    write_feature_frames,
)


@dataclass(frozen=True)
class FeatureSeriesKey:
    instrument_id: str
    contract_id: str | None


def _timeframe_delta(timeframe: str) -> pd.Timedelta:
    if timeframe.endswith("m"):
        return pd.Timedelta(minutes=int(timeframe[:-1]))
    if timeframe.endswith("h"):
        return pd.Timedelta(hours=int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return pd.Timedelta(days=int(timeframe[:-1]))
    if timeframe.endswith("w"):
        return pd.Timedelta(weeks=int(timeframe[:-1]))
    raise ValueError(f"unsupported timeframe token: {timeframe}")


def _series_key_from_bar(row: ResearchBarView, *, series_mode: str) -> FeatureSeriesKey:
    return FeatureSeriesKey(
        instrument_id=row.instrument_id,
        contract_id=None if series_mode == "continuous_front" else row.contract_id,
    )


def _series_key_from_indicator(row: IndicatorFrameRow, *, series_mode: str) -> FeatureSeriesKey:
    return FeatureSeriesKey(
        instrument_id=row.instrument_id,
        contract_id=None if series_mode == "continuous_front" else row.contract_id,
    )


def _group_bar_views(
    *,
    bar_views: list[ResearchBarView],
    series_mode: str,
) -> dict[FeatureSeriesKey, dict[str, list[ResearchBarView]]]:
    grouped: dict[FeatureSeriesKey, dict[str, list[ResearchBarView]]] = {}
    for row in sorted(bar_views, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe, item.ts)):
        series_key = _series_key_from_bar(row, series_mode=series_mode)
        grouped.setdefault(series_key, {}).setdefault(row.timeframe, []).append(row)
    return grouped


def _group_indicator_rows(
    *,
    rows: list[IndicatorFrameRow],
    series_mode: str,
) -> dict[FeatureSeriesKey, dict[str, list[IndicatorFrameRow]]]:
    grouped: dict[FeatureSeriesKey, dict[str, list[IndicatorFrameRow]]] = {}
    for row in sorted(rows, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe, item.ts)):
        series_key = _series_key_from_indicator(row, series_mode=series_mode)
        grouped.setdefault(series_key, {}).setdefault(row.timeframe, []).append(row)
    return grouped


def _group_existing_rows(
    *,
    rows: list[FeatureFrameRow],
    series_mode: str,
) -> dict[FeatureFramePartitionKey, list[FeatureFrameRow]]:
    grouped: dict[FeatureFramePartitionKey, list[FeatureFrameRow]] = {}
    for row in sorted(rows, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe, item.ts)):
        grouped.setdefault(row.partition_key(series_mode=series_mode), []).append(row)
    return grouped


def _bars_hash(rows: list[ResearchBarView]) -> str:
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
            "session_date": row.session_date,
            "active_contract_id": row.active_contract_id,
        }
        for row in rows
    ]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _indicator_hash(rows: list[IndicatorFrameRow]) -> str:
    payload = [
        {
            "contract_id": row.contract_id,
            "instrument_id": row.instrument_id,
            "timeframe": row.timeframe,
            "ts": row.ts,
            **row.values,
            "profile_version": row.profile_version,
            "source_bars_hash": row.source_bars_hash,
            "row_count": row.row_count,
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
        raise ValueError("feature materialization requires materialized indicators for every series/timeframe partition")

    bar_frame = pd.DataFrame([row.to_dict() for row in bars])
    indicator_frame = pd.DataFrame([row.to_dict() for row in indicators])
    keep_columns = [
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        *sorted(column for column in indicator_frame.columns if column not in {
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
        }),
    ]
    merged = bar_frame.merge(
        indicator_frame[keep_columns],
        on=["contract_id", "instrument_id", "timeframe", "ts"],
        how="left",
        validate="one_to_one",
    )
    return merged.sort_values("ts").reset_index(drop=True)


def _validate_required_columns(frame: pd.DataFrame, spec: FeatureSpec) -> None:
    missing = [column for column in spec.required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"feature `{spec.feature_id}` missing required columns: {', '.join(missing)}")


def _none_outputs(frame: pd.DataFrame, spec: FeatureSpec) -> dict[str, pd.Series]:
    return {
        column: pd.Series([None] * len(frame), index=frame.index, dtype="object")
        for column in spec.output_columns
    }


def _int_code_series(values: list[int | None], *, index: pd.Index) -> pd.Series:
    return pd.Series(values, index=index, dtype="object")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    sanitized = denominator.replace({0.0: pd.NA})
    return numerator / sanitized


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


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
            candidate_high = highs[candidate_index]
            candidate_low = lows[candidate_index]
            if candidate_high == max(high_window):
                last_high = float(candidate_high)
            if candidate_low == min(low_window):
                last_low = float(candidate_low)
        confirmed_highs.append(last_high)
        confirmed_lows.append(last_low)

    return {
        "swing_high_10": pd.Series(confirmed_highs, index=frame.index, dtype="object"),
        "swing_low_10": pd.Series(confirmed_lows, index=frame.index, dtype="object"),
    }


def _compute_session_vwap(frame: pd.DataFrame) -> pd.Series:
    typical_price = (frame["high"] + frame["low"] + frame["close"]) / 3.0
    weighted = typical_price * frame["volume"]
    session_cum_weight = weighted.groupby(frame["session_date"], sort=False).cumsum()
    session_cum_volume = frame["volume"].groupby(frame["session_date"], sort=False).cumsum()
    return _safe_divide(session_cum_weight, session_cum_volume)


def _compute_mtf_overlay(
    *,
    frame: pd.DataFrame,
    spec: FeatureSpec,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.Series], tuple[str, ...]]:
    params = spec.params_dict()
    source_timeframe = str(params["source_timeframe"])
    target_timeframe = str(params["target_timeframe"])
    if current_timeframe != target_timeframe:
        return _none_outputs(frame, spec), tuple()

    source_frame = source_frames.get(source_timeframe)
    if source_frame is None or source_frame.empty:
        return _none_outputs(frame, spec), tuple()

    fast_column = str(params["fast_column"])
    slow_column = str(params["slow_column"])
    adx_column = str(params["adx_column"])
    rsi_column = str(params["rsi_column"])
    required = {fast_column, slow_column, adx_column, rsi_column, "ts"}
    if not required <= set(source_frame.columns):
        return _none_outputs(frame, spec), tuple()

    source_relation: list[int | None] = []
    source_trend: list[int | None] = []
    for fast_value, slow_value in zip(source_frame[fast_column].tolist(), source_frame[slow_column].tolist(), strict=True):
        if pd.isna(fast_value) or pd.isna(slow_value):
            source_relation.append(None)
            source_trend.append(None)
        elif float(fast_value) > float(slow_value):
            source_relation.append(1)
            source_trend.append(1)
        elif float(fast_value) < float(slow_value):
            source_relation.append(-1)
            source_trend.append(-1)
        else:
            source_relation.append(0)
            source_trend.append(0)

    current_close_ts = pd.to_datetime(frame["ts"], utc=True) + _timeframe_delta(current_timeframe)
    source_close_ts = pd.to_datetime(source_frame["ts"], utc=True) + _timeframe_delta(source_timeframe)
    left = pd.DataFrame({"_row_index": frame.index, "current_close_ts": current_close_ts}).sort_values("current_close_ts")
    right = pd.DataFrame(
        {
            "source_close_ts": source_close_ts,
            "htf_ma_relation_code": source_relation,
            "htf_trend_state_code": source_trend,
            "htf_adx_14": source_frame[adx_column],
            "htf_rsi_14": source_frame[rsi_column],
        }
    ).sort_values("source_close_ts")

    merged = pd.merge_asof(
        left,
        right,
        left_on="current_close_ts",
        right_on="source_close_ts",
        direction="backward",
    ).set_index("_row_index").reindex(frame.index)
    return (
        {
            "htf_ma_relation_code": merged["htf_ma_relation_code"].astype("object"),
            "htf_trend_state_code": merged["htf_trend_state_code"].astype("object"),
            "htf_adx_14": merged["htf_adx_14"],
            "htf_rsi_14": merged["htf_rsi_14"],
        },
        spec.output_columns,
    )


def _compute_spec(
    *,
    frame: pd.DataFrame,
    spec: FeatureSpec,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.Series], tuple[str, ...]]:
    if spec.operation_key == "mtf_overlay":
        return _compute_mtf_overlay(
            frame=frame,
            spec=spec,
            current_timeframe=current_timeframe,
            source_frames=source_frames,
        )

    _validate_required_columns(frame, spec)
    params = spec.params_dict()

    if spec.operation_key == "trend_state_fast_slow":
        fast = _numeric(frame[str(params["fast_column"])])
        slow = _numeric(frame[str(params["slow_column"])])
        atr = _numeric(frame[str(params["atr_column"])])
        threshold = atr.fillna(0.0) * float(params["flat_band_atr"])
        codes: list[int | None] = []
        for fast_value, slow_value, threshold_value in zip(fast.tolist(), slow.tolist(), threshold.tolist(), strict=True):
            if pd.isna(fast_value) or pd.isna(slow_value):
                codes.append(None)
            else:
                diff = float(fast_value) - float(slow_value)
                if diff > float(threshold_value):
                    codes.append(1)
                elif diff < -float(threshold_value):
                    codes.append(-1)
                else:
                    codes.append(0)
        return {"trend_state_fast_slow_code": _int_code_series(codes, index=frame.index)}, spec.output_columns

    if spec.operation_key == "trend_strength":
        fast = _numeric(frame[str(params["fast_column"])])
        slow = _numeric(frame[str(params["slow_column"])])
        volatility = _numeric(frame[str(params["volatility_column"])]).replace({0.0: pd.NA})
        return {"trend_strength": (fast - slow).abs() / volatility}, spec.output_columns

    if spec.operation_key == "ma_stack_state":
        fast = _numeric(frame[str(params["fast_column"])])
        mid = _numeric(frame[str(params["mid_column"])])
        slow = _numeric(frame[str(params["slow_column"])])
        codes: list[int | None] = []
        for fast_value, mid_value, slow_value in zip(fast.tolist(), mid.tolist(), slow.tolist(), strict=True):
            if pd.isna(fast_value) or pd.isna(mid_value) or pd.isna(slow_value):
                codes.append(None)
            elif float(fast_value) > float(mid_value) > float(slow_value):
                codes.append(2)
            elif float(fast_value) < float(mid_value) < float(slow_value):
                codes.append(-2)
            elif float(mid_value) > float(slow_value):
                codes.append(1)
            elif float(mid_value) < float(slow_value):
                codes.append(-1)
            else:
                codes.append(0)
        return {"ma_stack_state_code": _int_code_series(codes, index=frame.index)}, spec.output_columns

    if spec.operation_key == "rolling_levels":
        length = int(params["length"])
        return {
            "rolling_high_20": frame["high"].rolling(window=length, min_periods=length).max(),
            "rolling_low_20": frame["low"].rolling(window=length, min_periods=length).min(),
        }, spec.output_columns

    if spec.operation_key == "opening_range":
        return _compute_opening_range(frame, opening_bars=int(params["opening_bars"])), spec.output_columns

    if spec.operation_key == "swing_levels":
        return _compute_swing_levels(frame, left_bars=int(params["left_bars"]), right_bars=int(params["right_bars"])), spec.output_columns

    if spec.operation_key == "session_vwap":
        return {"session_vwap": _compute_session_vwap(frame)}, spec.output_columns

    if spec.operation_key == "level_distances":
        atr = _numeric(frame[str(params["volatility_column"])]).replace({0.0: pd.NA})
        close = _numeric(frame["close"])
        return {
            "distance_to_session_vwap": (close - frame["session_vwap"]) / atr,
            "distance_to_rolling_high_20": (close - frame["rolling_high_20"]) / atr,
            "distance_to_rolling_low_20": (close - frame["rolling_low_20"]) / atr,
        }, spec.output_columns

    if spec.operation_key == "volatility_context":
        kc_upper = _numeric(frame["kc_upper_20_1_5"])
        kc_mid = _numeric(frame["kc_mid_20_1_5"])
        kc_lower = _numeric(frame["kc_lower_20_1_5"])
        kc_width = _safe_divide(kc_upper - kc_lower, kc_mid.abs())
        squeeze: list[int | None] = []
        breakout: list[int | None] = []
        distance_limit = float(params["breakout_distance_atr"])
        for bb_upper, bb_lower, kc_upper, kc_lower, trend_state, close, atr, rolling_high, rolling_low in zip(
            _numeric(frame["bb_upper_20_2"]).tolist(),
            _numeric(frame["bb_lower_20_2"]).tolist(),
            kc_upper.tolist(),
            kc_lower.tolist(),
            frame["trend_state_fast_slow_code"].tolist(),
            _numeric(frame["close"]).tolist(),
            _numeric(frame["atr_14"]).tolist(),
            _numeric(frame["rolling_high_20"]).tolist(),
            _numeric(frame["rolling_low_20"]).tolist(),
            strict=True,
        ):
            if any(pd.isna(item) for item in (bb_upper, bb_lower, kc_upper, kc_lower)):
                squeeze.append(None)
                breakout.append(None)
                continue
            squeeze_on = int(float(bb_upper) <= float(kc_upper) and float(bb_lower) >= float(kc_lower))
            squeeze.append(squeeze_on)
            if squeeze_on != 1 or any(pd.isna(item) for item in (trend_state, close, atr, rolling_high, rolling_low)) or float(atr) == 0.0:
                breakout.append(0 if squeeze_on == 1 else None)
            else:
                bullish_distance = abs(float(rolling_high) - float(close)) / float(atr)
                bearish_distance = abs(float(close) - float(rolling_low)) / float(atr)
                if int(trend_state) > 0 and bullish_distance <= distance_limit:
                    breakout.append(1)
                elif int(trend_state) < 0 and bearish_distance <= distance_limit:
                    breakout.append(-1)
                else:
                    breakout.append(0)
        return {
            "bb_width_20_2": frame["bb_width_20_2"],
            "kc_width_20_1_5": kc_width,
            "squeeze_on_code": _int_code_series(squeeze, index=frame.index),
            "breakout_ready_state_code": _int_code_series(breakout, index=frame.index),
        }, spec.output_columns

    if spec.operation_key == "volume_context":
        high_rvol = float(params["high_rvol_threshold"])
        low_rvol = float(params["low_rvol_threshold"])
        above_below: list[int | None] = []
        session_volume_state: list[int | None] = []
        for close, vwma, rvol, volume_z in zip(
            _numeric(frame["close"]).tolist(),
            _numeric(frame["vwma_20"]).tolist(),
            _numeric(frame["rvol_20"]).tolist(),
            _numeric(frame["volume_z_20"]).tolist(),
            strict=True,
        ):
            if pd.isna(close) or pd.isna(vwma):
                above_below.append(None)
            elif float(close) > float(vwma):
                above_below.append(1)
            elif float(close) < float(vwma):
                above_below.append(-1)
            else:
                above_below.append(0)

            if pd.isna(rvol):
                session_volume_state.append(None)
            elif float(rvol) >= high_rvol or (not pd.isna(volume_z) and float(volume_z) >= 1.0):
                session_volume_state.append(1)
            elif float(rvol) <= low_rvol or (not pd.isna(volume_z) and float(volume_z) <= -1.0):
                session_volume_state.append(-1)
            else:
                session_volume_state.append(0)
        return {
            "rvol_20": frame["rvol_20"],
            "volume_zscore_20": frame["volume_z_20"],
            "above_below_vwma_code": _int_code_series(above_below, index=frame.index),
            "session_volume_state_code": _int_code_series(session_volume_state, index=frame.index),
        }, spec.output_columns

    if spec.operation_key == "regime_state":
        adx_threshold = float(params["adx_threshold"])
        trend_threshold = float(params["trend_strength_threshold"])
        codes: list[int | None] = []
        for trend_state, trend_strength, adx, squeeze in zip(
            frame["trend_state_fast_slow_code"].tolist(),
            _numeric(frame["trend_strength"]).tolist(),
            _numeric(frame["adx_14"]).tolist(),
            frame["squeeze_on_code"].tolist(),
            strict=True,
        ):
            if pd.isna(trend_state) or pd.isna(trend_strength) or pd.isna(adx) or pd.isna(squeeze):
                codes.append(None)
            elif int(squeeze) == 1:
                codes.append(2)
            elif float(adx) >= adx_threshold and float(trend_strength) >= trend_threshold and int(trend_state) > 0:
                codes.append(1)
            elif float(adx) >= adx_threshold and float(trend_strength) >= trend_threshold and int(trend_state) < 0:
                codes.append(-1)
            else:
                codes.append(0)
        return {"regime_state_code": _int_code_series(codes, index=frame.index)}, spec.output_columns

    raise ValueError(f"unsupported feature operation: {spec.operation_key}")


def _compute_profile_frame(
    *,
    base_frame: pd.DataFrame,
    profile: FeatureProfile,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    result = base_frame.copy()
    active_outputs: list[str] = []
    for spec in profile.features:
        computed, active = _compute_spec(
            frame=result,
            spec=spec,
            current_timeframe=current_timeframe,
            source_frames=source_frames,
        )
        for column, series in computed.items():
            result[column] = series
        for column in active:
            if column not in active_outputs:
                active_outputs.append(column)
    return result, tuple(active_outputs)


def _null_warmup_span(payload_rows: list[dict[str, object]], output_columns: tuple[str, ...]) -> int:
    if not output_columns:
        return 0
    count = 0
    for row in payload_rows:
        if any(row.get(column) is None or pd.isna(row.get(column)) for column in output_columns):
            count += 1
        else:
            break
    return count


def _feature_value(output_column: str, value: object) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    value_type = phase2b_feature_store_contract()["research_feature_frames"]["columns"][output_column]
    if value_type == "int":
        return int(value)
    return float(value)


def _partition_indicator_rows_for_hash(
    *,
    current_timeframe: str,
    profile: FeatureProfile,
    local_indicator_rows: list[IndicatorFrameRow],
    indicator_rows_by_timeframe: dict[str, list[IndicatorFrameRow]],
) -> list[IndicatorFrameRow]:
    rows = list(local_indicator_rows)
    for spec in profile.features:
        if spec.operation_key != "mtf_overlay":
            continue
        params = spec.params_dict()
        if str(params["target_timeframe"]) == current_timeframe:
            rows.extend(indicator_rows_by_timeframe.get(str(params["source_timeframe"]), []))
    dedup: dict[tuple[str, str, str, str], IndicatorFrameRow] = {}
    for row in rows:
        dedup[(row.contract_id, row.instrument_id, row.timeframe, row.ts)] = row
    return [dedup[key] for key in sorted(dedup)]


def _build_partition_rows(
    *,
    dataset_version: str,
    indicator_set_version: str,
    feature_set_version: str,
    profile: FeatureProfile,
    local_series: list[ResearchBarView],
    local_indicator_rows: list[IndicatorFrameRow],
    indicator_rows_by_timeframe: dict[str, list[IndicatorFrameRow]],
    source_frames: dict[str, pd.DataFrame],
) -> list[FeatureFrameRow]:
    base_frame = _prepare_base_frame(bars=local_series, indicators=local_indicator_rows)
    current_timeframe = local_series[0].timeframe
    computed, active_output_columns = _compute_profile_frame(
        base_frame=base_frame,
        profile=profile,
        current_timeframe=current_timeframe,
        source_frames=source_frames,
    )
    payload_rows = computed.to_dict("records")
    source_bars_hash = _bars_hash(local_series)
    source_indicators_hash = _indicator_hash(
        _partition_indicator_rows_for_hash(
            current_timeframe=current_timeframe,
            profile=profile,
            local_indicator_rows=local_indicator_rows,
            indicator_rows_by_timeframe=indicator_rows_by_timeframe,
        )
    )
    null_warmup_span = _null_warmup_span(payload_rows, active_output_columns)
    created_at = pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")
    output_columns = profile.expected_output_columns()

    rows: list[FeatureFrameRow] = []
    for original, payload in zip(local_series, payload_rows, strict=True):
        rows.append(
            FeatureFrameRow(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                feature_set_version=feature_set_version,
                profile_version=profile.version,
                contract_id=original.contract_id,
                instrument_id=original.instrument_id,
                timeframe=original.timeframe,
                ts=original.ts,
                values={column: _feature_value(column, payload.get(column)) for column in output_columns},
                source_bars_hash=source_bars_hash,
                source_indicators_hash=source_indicators_hash,
                row_count=len(local_series),
                warmup_span=profile.max_warmup_bars(),
                null_warmup_span=null_warmup_span,
                created_at=created_at,
            )
        )
    return rows


def _existing_partition_matches(
    existing_rows: list[FeatureFrameRow],
    *,
    source_bars_hash: str,
    source_indicators_hash: str,
    profile_version: str,
    row_count: int,
) -> bool:
    if not existing_rows:
        return False
    head = existing_rows[0]
    return (
        head.source_bars_hash == source_bars_hash
        and head.source_indicators_hash == source_indicators_hash
        and head.profile_version == profile_version
        and head.row_count == row_count
    )


def build_feature_frames(
    *,
    dataset_version: str,
    indicator_set_version: str,
    feature_set_version: str,
    bar_views: list[ResearchBarView],
    indicator_rows: list[IndicatorFrameRow],
    series_mode: str = "contract",
    profile: FeatureProfile | None = None,
) -> list[FeatureFrameRow]:
    profile = profile or phase1_feature_profile()
    grouped_bars = _group_bar_views(bar_views=bar_views, series_mode=series_mode)
    grouped_indicators = _group_indicator_rows(rows=indicator_rows, series_mode=series_mode)

    rows: list[FeatureFrameRow] = []
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
                    feature_set_version=feature_set_version,
                    profile=profile,
                    local_series=local_series,
                    local_indicator_rows=indicators_by_timeframe.get(timeframe, []),
                    indicator_rows_by_timeframe=indicators_by_timeframe,
                    source_frames=source_frames,
                )
            )
    return rows


def materialize_feature_frames(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    feature_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    feature_set_version: str,
    profile: FeatureProfile | None = None,
    profile_version: str | None = None,
) -> dict[str, object]:
    loaded = load_materialized_research_dataset(output_dir=dataset_output_dir, dataset_version=dataset_version)
    series_mode = str(loaded["dataset_manifest"].get("series_mode", "contract"))
    indicator_rows = load_indicator_frames(
        output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    )
    registry: FeatureProfileRegistry = build_feature_profile_registry()
    resolved_profile = profile or registry.get(profile_version or "core_v1")

    grouped_bars = _group_bar_views(bar_views=loaded["bar_views"], series_mode=series_mode)
    grouped_indicators = _group_indicator_rows(rows=indicator_rows, series_mode=series_mode)
    existing_rows = load_feature_frames(
        output_dir=feature_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
    ) if (feature_output_dir / "research_feature_frames.delta" / "_delta_log").exists() else []
    existing_by_partition = _group_existing_rows(rows=existing_rows, series_mode=series_mode)

    refreshed_rows: list[FeatureFrameRow] = []
    replace_partitions: list[FeatureFramePartitionKey] = []
    current_partition_keys: set[FeatureFramePartitionKey] = set()
    refreshed_partitions = 0
    reused_partitions = 0

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
            partition_key = FeatureFramePartitionKey(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                feature_set_version=feature_set_version,
                timeframe=timeframe,
                instrument_id=series_key.instrument_id,
                contract_id=series_key.contract_id,
            )
            current_partition_keys.add(partition_key)
            local_indicator_rows = indicators_by_timeframe.get(timeframe, [])
            source_bars_hash = _bars_hash(local_series)
            source_indicators_hash = _indicator_hash(
                _partition_indicator_rows_for_hash(
                    current_timeframe=timeframe,
                    profile=resolved_profile,
                    local_indicator_rows=local_indicator_rows,
                    indicator_rows_by_timeframe=indicators_by_timeframe,
                )
            )
            if _existing_partition_matches(
                existing_by_partition.get(partition_key, []),
                source_bars_hash=source_bars_hash,
                source_indicators_hash=source_indicators_hash,
                profile_version=resolved_profile.version,
                row_count=len(local_series),
            ):
                reused_partitions += 1
                continue
            refreshed_rows.extend(
                _build_partition_rows(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    feature_set_version=feature_set_version,
                    profile=resolved_profile,
                    local_series=local_series,
                    local_indicator_rows=local_indicator_rows,
                    indicator_rows_by_timeframe=indicators_by_timeframe,
                    source_frames=source_frames,
                )
            )
            replace_partitions.append(partition_key)
            refreshed_partitions += 1

    stale_partitions = [key for key in existing_by_partition if key not in current_partition_keys]
    replace_partitions.extend(stale_partitions)
    output_paths = write_feature_frames(
        output_dir=feature_output_dir,
        rows=refreshed_rows,
        replace_partitions=tuple(dict.fromkeys(replace_partitions)),
    )
    return {
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "feature_set_version": feature_set_version,
        "profile_version": resolved_profile.version,
        "feature_row_count": len(refreshed_rows),
        "refreshed_partition_count": refreshed_partitions,
        "reused_partition_count": reused_partitions,
        "deleted_partition_count": len(stale_partitions),
        "output_paths": output_paths,
        "delta_manifest": phase2b_feature_store_contract(),
    }


def reload_feature_frames(
    *,
    feature_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    feature_set_version: str,
) -> list[FeatureFrameRow]:
    return load_feature_frames(
        output_dir=feature_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        feature_set_version=feature_set_version,
    )
