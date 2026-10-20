from __future__ import annotations

import hashlib
import json
import math
import os
from bisect import bisect_right
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, replace
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import AbstractSet

import numpy as np
import pandas as pd
import pandas_ta_classic as ta

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    delta_table_columns,
    delta_table_identity,
    delta_table_version,
    iter_delta_table_row_batches,
    read_delta_table_arrow,
    read_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.bar_usage_policy import (
    EVENT_UPDATE_ZERO,
    INDICATOR_USAGE_POLICY_ID,
    POINT_UPDATE_NULL,
    STATE_UPDATE_HOLD,
    STATE_UPDATE_RESET_SCOPE,
    BarUsageCalculationRule,
    assert_indicator_bar_usage_policy_coverage,
    has_required_bar_usage_flags,
    indicator_bar_usage_groups_for_outputs,
)
from trading_advisor_3000.product_plane.research.continuous_front_indicators.rules import (
    assert_rule_coverage,
    rules_for_indicator_profile,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.datasets.bar_usage import BAR_USAGE_POLICY_ID
from trading_advisor_3000.product_plane.runtime.stage_timings import (
    StageTimings,
    record_skipped_stage,
    record_stage_timing,
    stage_timer,
)

from ..compute_contract import native_compute_scope
from .registry import (
    IndicatorProfile,
    IndicatorProfileRegistry,
    IndicatorSpec,
    build_indicator_profile_registry,
    default_indicator_profile,
)
from .store import (
    IndicatorFramePartitionKey,
    IndicatorFrameRow,
    existing_indicator_value_columns,
    indicator_output_columns_hash,
    indicator_store_contract,
    load_indicator_frames,
    load_indicator_partition_metadata,
    load_indicator_partition_rows,
    write_indicator_frame_partition_batches,
)
from .volume_profile import (
    VOLUME_PROFILE_INDICATOR_COLUMNS,
    VOLUME_PROFILE_INT_COLUMNS,
    VP_QUALITY_NO_SOURCE,
    compute_volume_profile_features,
)

INDICATOR_FRAME_MATERIALIZATION_LOCK_FILENAME = ".research_indicator_frames.lock.json"


def _resolved_series_mode(row_series_mode: object, *, requested_series_mode: str) -> str:
    row_mode = str(row_series_mode or "")
    if requested_series_mode == "continuous_front" and row_mode in {"", "contract"}:
        return "continuous_front"
    return row_mode or requested_series_mode


def _bars_hash(
    rows: list[ResearchBarView], *, adjustment_ladder_rows: tuple[dict[str, object], ...] = ()
) -> str:
    # Preserve the existing canonical JSON hash without duplicating a full series.
    digest = hashlib.sha256()
    digest.update(b'{"adjustment_ladder":')
    digest.update(_canonical_json_bytes(_normalize_ladder_rows(adjustment_ladder_rows)))
    digest.update(b',"bars":[')
    for index, row in enumerate(rows):
        if index:
            digest.update(b",")
        digest.update(_canonical_json_bytes(row.to_dict()))
    digest.update(b"]}")
    return digest.hexdigest()[:16].upper()


def _normalize_ladder_rows(rows: tuple[dict[str, object], ...]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                "instrument_id": str(row["instrument_id"]),
                "timeframe": str(row["timeframe"]),
                "roll_sequence": int(row["roll_sequence"]),
                "effective_ts": str(row["effective_ts"]),
                "additive_gap": float(row["additive_gap"]),
            }
        )
    return sorted(
        normalized,
        key=lambda row: (
            str(row["instrument_id"]),
            str(row["timeframe"]),
            int(row["roll_sequence"]),
        ),
    )


def _format_library_number(value: object) -> str:
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value)}.0"
        return str(value)
    return str(value)


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace({0.0: pd.NA})


def _profile_requires_volume_profile(profile: IndicatorProfile) -> bool:
    outputs = set(profile.expected_output_columns())
    return any(column in outputs for column in VOLUME_PROFILE_INDICATOR_COLUMNS)


def _profile_has_volume_profile_spec(profile: IndicatorProfile) -> bool:
    return any(spec.operation_key == "volume_profile" for spec in profile.indicators)


def _volume_profile_key(instrument_id: str, contract_id: str) -> tuple[str, str]:
    return str(instrument_id), str(contract_id)


def _volume_profile_contract_id(row: ResearchBarView) -> str:
    return row.active_contract_id or row.contract_id


def _utc_timestamp(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _optional_utc_timestamp(value: object) -> pd.Timestamp | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {"", "none", "null", "nat"}:
        return None
    return _utc_timestamp(value)


def _timeframe_delta(timeframe: str) -> pd.Timedelta:
    normalized = str(timeframe).strip().lower()
    if normalized.endswith("m"):
        return pd.Timedelta(minutes=int(normalized[:-1]))
    if normalized.endswith("h"):
        return pd.Timedelta(hours=int(normalized[:-1]))
    if normalized.endswith("d"):
        return pd.Timedelta(days=int(normalized[:-1] or "1"))
    if normalized.endswith("w"):
        return pd.Timedelta(weeks=int(normalized[:-1] or "1"))
    raise ValueError(f"unsupported timeframe for volume profile: {timeframe}")


def _target_bar_close_ts(row: ResearchBarView) -> pd.Timestamp:
    open_ts = _utc_timestamp(row.ts)
    bar_end_ts = _optional_utc_timestamp(row.bar_end_ts)
    session_close_ts = _optional_utc_timestamp(row.session_close_ts)
    if row.timeframe.endswith("d") and session_close_ts is not None:
        return session_close_ts
    close_ts = bar_end_ts or open_ts + _timeframe_delta(row.timeframe)
    if session_close_ts is not None:
        close_ts = min(close_ts, session_close_ts)
    return close_ts


def _expected_1m_source_bars(row: ResearchBarView) -> int:
    open_ts = _utc_timestamp(row.ts)
    close_ts = _target_bar_close_ts(row)
    minutes = (close_ts - open_ts).total_seconds() / 60.0
    return max(0, round(minutes))


def _source_rows_frame(rows: Sequence[Mapping[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["_ts_open", "low", "high", "volume"])
    frame = pd.DataFrame(rows)
    ts_column = "ts_open" if "ts_open" in frame.columns else "ts"
    frame["_ts_open"] = pd.to_datetime(frame[ts_column], utc=True)
    return frame.sort_values("_ts_open").reset_index(drop=True)


def _volume_profile_source_row_sort_key(row: Mapping[str, object]) -> tuple[str, ...]:
    return (
        str(row.get("instrument_id") or row.get("internal_id") or ""),
        str(row.get("contract_id") or row.get("finam_symbol") or row.get("moex_secid") or ""),
        str(row.get("ts_open") or row.get("ts") or ""),
        str(row.get("low") or ""),
        str(row.get("high") or ""),
        str(row.get("volume") or ""),
        repr(sorted((str(key), str(value)) for key, value in row.items())),
    )


def _load_volume_profile_source_rows(
    *,
    raw_1m_table_path: Path,
    local_series: list[ResearchBarView],
) -> dict[tuple[str, str], list[dict[str, object]]]:
    if not local_series:
        return {}
    available_columns = set(delta_table_columns(raw_1m_table_path))
    instrument_column = "instrument_id" if "instrument_id" in available_columns else "internal_id"
    contract_column = (
        "contract_id"
        if "contract_id" in available_columns
        else "finam_symbol"
        if "finam_symbol" in available_columns
        else "moex_secid"
    )
    timestamp_column = (
        "ts_open" if "ts_open" in available_columns else "ts" if "ts" in available_columns else ""
    )
    if not timestamp_column:
        raise ValueError("volume profile raw 1m table is missing required columns: ts or ts_open")
    required_columns = {
        instrument_column,
        contract_column,
        "timeframe",
        timestamp_column,
        "low",
        "high",
        "volume",
    }
    missing_columns = sorted(required_columns - available_columns)
    if missing_columns:
        raise ValueError(
            "volume profile raw 1m table is missing required columns: " + ", ".join(missing_columns)
        )
    source_keys = {
        _volume_profile_key(row.instrument_id, _volume_profile_contract_id(row))
        for row in local_series
    }
    intervals: dict[tuple[str, str], list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
    for bar in sorted(local_series, key=lambda row: _utc_timestamp(row.ts)):
        key = _volume_profile_key(bar.instrument_id, _volume_profile_contract_id(bar))
        start, end = _utc_timestamp(bar.ts), _target_bar_close_ts(bar)
        merged = intervals.setdefault(key, [])
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    # A sparse full-history series can have one interval per bar. Sending that
    # DNF to Arrow makes the native expression tree deep enough to crash.
    # Read contract envelopes, then apply the exact half-open interval union.
    filter_groups = [
        [
            ("timeframe", "=", "1m"),
            (instrument_column, "=", key[0]),
            (contract_column, "=", key[1]),
            (timestamp_column, ">=", ranges[0][0].isoformat().replace("+00:00", "Z")),
            (timestamp_column, "<", ranges[-1][1].isoformat().replace("+00:00", "Z")),
        ]
        for key, ranges in sorted(intervals.items())
    ]
    starts = {key: [start.value for start, _ in ranges] for key, ranges in intervals.items()}
    ends = {key: [end.value for _, end in ranges] for key, ranges in intervals.items()}
    selected_columns = [
        contract_column,
        instrument_column,
        "timeframe",
        timestamp_column,
        "low",
        "high",
        "volume",
    ]
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {key: [] for key in source_keys}
    for offset in range(0, len(filter_groups), 32):
        for batch in iter_delta_table_row_batches(
            raw_1m_table_path, columns=selected_columns, filters=filter_groups[offset : offset + 32]
        ):
            for row in batch:
                key = _volume_profile_key(
                    str(row.get(instrument_column, "")), str(row.get(contract_column, ""))
                )
                if key not in grouped:
                    continue
                timestamp = _utc_timestamp(row[timestamp_column]).value
                index = bisect_right(starts[key], timestamp) - 1
                if index < 0 or timestamp >= ends[key][index]:
                    continue
                normalized = dict(row)
                normalized["instrument_id"] = key[0]
                normalized["contract_id"] = key[1]
                grouped[key].append(normalized)
    for key, key_rows in grouped.items():
        grouped[key] = sorted(key_rows, key=_volume_profile_source_row_sort_key)
    return grouped


def _volume_profile_source_hash(
    *,
    target_bars_hash: str,
    local_series: list[ResearchBarView],
    source_rows_by_series: Mapping[tuple[str, str], Sequence[Mapping[str, object]]],
    tick_size_by_instrument: Mapping[str, float] | None,
) -> str:
    source_keys = sorted(
        {
            _volume_profile_key(row.instrument_id, _volume_profile_contract_id(row))
            for row in local_series
        }
    )
    digest = hashlib.sha256()
    digest.update(b'{"raw_1m":[')
    first = True
    for instrument_id, contract_id in source_keys:
        for row in sorted(
            source_rows_by_series.get((instrument_id, contract_id), ()),
            key=_volume_profile_source_row_sort_key,
        ):
            if not first:
                digest.update(b",")
            first = False
            digest.update(
                _canonical_json_bytes(
                    {
                        "instrument_id": instrument_id,
                        "contract_id": contract_id,
                        "ts_open": str(row.get("ts_open") or row.get("ts") or ""),
                        "low": row.get("low"),
                        "high": row.get("high"),
                        "volume": row.get("volume"),
                    }
                )
            )
    digest.update(b'],"target_bars_hash":')
    digest.update(_canonical_json_bytes(target_bars_hash))
    digest.update(b',"tick_size_by_instrument":')
    digest.update(
        _canonical_json_bytes(
            {
                instrument_id: (
                    float(tick_size_by_instrument[instrument_id])
                    if tick_size_by_instrument is not None
                    and instrument_id in tick_size_by_instrument
                    else None
                )
                for instrument_id in sorted({instrument_id for instrument_id, _ in source_keys})
            }
        )
    )
    digest.update(b"}")
    return digest.hexdigest()[:16].upper()


def _empty_volume_profile_features() -> dict[str, float | int | None]:
    return {
        column: (
            0
            if column == "vp_shape_code"
            else 0.0
            if column in {"vp_source_1m_coverage_ratio", "vp_volume_conservation_ratio"}
            else VP_QUALITY_NO_SOURCE
            if column == "vp_quality_code"
            else None
        )
        for column in VOLUME_PROFILE_INDICATOR_COLUMNS
    }


def _compute_volume_profile_columns(
    *,
    local_series: list[ResearchBarView],
    index: pd.Index,
    source_rows_by_series: Mapping[tuple[str, str], Sequence[Mapping[str, object]]] | None,
    tick_size_by_instrument: Mapping[str, float] | None,
) -> dict[str, pd.Series]:
    if not local_series:
        return {}
    instrument_id = local_series[0].instrument_id
    has_source_config = source_rows_by_series is not None
    tick_size = (tick_size_by_instrument or {}).get(instrument_id)
    has_source_rows = any(
        (source_rows_by_series or {}).get(
            _volume_profile_key(instrument_id, _volume_profile_contract_id(row)), ()
        )
        for row in local_series
    )
    if tick_size is None and (not has_source_config or not has_source_rows):
        values_by_column = {
            column: [
                features[column]
                for features in (_empty_volume_profile_features() for _ in local_series)
            ]
            for column in VOLUME_PROFILE_INDICATOR_COLUMNS
        }
        return {
            column: pd.Series(values, index=index, dtype="object")
            for column, values in values_by_column.items()
        }
    if tick_size is None:
        raise ValueError(f"volume profile tick size missing for instrument_id `{instrument_id}`")

    frames_by_contract: dict[str, pd.DataFrame] = {}

    def source_frame(contract_id: str) -> pd.DataFrame:
        if contract_id not in frames_by_contract:
            rows = (source_rows_by_series or {}).get(
                _volume_profile_key(instrument_id, contract_id), ()
            )
            frames_by_contract[contract_id] = _source_rows_frame(rows)
        return frames_by_contract[contract_id]

    values_by_column: dict[str, list[float | int | None]] = {
        column: [] for column in VOLUME_PROFILE_INDICATOR_COLUMNS
    }
    for row in local_series:
        contract_id = _volume_profile_contract_id(row)
        source = source_frame(contract_id)
        open_ts = _utc_timestamp(row.ts)
        close_ts = _target_bar_close_ts(row)
        if source.empty:
            window_rows: list[dict[str, object]] = []
        else:
            ts_open = source["_ts_open"]
            left = int(ts_open.searchsorted(open_ts, side="left"))
            right = int(ts_open.searchsorted(close_ts, side="left"))
            window_rows = source.iloc[left:right][["low", "high", "volume"]].to_dict("records")
        features = compute_volume_profile_features(
            window_rows,
            tick_size=float(tick_size),
            target_volume=float(row.volume),
            expected_source_bars=_expected_1m_source_bars(row),
        )
        for column in VOLUME_PROFILE_INDICATOR_COLUMNS:
            values_by_column[column].append(features[column])
    return {
        column: pd.Series(values, index=index, dtype="object")
        for column, values in values_by_column.items()
    }


def _series_group_key(
    row: ResearchBarView, *, dataset_version: str, indicator_set_version: str, series_mode: str
) -> IndicatorFramePartitionKey:
    resolved_series_mode = _resolved_series_mode(row.series_mode, requested_series_mode=series_mode)
    fallback_series_id = (
        row.instrument_id if resolved_series_mode == "continuous_front" else row.contract_id
    )
    return IndicatorFramePartitionKey(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        timeframe=row.timeframe,
        instrument_id=row.instrument_id,
        contract_id=None if resolved_series_mode == "continuous_front" else row.contract_id,
        contour_id=row.contour_id,
        series_mode=resolved_series_mode,
        series_id=row.series_id or fallback_series_id or "",
    )


def _group_bar_views(
    *,
    dataset_version: str,
    indicator_set_version: str,
    bar_views: list[ResearchBarView],
    series_mode: str,
) -> dict[IndicatorFramePartitionKey, list[ResearchBarView]]:
    grouped: dict[IndicatorFramePartitionKey, list[ResearchBarView]] = {}
    for row in sorted(
        bar_views, key=lambda item: (item.instrument_id, item.timeframe, item.ts, item.contract_id)
    ):
        key = _series_group_key(
            row,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            series_mode=series_mode,
        )
        grouped.setdefault(key, []).append(row)
    return grouped


def _group_existing_rows(
    *,
    rows: list[IndicatorFrameRow],
    series_mode: str,
) -> dict[IndicatorFramePartitionKey, list[IndicatorFrameRow]]:
    grouped: dict[IndicatorFramePartitionKey, list[IndicatorFrameRow]] = {}
    for row in sorted(
        rows, key=lambda item: (item.instrument_id, item.timeframe, item.ts, item.contract_id)
    ):
        grouped.setdefault(row.partition_key(series_mode=series_mode), []).append(row)
    return grouped


def _metadata_partition_key(
    row: dict[str, object], *, series_mode: str
) -> IndicatorFramePartitionKey:
    resolved_series_mode = _resolved_series_mode(
        row.get("series_mode"), requested_series_mode=series_mode
    )
    fallback_series_id = (
        str(row.get("instrument_id") or "")
        if resolved_series_mode == "continuous_front"
        else str(row.get("contract_id") or "")
    )
    return IndicatorFramePartitionKey(
        dataset_version=str(row["dataset_version"]),
        indicator_set_version=str(row["indicator_set_version"]),
        timeframe=str(row["timeframe"]),
        instrument_id=str(row["instrument_id"]),
        contract_id=None if resolved_series_mode == "continuous_front" else str(row["contract_id"]),
        contour_id=str(row.get("contour_id") or "native_tradable"),
        series_mode=resolved_series_mode,
        series_id=str(row.get("series_id") or fallback_series_id),
    )


def _group_existing_partition_metadata(
    *,
    rows: list[dict[str, object]],
    series_mode: str,
) -> dict[IndicatorFramePartitionKey, dict[str, object]]:
    grouped: dict[IndicatorFramePartitionKey, dict[str, object]] = {}
    for row in rows:
        grouped.setdefault(_metadata_partition_key(row, series_mode=series_mode), row)
    return grouped


def _load_dataset_manifest(
    *, output_dir: Path, dataset_version: str, contour_id: str = "native_tradable"
) -> dict[str, object]:
    rows = read_delta_table_rows(
        output_dir / "research_datasets.delta",
        filters=[("dataset_version", "=", dataset_version), ("contour_id", "=", contour_id)],
    )
    if not rows:
        raise KeyError(f"dataset_version not found: {dataset_version}")
    return rows[0]


def _dataset_manifest_timeframes(dataset_manifest: Mapping[str, object]) -> frozenset[str]:
    raw = dataset_manifest.get("timeframes_json")
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return frozenset()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = [item.strip() for item in text.split(",")]
    elif isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray)):
        parsed = raw
    else:
        parsed = ()
    return frozenset(str(item).strip() for item in parsed if str(item).strip())


def _bar_partition_key_from_row(
    row: dict[str, object],
    *,
    dataset_version: str,
    indicator_set_version: str,
    series_mode: str,
) -> IndicatorFramePartitionKey:
    resolved_series_mode = _resolved_series_mode(
        row.get("series_mode"), requested_series_mode=series_mode
    )
    fallback_series_id = (
        str(row["instrument_id"])
        if resolved_series_mode == "continuous_front"
        else str(row["contract_id"])
    )
    return IndicatorFramePartitionKey(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        timeframe=str(row["timeframe"]),
        instrument_id=str(row["instrument_id"]),
        contract_id=None if resolved_series_mode == "continuous_front" else str(row["contract_id"]),
        contour_id=str(row.get("contour_id") or "native_tradable"),
        series_mode=resolved_series_mode,
        series_id=str(row.get("series_id") or fallback_series_id),
    )


def _load_bar_partition_counts(
    *,
    dataset_output_dir: Path,
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str,
    series_mode: str,
    timeframes: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    active_timeframes: AbstractSet[str] = frozenset(),
) -> dict[IndicatorFramePartitionKey, int]:
    table_path = dataset_output_dir / "research_bar_views.delta"
    requested_columns = [
        "dataset_version",
        "contour_id",
        "series_mode",
        "series_id",
        "contract_id",
        "instrument_id",
        "timeframe",
    ]
    if series_mode == "continuous_front":
        requested_columns.remove("contract_id")
    available_columns = set(delta_table_columns(table_path))
    read_columns = [column for column in requested_columns if column in available_columns]
    group_column_candidates = [
        "dataset_version",
        "contour_id",
        "series_mode",
        "series_id",
        "instrument_id",
        "timeframe",
    ]
    if series_mode != "continuous_front":
        group_column_candidates.insert(4, "contract_id")
    group_columns = [column for column in group_column_candidates if column in read_columns]
    if not group_columns:
        return {}
    filters = [
        item
        for item in (
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", contour_id),
        )
        if item[0] in available_columns
    ]
    scoped_timeframes = tuple(str(item).strip() for item in timeframes if str(item).strip())
    if scoped_timeframes and "timeframe" in available_columns:
        filters.append(("timeframe", "in", scoped_timeframes))
    scoped_instruments = tuple(
        str(item).strip() for item in dataset_instrument_ids if str(item).strip()
    )
    if scoped_instruments and "instrument_id" in available_columns:
        filters.append(("instrument_id", "in", scoped_instruments))
    table = read_delta_table_arrow(table_path, columns=read_columns, filters=filters)
    if table.num_rows == 0:
        return {}
    grouped = table.group_by(group_columns).aggregate([([], "count_all")])
    counts: dict[IndicatorFramePartitionKey, int] = {}
    for row in grouped.to_pylist():
        key = _bar_partition_key_from_row(
            row,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            series_mode=series_mode,
        )
        if active_timeframes and key.timeframe not in active_timeframes:
            continue
        counts[key] = int(row.get("count_all") or 0)
    return counts


def _load_bar_partition_rows(
    *,
    dataset_output_dir: Path,
    dataset_version: str,
    contour_id: str,
    partition: IndicatorFramePartitionKey,
) -> list[ResearchBarView]:
    table_path = dataset_output_dir / "research_bar_views.delta"
    available_columns = set(delta_table_columns(table_path))
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
        ("instrument_id", "=", partition.instrument_id),
        ("timeframe", "=", partition.timeframe),
    ]
    if partition.contract_id is not None:
        filters.append(("contract_id", "=", partition.contract_id))
    if partition.series_mode:
        filters.append(("series_mode", "=", partition.series_mode))
    if partition.series_id:
        filters.append(("series_id", "=", partition.series_id))
    filters = [item for item in filters if item[0] in available_columns]
    rows = read_delta_table_rows(table_path, filters=filters)
    return [
        ResearchBarView.from_dict(row) for row in sorted(rows, key=lambda item: str(item["ts"]))
    ]


def _load_adjustment_ladder_rows(
    *, dataset_output_dir: Path, dataset_version: str
) -> tuple[dict[str, object], ...]:
    table_path = dataset_output_dir / "continuous_front_adjustment_ladder.delta"
    if not (table_path / "_delta_log").exists():
        return ()
    rows = read_delta_table_rows(table_path, filters=[("dataset_version", "=", dataset_version)])
    return tuple(dict(row) for row in rows)


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


def _validate_required_inputs(frame: pd.DataFrame, spec: IndicatorSpec) -> None:
    missing = [column for column in spec.required_input_columns if column not in frame.columns]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(
            f"indicator `{spec.indicator_id}` missing required input columns: {joined}"
        )


def _compute_spec(frame: pd.DataFrame, spec: IndicatorSpec) -> dict[str, pd.Series]:
    _validate_required_inputs(frame, spec)
    params = spec.params_dict()
    close = frame["close"]
    high = frame["high"] if "high" in frame.columns else None
    low = frame["low"] if "low" in frame.columns else None
    volume = frame["volume"] if "volume" in frame.columns else None
    open_interest = frame["open_interest"] if "open_interest" in frame.columns else None

    def _none_outputs() -> dict[str, pd.Series]:
        return {
            column: pd.Series([None] * len(frame), index=frame.index, dtype="object")
            for column in spec.output_columns
        }

    if spec.operation_key == "sma":
        series = ta.sma(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "ema":
        series = ta.ema(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "hma":
        series = ta.hma(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "slope":
        series = ta.slope(close, length=int(params["length"]), as_angle=True, to_degrees=True)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "atr":
        series = ta.atr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "natr":
        series = ta.natr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "chop":
        series = ta.chop(high, low, close, length=int(params["length"]))
        if series is None:
            return _none_outputs()
        # A zero price range makes CHOP undefined; preserve finite native values.
        return {spec.output_columns[0]: series.replace([math.inf, -math.inf], float("nan"))}
    if spec.operation_key == "supertrend":
        length = int(params["length"])
        multiplier = float(params["multiplier"])
        supertrend = ta.supertrend(high, low, close, length=length, multiplier=multiplier)
        if supertrend is None:
            return _none_outputs()
        suffix = f"{length}_{_format_library_number(multiplier)}"
        return {
            spec.output_columns[0]: supertrend[f"SUPERT_{suffix}"],
            spec.output_columns[1]: supertrend[f"SUPERTd_{suffix}"],
        }
    if spec.operation_key == "rsi":
        series = ta.rsi(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "roc":
        series = ta.roc(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "mom":
        series = ta.mom(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "obv":
        series = ta.obv(close, volume)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "mfi":
        series = ta.mfi(high, low, close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "cmf":
        series = ta.cmf(high, low, close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "vwma":
        series = ta.vwma(close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "ad":
        series = ta.ad(high, low, close, volume)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "adosc":
        fast = int(params["fast"])
        slow = int(params["slow"])
        series = ta.adosc(high, low, close, volume, fast=fast, slow=slow)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "force_index":
        series = ta.efi(close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "pvt":
        series = ta.pvt(close, volume)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "cci":
        series = ta.cci(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "willr":
        series = ta.willr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "stochrsi":
        length = int(params["length"])
        rsi_length = int(params["rsi_length"])
        k = int(params["k"])
        d = int(params["d"])
        stochrsi = ta.stochrsi(close, length=length, rsi_length=rsi_length, k=k, d=d)
        if stochrsi is None:
            return _none_outputs()
        suffix = f"{length}_{rsi_length}_{k}_{d}"
        return {
            spec.output_columns[0]: stochrsi[f"STOCHRSIk_{suffix}"],
            spec.output_columns[1]: stochrsi[f"STOCHRSId_{suffix}"],
        }
    if spec.operation_key == "ultimate_oscillator":
        fast = int(params["fast"])
        medium = int(params["medium"])
        slow = int(params["slow"])
        series = ta.uo(high, low, close, fast=fast, medium=medium, slow=slow)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "true_range":
        return {spec.output_columns[0]: _numeric(frame["true_range"])}
    if spec.operation_key == "realized_volatility":
        length = int(params["length"])
        return {
            spec.output_columns[0]: _numeric(frame["log_ret_1"])
            .rolling(window=length, min_periods=length)
            .std(ddof=0)
        }
    if spec.operation_key == "ulcer_index":
        series = ta.ui(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "volume_norm":
        length = int(params["length"])
        mean = volume.rolling(window=length, min_periods=length).mean()
        std = volume.rolling(window=length, min_periods=length).std(ddof=0).replace({0.0: pd.NA})
        return {
            spec.output_columns[0]: volume / mean,
            spec.output_columns[1]: (volume - mean) / std,
        }
    if spec.operation_key == "stoch":
        stoch = ta.stoch(
            high,
            low,
            close,
            k=int(params["k"]),
            d=int(params["d"]),
            smooth_k=int(params["smooth_k"]),
        )
        if stoch is None:
            return _none_outputs()
        suffix = f"{params['k']}_{params['smooth_k']}_{params['d']}"
        return {
            spec.output_columns[0]: stoch[f"STOCHk_{suffix}"],
            spec.output_columns[1]: stoch[f"STOCHd_{suffix}"],
        }
    if spec.operation_key == "macd":
        fast = int(params["fast"])
        slow = int(params["slow"])
        signal = int(params["signal"])
        macd = ta.macd(close, fast=fast, slow=slow, signal=signal)
        if macd is None:
            return _none_outputs()
        suffix = f"{fast}_{slow}_{signal}"
        return {
            spec.output_columns[0]: macd[f"MACD_{suffix}"],
            spec.output_columns[1]: macd[f"MACDs_{suffix}"],
            spec.output_columns[2]: macd[f"MACDh_{suffix}"],
        }
    if spec.operation_key == "adx":
        length = int(params["length"])
        adx = ta.adx(high, low, close, length=length)
        if adx is None:
            return _none_outputs()
        suffix = str(length)
        return {
            spec.output_columns[0]: adx[f"ADX_{suffix}"],
            spec.output_columns[1]: adx[f"DMP_{suffix}"],
            spec.output_columns[2]: adx[f"DMN_{suffix}"],
        }
    if spec.operation_key == "aroon":
        length = int(params["length"])
        aroon = ta.aroon(high, low, length=length)
        if aroon is None:
            return _none_outputs()
        suffix = str(length)
        return {
            spec.output_columns[0]: aroon[f"AROONU_{suffix}"],
            spec.output_columns[1]: aroon[f"AROOND_{suffix}"],
        }
    if spec.operation_key == "donchian":
        lower_length = int(params["lower_length"])
        upper_length = int(params["upper_length"])
        donchian = ta.donchian(high, low, lower_length=lower_length, upper_length=upper_length)
        if donchian is None:
            return _none_outputs()
        suffix = f"{lower_length}_{upper_length}"
        upper = donchian[f"DCU_{suffix}"]
        lower = donchian[f"DCL_{suffix}"]
        return {
            spec.output_columns[0]: upper,
            spec.output_columns[1]: lower,
            spec.output_columns[2]: donchian[f"DCM_{suffix}"],
            spec.output_columns[3]: upper - lower,
        }
    if spec.operation_key == "bbands":
        length = int(params["length"])
        std = params["std"]
        bbands = ta.bbands(close, length=length, std=std)
        if bbands is None:
            return _none_outputs()
        suffix = f"{length}_{_format_library_number(float(std))}"
        return {
            spec.output_columns[0]: bbands[f"BBU_{suffix}"],
            spec.output_columns[1]: bbands[f"BBM_{suffix}"],
            spec.output_columns[2]: bbands[f"BBL_{suffix}"],
            spec.output_columns[3]: bbands[f"BBB_{suffix}"],
            spec.output_columns[4]: bbands[f"BBP_{suffix}"],
        }
    if spec.operation_key == "kc":
        length = int(params["length"])
        scalar = float(params["scalar"])
        kc = ta.kc(high, low, close, length=length, scalar=scalar)
        if kc is None:
            return _none_outputs()
        suffix = f"{length}_{_format_library_number(scalar)}"
        return {
            spec.output_columns[0]: kc[f"KCUe_{suffix}"],
            spec.output_columns[1]: kc[f"KCBe_{suffix}"],
            spec.output_columns[2]: kc[f"KCLe_{suffix}"],
        }
    if spec.operation_key == "ppo":
        fast = int(params["fast"])
        slow = int(params["slow"])
        signal = int(params["signal"])
        ppo = ta.ppo(close, fast=fast, slow=slow, signal=signal)
        if ppo is None:
            return _none_outputs()
        suffix = f"{fast}_{slow}_{signal}"
        return {
            spec.output_columns[0]: ppo[f"PPO_{suffix}"],
            spec.output_columns[1]: ppo[f"PPOs_{suffix}"],
            spec.output_columns[2]: ppo[f"PPOh_{suffix}"],
        }
    if spec.operation_key == "tsi":
        fast = int(params["fast"])
        slow = int(params["slow"])
        tsi = ta.tsi(close, fast=fast, slow=slow)
        if tsi is None:
            return _none_outputs()
        return {spec.output_columns[0]: tsi.iloc[:, 0]}
    if spec.operation_key == "trix":
        length = int(params["length"])
        signal = int(params["signal"])
        trix = ta.trix(close, length=length, signal=signal)
        if trix is None:
            return _none_outputs()
        suffix = f"{length}_{signal}"
        return {
            spec.output_columns[0]: trix[f"TRIX_{suffix}"],
            spec.output_columns[1]: trix[f"TRIXs_{suffix}"],
        }
    if spec.operation_key == "kst":
        kst = ta.kst(close)
        if kst is None:
            return _none_outputs()
        return {
            spec.output_columns[0]: kst.iloc[:, 0],
            spec.output_columns[1]: kst.iloc[:, 1],
        }
    if spec.operation_key == "pvo":
        fast = int(params["fast"])
        slow = int(params["slow"])
        signal = int(params["signal"])
        pvo = ta.pvo(volume, fast=fast, slow=slow, signal=signal)
        if pvo is None:
            return _none_outputs()
        suffix = f"{fast}_{slow}_{signal}"
        return {
            spec.output_columns[0]: pvo[f"PVO_{suffix}"],
            spec.output_columns[1]: pvo[f"PVOs_{suffix}"],
            spec.output_columns[2]: pvo[f"PVOh_{suffix}"],
        }
    if spec.operation_key == "oi_change":
        return {spec.output_columns[0]: _numeric(open_interest).diff(int(params["length"]))}
    if spec.operation_key == "oi_roc":
        return {
            spec.output_columns[0]: _numeric(open_interest)
            .pct_change(int(params["length"]), fill_method=None)
            .replace([math.inf, -math.inf], float("nan"))
            * 100.0
        }
    if spec.operation_key == "oi_z":
        length = int(params["length"])
        oi = _numeric(open_interest)
        mean = oi.rolling(window=length, min_periods=length).mean()
        std = oi.rolling(window=length, min_periods=length).std(ddof=0)
        return {spec.output_columns[0]: _safe_divide(oi - mean, std)}
    if spec.operation_key == "oi_relative_activity":
        length = int(params["length"])
        oi = _numeric(open_interest)
        mean = oi.rolling(window=length, min_periods=length).mean()
        return {spec.output_columns[0]: _safe_divide(oi, mean)}
    if spec.operation_key == "volume_oi_ratio":
        return {spec.output_columns[0]: _safe_divide(_numeric(volume), _numeric(open_interest))}
    if spec.operation_key == "volume_profile":
        return _none_outputs()
    raise ValueError(f"unsupported indicator operation: {spec.operation_key}")


def _append_computed_columns(frame: pd.DataFrame, columns: Mapping[str, pd.Series]) -> pd.DataFrame:
    if not columns:
        return frame
    output_frame = pd.DataFrame(dict(columns), index=frame.index)
    overlapping_columns = [column for column in output_frame.columns if column in frame.columns]
    if overlapping_columns:
        frame = frame.drop(columns=overlapping_columns)
    return pd.concat([frame, output_frame], axis=1)


def _compute_profile_frame_unmasked(frame: pd.DataFrame, profile: IndicatorProfile) -> pd.DataFrame:
    result = frame.copy()
    pending_outputs: dict[str, pd.Series] = {}
    for spec in profile.indicators:
        if set(spec.required_input_columns) & set(pending_outputs):
            result = _append_computed_columns(result, pending_outputs)
            pending_outputs = {}
        pending_outputs.update(_compute_spec(result, spec))
    return _append_computed_columns(result, pending_outputs)


def _bar_usage_eligible_mask(frame: pd.DataFrame, required_flags: int) -> pd.Series:
    if "bar_usage_flags" not in frame.columns:
        return pd.Series([True] * len(frame), index=frame.index)
    return frame["bar_usage_flags"].map(
        lambda flags: has_required_bar_usage_flags(flags, required_flags)
    )


def _project_bar_usage_computed_columns(
    *,
    frame: pd.DataFrame,
    computed: pd.DataFrame,
    output_columns: tuple[str, ...],
    rule: BarUsageCalculationRule,
) -> dict[str, pd.Series]:
    projected = computed.reindex(frame.index)
    for column in output_columns:
        if column not in projected.columns:
            projected[column] = pd.NA
    projected = projected.loc[:, list(output_columns)]
    if rule.mode == STATE_UPDATE_HOLD:
        projected = _fill_bar_usage_noncomputed_rows(projected, computed_index=computed.index)
        projected = projected.infer_objects(copy=False)
    elif rule.mode == STATE_UPDATE_RESET_SCOPE:
        scope_key = (
            frame["session_date"]
            if rule.scope_id == "session" and "session_date" in frame.columns
            else None
        )
        projected = _fill_bar_usage_noncomputed_rows(
            projected,
            computed_index=computed.index,
            scope_key=scope_key,
        )
        projected = projected.infer_objects(copy=False)
    elif rule.mode == POINT_UPDATE_NULL:
        pass
    elif rule.mode == EVENT_UPDATE_ZERO:
        noneligible_index = ~projected.index.isin(computed.index)
        projected.loc[noneligible_index, list(output_columns)] = 0
        projected = projected.infer_objects(copy=False)
    else:
        raise ValueError(f"unsupported bar usage calculation mode: {rule.mode}")
    return {column: projected[column] for column in output_columns}


def _fill_bar_usage_noncomputed_rows(
    projected: pd.DataFrame,
    *,
    computed_index: pd.Index,
    scope_key: pd.Series | None = None,
) -> pd.DataFrame:
    filled = projected.copy()
    eligible = filled.index.isin(computed_index)
    if eligible.all():
        return filled
    # Carry positions, not values: an explicitly computed NULL must reset state.
    positions = pd.Series(range(len(filled)), index=filled.index, dtype="float64").where(eligible)
    if scope_key is None:
        positions = positions.ffill()
    else:
        scopes = scope_key.reindex(filled.index)
        previous_scopes = scopes.shift()
        same_scope = scopes.eq(previous_scopes).fillna(False) | (
            scopes.isna() & previous_scopes.isna()
        )
        positions = positions.groupby((~same_scope).cumsum(), sort=False).ffill()
    targets = (~eligible) & positions.notna().to_numpy()
    carried = filled.iloc[positions.loc[targets].astype(int).to_numpy()].to_numpy()
    filled.loc[~eligible, :] = pd.NA
    filled.loc[targets, :] = carried
    return filled


def _compute_profile_frame(frame: pd.DataFrame, profile: IndicatorProfile) -> pd.DataFrame:
    assert_indicator_bar_usage_policy_coverage(profile)
    if frame.empty or "bar_usage_flags" not in frame.columns:
        return _compute_profile_frame_unmasked(frame, profile)
    result = frame.copy()
    for rule, output_columns in indicator_bar_usage_groups_for_outputs(
        profile.expected_output_columns()
    ):
        specs = _specs_for_columns(profile, set(output_columns))
        if not specs:
            continue
        eligible = _bar_usage_eligible_mask(frame, rule.required_flags)
        eligible_frame = frame.loc[eligible].copy()
        if eligible_frame.empty:
            computed = pd.DataFrame(index=eligible_frame.index)
        else:
            computed = _compute_profile_frame_unmasked(
                eligible_frame, _profile_for_specs(profile, specs)
            )
        result = _append_computed_columns(
            result,
            _project_bar_usage_computed_columns(
                frame=frame,
                computed=computed,
                output_columns=output_columns,
                rule=rule,
            ),
        )
    return result


def _ladder_rows_for_series(
    adjustment_ladder_rows: tuple[dict[str, object], ...],
    *,
    instrument_id: str,
    timeframe: str,
) -> tuple[dict[str, object], ...]:
    return tuple(
        sorted(
            (
                row
                for row in adjustment_ladder_rows
                if str(row.get("instrument_id")) == instrument_id
                and str(row.get("timeframe")) == timeframe
            ),
            key=lambda row: int(row["roll_sequence"]),
        )
    )


def _offset_by_epoch(max_epoch: int, gap_by_sequence: dict[int, float]) -> dict[int, float]:
    return {
        epoch: sum(gap_by_sequence[sequence] for sequence in range(1, epoch + 1))
        for epoch in range(0, max_epoch + 1)
    }


def _recompute_price_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    adjusted = frame.copy()
    close = _numeric(adjusted["close"])
    prev_close = close.shift(1)
    adjusted["ret_1"] = (close / prev_close) - 1.0
    adjusted.loc[prev_close.isna() | (prev_close == 0.0), "ret_1"] = pd.NA
    adjusted["log_ret_1"] = pd.NA
    valid_ret = prev_close.notna() & (prev_close > 0.0) & (close > 0.0)
    adjusted.loc[valid_ret, "log_ret_1"] = (close[valid_ret] / prev_close[valid_ret]).map(math.log)
    high = _numeric(adjusted["high"])
    low = _numeric(adjusted["low"])
    open_ = _numeric(adjusted["open"])
    adjusted["hl_range"] = high - low
    adjusted["oc_range"] = close - open_
    adjusted["true_range"] = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return adjusted


def _native_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    native = frame.copy()
    for column in ("open", "high", "low", "close"):
        native_column = f"native_{column}"
        if native_column in native.columns:
            native[column] = pd.to_numeric(native[native_column], errors="coerce")
    return _recompute_price_inputs(native)


def _zero_anchor_price_frame(
    frame: pd.DataFrame, *, gap_by_sequence: dict[int, float]
) -> pd.DataFrame:
    zero_anchor = frame.copy()
    row_epochs = pd.to_numeric(zero_anchor["roll_epoch"], errors="coerce").fillna(0).astype(int)
    cumulative_offsets = row_epochs.map(
        lambda epoch: sum(gap_by_sequence[sequence] for sequence in range(1, int(epoch) + 1))
    )
    for column in ("open", "high", "low", "close"):
        native_column = f"native_{column}"
        source = (
            zero_anchor[native_column]
            if native_column in zero_anchor.columns
            else zero_anchor[column]
        )
        zero_anchor[column] = pd.to_numeric(source, errors="coerce") - cumulative_offsets
    return _recompute_price_inputs(zero_anchor)


def _asof_adjusted_price_frame(
    frame: pd.DataFrame,
    *,
    target_epoch: int,
    gap_by_sequence: dict[int, float],
) -> pd.DataFrame:
    adjusted = frame[frame["roll_epoch"] <= target_epoch].copy()
    row_epochs = pd.to_numeric(adjusted["roll_epoch"], errors="coerce").fillna(0).astype(int)
    offsets = row_epochs.map(
        lambda row_epoch: sum(
            gap_by_sequence[sequence] for sequence in range(row_epoch + 1, target_epoch + 1)
        )
    )
    for column in ("open", "high", "low", "close"):
        native_column = f"native_{column}"
        source = adjusted[native_column] if native_column in adjusted.columns else adjusted[column]
        adjusted[column] = pd.to_numeric(source, errors="coerce") + offsets
    return _recompute_price_inputs(adjusted)


def _output_groups_for_spec(
    spec: IndicatorSpec,
    rules_by_output: dict[str, object],
) -> dict[str, tuple[str, ...]]:
    groups: dict[str, list[str]] = {}
    for output_column in spec.output_columns:
        rule = rules_by_output[output_column]
        group_id = rule.calculation_group_id
        groups.setdefault(group_id, []).append(output_column)
    return {group_id: tuple(columns) for group_id, columns in groups.items()}


def _frame_for_calculation_group(
    group_id: str,
    *,
    zero_anchor_frame: pd.DataFrame,
    target_anchor_frame: pd.DataFrame,
    native_frame: pd.DataFrame,
) -> pd.DataFrame:
    if group_id in {"price_level_post_transform", "price_range_on_p0", "oscillator_on_p0"}:
        return zero_anchor_frame
    if group_id in {"anchor_sensitive_roll_aware", "price_volume_roll_aware"}:
        return target_anchor_frame
    if group_id in {"native_volume_oi_roll_aware", "grid_locked_native"}:
        return native_frame
    return target_anchor_frame


def _compute_continuous_front_profile_frame(
    frame: pd.DataFrame,
    profile: IndicatorProfile,
    *,
    adjustment_ladder_rows: tuple[dict[str, object], ...],
) -> pd.DataFrame:
    if frame.empty:
        return _compute_profile_frame(frame, profile)
    rules = rules_for_indicator_profile(profile)
    assert_rule_coverage(output_columns=set(profile.expected_output_columns()), rules=rules)
    if "roll_epoch" not in frame.columns:
        raise ValueError(
            "continuous_front indicator materialization requires roll_epoch on research_bar_views"
        )
    instrument_id = str(frame["instrument_id"].iloc[0])
    timeframe = str(frame["timeframe"].iloc[0])
    ladder_rows = _ladder_rows_for_series(
        adjustment_ladder_rows,
        instrument_id=instrument_id,
        timeframe=timeframe,
    )
    frame = frame.copy()
    frame["roll_epoch"] = pd.to_numeric(frame["roll_epoch"], errors="coerce").fillna(0).astype(int)
    max_epoch = int(frame["roll_epoch"].max() or 0)
    if not ladder_rows:
        if max_epoch > 0:
            raise ValueError(
                "continuous_front indicator materialization requires adjustment "
                "ladder rows for rolled series "
                f"{instrument_id}|{timeframe}"
            )
        gap_by_sequence: dict[int, float] = {}
    else:
        gap_by_sequence = {
            int(row["roll_sequence"]): float(row["additive_gap"]) for row in ladder_rows
        }
    missing_sequences = [
        sequence for sequence in range(1, max_epoch + 1) if sequence not in gap_by_sequence
    ]
    if missing_sequences:
        joined = ", ".join(str(sequence) for sequence in missing_sequences)
        raise ValueError(
            "continuous_front indicator materialization missing adjustment ladder roll_sequence "
            f"{joined} for {instrument_id}|{timeframe}"
        )

    rules_by_output = {rule.output_column: rule for rule in rules}
    target_offset_by_epoch = _offset_by_epoch(max_epoch=max_epoch, gap_by_sequence=gap_by_sequence)
    invariant_group_ids = {
        "price_level_post_transform",
        "price_range_on_p0",
        "oscillator_on_p0",
        "native_volume_oi_roll_aware",
        "grid_locked_native",
    }
    full_zero_anchor_frame = _zero_anchor_price_frame(frame, gap_by_sequence=gap_by_sequence)
    full_native_frame = _native_price_frame(frame)
    invariant_compute_cache: dict[tuple[str, str], dict[str, pd.Series]] = {}
    computed_segments: list[pd.DataFrame] = []
    for target_epoch in sorted(frame["roll_epoch"].unique()):
        epoch = int(target_epoch)
        target_index = frame.index[frame["roll_epoch"] == epoch]
        source_frame = frame[frame["roll_epoch"] <= epoch].copy()
        zero_anchor_frame = full_zero_anchor_frame.loc[source_frame.index]
        target_anchor_frame = _asof_adjusted_price_frame(
            frame, target_epoch=epoch, gap_by_sequence=gap_by_sequence
        )
        native_frame = full_native_frame.loc[source_frame.index]
        segment = target_anchor_frame.loc[target_index].copy()
        target_offset = float(target_offset_by_epoch[epoch])
        compute_cache: dict[tuple[str, str], dict[str, pd.Series]] = {}
        segment_outputs: dict[str, pd.Series] = {}

        for spec in profile.indicators:
            for group_id, output_columns in _output_groups_for_spec(spec, rules_by_output).items():
                cache_key = (spec.indicator_id, group_id)
                if cache_key not in compute_cache:
                    group_frame = _frame_for_calculation_group(
                        group_id,
                        zero_anchor_frame=zero_anchor_frame,
                        target_anchor_frame=target_anchor_frame,
                        native_frame=native_frame,
                    )
                    if (
                        group_id in invariant_group_ids
                        and len(source_frame) >= profile.max_warmup_bars()
                    ):
                        if cache_key not in invariant_compute_cache:
                            invariant_frame = _frame_for_calculation_group(
                                group_id,
                                zero_anchor_frame=full_zero_anchor_frame,
                                target_anchor_frame=target_anchor_frame,
                                native_frame=full_native_frame,
                            )
                            invariant_compute_cache[cache_key] = _compute_spec(
                                invariant_frame, spec
                            )
                        compute_cache[cache_key] = invariant_compute_cache[cache_key]
                    else:
                        compute_cache[cache_key] = _compute_spec(group_frame, spec)
                computed_outputs = compute_cache[cache_key]
                for output_column in output_columns:
                    series = computed_outputs[output_column].reindex(target_anchor_frame.index)
                    if group_id == "price_level_post_transform":
                        series = series + target_offset
                    segment_outputs[output_column] = series.loc[target_index]
        if segment_outputs:
            segment = _append_computed_columns(segment, segment_outputs)
        computed_segments.append(segment)
    computed_columns = list(
        dict.fromkeys(column for segment in computed_segments for column in segment.columns)
    )
    concat_segments = [segment.dropna(axis=1, how="all") for segment in computed_segments]
    computed = pd.concat(concat_segments).sort_index().reindex(columns=computed_columns)
    return _enforce_continuous_front_native_boundaries(computed, profile)


def _enforce_continuous_front_native_boundaries(
    frame: pd.DataFrame, profile: IndicatorProfile
) -> pd.DataFrame:
    result = frame.copy()
    roll_epoch = pd.to_numeric(result["roll_epoch"], errors="coerce").fillna(0).astype(int)
    specs_by_output = {
        output_column: spec for spec in profile.indicators for output_column in spec.output_columns
    }
    rules_by_output = {rule.output_column: rule for rule in rules_for_indicator_profile(profile)}

    reset_operation_keys = {"obv", "ad", "adosc", "pvt", "pvo"}
    for spec in profile.indicators:
        spec_rules = [rules_by_output[column] for column in spec.output_columns]
        if spec.operation_key not in reset_operation_keys or not any(
            rule.group.reset_on_roll for rule in spec_rules
        ):
            continue
        recomputed_outputs = pd.DataFrame(
            index=result.index, columns=list(spec.output_columns), dtype="object"
        )
        for _, segment in result.groupby(roll_epoch, sort=False):
            recomputed = _compute_profile_frame(
                segment.copy(), _profile_for_specs(profile, (spec,))
            )
            segment_outputs = recomputed[list(spec.output_columns)]
            recomputed_outputs.loc[segment_outputs.index, list(spec.output_columns)] = (
                segment_outputs
            )
        for column in spec.output_columns:
            result[column] = recomputed_outputs[column]

    for column, rule in rules_by_output.items():
        if column not in result.columns or rule.group.allow_cross_contract_window:
            continue
        spec = specs_by_output[column]
        if spec.operation_key in {"oi_change", "oi_roc"}:
            lag = max(1, int(spec.params_dict().get("length", 1)))
            same_epoch_lag = roll_epoch == roll_epoch.shift(lag)
            result.loc[~same_epoch_lag.fillna(False), column] = pd.NA
            continue
        if spec.operation_key == "volume_oi_ratio":
            continue
        length = max(1, int(spec.params_dict().get("length", spec.warmup_bars)))
        window_crosses = roll_epoch.rolling(window=length, min_periods=length).apply(
            lambda values: 0.0 if len(set(int(value) for value in values)) == 1 else 1.0,
            raw=False,
        )
        result.loc[window_crosses.fillna(1.0).astype(bool), column] = pd.NA
    return result


def _compute_profile_frame_for_series(
    frame: pd.DataFrame,
    profile: IndicatorProfile,
    *,
    series_mode: str,
    adjustment_ladder_rows: tuple[dict[str, object], ...],
) -> pd.DataFrame:
    if series_mode != "continuous_front":
        return _compute_profile_frame(frame, profile)
    if frame.empty or "bar_usage_flags" not in frame.columns:
        return _compute_continuous_front_profile_frame(
            frame,
            profile,
            adjustment_ladder_rows=adjustment_ladder_rows,
        )
    result = frame.copy()
    for rule, output_columns in indicator_bar_usage_groups_for_outputs(
        profile.expected_output_columns()
    ):
        specs = _specs_for_columns(profile, set(output_columns))
        if not specs:
            continue
        eligible = _bar_usage_eligible_mask(frame, rule.required_flags)
        eligible_frame = frame.loc[eligible].copy()
        if eligible_frame.empty:
            computed = pd.DataFrame(index=eligible_frame.index)
        else:
            computed = _compute_continuous_front_profile_frame(
                eligible_frame,
                _profile_for_specs(profile, specs),
                adjustment_ladder_rows=adjustment_ladder_rows,
            )
        result = _append_computed_columns(
            result,
            _project_bar_usage_computed_columns(
                frame=frame,
                computed=computed,
                output_columns=output_columns,
                rule=rule,
            ),
        )
    return result


def _null_warmup_span(
    payload_rows: list[dict[str, object]], output_columns: tuple[str, ...]
) -> int:
    count = 0
    for row in payload_rows:
        if any(row.get(column) is None or pd.isna(row.get(column)) for column in output_columns):
            count += 1
            continue
        break
    return count


def _profile_for_specs(
    profile: IndicatorProfile, specs: tuple[IndicatorSpec, ...]
) -> IndicatorProfile:
    return IndicatorProfile(
        version=profile.version,
        description=profile.description,
        indicators=specs,
    )


def _specs_for_columns(profile: IndicatorProfile, columns: set[str]) -> tuple[IndicatorSpec, ...]:
    return tuple(
        spec
        for spec in profile.indicators
        if any(column in columns for column in spec.output_columns)
    )


def _existing_partition_matches(
    existing_metadata: dict[str, object] | None,
    *,
    source_hash: str,
    profile_version: str,
    row_count: int,
    output_columns_hash: str,
    legacy_output_columns_match: bool = False,
    compute_contract_hash: str = "",
    refresh_context_hash: str = "",
) -> bool:
    if not existing_metadata:
        return False
    existing_output_columns_hash = str(existing_metadata.get("output_columns_hash") or "")
    output_hash_matches = existing_output_columns_hash == output_columns_hash or (
        not existing_output_columns_hash and legacy_output_columns_match
    )
    return (
        existing_metadata.get("source_bars_hash") == source_hash
        and existing_metadata.get("profile_version") == profile_version
        and int(existing_metadata.get("row_count", -1) or -1) == row_count
        and output_hash_matches
        and bool(refresh_context_hash)
        and existing_metadata.get("refresh_context_hash") == refresh_context_hash
        and bool(compute_contract_hash)
        and existing_metadata.get("compute_contract_hash") == compute_contract_hash
    )


def _metadata_fast_reuse_report(
    *,
    existing_by_partition: dict[IndicatorFramePartitionKey, dict[str, object]],
    current_dataset_bars_hash: str,
    profile_version: str,
    target_output_columns: tuple[str, ...],
    target_output_columns_hash: str,
    existing_output_columns: set[str],
    output_paths: dict[str, str],
    resolved_profile: IndicatorProfile,
    adjustment_ladder_rows: tuple[dict[str, object], ...],
    source_snapshot_hash: str,
    volume_profile_raw_1m_table_path: Path | None = None,
    volume_profile_tick_size_by_instrument: Mapping[str, float] | None = None,
) -> dict[str, object] | None:
    if (
        _profile_has_volume_profile_spec(resolved_profile)
        and volume_profile_raw_1m_table_path is None
    ):
        return None
    if not current_dataset_bars_hash or not existing_by_partition:
        return None
    legacy_output_columns_match = set(target_output_columns).issubset(existing_output_columns)
    indicator_row_count = 0
    for partition, metadata in existing_by_partition.items():
        row_count = int(metadata.get("row_count", -1) or -1)
        source_hash = str(metadata.get("source_bars_hash") or "")
        if (
            not source_hash
            or not source_snapshot_hash
            or metadata.get("source_snapshot_hash") != source_snapshot_hash
            or row_count < 0
            or str(metadata.get("source_dataset_bars_hash") or "") != current_dataset_bars_hash
            or not _existing_partition_matches(
                metadata,
                source_hash=source_hash,
                profile_version=profile_version,
                row_count=row_count,
                output_columns_hash=target_output_columns_hash,
                legacy_output_columns_match=legacy_output_columns_match,
                compute_contract_hash=_indicator_compute_contract_hash(resolved_profile),
                refresh_context_hash=_refresh_context_hash(
                    resolved_profile,
                    instrument_id=partition.instrument_id,
                    ladder_rows=_ladder_rows_for_series(
                        adjustment_ladder_rows,
                        instrument_id=partition.instrument_id,
                        timeframe=partition.timeframe,
                    )
                    if partition.series_mode == "continuous_front"
                    else (),
                    raw_1m_path=volume_profile_raw_1m_table_path,
                    source_rows=None,
                    tick_sizes=volume_profile_tick_size_by_instrument,
                ),
            )
        ):
            return None
        indicator_row_count += row_count

    volume_profile_source_status = "not_required"
    if _profile_requires_volume_profile(resolved_profile):
        volume_profile_source_status = "reused_without_source_scan"
    return {
        "indicator_row_count": indicator_row_count,
        "bar_usage_policy_id": BAR_USAGE_POLICY_ID,
        "indicator_usage_policy_id": INDICATOR_USAGE_POLICY_ID,
        "refreshed_row_count": 0,
        "refreshed_partition_count": 0,
        "reused_partition_count": len(existing_by_partition),
        "extended_partition_count": 0,
        "recomputed_partition_count": 0,
        "deleted_partition_count": 0,
        "write_batch_count": 0,
        "volume_profile_source_status": volume_profile_source_status,
        "volume_profile_missing_source_partition_count": 0,
        "profile_version": resolved_profile.version,
        "output_columns_hash": target_output_columns_hash,
        "reuse_plan_mode": "metadata_fast_path",
        "output_paths": output_paths,
        "delta_manifest": indicator_store_contract(profile=resolved_profile),
    }


def _timestamp_after(left: object, right: object) -> bool:
    if not left or not right:
        return False
    try:
        return pd.Timestamp(left) > pd.Timestamp(right)
    except (TypeError, ValueError):
        return str(left) > str(right)


def _native_indicator_source_hash(series, input_hashes, ladder, *, bars_digest=None) -> str:
    return hashlib.sha256(
        _canonical_json_bytes(
            {
                "bars": bars_digest or _bars_hash(series, adjustment_ladder_rows=ladder),
                "inputs": input_hashes,
                "ladder": _normalize_ladder_rows(ladder),
            }
        )
    ).hexdigest()


def _indicator_value(column: str, value: object) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    if column in VOLUME_PROFILE_INT_COLUMNS:
        return int(value)
    return float(value)


def _merge_indicator_values(
    *, series, payload_rows, output_columns, computed_columns, existing_by_ts
):
    prepared_values: list[
        tuple[ResearchBarView, dict[str, float | int | None], IndicatorFrameRow | None]
    ] = []
    merged_payload_rows: list[dict[str, object]] = []

    for original, payload in zip(series, payload_rows, strict=True):
        existing = existing_by_ts.get(original.ts)
        values: dict[str, float | int | None] = {}
        for column in output_columns:
            if column in computed_columns:
                value = payload.get(column)
            elif existing is not None and column in existing.values:
                value = existing.values[column]
            else:
                value = None
            values[column] = _indicator_value(column, value)
        prepared_values.append((original, values, existing))
        merged_payload_rows.append(values)

    null_warmup_span = _null_warmup_span(merged_payload_rows, output_columns)
    return prepared_values, null_warmup_span


def _apply_volume_profile_columns(
    *,
    series,
    computed,
    frame,
    prefix_length,
    volume_profile_refresh_ts,
    resolved_volume_profile_source_rows,
    volume_profile_tick_size_by_instrument,
):
    volume_indices = [
        index
        for index, bar in enumerate(series)
        if index >= prefix_length
        and (volume_profile_refresh_ts is None or bar.ts in volume_profile_refresh_ts)
    ]
    for column, series_values in _compute_volume_profile_columns(
        local_series=[series[index] for index in volume_indices],
        index=computed.index[volume_indices],
        source_rows_by_series=resolved_volume_profile_source_rows,
        tick_size_by_instrument=volume_profile_tick_size_by_instrument,
    ).items():
        if prefix_length or volume_profile_refresh_ts is not None:
            computed[column] = computed[column].astype(object)
            computed.loc[series_values.index, column] = series_values
        else:
            computed[column] = series_values
    volume_profile_columns = tuple(
        column for column in VOLUME_PROFILE_INDICATOR_COLUMNS if column in computed.columns
    )
    for rule, output_columns in indicator_bar_usage_groups_for_outputs(volume_profile_columns):
        eligible = _bar_usage_eligible_mask(frame, rule.required_flags)
        projected = _project_bar_usage_computed_columns(
            frame=frame,
            computed=computed.loc[eligible, list(output_columns)],
            output_columns=output_columns,
            rule=rule,
        )
        for column, series_values in projected.items():
            computed[column] = series_values
    return computed


def _compute_partition_frame(
    *,
    frame,
    series,
    compute_profile,
    prefix_length,
    cached_rows,
    diagnostics,
    series_mode,
    series_ladder_rows,
    volume_profile_refresh_ts,
    existing_by_ts,
    resolved_volume_profile_source_rows,
    volume_profile_tick_size_by_instrument,
):
    if prefix_length:
        existing_frame = pd.DataFrame([row.values for row in cached_rows[:prefix_length]])
        computed = _compute_cached_profile_frame(
            frame,
            compute_profile,
            existing=existing_frame,
            prefix_length=prefix_length,
            diagnostics=diagnostics,
        )
    else:
        computed = _compute_profile_frame_for_series(
            frame,
            compute_profile,
            series_mode=series_mode,
            adjustment_ladder_rows=series_ladder_rows,
        )
    if _profile_has_volume_profile_spec(compute_profile):
        if volume_profile_refresh_ts is not None:
            cached_indices = [
                index for index, bar in enumerate(series) if bar.ts not in volume_profile_refresh_ts
            ]
            for column in VOLUME_PROFILE_INDICATOR_COLUMNS:
                computed[column] = computed[column].astype(object)
                computed.loc[cached_indices, column] = pd.Series(
                    [
                        existing_by_ts[series[index].ts].values.get(column)
                        for index in cached_indices
                    ],
                    index=cached_indices,
                    dtype="object",
                )
        computed = _apply_volume_profile_columns(
            series=series,
            computed=computed,
            frame=frame,
            prefix_length=prefix_length,
            volume_profile_refresh_ts=volume_profile_refresh_ts,
            resolved_volume_profile_source_rows=resolved_volume_profile_source_rows,
            volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
        )
    return computed


def _build_partition_rows(
    *,
    dataset_version: str,
    indicator_set_version: str,
    profile: IndicatorProfile,
    series: list[ResearchBarView],
    series_mode: str = "contract",
    source_dataset_bars_hash: str = "",
    output_columns_hash: str | None = None,
    compute_profile: IndicatorProfile | None = None,
    existing_rows: list[IndicatorFrameRow] | None = None,
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
    volume_profile_source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]]
    | None = None,
    volume_profile_raw_1m_table_path: Path | None = None,
    volume_profile_tick_size_by_instrument: Mapping[str, float] | None = None,
    compute_diagnostics: dict[str, object] | None = None,
    precomputed_source_hash: str | None = None,
    native_full_compute: bool = False,
    volume_profile_refresh_ts: set[str] | None = None,
    source_snapshot_hash: str = "",
) -> list[IndicatorFrameRow]:
    frame = pd.DataFrame([row.to_dict() for row in series])
    compute_profile = compute_profile or profile
    series_ladder_rows = ()
    if series_mode == "continuous_front" and series:
        series_ladder_rows = _ladder_rows_for_series(
            adjustment_ladder_rows,
            instrument_id=series[0].instrument_id,
            timeframe=series[0].timeframe,
        )
    resolved_volume_profile_source_rows = volume_profile_source_rows
    existing_by_ts = {row.ts: row for row in existing_rows or []}
    volume_profile_series = [
        row
        for row in series
        if volume_profile_refresh_ts is None or row.ts in volume_profile_refresh_ts
    ]
    if (
        _profile_has_volume_profile_spec(compute_profile)
        and resolved_volume_profile_source_rows is None
        and volume_profile_raw_1m_table_path is not None
    ):
        resolved_volume_profile_source_rows = _load_volume_profile_source_rows(
            raw_1m_table_path=volume_profile_raw_1m_table_path,
            local_series=volume_profile_series,
        )
    cached_volume_profile_hashes = {
        ts: row.volume_profile_input_hash
        for ts, row in existing_by_ts.items()
        if volume_profile_refresh_ts is not None
        and ts not in volume_profile_refresh_ts
        and row.volume_profile_input_hash
    }
    volume_profile_hashes: dict[str, str] = {}
    compute_contract_hash = _indicator_compute_contract_hash(profile)
    input_row_hashes = _indicator_input_row_hashes(
        series,
        compute_contract_hash=compute_contract_hash,
        volume_profile_source_rows=resolved_volume_profile_source_rows,
        tick_size_by_instrument=volume_profile_tick_size_by_instrument,
        has_volume_profile=_profile_has_volume_profile_spec(profile),
        required_columns=profile.required_input_columns(),
        series_mode=series_mode,
        cached_volume_profile_hashes=cached_volume_profile_hashes,
        volume_profile_hashes=volume_profile_hashes,
    )
    cached_rows = sorted(existing_rows or [], key=lambda row: row.ts)
    prefix_length = 0
    # Native source-hash format is independent of whether a verified prefix
    # can continue computation. Unsupported specs still use full history.
    if series_mode != "continuous_front" and compute_profile == profile:
        for original, cached, row_hash in zip(series, cached_rows, input_row_hashes):
            if (
                cached.ts != original.ts
                or cached.input_row_hash != row_hash
                or cached.compute_contract_hash != compute_contract_hash
                or not set(profile.expected_output_columns()).issubset(cached.values)
            ):
                break
            prefix_length += 1
    diagnostics = compute_diagnostics if compute_diagnostics is not None else {}
    diagnostics["reused_prefix_row_count"] = (
        int(diagnostics.get("reused_prefix_row_count", 0)) + prefix_length
    )
    if not prefix_length:
        diagnostics["full_history_spec_count"] = int(
            diagnostics.get("full_history_spec_count", 0)
        ) + len(compute_profile.indicators)
    computed = _compute_partition_frame(
        frame=frame,
        series=series,
        compute_profile=compute_profile,
        prefix_length=prefix_length,
        cached_rows=cached_rows,
        diagnostics=diagnostics,
        series_mode=series_mode,
        series_ladder_rows=series_ladder_rows,
        volume_profile_refresh_ts=volume_profile_refresh_ts,
        existing_by_ts=existing_by_ts,
        resolved_volume_profile_source_rows=resolved_volume_profile_source_rows,
        volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
    )
    payload_rows = computed.to_dict("records")
    output_columns = profile.expected_output_columns()
    computed_columns = set(compute_profile.expected_output_columns())
    source_hash = precomputed_source_hash or _bars_hash(
        series, adjustment_ladder_rows=series_ladder_rows
    )
    if (
        _profile_requires_volume_profile(profile)
        and resolved_volume_profile_source_rows is not None
        and precomputed_source_hash is None
        and not native_full_compute
    ):
        source_hash = _volume_profile_source_hash(
            target_bars_hash=source_hash,
            local_series=series,
            source_rows_by_series=resolved_volume_profile_source_rows,
            tick_size_by_instrument=volume_profile_tick_size_by_instrument,
        )
    if native_full_compute:
        source_hash = _native_indicator_source_hash(series, input_row_hashes, series_ladder_rows)
    refresh_context_hash = _refresh_context_hash(
        profile,
        instrument_id=series[0].instrument_id,
        ladder_rows=series_ladder_rows,
        raw_1m_path=volume_profile_raw_1m_table_path,
        source_rows=volume_profile_source_rows,
        tick_sizes=volume_profile_tick_size_by_instrument,
    )
    output_columns_hash = output_columns_hash or indicator_output_columns_hash(output_columns)
    prepared_values, null_warmup_span = _merge_indicator_values(
        series=series,
        payload_rows=payload_rows,
        output_columns=output_columns,
        computed_columns=computed_columns,
        existing_by_ts=existing_by_ts,
    )
    materialized_at = pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")

    rows: list[IndicatorFrameRow] = []
    for index, (original, values, existing) in enumerate(prepared_values):
        series_key = _series_group_key(
            original,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            series_mode=series_mode,
        )
        rows.append(
            IndicatorFrameRow(
                dataset_version=dataset_version,
                contour_id=original.contour_id,
                series_mode=series_key.series_mode,
                series_id=series_key.series_id,
                indicator_set_version=indicator_set_version,
                profile_version=profile.version,
                contract_id=original.contract_id,
                instrument_id=original.instrument_id,
                timeframe=original.timeframe,
                ts=original.ts,
                bar_end_ts=original.bar_end_ts,
                values=values,
                source_bars_hash=source_hash,
                row_count=len(series),
                warmup_span=profile.max_warmup_bars(),
                null_warmup_span=null_warmup_span,
                created_at=existing.created_at
                if existing is not None and (index < prefix_length or compute_profile != profile)
                else materialized_at,
                output_columns_hash=output_columns_hash,
                source_dataset_bars_hash=source_dataset_bars_hash,
                compute_contract_hash=compute_contract_hash,
                input_row_hash=input_row_hashes[index],
                refresh_context_hash=refresh_context_hash,
                source_snapshot_hash=source_snapshot_hash,
                volume_profile_input_hash=volume_profile_hashes.get(original.ts, ""),
            )
        )
    return rows


def build_indicator_frames(
    *,
    dataset_version: str,
    indicator_set_version: str,
    bar_views: list[ResearchBarView],
    series_mode: str = "contract",
    profile: IndicatorProfile | None = None,
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
    volume_profile_source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]]
    | None = None,
    volume_profile_raw_1m_table_path: Path | None = None,
    volume_profile_tick_size_by_instrument: Mapping[str, float] | None = None,
) -> list[IndicatorFrameRow]:
    profile = profile or default_indicator_profile()
    grouped = _group_bar_views(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        bar_views=bar_views,
        series_mode=series_mode,
    )
    rows: list[IndicatorFrameRow] = []
    for _, series in sorted(grouped.items(), key=lambda item: item[0].partition_path()):
        rows.extend(
            _build_partition_rows(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                profile=profile,
                series=series,
                series_mode=series_mode,
                adjustment_ladder_rows=adjustment_ladder_rows,
                volume_profile_source_rows=volume_profile_source_rows,
                volume_profile_raw_1m_table_path=volume_profile_raw_1m_table_path,
                volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
            )
        )
    return rows


@native_compute_scope
def materialize_indicator_frames(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    contour_id: str = "native_tradable",
    profile: IndicatorProfile | None = None,
    profile_version: str | None = None,
    timeframes: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    changed_windows: Sequence[Mapping[str, object]] | None = None,
    force_recompute: bool = False,
    volume_profile_source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]]
    | None = None,
    volume_profile_raw_1m_table_path: Path | None = None,
    volume_profile_tick_size_by_instrument: Mapping[str, float] | None = None,
) -> dict[str, object]:
    lock_path = _acquire_indicator_frame_materialization_lock(
        indicator_output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        contour_id=contour_id,
    )
    try:
        return _materialize_indicator_frames_unlocked(
            dataset_output_dir=dataset_output_dir,
            indicator_output_dir=indicator_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            contour_id=contour_id,
            profile=profile,
            profile_version=profile_version,
            timeframes=timeframes,
            dataset_instrument_ids=dataset_instrument_ids,
            changed_windows=changed_windows,
            force_recompute=force_recompute,
            volume_profile_source_rows=volume_profile_source_rows,
            volume_profile_raw_1m_table_path=volume_profile_raw_1m_table_path,
            volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
        )
    finally:
        _release_indicator_frame_materialization_lock(lock_path)


def _acquire_indicator_frame_materialization_lock(
    *,
    indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    contour_id: str,
) -> Path:
    from trading_advisor_3000.product_plane.research.operations import worker_identity

    indicator_output_dir.mkdir(parents=True, exist_ok=True)
    lock_path = indicator_output_dir / INDICATOR_FRAME_MATERIALIZATION_LOCK_FILENAME
    payload = {
        **worker_identity(),
        "table_name": "research_indicator_frames",
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "contour_id": contour_id,
        "pid": os.getpid(),
        "created_at_utc": pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z"),
    }
    try:
        descriptor = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(
            f"blocked by active research_indicator_frames writer lock: {lock_path.as_posix()}"
        ) from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return lock_path


def _release_indicator_frame_materialization_lock(lock_path: Path) -> None:
    lock_path.unlink(missing_ok=True)


def _materialize_indicator_frames_unlocked(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    contour_id: str = "native_tradable",
    profile: IndicatorProfile | None = None,
    profile_version: str | None = None,
    timeframes: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    changed_windows: Sequence[Mapping[str, object]] | None = None,
    force_recompute: bool = False,
    volume_profile_source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]]
    | None = None,
    volume_profile_raw_1m_table_path: Path | None = None,
    volume_profile_tick_size_by_instrument: Mapping[str, float] | None = None,
) -> dict[str, object]:
    from ..hash_compatibility import migration_source

    refresh_windows = (
        _normalize_indicator_windows(changed_windows)
        if changed_windows is not None and not force_recompute
        else None
    )
    stage_timings: StageTimings = {}
    compute_diagnostics: dict[str, object] = {}
    stage_started = stage_timer("load_dataset_manifest")
    dataset_manifest = _load_dataset_manifest(
        output_dir=dataset_output_dir,
        dataset_version=dataset_version,
        contour_id=contour_id,
    )
    series_mode = str(dataset_manifest.get("series_mode", "contract"))
    active_timeframes = _dataset_manifest_timeframes(dataset_manifest)
    current_dataset_bars_hash = str(dataset_manifest.get("bars_hash") or "")
    current_dataset_created_at = str(dataset_manifest.get("created_at") or "")
    registry: IndicatorProfileRegistry = build_indicator_profile_registry()
    resolved_profile = profile or registry.get(profile_version or "core_v1")
    source_snapshot_hash = _source_snapshot_hash(
        dataset_output_dir / "research_bar_views.delta",
        volume_profile_raw_1m_table_path
        if _profile_has_volume_profile_spec(resolved_profile)
        else None,
    )
    scoped_timeframes = frozenset(str(item).strip() for item in timeframes if str(item).strip())
    scoped_instruments = frozenset(
        str(item).strip() for item in dataset_instrument_ids if str(item).strip()
    )
    record_stage_timing(
        stage_timings,
        "load_dataset_manifest",
        stage_started,
        series_mode=series_mode,
        profile_version=resolved_profile.version,
    )

    def _partition_in_requested_scope(partition: IndicatorFramePartitionKey) -> bool:
        return (not scoped_timeframes or partition.timeframe in scoped_timeframes) and (
            not scoped_instruments or partition.instrument_id in scoped_instruments
        )

    table_exists = (
        indicator_output_dir / "research_indicator_frames.delta" / "_delta_log"
    ).exists()
    stage_started = stage_timer("load_existing_metadata")
    existing_metadata = (
        load_indicator_partition_metadata(
            output_dir=indicator_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            contour_id=contour_id,
        )
        if table_exists
        else []
    )
    existing_by_partition = _group_existing_partition_metadata(
        rows=existing_metadata, series_mode=series_mode
    )
    existing_by_partition = {
        key: metadata
        for key, metadata in existing_by_partition.items()
        if (not active_timeframes or key.timeframe in active_timeframes)
        and _partition_in_requested_scope(key)
    }
    target_output_columns = resolved_profile.expected_output_columns()
    target_output_columns_hash = indicator_output_columns_hash(target_output_columns)
    existing_output_columns = (
        set(existing_indicator_value_columns(output_dir=indicator_output_dir))
        if table_exists
        else set()
    )
    reusable_existing_columns = tuple(
        column for column in target_output_columns if column in existing_output_columns
    )
    output_paths = {
        "research_indicator_frames": (
            indicator_output_dir / "research_indicator_frames.delta"
        ).as_posix()
    }
    record_stage_timing(
        stage_timings,
        "load_existing_metadata",
        stage_started,
        table_exists=table_exists,
        existing_partition_count=len(existing_by_partition),
        reusable_existing_column_count=len(reusable_existing_columns),
    )

    adjustment_ladder_rows = (
        _load_adjustment_ladder_rows(
            dataset_output_dir=dataset_output_dir, dataset_version=dataset_version
        )
        if series_mode == "continuous_front"
        else ()
    )

    stage_started = stage_timer("reuse_decision")
    fast_reuse_report = (
        _metadata_fast_reuse_report(
            existing_by_partition=existing_by_partition,
            current_dataset_bars_hash=current_dataset_bars_hash,
            profile_version=resolved_profile.version,
            target_output_columns=target_output_columns,
            target_output_columns_hash=target_output_columns_hash,
            existing_output_columns=existing_output_columns,
            output_paths=output_paths,
            resolved_profile=resolved_profile,
            adjustment_ladder_rows=adjustment_ladder_rows,
            volume_profile_raw_1m_table_path=volume_profile_raw_1m_table_path,
            volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
            source_snapshot_hash=source_snapshot_hash,
        )
        if volume_profile_source_rows is None and not migration_source() and not force_recompute
        else None
    )
    if fast_reuse_report is not None:
        # A matching fingerprint on retained partitions does not prove full coverage.
        coverage_filters = [
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", contour_id),
        ]
        if active_timeframes:
            coverage_filters.append(("timeframe", "in", tuple(sorted(active_timeframes))))
        if scoped_timeframes:
            coverage_filters.append(("timeframe", "in", tuple(sorted(scoped_timeframes))))
        if scoped_instruments:
            coverage_filters.append(("instrument_id", "in", tuple(sorted(scoped_instruments))))
        expected_row_count = count_delta_table_rows(
            dataset_output_dir / "research_bar_views.delta", filters=coverage_filters
        )
        stored_row_count = count_delta_table_rows(
            indicator_output_dir / "research_indicator_frames.delta",
            filters=[*coverage_filters, ("indicator_set_version", "=", indicator_set_version)],
        )
        if (
            stored_row_count != expected_row_count
            or fast_reuse_report["indicator_row_count"] != expected_row_count
        ):
            fast_reuse_report = None
    if fast_reuse_report is not None:
        from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
            delta_table_version,
            finalize_research_delta_table,
        )

        path = indicator_output_dir / "research_indicator_frames.delta"
        finalize_research_delta_table(
            path,
            since_version=delta_table_version(path),
            partition_by=("ts_close_year",),
        )
        if (
            _source_snapshot_hash(
                dataset_output_dir / "research_bar_views.delta",
                volume_profile_raw_1m_table_path
                if _profile_has_volume_profile_spec(resolved_profile)
                else None,
            )
            != source_snapshot_hash
        ):
            raise ValueError("L1 inputs changed during snapshot reuse")
        record_stage_timing(
            stage_timings,
            "reuse_decision",
            stage_started,
            reused=True,
            partition_count=len(existing_by_partition),
        )
        return {**fast_reuse_report, "stage_timings": stage_timings}
    record_stage_timing(stage_timings, "reuse_decision", stage_started, reused=False)

    stage_started = stage_timer("load_source_partitions")
    partition_counts = _load_bar_partition_counts(
        dataset_output_dir=dataset_output_dir,
        dataset_version=dataset_version,
        contour_id=contour_id,
        indicator_set_version=indicator_set_version,
        series_mode=series_mode,
        timeframes=timeframes,
        dataset_instrument_ids=dataset_instrument_ids,
        active_timeframes=active_timeframes,
    )
    source_table_commit_ts = _latest_delta_commit_timestamp(
        dataset_output_dir / "research_bar_views.delta"
    )
    indicator_table_commit_ts = (
        _latest_delta_commit_timestamp(indicator_output_dir / "research_indicator_frames.delta")
        if table_exists
        else None
    )
    legacy_table_covers_source_commit = bool(
        source_table_commit_ts is not None
        and indicator_table_commit_ts is not None
        and indicator_table_commit_ts >= source_table_commit_ts
    )
    record_stage_timing(
        stage_timings,
        "load_source_partitions",
        stage_started,
        partition_count=len(partition_counts),
        adjustment_ladder_row_count=len(adjustment_ladder_rows),
    )

    reused_partitions = 0
    refreshed_partitions = 0
    extended_partitions = 0
    recomputed_partitions = 0
    volume_profile_missing_source_partitions = 0

    def _load_volume_profile_rows(
        partition_key: IndicatorFramePartitionKey,
        local_series: list[ResearchBarView],
    ) -> Mapping[tuple[str, str], Sequence[Mapping[str, object]]] | None:
        if not _profile_requires_volume_profile(resolved_profile):
            return None
        if volume_profile_source_rows is not None:
            return volume_profile_source_rows
        if volume_profile_raw_1m_table_path is None:
            return None
        return _load_volume_profile_source_rows(
            raw_1m_table_path=volume_profile_raw_1m_table_path,
            local_series=local_series,
        )

    sorted_partition_items = tuple(
        sorted(
            partition_counts.items(),
            key=lambda item: (
                item[0].instrument_id,
                item[0].contract_id or "",
                item[0].timeframe,
            ),
        )
    )

    if not table_exists:

        def _fresh_row_batches() -> Iterator[
            tuple[IndicatorFramePartitionKey, list[IndicatorFrameRow]]
        ]:
            nonlocal volume_profile_missing_source_partitions
            for partition_key, _ in sorted_partition_items:
                partition_started = stage_timer("partition.read_source")
                series = _load_bar_partition_rows(
                    dataset_output_dir=dataset_output_dir,
                    dataset_version=dataset_version,
                    contour_id=contour_id,
                    partition=partition_key,
                )
                local_volume_profile_source_rows = None
                if _profile_requires_volume_profile(resolved_profile):
                    local_volume_profile_source_rows = _load_volume_profile_rows(
                        partition_key, series
                    )
                    if local_volume_profile_source_rows is None:
                        volume_profile_missing_source_partitions += 1
                record_stage_timing(
                    {},
                    "partition.read_source",
                    partition_started,
                    instrument_id=partition_key.instrument_id,
                    timeframe=partition_key.timeframe,
                    row_count=len(series),
                )
                partition_started = stage_timer("partition.compute")
                partition_rows = _build_partition_rows(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    profile=resolved_profile,
                    series=series,
                    series_mode=series_mode,
                    source_dataset_bars_hash=current_dataset_bars_hash,
                    source_snapshot_hash=source_snapshot_hash,
                    output_columns_hash=target_output_columns_hash,
                    compute_profile=resolved_profile,
                    existing_rows=None,
                    adjustment_ladder_rows=adjustment_ladder_rows,
                    volume_profile_source_rows=local_volume_profile_source_rows,
                    volume_profile_raw_1m_table_path=volume_profile_raw_1m_table_path,
                    volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
                    compute_diagnostics=compute_diagnostics,
                )
                record_stage_timing(
                    {},
                    "partition.compute",
                    partition_started,
                    instrument_id=partition_key.instrument_id,
                    timeframe=partition_key.timeframe,
                    row_count=len(partition_rows),
                )
                yield partition_key, partition_rows
                del series, local_volume_profile_source_rows, partition_rows

        record_skipped_stage(
            stage_timings,
            "build_refresh_plan",
            reason="table does not exist; all source partitions are fresh",
        )
        stage_started = stage_timer("write_indicator_frames")
        output_paths, refreshed_row_count, batch_count = write_indicator_frame_partition_batches(
            output_dir=indicator_output_dir,
            partition_row_batches=_fresh_row_batches(),
            profile=resolved_profile,
        )
        record_stage_timing(
            stage_timings,
            "write_indicator_frames",
            stage_started,
            refreshed_partition_count=len(sorted_partition_items),
            refreshed_row_count=refreshed_row_count,
            write_batch_count=batch_count,
        )
        current_total_rows = sum(partition_counts.values())
        volume_profile_source_status = "not_required"
        if _profile_requires_volume_profile(resolved_profile):
            volume_profile_source_status = (
                "missing_source" if volume_profile_missing_source_partitions else "configured"
            )
        refreshed_partitions = len(sorted_partition_items)
        return {
            "indicator_row_count": current_total_rows,
            "bar_usage_policy_id": BAR_USAGE_POLICY_ID,
            "indicator_usage_policy_id": INDICATOR_USAGE_POLICY_ID,
            "refreshed_row_count": refreshed_row_count,
            "refreshed_partition_count": refreshed_partitions,
            "reused_partition_count": 0,
            "extended_partition_count": 0,
            "recomputed_partition_count": refreshed_partitions,
            "deleted_partition_count": 0,
            "write_batch_count": batch_count,
            "volume_profile_source_status": volume_profile_source_status,
            "volume_profile_missing_source_partition_count": (
                volume_profile_missing_source_partitions
            ),
            "profile_version": resolved_profile.version,
            "output_columns_hash": target_output_columns_hash,
            "output_paths": output_paths,
            "stage_timings": stage_timings,
            "delta_manifest": indicator_store_contract(profile=resolved_profile),
            "compute_diagnostics": compute_diagnostics,
        }

    current_partitions = set(partition_counts)
    deleted_partitions = tuple(
        partition
        for partition in existing_by_partition
        if partition not in current_partitions and _partition_in_requested_scope(partition)
    )
    if refresh_windows is not None:
        return _materialize_indicator_window_tails(
            dataset_output_dir=dataset_output_dir,
            indicator_output_dir=indicator_output_dir,
            profile=resolved_profile,
            partition_counts=partition_counts,
            existing_by_partition=existing_by_partition,
            windows=refresh_windows,
            adjustment_ladder_rows=adjustment_ladder_rows,
            source_dataset_bars_hash=current_dataset_bars_hash,
            source_snapshot_hash=source_snapshot_hash,
            volume_profile_source_rows=volume_profile_source_rows,
            volume_profile_raw_1m_table_path=volume_profile_raw_1m_table_path,
            volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
            deleted_partitions=deleted_partitions,
            stage_timings=stage_timings,
        )

    def _partition_row_batches() -> Iterator[
        tuple[IndicatorFramePartitionKey, list[IndicatorFrameRow]]
    ]:
        nonlocal reused_partitions, refreshed_partitions, extended_partitions
        nonlocal recomputed_partitions, volume_profile_missing_source_partitions
        for partition_key, row_count in sorted_partition_items:
            existing_partition_metadata = existing_by_partition.get(partition_key)
            source_hash = (
                str(existing_partition_metadata.get("source_bars_hash") or "")
                if existing_partition_metadata
                else ""
            )
            source_row_count = (
                int(existing_partition_metadata.get("row_count", -1) or -1)
                if existing_partition_metadata
                else -1
            )
            existing_dataset_bars_hash = (
                str(existing_partition_metadata.get("source_dataset_bars_hash") or "")
                if existing_partition_metadata
                else ""
            )
            existing_materialized_at = (
                str(existing_partition_metadata.get("created_at") or "")
                if existing_partition_metadata
                else ""
            )
            series: list[ResearchBarView] | None = None
            dataset_may_have_changed = bool(
                current_dataset_bars_hash
                and existing_dataset_bars_hash
                and current_dataset_bars_hash != existing_dataset_bars_hash
            ) or bool(
                current_dataset_bars_hash
                and not existing_dataset_bars_hash
                and not legacy_table_covers_source_commit
                and _timestamp_after(current_dataset_created_at, existing_materialized_at)
            )
            source_changed = (
                source_row_count != row_count
                or not source_hash
                or dataset_may_have_changed
                or existing_partition_metadata.get("source_snapshot_hash") != source_snapshot_hash
                if existing_partition_metadata
                else True
            )
            if source_changed:
                series = _load_bar_partition_rows(
                    dataset_output_dir=dataset_output_dir,
                    dataset_version=dataset_version,
                    contour_id=contour_id,
                    partition=partition_key,
                )
                series_ladder_rows = _ladder_rows_for_series(
                    adjustment_ladder_rows,
                    instrument_id=partition_key.instrument_id,
                    timeframe=partition_key.timeframe,
                )
                source_hash = _bars_hash(series, adjustment_ladder_rows=series_ladder_rows)
            local_volume_profile_source_rows: (
                Mapping[tuple[str, str], Sequence[Mapping[str, object]]] | None
            ) = None
            if _profile_requires_volume_profile(resolved_profile):
                if series is None:
                    series = _load_bar_partition_rows(
                        dataset_output_dir=dataset_output_dir,
                        dataset_version=dataset_version,
                        contour_id=contour_id,
                        partition=partition_key,
                    )
                series_ladder_rows = _ladder_rows_for_series(
                    adjustment_ladder_rows,
                    instrument_id=partition_key.instrument_id,
                    timeframe=partition_key.timeframe,
                )
                target_bars_hash = _bars_hash(series, adjustment_ladder_rows=series_ladder_rows)
                local_volume_profile_source_rows = _load_volume_profile_rows(partition_key, series)
                if local_volume_profile_source_rows is None:
                    volume_profile_missing_source_partitions += 1
                source_hash = (
                    _volume_profile_source_hash(
                        target_bars_hash=target_bars_hash,
                        local_series=series,
                        source_rows_by_series=local_volume_profile_source_rows,
                        tick_size_by_instrument=volume_profile_tick_size_by_instrument,
                    )
                    if local_volume_profile_source_rows is not None
                    else target_bars_hash
                )
            context_ladder = (
                _ladder_rows_for_series(
                    adjustment_ladder_rows,
                    instrument_id=partition_key.instrument_id,
                    timeframe=partition_key.timeframe,
                )
                if series_mode == "continuous_front"
                else ()
            )
            current_refresh_context = _refresh_context_hash(
                resolved_profile,
                instrument_id=partition_key.instrument_id,
                ladder_rows=context_ladder,
                raw_1m_path=volume_profile_raw_1m_table_path,
                source_rows=volume_profile_source_rows,
                tick_sizes=volume_profile_tick_size_by_instrument,
            )
            if (
                not force_recompute
                and _existing_partition_matches(
                    existing_partition_metadata,
                    source_hash=source_hash,
                    profile_version=resolved_profile.version,
                    row_count=row_count,
                    output_columns_hash=target_output_columns_hash,
                    legacy_output_columns_match=set(target_output_columns).issubset(
                        existing_output_columns
                    ),
                    compute_contract_hash=_indicator_compute_contract_hash(resolved_profile),
                    refresh_context_hash=current_refresh_context,
                )
                and existing_partition_metadata.get("source_snapshot_hash")
            ):
                reused_partitions += 1
                continue

            source_unchanged = (
                not force_recompute
                and bool(existing_partition_metadata)
                and (
                    existing_partition_metadata.get("source_bars_hash") == source_hash
                    and int(existing_partition_metadata.get("row_count", -1) or -1) == row_count
                    and (
                        bool(set(target_output_columns) - existing_output_columns)
                        or not existing_partition_metadata.get("compute_contract_hash")
                        or existing_partition_metadata["compute_contract_hash"]
                        == _indicator_compute_contract_hash(resolved_profile)
                    )
                )
            )
            missing_columns = set(target_output_columns) - existing_output_columns
            can_extend_from_existing = bool(
                source_unchanged
                and series_mode != "continuous_front"
                and missing_columns
                and reusable_existing_columns
                and _profile_extension_preserves_existing_contract(
                    resolved_profile,
                    existing_partition_metadata,
                    reusable_existing_columns,
                    instrument_id=partition_key.instrument_id,
                    ladder_rows=context_ladder,
                    raw_1m_path=volume_profile_raw_1m_table_path,
                    source_rows=volume_profile_source_rows,
                    tick_sizes=volume_profile_tick_size_by_instrument,
                )
            )
            if series is None:
                series = _load_bar_partition_rows(
                    dataset_output_dir=dataset_output_dir,
                    dataset_version=dataset_version,
                    contour_id=contour_id,
                    partition=partition_key,
                )
            if can_extend_from_existing:
                missing_specs = _specs_for_columns(resolved_profile, missing_columns)
                compute_profile = _profile_for_specs(resolved_profile, missing_specs)
                existing_value_columns: tuple[str, ...] | None = reusable_existing_columns
                extended_partitions += 1
            else:
                compute_profile = resolved_profile
                existing_value_columns = None
                recomputed_partitions += 1
            refreshed_partitions += 1
            partition_started = stage_timer("partition.load_existing")
            existing_rows = (
                load_indicator_partition_rows(
                    output_dir=indicator_output_dir,
                    partition=partition_key,
                    value_columns=existing_value_columns or reusable_existing_columns,
                )
                if not force_recompute
                and existing_partition_metadata is not None
                and reusable_existing_columns
                and (
                    can_extend_from_existing
                    or existing_partition_metadata.get("refresh_context_hash")
                    == current_refresh_context
                )
                and (series_mode != "continuous_front" or existing_value_columns is not None)
                else None
            )
            record_stage_timing(
                {},
                "partition.load_existing",
                partition_started,
                instrument_id=partition_key.instrument_id,
                timeframe=partition_key.timeframe,
            )
            partition_started = stage_timer("partition.compute")
            partition_rows = _build_partition_rows(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                profile=resolved_profile,
                series=series,
                series_mode=series_mode,
                source_dataset_bars_hash=current_dataset_bars_hash,
                source_snapshot_hash=source_snapshot_hash,
                output_columns_hash=target_output_columns_hash,
                compute_profile=compute_profile,
                existing_rows=existing_rows,
                adjustment_ladder_rows=adjustment_ladder_rows,
                volume_profile_source_rows=local_volume_profile_source_rows,
                volume_profile_raw_1m_table_path=volume_profile_raw_1m_table_path,
                volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
                compute_diagnostics=compute_diagnostics,
                precomputed_source_hash=source_hash,
            )
            record_stage_timing(
                {},
                "partition.compute",
                partition_started,
                instrument_id=partition_key.instrument_id,
                timeframe=partition_key.timeframe,
                row_count=len(partition_rows),
            )
            yield (
                partition_key,
                partition_rows,
            )
            # The plan never retains a completed series or its minute inputs.
            del series, local_volume_profile_source_rows, existing_rows, partition_rows

    stage_started = stage_timer("write_indicator_frames")
    output_paths, refreshed_row_count, batch_count = write_indicator_frame_partition_batches(
        output_dir=indicator_output_dir,
        partition_row_batches=_partition_row_batches(),
        delete_partitions=deleted_partitions,
        profile=resolved_profile,
    )
    record_stage_timing(
        stage_timings,
        "write_indicator_frames",
        stage_started,
        refreshed_row_count=refreshed_row_count,
        write_batch_count=batch_count,
    )

    current_total_rows = sum(partition_counts.values())
    volume_profile_source_status = "not_required"
    if _profile_requires_volume_profile(resolved_profile):
        volume_profile_source_status = (
            "missing_source" if volume_profile_missing_source_partitions else "configured"
        )
    return {
        "indicator_row_count": current_total_rows,
        "bar_usage_policy_id": BAR_USAGE_POLICY_ID,
        "indicator_usage_policy_id": INDICATOR_USAGE_POLICY_ID,
        "refreshed_row_count": refreshed_row_count,
        "refreshed_partition_count": refreshed_partitions,
        "reused_partition_count": reused_partitions,
        "extended_partition_count": extended_partitions,
        "recomputed_partition_count": recomputed_partitions,
        "deleted_partition_count": len(deleted_partitions),
        "write_batch_count": batch_count,
        "volume_profile_source_status": volume_profile_source_status,
        "volume_profile_missing_source_partition_count": volume_profile_missing_source_partitions,
        "profile_version": resolved_profile.version,
        "output_columns_hash": target_output_columns_hash,
        "output_paths": output_paths,
        "stage_timings": stage_timings,
        "delta_manifest": indicator_store_contract(profile=resolved_profile),
        "compute_diagnostics": compute_diagnostics,
    }


def reload_indicator_frames(
    *,
    indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    contour_id: str = "native_tradable",
) -> list[IndicatorFrameRow]:
    return load_indicator_frames(
        output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        contour_id=contour_id,
    )


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _legacy_indicator_compute_contract_hash(profile: IndicatorProfile) -> str:
    definition = asdict(profile)
    definition.pop("description")
    payload = {
        "profile": definition,
        "library": ta.__version__,
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "talib_available": bool(ta.Imports.get("talib")),
        "talib_version": distribution_version("TA-Lib") if ta.Imports.get("talib") else None,
        "policy": INDICATOR_USAGE_POLICY_ID,
        "cache_version": 1,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _indicator_compute_contract_hash(profile: IndicatorProfile) -> str:
    from ..compute_contract import numerical_identity
    from ..semantic_fingerprint import digest

    identity = numerical_identity()
    definition = asdict(profile)
    definition.pop("description")
    return digest(
        {
            "version": "indicator-compute.v2",
            "engine": identity["engine"],
            "kernel": identity["kernels"]["l1"],
            "profile": definition,
            "dependencies": identity["dependencies"],
        }
    )


def _profile_extension_preserves_existing_contract(
    profile: IndicatorProfile,
    metadata: Mapping[str, object],
    existing_columns: tuple[str, ...],
    *,
    instrument_id: str,
    ladder_rows: tuple[dict[str, object], ...],
    raw_1m_path: Path | None,
    source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]] | None,
    tick_sizes: Mapping[str, float] | None,
) -> bool:
    if not metadata.get("compute_contract_hash"):
        return False
    prior_definition = IndicatorProfile(
        version=str(metadata.get("profile_version") or ""),
        description="",
        indicators=_specs_for_columns(profile, set(existing_columns)),
    )
    return _indicator_compute_contract_hash(prior_definition) == metadata[
        "compute_contract_hash"
    ] and metadata.get("refresh_context_hash") == _refresh_context_hash(
        prior_definition,
        instrument_id=instrument_id,
        ladder_rows=ladder_rows,
        raw_1m_path=raw_1m_path,
        source_rows=source_rows,
        tick_sizes=tick_sizes,
    )


def _indicator_input_row_hashes(
    series: list[ResearchBarView],
    *,
    compute_contract_hash: str,
    volume_profile_source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]] | None,
    tick_size_by_instrument: Mapping[str, float] | None,
    has_volume_profile: bool,
    required_columns: tuple[str, ...],
    series_mode: str = "contract",
    cached_volume_profile_hashes: Mapping[str, str] | None = None,
    volume_profile_hashes: dict[str, str] | None = None,
) -> list[str]:
    minute_frames: dict[tuple[str, str], pd.DataFrame] = {}
    input_columns = set(required_columns) | {
        "dataset_version",
        "contract_id",
        "instrument_id",
        "timeframe",
        "ts",
        "session_date",
        "bar_usage_flags",
        "contour_id",
        "series_mode",
        "series_id",
    }
    hashes = []
    for row in series:
        payload: dict[str, object] = {
            "bar": {key: value for key, value in row.to_dict().items() if key in input_columns},
            "compute": compute_contract_hash,
        }
        if series_mode == "continuous_front" or row.series_mode == "continuous_front":
            # CF math consumes native OHLC, roll epochs and session projections,
            # beyond a profile's declared public-price columns. Old partial row
            # proofs must not certify reuse after a native/projection correction.
            payload["cf_input_version"] = 2
            payload["bar"] = row.to_dict()
        if has_volume_profile and (cached_volume_profile_hashes or {}).get(row.ts):
            payload["volume_profile"] = cached_volume_profile_hashes[row.ts]
        elif has_volume_profile:
            key = _volume_profile_key(row.instrument_id, _volume_profile_contract_id(row))
            if key not in minute_frames:
                minute_frames[key] = _source_rows_frame(
                    (volume_profile_source_rows or {}).get(key, ())
                )
            minute_frame = minute_frames[key]
            minute_rows = []
            bar_end = None
            if not minute_frame.empty:
                timestamps = minute_frame["_ts_open"]
                left = int(timestamps.searchsorted(_utc_timestamp(row.ts)))
                bar_end = _target_bar_close_ts(row)
                right = int(timestamps.searchsorted(bar_end))
                minute_rows = minute_frame.iloc[left:right][["low", "high", "volume"]].to_dict(
                    "records"
                )
            elif (tick_size_by_instrument or {}).get(row.instrument_id) is not None:
                bar_end = _target_bar_close_ts(row)
            volume_payload = {
                "configured": volume_profile_source_rows is not None,
                "tick_size": (tick_size_by_instrument or {}).get(row.instrument_id),
                "contract": key,
                "bar_end": bar_end.isoformat() if bar_end is not None else None,
                "minutes": minute_rows,
            }
            payload["volume_profile"] = hashlib.sha256(
                json.dumps(volume_payload, sort_keys=True, default=str).encode()
            ).hexdigest()
        if has_volume_profile and volume_profile_hashes is not None:
            volume_profile_hashes[row.ts] = str(payload["volume_profile"])
        hashes.append(
            hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
        )
    return hashes


def _finite_indicator_history(spec: IndicatorSpec) -> int | None:
    params = spec.params_dict()
    if any(
        int(params[name]) <= 0
        for name in ("length", "k", "d", "smooth_k", "lower_length", "upper_length")
        if name in params
    ):
        return None
    length = int(params.get("length", spec.warmup_bars))
    operation = spec.operation_key
    if operation in {
        "sma",
        "slope",
        "cmf",
        "vwma",
        "cci",
        "willr",
        "bbands",
        "volume_norm",
        "oi_z",
        "oi_relative_activity",
        "realized_volatility",
    }:
        return max(1, length + 1)
    if operation in {"roc", "mom", "mfi", "oi_change", "oi_roc", "aroon"}:
        return max(1, length + 2)
    if operation == "hma":
        return max(1, length + int(math.sqrt(length)) + 1)
    if operation == "stoch":
        return sum(int(params[name]) for name in ("k", "d", "smooth_k")) + 1
    if operation == "donchian":
        return (
            max(int(params.get("lower_length", length)), int(params.get("upper_length", length)))
            + 1
        )
    if operation == "ulcer_index":
        return length * 2 + 1
    if operation == "true_range":
        return 2
    if operation == "volume_oi_ratio":
        return 1
    # A warmup declaration is not a bound on recursive/cumulative dependencies.
    return None


def _compute_cached_profile_frame(
    frame: pd.DataFrame,
    profile: IndicatorProfile,
    *,
    existing: pd.DataFrame,
    prefix_length: int,
    diagnostics: dict[str, object],
) -> pd.DataFrame:
    assert_indicator_bar_usage_policy_coverage(profile)
    result = frame.copy()
    for rule, output_columns in indicator_bar_usage_groups_for_outputs(
        profile.expected_output_columns()
    ):
        specs = _specs_for_columns(profile, set(output_columns))
        eligible_frame = frame.loc[_bar_usage_eligible_mask(frame, rule.required_flags)].copy()
        prefix_index = eligible_frame.index[eligible_frame.index < prefix_length]
        tail_index = eligible_frame.index[eligible_frame.index >= prefix_length]
        computed = pd.DataFrame(index=eligible_frame.index)
        for spec in specs:
            history = None if ta.Imports.get("talib") else _finite_indicator_history(spec)
            if spec.operation_key == "oi_roc" and eligible_frame["open_interest"].isna().any():
                # pandas pct_change's forward-fill can reach before a finite window.
                history = None
            dependencies = set(spec.required_input_columns) & set(computed.columns)
            if dependencies:
                eligible_frame = _append_computed_columns(
                    eligible_frame, {column: computed[column] for column in dependencies}
                )
            if not len(tail_index):
                outputs = {
                    column: existing.loc[eligible_frame.index, column]
                    for column in spec.output_columns
                }
            elif spec.operation_key == "ema" and len(prefix_index) and not ta.Imports.get("talib"):
                seed = existing.loc[prefix_index[-1], spec.output_columns[0]]
                close = eligible_frame.loc[tail_index, "close"]
                previous_close = eligible_frame.loc[prefix_index[-1], "close"]
                length = int(spec.params_dict()["length"])
                if (
                    length > 0
                    and pd.notna(seed)
                    and math.isfinite(float(seed))
                    and pd.notna(previous_close)
                    and math.isfinite(float(previous_close))
                    and np.isfinite(close.to_numpy(dtype=float)).all()
                ):
                    # pandas-ta's EMA is adjust=False EWM. A constant seed prefix
                    # supplies the checkpoint and satisfies its minimum input length.
                    seeded = pd.Series([float(seed)] * length + close.tolist(), dtype=float)
                    values = ta.ema(seeded, length=length, sma=False, talib=False).iloc[length:]
                    outputs = {
                        spec.output_columns[0]: pd.Series(values.to_numpy(), index=tail_index)
                    }
                    diagnostics["continued_ema_count"] = (
                        int(diagnostics.get("continued_ema_count", 0)) + 1
                    )
                else:
                    outputs = _compute_spec(eligible_frame, spec)
                    diagnostics["full_history_spec_count"] = (
                        int(diagnostics.get("full_history_spec_count", 0)) + 1
                    )
            elif history is not None or spec.operation_key == "volume_profile":
                start = max(0, len(prefix_index) - (history or 0))
                outputs = _compute_spec(eligible_frame.iloc[start:], spec)
                diagnostics["tail_spec_count"] = int(diagnostics.get("tail_spec_count", 0)) + 1
            else:
                outputs = _compute_spec(eligible_frame, spec)
                diagnostics["full_history_spec_count"] = (
                    int(diagnostics.get("full_history_spec_count", 0)) + 1
                )
                operations = diagnostics.setdefault("full_history_operations", [])
                if spec.operation_key not in operations:
                    operations.append(spec.operation_key)
            for column, values in outputs.items():
                projected = values.reindex(eligible_frame.index).astype(object)
                if len(prefix_index):
                    projected.loc[prefix_index] = existing.loc[prefix_index, column]
                computed[column] = projected
        result = _append_computed_columns(
            result,
            _project_bar_usage_computed_columns(
                frame=frame,
                computed=computed,
                output_columns=output_columns,
                rule=rule,
            ),
        )
    return result


def _volume_profile_delta_identity(table_path: Path) -> dict[str, str]:
    available = set(delta_table_columns(table_path))
    instrument = "instrument_id" if "instrument_id" in available else "internal_id"
    contract = (
        "contract_id"
        if "contract_id" in available
        else "finam_symbol"
        if "finam_symbol" in available
        else "moex_secid"
    )
    timestamp = "ts_open" if "ts_open" in available else "ts" if "ts" in available else ""
    projection = [contract, instrument, "timeframe", timestamp, "low", "high", "volume"]
    missing = sorted(set(projection) - available)
    if missing:
        raise ValueError(
            "volume profile 1m identity is missing required columns: " + ", ".join(missing)
        )
    read_contract = {
        "version": 1,
        "projection": projection,
        "timeframe": "1m",
        "key_mapping": {"instrument_id": instrument, "contract_id": contract},
        "timestamp": timestamp,
        "target_contract": "active_contract_id_or_contract_id",
        "interval": "[target_open,target_close)",
        "target_close": "bar_end_or_timeframe;daily_session_close;cap_session_close",
        "selection": "contract_envelopes_then_exact_interval_union",
    }
    return {
        "identity_contract": "delta_source_identity.v1",
        "role": "volume_profile_1m",
        **delta_table_identity(table_path),
        "read_contract_hash": hashlib.sha256(_canonical_json_bytes(read_contract)).hexdigest(),
    }


def _refresh_context_hash(
    profile: IndicatorProfile,
    *,
    instrument_id: str,
    ladder_rows: tuple[dict[str, object], ...],
    raw_1m_path: Path | None,
    source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]] | None,
    tick_sizes: Mapping[str, float] | None,
    _legacy_compute: bool = False,
) -> str:
    return hashlib.sha256(
        _canonical_json_bytes(
            {
                "refresh_context_version": 2,
                "compute": (
                    _legacy_indicator_compute_contract_hash(profile)
                    if _legacy_compute
                    else _indicator_compute_contract_hash(profile)
                ),
                "ladder": _normalize_ladder_rows(ladder_rows),
                "volume_profile": {
                    "source": _volume_profile_delta_identity(raw_1m_path)
                    if raw_1m_path is not None
                    else source_rows is not None,
                    "tick_size": (tick_sizes or {}).get(instrument_id),
                }
                if _profile_has_volume_profile_spec(profile)
                else None,
            }
        )
    ).hexdigest()


def _normalize_indicator_windows(
    windows: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    normalized = []
    for window in windows:
        instrument = str(window.get("instrument_id") or window.get("internal_id") or "").strip()
        timeframe = str(window.get("timeframe") or "").strip()
        start = window.get("start_ts") or window.get("window_start_utc")
        end = window.get("end_ts") or window.get("window_end_utc")
        if not instrument or not timeframe or not start or not end:
            raise ValueError(
                "indicator changed_windows requires instrument, timeframe, start and end"
            )
        start_ts, end_ts = _utc_timestamp(start), _utc_timestamp(end)
        if pd.isna(start_ts) or pd.isna(end_ts) or start_ts > end_ts:
            raise ValueError("indicator changed_windows has an invalid interval")
        normalized.append(
            {
                "instrument_id": instrument,
                "timeframe": timeframe,
                "contract_id": str(window.get("contract_id") or ""),
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )
    return tuple(normalized)


def _source_snapshot_hash(bar_path: Path, raw_1m_path: Path | None) -> str:
    """Exact inputs for no-op only; stable checkpoint context stays version-free."""
    sources = [bar_path, *([raw_1m_path] if raw_1m_path is not None else [])]
    payload = [
        {**delta_table_identity(path), "version": delta_table_version(path)} for path in sources
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _materialize_indicator_window_tails(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    profile: IndicatorProfile,
    partition_counts: Mapping[IndicatorFramePartitionKey, int],
    existing_by_partition: Mapping[IndicatorFramePartitionKey, Mapping[str, object]],
    windows: tuple[dict[str, object], ...],
    adjustment_ladder_rows: tuple[dict[str, object], ...],
    source_dataset_bars_hash: str,
    source_snapshot_hash: str,
    volume_profile_source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]] | None,
    volume_profile_raw_1m_table_path: Path | None,
    volume_profile_tick_size_by_instrument: Mapping[str, float] | None,
    deleted_partitions: tuple[IndicatorFramePartitionKey, ...],
    stage_timings: StageTimings,
) -> dict[str, object]:
    from ..hash_compatibility import migration_source

    migration = migration_source()
    compute_hash = _indicator_compute_contract_hash(profile)
    starts: dict[IndicatorFramePartitionKey, str] = {}
    diagnostics: dict[str, object] = {}
    refreshed = reused = missing_source = metadata_rebased = migrated = 0
    migration_rows = migration_batches = 0
    migration_full_partitions = migration_full_rows = migration_full_batches = 0
    has_volume_profile = _profile_has_volume_profile_spec(profile)
    columns = profile.expected_output_columns()

    def assert_source_stable() -> None:
        if (has_volume_profile or migration) and _source_snapshot_hash(
            dataset_output_dir / "research_bar_views.delta",
            volume_profile_raw_1m_table_path if has_volume_profile else None,
        ) != source_snapshot_hash:
            raise ValueError("L1 inputs changed during partition proof")

    def batches() -> Iterator[tuple[IndicatorFramePartitionKey, list[IndicatorFrameRow]]]:
        nonlocal refreshed, reused, missing_source, metadata_rebased, migrated
        nonlocal migration_rows, migration_batches, migration_full_partitions, migration_full_rows
        nonlocal migration_full_batches
        for partition, count in sorted(
            partition_counts.items(),
            key=lambda item: (
                item[0].instrument_id,
                item[0].contract_id or "",
                item[0].timeframe,
            ),
        ):
            metadata = existing_by_partition.get(partition)
            ladder = (
                _ladder_rows_for_series(
                    adjustment_ladder_rows,
                    instrument_id=partition.instrument_id,
                    timeframe=partition.timeframe,
                )
                if partition.series_mode == "continuous_front"
                else ()
            )
            context_hash = _refresh_context_hash(
                profile,
                instrument_id=partition.instrument_id,
                ladder_rows=ladder,
                raw_1m_path=volume_profile_raw_1m_table_path,
                source_rows=volume_profile_source_rows,
                tick_sizes=volume_profile_tick_size_by_instrument,
            )
            legacy_compute = bool(
                migration
                and metadata
                and metadata.get("compute_contract_hash")
                == _legacy_indicator_compute_contract_hash(profile)
                and metadata.get("refresh_context_hash")
                == _refresh_context_hash(
                    profile,
                    instrument_id=partition.instrument_id,
                    ladder_rows=ladder,
                    raw_1m_path=volume_profile_raw_1m_table_path,
                    source_rows=volume_profile_source_rows,
                    tick_sizes=volume_profile_tick_size_by_instrument,
                    _legacy_compute=True,
                )
            )
            compatible = bool(metadata) and (
                metadata.get("refresh_context_hash") == context_hash
                and metadata.get("compute_contract_hash") == compute_hash
                or legacy_compute
            )
            selected_windows = [
                window
                for window in windows
                if (
                    window["instrument_id"] == partition.instrument_id
                    and window["timeframe"] == partition.timeframe
                    and (
                        not window["contract_id"]
                        or partition.contract_id is None
                        or window["contract_id"] == partition.contract_id
                    )
                )
            ]
            snapshot_matches = (
                bool(metadata) and metadata.get("source_snapshot_hash") == source_snapshot_hash
            )
            # A global Delta version change is not a partition content change.
            # Legacy carriers cannot authorize selective reuse, even with matching context.
            if not snapshot_matches and not (metadata or {}).get("source_snapshot_hash"):
                compatible = False
            if (
                compatible
                and snapshot_matches
                and volume_profile_source_rows is None
                and not selected_windows
                and int(metadata.get("row_count", -1)) == count
                and not migration
            ):
                reused += 1
                continue
            series = _load_bar_partition_rows(
                dataset_output_dir=dataset_output_dir,
                dataset_version=partition.dataset_version,
                contour_id=partition.contour_id,
                partition=partition,
            )
            existing = (
                load_indicator_partition_rows(
                    output_dir=indicator_output_dir,
                    partition=partition,
                    value_columns=columns,
                )
                if compatible
                else []
            )
            existing_by_ts = {row.ts: row for row in existing}
            source_ts = {bar.ts for bar in series}
            changed_ts = {
                row.ts
                for row in series
                if row.ts not in existing_by_ts
                or any(
                    _utc_timestamp(row.ts) <= window["end_ts"]
                    and _target_bar_close_ts(row) > window["start_ts"]
                    and (
                        not window["contract_id"]
                        or _volume_profile_contract_id(row) == window["contract_id"]
                    )
                    for window in selected_windows
                )
            }
            resolved_minutes = volume_profile_source_rows
            if compatible and has_volume_profile and resolved_minutes is None:
                if volume_profile_raw_1m_table_path is not None:
                    resolved_minutes = _load_volume_profile_source_rows(
                        raw_1m_table_path=volume_profile_raw_1m_table_path, local_series=series
                    )
            if compatible:
                if has_volume_profile:
                    # Exact content proof supersedes potentially overbroad L0 hints.
                    changed_ts.clear()
                # Prove consumed inputs, including minute-only corrections/deletions
                # absent from caller L0 windows. Never use cached VP hashes as proof.
                volume_hashes: dict[str, str] = {}
                input_hashes = _indicator_input_row_hashes(
                    series,
                    compute_contract_hash=compute_hash,
                    volume_profile_source_rows=resolved_minutes,
                    tick_size_by_instrument=volume_profile_tick_size_by_instrument,
                    has_volume_profile=has_volume_profile,
                    required_columns=profile.required_input_columns(),
                    series_mode=partition.series_mode,
                    volume_profile_hashes=volume_hashes,
                )
                legacy_hash = ""
                bars_digest = ""
                if migration:
                    bars_digest = _bars_hash(series, adjustment_ladder_rows=ladder)
                    legacy_hash = bars_digest
                    if (
                        has_volume_profile
                        and resolved_minutes is not None
                        and len(str(metadata.get("source_bars_hash") or "")) != 64
                    ):
                        legacy_hash = _volume_profile_source_hash(
                            target_bars_hash=legacy_hash,
                            local_series=series,
                            source_rows_by_series=resolved_minutes,
                            tick_size_by_instrument=volume_profile_tick_size_by_instrument,
                        )
                native_receipt = False
                if migration:
                    old_inputs = _indicator_input_row_hashes(
                        series,
                        compute_contract_hash=str(metadata["compute_contract_hash"]),
                        volume_profile_source_rows=resolved_minutes,
                        tick_size_by_instrument=volume_profile_tick_size_by_instrument,
                        has_volume_profile=has_volume_profile,
                        required_columns=profile.required_input_columns(),
                        series_mode=partition.series_mode,
                        # Fresh current-content proof above; never old cached metadata.
                        cached_volume_profile_hashes=volume_hashes,
                    )
                    native_receipt = metadata.get(
                        "source_bars_hash"
                    ) == _native_indicator_source_hash(
                        series, old_inputs, ladder, bars_digest=bars_digest
                    )
                legacy_receipt = metadata.get("source_bars_hash") == legacy_hash and all(
                    row.source_bars_hash == legacy_hash and row.row_count == count
                    for row in existing
                )
                if (
                    migration
                    and (native_receipt or legacy_receipt)
                    and len(existing) == len(series) == count
                    and len(existing_by_ts) == count
                    and set(existing_by_ts) == source_ts
                    and all(
                        bar.contract_id is not None
                        and existing_by_ts[bar.ts].contract_id == bar.contract_id
                        for bar in series
                    )
                    and metadata.get("profile_version") == profile.version
                    and metadata.get("output_columns_hash")
                    == indicator_output_columns_hash(columns)
                    and metadata.get("warmup_span") == profile.max_warmup_bars()
                    and metadata.get("null_warmup_span") is not None
                    and 0 <= metadata["null_warmup_span"] <= count
                ):
                    current_source_hash = _native_indicator_source_hash(
                        series, input_hashes, ladder, bars_digest=bars_digest
                    )
                    stamp = max(
                        pd.Timestamp.now(tz="UTC"),
                        _utc_timestamp(str(metadata["created_at"])) + pd.Timedelta(1, unit="us"),
                    )
                    rows = [
                        replace(
                            existing_by_ts[bar.ts],
                            bar_end_ts=bar.bar_end_ts,
                            source_bars_hash=current_source_hash,
                            row_count=count,
                            warmup_span=profile.max_warmup_bars(),
                            null_warmup_span=int(metadata["null_warmup_span"]),
                            compute_contract_hash=compute_hash,
                            refresh_context_hash=context_hash,
                            input_row_hash=row_hash,
                            volume_profile_input_hash=volume_hashes.get(bar.ts, ""),
                            source_snapshot_hash=source_snapshot_hash,
                            source_dataset_bars_hash=source_dataset_bars_hash,
                            created_at=stamp.isoformat().replace("+00:00", "Z"),
                        )
                        for bar, row_hash in zip(series, input_hashes)
                    ]
                    assert_source_stable()
                    migration_source()
                    # A complete proven partition can exceed the ordinary suffix
                    # budget (accepted 15m partitions exceed 100k rows). Keep one
                    # atomic overwrite, bounded by its exact proven row count;
                    # computed/changed suffixes retain the outer default limit.
                    _, written, writes = write_indicator_frame_partition_batches(
                        output_dir=indicator_output_dir,
                        partition_row_batches=((partition, rows),),
                        profile=profile,
                        partition_start_ts={partition: series[0].ts},
                        max_rows_per_delta_write=count,
                    )
                    assert_source_stable()
                    migration_source()
                    migration_rows += written
                    migration_batches += writes
                    reused += 1
                    migrated += 1
                    continue
                changed_ts.update(
                    row.ts
                    for row, row_hash in zip(series, input_hashes)
                    if row.ts not in existing_by_ts
                    or existing_by_ts[row.ts].input_row_hash != row_hash
                    or (has_volume_profile and not existing_by_ts[row.ts].volume_profile_input_hash)
                )
            if not compatible:
                start = series[0].ts
                changed_ts = {row.ts for row in series}
            else:
                candidates = [*changed_ts, *[row.ts for row in existing if row.ts not in source_ts]]
                if not candidates:
                    reused += 1
                    if not snapshot_matches:
                        # Advance one complete carrier after actual content proof;
                        # preserve every earlier row and all numerical values.
                        carrier = existing_by_ts[series[-1].ts]
                        stamp = max(
                            pd.Timestamp.now(tz="UTC"),
                            _utc_timestamp(str(metadata.get("created_at") or carrier.created_at))
                            + pd.Timedelta(1, unit="us"),
                        )
                        starts[partition] = carrier.ts
                        assert_source_stable()
                        yield (
                            partition,
                            [
                                replace(
                                    carrier,
                                    source_snapshot_hash=source_snapshot_hash,
                                    source_dataset_bars_hash=source_dataset_bars_hash,
                                    row_count=count,
                                    bar_end_ts=series[-1].bar_end_ts,
                                    created_at=stamp.isoformat().replace("+00:00", "Z"),
                                )
                            ],
                        )
                        metadata_rebased += 1
                    continue
                # When the old last bar was deleted, rewrite the last surviving
                # bar as the metadata carrier for the smaller current series.
                start = min(min(candidates), series[-1].ts)
            # A genuinely changed/unprovable legacy partition cannot use a
            # cached v1 prefix. Compute the complete native partition and use
            # the existing full-partition writer; ordinary suffix limits stay.
            full_legacy_partition = bool(
                migration and _utc_timestamp(start) <= _utc_timestamp(series[0].ts)
            )
            if not full_legacy_partition:
                starts[partition] = start
            rows = _build_partition_rows(
                dataset_version=partition.dataset_version,
                indicator_set_version=partition.indicator_set_version,
                profile=profile,
                series=series,
                series_mode=partition.series_mode,
                source_dataset_bars_hash=source_dataset_bars_hash,
                source_snapshot_hash=source_snapshot_hash,
                existing_rows=None if full_legacy_partition else existing,
                adjustment_ladder_rows=adjustment_ladder_rows,
                volume_profile_source_rows=resolved_minutes,
                volume_profile_raw_1m_table_path=volume_profile_raw_1m_table_path,
                volume_profile_tick_size_by_instrument=volume_profile_tick_size_by_instrument,
                compute_diagnostics=diagnostics,
                native_full_compute=True,
                volume_profile_refresh_ts=None if full_legacy_partition else changed_ts,
            )
            if _profile_has_volume_profile_spec(profile) and (
                volume_profile_source_rows is None and volume_profile_raw_1m_table_path is None
            ):
                missing_source += 1
            assert_source_stable()
            refreshed += 1
            if full_legacy_partition:
                # A full legacy replacement has no valid suffix boundary.
                # Keep the existing complete-partition writer and its default
                # budget/recovery behavior; the batch adapter owns suffixes.
                migration_source()
                _, written, writes = write_indicator_frame_partition_batches(
                    output_dir=indicator_output_dir,
                    partition_row_batches=((partition, rows),),
                    profile=profile,
                )
                assert_source_stable()
                migration_source()
                migration_full_partitions += 1
                migration_full_rows += written
                migration_full_batches += writes
            else:
                yield (
                    partition,
                    [row for row in rows if _utc_timestamp(row.ts) >= _utc_timestamp(start)],
                )
            del series, existing, existing_by_ts, rows, changed_ts, source_ts

    started = stage_timer("write_indicator_frames")
    from .batch_writer import write_indicator_frame_scoped_batches as write_scoped_batches

    paths, row_count, batch_count = write_scoped_batches(
        output_dir=indicator_output_dir,
        partition_row_batches=batches(),
        delete_partitions=deleted_partitions,
        profile=profile,
        partition_start_ts=starts,
    )
    assert_source_stable()
    row_count += migration_rows + migration_full_rows
    batch_count += migration_batches + migration_full_batches
    record_stage_timing(
        stage_timings,
        "write_indicator_frames",
        started,
        refreshed_row_count=row_count,
        write_batch_count=batch_count,
        refreshed_partition_count=refreshed,
        reused_partition_count=reused,
        hash_migrated_partition_count=migrated,
        hash_migrated_row_count=migration_rows,
        migration_full_partition_count=migration_full_partitions,
        migration_full_row_count=migration_full_rows,
    )
    return {
        "indicator_row_count": sum(partition_counts.values()),
        "bar_usage_policy_id": BAR_USAGE_POLICY_ID,
        "indicator_usage_policy_id": INDICATOR_USAGE_POLICY_ID,
        "refreshed_row_count": row_count,
        "refreshed_partition_count": refreshed,
        "reused_partition_count": reused,
        "extended_partition_count": 0,
        "recomputed_partition_count": refreshed,
        "deleted_partition_count": len(deleted_partitions),
        "write_batch_count": batch_count,
        "metadata_rebased_partition_count": metadata_rebased,
        "hash_migrated_partition_count": migrated,
        "hash_migrated_row_count": migration_rows,
        "migration_full_partition_count": migration_full_partitions,
        "migration_full_row_count": migration_full_rows,
        "profile_version": profile.version,
        "output_columns_hash": indicator_output_columns_hash(columns),
        "volume_profile_missing_source_partition_count": missing_source,
        "volume_profile_source_status": "not_required"
        if not _profile_has_volume_profile_spec(profile)
        else "missing_source"
        if missing_source
        else "configured",
        "output_paths": paths,
        "stage_timings": stage_timings,
        "delta_manifest": indicator_store_contract(profile=profile),
        "compute_diagnostics": diagnostics,
        "refresh_mode": "changed_window_tail",
    }
