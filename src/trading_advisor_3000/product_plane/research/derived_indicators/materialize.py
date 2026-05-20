from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    iter_delta_table_row_batches,
    read_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.bar_usage_policy import (
    EVENT_UPDATE_ZERO,
    INDICATOR_USAGE_POLICY_ID,
    POINT_UPDATE_NULL,
    STATE_UPDATE_HOLD,
    STATE_UPDATE_RESET_SCOPE,
    BarUsageCalculationRule,
    assert_derived_bar_usage_policy_coverage,
    derived_bar_usage_groups_for_outputs,
    has_required_bar_usage_flags,
    required_flags_for_mtf_source_column,
)
from trading_advisor_3000.product_plane.research.continuous_front_indicators import (
    input_projection as cf_input_projection,
)
from trading_advisor_3000.product_plane.research.continuous_front_indicators.rules import (
    ROLL_BOUNDARY_DERIVED_SOURCE_COLUMNS,
    assert_rule_coverage,
    rules_for_derived_profile,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.datasets.bar_usage import BAR_USAGE_POLICY_ID
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    MTF_MAPPINGS,
    DerivedIndicatorProfile,
    DerivedIndicatorProfileRegistry,
    build_derived_indicator_profile_registry,
    current_derived_indicator_profile,
    mtf_carried_columns,
    mtf_column_name,
)
from trading_advisor_3000.product_plane.research.indicators import (
    IndicatorFramePartitionKey,
    IndicatorFrameRow,
)
from trading_advisor_3000.product_plane.research.indicators.store import (
    existing_indicator_value_columns,
)
from trading_advisor_3000.spark_jobs.research_derived_source_frames_job import (
    run_research_derived_source_frames_spark_job,
)

from .source_frames import (
    DERIVED_SOURCE_FRAME_TABLE,
    DerivedSourceFramePartitionKey,
    load_derived_source_frame_partition_metadata,
    load_derived_source_frame_partition_rows,
    research_derived_source_frame_store_contract,
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
    contour_id: str = "native_tradable"
    series_mode: str = "contract"
    series_id: str = ""


def _resolved_series_mode(row_series_mode: object, *, requested_series_mode: str) -> str:
    row_mode = str(row_series_mode or "")
    if requested_series_mode == "continuous_front" and row_mode in {"", "contract"}:
        return "continuous_front"
    return row_mode or requested_series_mode


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
    resolved_series_mode = _resolved_series_mode(row.series_mode, requested_series_mode=series_mode)
    fallback_series_id = (
        row.instrument_id if resolved_series_mode == "continuous_front" else row.contract_id
    )
    return DerivedSeriesKey(
        instrument_id=row.instrument_id,
        contract_id=None if resolved_series_mode == "continuous_front" else row.contract_id,
        contour_id=row.contour_id,
        series_mode=resolved_series_mode,
        series_id=row.series_id or fallback_series_id,
    )


def _series_key_from_indicator(row: IndicatorFrameRow, *, series_mode: str) -> DerivedSeriesKey:
    resolved_series_mode = _resolved_series_mode(row.series_mode, requested_series_mode=series_mode)
    fallback_series_id = (
        row.instrument_id if resolved_series_mode == "continuous_front" else row.contract_id
    )
    return DerivedSeriesKey(
        instrument_id=row.instrument_id,
        contract_id=None if resolved_series_mode == "continuous_front" else row.contract_id,
        contour_id=row.contour_id,
        series_mode=resolved_series_mode,
        series_id=row.series_id or fallback_series_id,
    )


def _mtf_source_family_key(series_key: DerivedSeriesKey) -> DerivedSeriesKey:
    if series_key.series_mode != "continuous_front":
        return series_key
    return DerivedSeriesKey(
        instrument_id=series_key.instrument_id,
        contract_id=None,
        contour_id=series_key.contour_id,
        series_mode=series_key.series_mode,
    )


def _series_sort_key(
    row: ResearchBarView | IndicatorFrameRow,
    *,
    series_mode: str,
) -> tuple[str, str, str, str]:
    if series_mode == "continuous_front":
        return (row.instrument_id, row.timeframe, row.ts, row.contract_id)
    return (row.instrument_id, row.contract_id, row.timeframe, row.ts)


def _group_bar_views(
    *,
    bar_views: list[ResearchBarView],
    series_mode: str,
) -> dict[DerivedSeriesKey, dict[str, list[ResearchBarView]]]:
    grouped: dict[DerivedSeriesKey, dict[str, list[ResearchBarView]]] = {}
    for row in sorted(bar_views, key=lambda item: _series_sort_key(item, series_mode=series_mode)):
        series_key = _series_key_from_bar(row, series_mode=series_mode)
        grouped.setdefault(series_key, {}).setdefault(row.timeframe, []).append(row)
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


def _series_key_from_bar_row(row: dict[str, object], *, series_mode: str) -> DerivedSeriesKey:
    resolved_series_mode = _resolved_series_mode(
        row.get("series_mode"), requested_series_mode=series_mode
    )
    fallback_series_id = (
        str(row["instrument_id"])
        if resolved_series_mode == "continuous_front"
        else str(row["contract_id"])
    )
    return DerivedSeriesKey(
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
    series_mode: str,
) -> dict[DerivedSeriesKey, dict[str, int]]:
    counts: dict[DerivedSeriesKey, dict[str, int]] = {}
    for batch in iter_delta_table_row_batches(
        dataset_output_dir / "research_bar_views.delta",
        columns=[
            "dataset_version",
            "contour_id",
            "series_mode",
            "series_id",
            "contract_id",
            "instrument_id",
            "timeframe",
        ],
        filters=[("dataset_version", "=", dataset_version), ("contour_id", "=", contour_id)],
    ):
        for row in batch:
            series_key = _series_key_from_bar_row(row, series_mode=series_mode)
            timeframe = str(row["timeframe"])
            counts.setdefault(series_key, {})[timeframe] = (
                counts.setdefault(series_key, {}).get(timeframe, 0) + 1
            )
    return counts


def _load_bar_partition_rows(
    *,
    dataset_output_dir: Path,
    dataset_version: str,
    contour_id: str,
    series_key: DerivedSeriesKey,
    timeframe: str,
) -> list[ResearchBarView]:
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
        ("instrument_id", "=", series_key.instrument_id),
        ("timeframe", "=", timeframe),
    ]
    if series_key.contract_id is not None:
        filters.append(("contract_id", "=", series_key.contract_id))
    if series_key.series_mode:
        filters.append(("series_mode", "=", series_key.series_mode))
    if series_key.series_id:
        filters.append(("series_id", "=", series_key.series_id))
    rows = read_delta_table_rows(dataset_output_dir / "research_bar_views.delta", filters=filters)
    return [
        ResearchBarView.from_dict(row) for row in sorted(rows, key=lambda item: str(item["ts"]))
    ]


def _source_frame_partition_key(
    *,
    dataset_version: str,
    indicator_set_version: str,
    series_key: DerivedSeriesKey,
    timeframe: str,
) -> DerivedSourceFramePartitionKey:
    return DerivedSourceFramePartitionKey(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        timeframe=timeframe,
        instrument_id=series_key.instrument_id,
        contract_id=series_key.contract_id,
        contour_id=series_key.contour_id,
        series_mode=series_key.series_mode,
        series_id=series_key.series_id,
    )


def _series_key_from_source_metadata(
    row: dict[str, object], *, requested_series_mode: str
) -> DerivedSeriesKey:
    series_mode = _resolved_series_mode(
        row.get("series_mode"), requested_series_mode=requested_series_mode
    )
    contract_id = None if series_mode == "continuous_front" else str(row.get("contract_id") or "")
    return DerivedSeriesKey(
        instrument_id=str(row["instrument_id"]),
        contract_id=contract_id,
        contour_id=str(row.get("contour_id") or "native_tradable"),
        series_mode=series_mode,
        series_id=str(row.get("series_id") or contract_id or row["instrument_id"]),
    )


def _source_frame_rows_to_inputs(
    *,
    rows: list[dict[str, object]],
    source_indicator_columns: tuple[str, ...],
) -> tuple[list[ResearchBarView], list[IndicatorFrameRow], pd.DataFrame]:
    bar_rows = [ResearchBarView.from_dict(row) for row in rows]
    indicator_rows: list[IndicatorFrameRow] = []
    for row in rows:
        values: dict[str, float | int | None] = {}
        for column in source_indicator_columns:
            value = row.get(column)
            values[column] = None if value is None else float(value)
        indicator_rows.append(
            IndicatorFrameRow(
                dataset_version=str(row["dataset_version"]),
                contour_id=str(row.get("contour_id") or "native_tradable"),
                series_mode=str(row.get("series_mode") or "contract"),
                series_id=str(row.get("series_id") or row.get("contract_id") or ""),
                indicator_set_version=str(row["indicator_set_version"]),
                profile_version=str(row.get("indicator_profile_version") or ""),
                contract_id=str(row.get("contract_id") or ""),
                instrument_id=str(row["instrument_id"]),
                timeframe=str(row["timeframe"]),
                ts=str(row["ts"]),
                values=values,
                source_bars_hash=str(row.get("indicator_source_bars_hash") or ""),
                source_dataset_bars_hash=str(row.get("indicator_source_dataset_bars_hash") or ""),
                row_count=int(row.get("indicator_row_count") or len(rows)),
                warmup_span=int(row.get("indicator_warmup_span") or 0),
                null_warmup_span=int(row.get("indicator_null_warmup_span") or 0),
                created_at=str(row.get("indicator_created_at") or ""),
                output_columns_hash=str(row.get("indicator_output_columns_hash") or ""),
            )
        )
    frame = pd.DataFrame(rows).sort_values("ts").reset_index(drop=True)
    return bar_rows, indicator_rows, frame


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
    for row in sorted(rows, key=lambda item: _series_sort_key(item, series_mode=series_mode)):
        series_key = _series_key_from_indicator(row, series_mode=series_mode)
        grouped.setdefault(series_key, {}).setdefault(row.timeframe, []).append(row)
    return grouped


def _indicator_metadata_key(
    row: dict[str, object], *, series_mode: str
) -> tuple[DerivedSeriesKey, str]:
    resolved_series_mode = _resolved_series_mode(
        row.get("series_mode"), requested_series_mode=series_mode
    )
    fallback_series_id = (
        str(row.get("instrument_id") or "")
        if resolved_series_mode == "continuous_front"
        else str(row.get("contract_id") or "")
    )
    series_key = DerivedSeriesKey(
        instrument_id=str(row["instrument_id"]),
        contract_id=None if resolved_series_mode == "continuous_front" else str(row["contract_id"]),
        contour_id=str(row.get("contour_id") or "native_tradable"),
        series_mode=resolved_series_mode,
        series_id=str(row.get("series_id") or fallback_series_id),
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
        contour_id=series_key.contour_id,
        series_mode=series_key.series_mode,
        series_id=series_key.series_id,
    )


def _metadata_partition_key(
    row: dict[str, object], *, series_mode: str
) -> DerivedIndicatorFramePartitionKey:
    resolved_series_mode = _resolved_series_mode(
        row.get("series_mode"), requested_series_mode=series_mode
    )
    fallback_series_id = (
        str(row.get("instrument_id") or "")
        if resolved_series_mode == "continuous_front"
        else str(row.get("contract_id") or "")
    )
    return DerivedIndicatorFramePartitionKey(
        dataset_version=str(row["dataset_version"]),
        indicator_set_version=str(row["indicator_set_version"]),
        derived_indicator_set_version=str(row["derived_indicator_set_version"]),
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
        contour_id=series_key.contour_id,
        series_mode=series_key.series_mode,
        series_id=series_key.series_id,
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


def _indicator_hash(
    rows: list[IndicatorFrameRow],
    *,
    value_columns: tuple[str, ...] = DERIVED_SOURCE_INDICATOR_COLUMNS,
) -> str:
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
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
) -> pd.DataFrame:
    if not indicators:
        raise ValueError(
            "derived indicator materialization requires base indicators for "
            "every series/timeframe partition"
        )

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
    merged = merged.sort_values("ts").reset_index(drop=True)
    return _with_continuous_front_projection(merged, adjustment_ladder_rows=adjustment_ladder_rows)


def _with_continuous_front_projection(
    frame: pd.DataFrame,
    *,
    adjustment_ladder_rows: tuple[dict[str, object], ...],
) -> pd.DataFrame:
    if frame.empty or "roll_epoch" not in frame.columns:
        return frame
    result = frame.copy()
    roll_epoch = pd.to_numeric(result["roll_epoch"], errors="coerce").fillna(0).astype(int)
    max_epoch = int(roll_epoch.max() or 0)
    if max_epoch == 0:
        return result
    instrument_id = str(result["instrument_id"].iloc[0])
    timeframe = str(result["timeframe"].iloc[0])
    series_ladder_rows = tuple(
        row
        for row in adjustment_ladder_rows
        if str(row.get("instrument_id")) == instrument_id and str(row.get("timeframe")) == timeframe
    )
    if not series_ladder_rows:
        raise ValueError(
            "continuous_front derived indicator materialization requires adjustment ladder rows "
            f"for {instrument_id}|{timeframe}"
        )
    gap_by_sequence = {
        int(row["roll_sequence"]): float(row["additive_gap"]) for row in series_ladder_rows
    }
    missing_sequences = [
        sequence for sequence in range(1, max_epoch + 1) if sequence not in gap_by_sequence
    ]
    if missing_sequences:
        joined = ", ".join(str(sequence) for sequence in missing_sequences)
        raise ValueError(
            "continuous_front derived indicator materialization missing "
            "adjustment ladder roll_sequence "
            f"{joined}"
        )
    offsets = roll_epoch.map(
        lambda epoch: sum(gap_by_sequence[sequence] for sequence in range(1, int(epoch) + 1))
    )
    result["_cf_offset"] = offsets.astype(float)
    for column in ("open", "high", "low", "close"):
        native_column = f"native_{column}"
        source = (
            _numeric(result, native_column)
            if native_column in result.columns
            else _numeric(result, column)
        )
        result[f"_cf_{column}0"] = source - result["_cf_offset"]
    return result


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")
    return pd.to_numeric(frame[column], errors="coerce")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace({0.0: pd.NA})


def _roll_epoch_cross_window(frame: pd.DataFrame, *, window: int) -> pd.Series:
    if "roll_epoch" not in frame.columns:
        return pd.Series([False] * len(frame), index=frame.index)
    roll_epoch = _numeric(frame, "roll_epoch")
    return (
        roll_epoch.rolling(window=window, min_periods=window)
        .apply(
            lambda values: 0.0 if len(set(int(value) for value in values)) == 1 else 1.0,
            raw=False,
        )
        .fillna(1.0)
        .astype(bool)
    )


def _append_columns(frame: pd.DataFrame, columns: dict[str, pd.Series]) -> pd.DataFrame:
    if not columns:
        return frame
    overlap = [column for column in columns if column in frame.columns]
    base = frame.drop(columns=overlap) if overlap else frame
    return pd.concat([base, pd.DataFrame(columns, index=frame.index)], axis=1)


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
    series = pd.to_numeric(series, errors="coerce")
    movement_per_bar = (series - series.shift(length)) / float(length)
    return pd.Series(np.degrees(np.arctan(movement_per_bar)), index=series.index)


def _divergence_score(price: pd.Series, indicator: pd.Series, *, length: int) -> pd.Series:
    price = pd.to_numeric(price, errors="coerce")
    indicator = pd.to_numeric(indicator, errors="coerce")
    price_angle = _slope(price, length=length)
    indicator_angle = _slope(indicator, length=length)
    return price_angle - indicator_angle


def _compute_opening_range(frame: pd.DataFrame, *, opening_bars: int) -> dict[str, pd.Series]:
    highs: list[float | None] = []
    lows: list[float | None] = []
    for _, group in frame.groupby("session_date", sort=False):
        session_highs = group["high"].tolist()
        session_lows = group["low"].tolist()
        fixed_high = (
            max(session_highs[:opening_bars]) if len(session_highs) >= opening_bars else None
        )
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


def _compute_swing_levels(
    frame: pd.DataFrame, *, left_bars: int, right_bars: int
) -> dict[str, pd.Series]:
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
    if "_cf_offset" in frame.columns:
        typical_price = (
            _numeric(frame, "_cf_high0")
            + _numeric(frame, "_cf_low0")
            + _numeric(frame, "_cf_close0")
        ) / 3.0
        post_transform = _numeric(frame, "_cf_offset")
    else:
        typical_price = (
            _numeric(frame, "high") + _numeric(frame, "low") + _numeric(frame, "close")
        ) / 3.0
        post_transform = pd.Series([0.0] * len(frame), index=frame.index)
    weighted = typical_price * _numeric(frame, "volume")
    session_cum_weight = weighted.groupby(frame["session_date"], sort=False).cumsum()
    session_cum_volume = (
        _numeric(frame, "volume").groupby(frame["session_date"], sort=False).cumsum()
    )
    return _safe_divide(session_cum_weight, session_cum_volume) + post_transform


def _bar_close_times(frame: pd.DataFrame, timeframe: str) -> pd.Series:
    if timeframe.endswith("d") and "session_close_ts" in frame.columns:
        return pd.to_datetime(frame["session_close_ts"], utc=True)
    return pd.to_datetime(frame["ts"], utc=True) + _timeframe_delta(timeframe)


def _compute_mtf_overlay(
    *,
    frame: pd.DataFrame,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
    output_columns: set[str] | None = None,
) -> dict[str, pd.Series]:
    output: dict[str, pd.Series] = {}
    for source_timeframe, target_timeframe in MTF_MAPPINGS:
        source_columns = mtf_carried_columns(source_timeframe, target_timeframe)
        selected_columns = tuple(
            (source_column, mtf_column_name(source_timeframe, target_timeframe, source_column))
            for source_column in source_columns
            if output_columns is None
            or mtf_column_name(source_timeframe, target_timeframe, source_column) in output_columns
        )
        if not selected_columns:
            continue
        columns = tuple(output_column for _, output_column in selected_columns)
        if current_timeframe != target_timeframe or source_timeframe not in source_frames:
            for column in columns:
                output[column] = pd.Series([None] * len(frame), index=frame.index, dtype="object")
            continue
        source_frame = source_frames[source_timeframe]
        if not set(source_columns) <= set(source_frame.columns):
            for column in columns:
                output[column] = pd.Series([None] * len(frame), index=frame.index, dtype="object")
            continue
        current_close_ts = _bar_close_times(frame, current_timeframe)
        source_close_ts = _bar_close_times(source_frame, source_timeframe)
        left = pd.DataFrame(
            {"_row_index": frame.index, "current_close_ts": current_close_ts}
        ).sort_values("current_close_ts")
        right = pd.DataFrame(
            {
                "source_close_ts": source_close_ts,
                **{
                    output_column: _numeric(source_frame, source_column)
                    for source_column, output_column in selected_columns
                },
            }
        ).sort_values("source_close_ts")
        merged = pd.merge_asof(
            left,
            right,
            left_on="current_close_ts",
            right_on="source_close_ts",
            direction="backward",
        )
        merged = merged.set_index("_row_index").reindex(frame.index)
        for column in columns:
            output[column] = merged[column].astype("object")
    return output


def _bar_usage_eligible_mask(frame: pd.DataFrame, required_flags: int) -> pd.Series:
    if "bar_usage_flags" not in frame.columns:
        return pd.Series([True] * len(frame), index=frame.index)
    return frame["bar_usage_flags"].map(
        lambda flags: has_required_bar_usage_flags(flags, required_flags)
    )


def _bar_usage_scope_key(frame: pd.DataFrame, scope_id: str) -> pd.Series | None:
    if scope_id == "session" and "session_date" in frame.columns:
        return frame["session_date"]
    if scope_id == "week" and "ts" in frame.columns:
        return pd.to_datetime(frame["ts"], utc=True).dt.strftime("%G-%V")
    return None


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
        with pd.option_context("future.no_silent_downcasting", True):
            projected = projected.ffill()
        projected = projected.infer_objects(copy=False)
    elif rule.mode == STATE_UPDATE_RESET_SCOPE:
        scope_key = _bar_usage_scope_key(frame, rule.scope_id)
        with pd.option_context("future.no_silent_downcasting", True):
            projected = (
                projected.groupby(scope_key, sort=False).ffill()
                if scope_key is not None
                else projected.ffill()
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


def _mtf_source_column_for_output(output_column: str) -> tuple[str, str] | None:
    for source_timeframe, target_timeframe in MTF_MAPPINGS:
        prefix = f"mtf_{source_timeframe}_to_{target_timeframe}_"
        if output_column.startswith(prefix):
            return source_timeframe, output_column.removeprefix(prefix)
    return None


def _split_mtf_output_columns_by_source_flags(
    output_columns: tuple[str, ...],
) -> tuple[tuple[int, tuple[str, ...]], ...]:
    grouped: list[tuple[int, list[str]]] = []
    for output_column in output_columns:
        parsed = _mtf_source_column_for_output(output_column)
        required_flags = required_flags_for_mtf_source_column(parsed[1]) if parsed else 0
        for existing_flags, columns in grouped:
            if existing_flags == required_flags:
                columns.append(output_column)
                break
        else:
            grouped.append((required_flags, [output_column]))
    return tuple((flags, tuple(columns)) for flags, columns in grouped)


def _filter_mtf_source_frames_for_bar_usage(
    source_frames: dict[str, pd.DataFrame], *, required_flags: int
) -> dict[str, pd.DataFrame]:
    if not required_flags:
        return source_frames
    filtered: dict[str, pd.DataFrame] = {}
    for timeframe, frame in source_frames.items():
        mask = _bar_usage_eligible_mask(frame, required_flags)
        filtered[timeframe] = frame.loc[mask].copy()
    return filtered


def _compute_derived_frame_unmasked(
    *,
    base_frame: pd.DataFrame,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
    profile: DerivedIndicatorProfile,
) -> pd.DataFrame:
    assert_rule_coverage(
        output_columns=set(profile.output_columns), rules=rules_for_derived_profile(profile)
    )
    requested = set(profile.output_columns)
    result = base_frame.copy()
    is_continuous_front = "_cf_offset" in result.columns
    offset = (
        _numeric(result, "_cf_offset")
        if is_continuous_front
        else pd.Series([0.0] * len(result), index=result.index)
    )
    close0 = _numeric(result, "_cf_close0") if is_continuous_front else _numeric(result, "close")
    high0 = _numeric(result, "_cf_high0") if is_continuous_front else _numeric(result, "high")
    low0 = _numeric(result, "_cf_low0") if is_continuous_front else _numeric(result, "low")
    close = close0 + offset
    high = high0 + offset
    low = low0 + offset
    atr = _numeric(result, "atr_14").replace({0.0: pd.NA})

    result["close"] = close
    result["high"] = high
    result["low"] = low

    def ensure_rolling_levels() -> None:
        if "rolling_high_20" not in result.columns:
            result["rolling_high_20"] = high0.rolling(window=20, min_periods=20).max() + offset
        if "rolling_low_20" not in result.columns:
            result["rolling_low_20"] = low0.rolling(window=20, min_periods=20).min() + offset

    def ensure_session_levels() -> None:
        if "session_high" not in result.columns:
            result["session_high"] = (
                high0.groupby(result["session_date"], sort=False).cummax() + offset
            )
        if "session_low" not in result.columns:
            result["session_low"] = (
                low0.groupby(result["session_date"], sort=False).cummin() + offset
            )

    def ensure_week_levels() -> None:
        week_key = pd.to_datetime(result["ts"], utc=True).dt.strftime("%G-%V")
        if "week_high" not in result.columns:
            result["week_high"] = high0.groupby(week_key, sort=False).cummax() + offset
        if "week_low" not in result.columns:
            result["week_low"] = low0.groupby(week_key, sort=False).cummin() + offset

    def ensure_opening_range() -> None:
        if {"opening_range_high", "opening_range_low"} <= set(result.columns):
            return
        level_source = result.copy()
        if is_continuous_front:
            level_source["high"] = high0
            level_source["low"] = low0
        opening_range = _compute_opening_range(level_source, opening_bars=4)
        if is_continuous_front:
            opening_range = {
                column: pd.to_numeric(series, errors="coerce") + offset
                for column, series in opening_range.items()
            }
        for column, series in opening_range.items():
            result[column] = series

    def ensure_swing_levels() -> None:
        if {"swing_high_10", "swing_low_10"} <= set(result.columns):
            return
        level_source = result.copy()
        if is_continuous_front:
            level_source["high"] = high0
            level_source["low"] = low0
        swing_levels = _compute_swing_levels(level_source, left_bars=5, right_bars=5)
        if is_continuous_front:
            swing_levels = {
                column: pd.to_numeric(series, errors="coerce") + offset
                for column, series in swing_levels.items()
            }
        for column, series in swing_levels.items():
            result[column] = series

    def ensure_session_vwap() -> None:
        if "session_vwap" not in result.columns:
            result["session_vwap"] = _compute_session_vwap(result)

    def numeric_input(column: str) -> pd.Series:
        return pd.to_numeric(_numeric(result, column), errors="coerce")

    def ensure_target_column(target_column: str) -> None:
        if target_column in {"rolling_high_20", "rolling_low_20"}:
            ensure_rolling_levels()
        elif target_column in {"session_high", "session_low"}:
            ensure_session_levels()
        elif target_column in {"week_high", "week_low"}:
            ensure_week_levels()
        elif target_column in {"opening_range_high", "opening_range_low"}:
            ensure_opening_range()
        elif target_column in {"swing_high_10", "swing_low_10"}:
            ensure_swing_levels()
        elif target_column == "session_vwap":
            ensure_session_vwap()

    if requested & {"rolling_high_20", "rolling_low_20", "rolling_position_20"} or any(
        column in requested
        for column in (
            "distance_to_rolling_high_20",
            "distance_to_rolling_low_20",
            "cross_close_rolling_high_20_code",
            "cross_close_rolling_low_20_code",
        )
    ):
        ensure_rolling_levels()
    if requested & {"session_high", "session_low", "session_position"} or any(
        column in requested
        for column in (
            "distance_to_session_high",
            "distance_to_session_low",
            "cross_close_session_vwap_code",
        )
    ):
        ensure_session_levels()
    if requested & {"week_high", "week_low", "week_position"} or any(
        column in requested for column in ("distance_to_week_high", "distance_to_week_low")
    ):
        ensure_week_levels()
    if requested & {"opening_range_high", "opening_range_low"}:
        ensure_opening_range()
    if requested & {"swing_high_10", "swing_low_10"}:
        ensure_swing_levels()
    if requested & {"session_vwap", "distance_to_session_vwap", "cross_close_session_vwap_code"}:
        ensure_session_vwap()

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
        output_column = f"distance_to_{output_suffix}"
        if output_column not in requested:
            continue
        ensure_target_column(target_column)
        result[output_column] = _distance_to(result, target_column, denominator=atr)
    for column in requested:
        if (
            column in result.columns
            or not column.startswith("distance_to_")
            or not column.endswith("_atr")
        ):
            continue
        target_column = column.removeprefix("distance_to_").removesuffix("_atr")
        ensure_target_column(target_column)
        if target_column in result.columns:
            result[column] = _distance_to(result, target_column, denominator=atr)

    if "rolling_position_20" in requested:
        ensure_rolling_levels()
        result["rolling_position_20"] = _position_between(
            close, _numeric(result, "rolling_low_20"), _numeric(result, "rolling_high_20")
        )
    if "session_position" in requested:
        ensure_session_levels()
        result["session_position"] = _position_between(
            close, _numeric(result, "session_low"), _numeric(result, "session_high")
        )
    if "week_position" in requested:
        ensure_week_levels()
        result["week_position"] = _position_between(
            close, _numeric(result, "week_low"), _numeric(result, "week_high")
        )
    if "bb_position_20_2" in requested:
        result["bb_position_20_2"] = _position_between(
            close, _numeric(result, "bb_lower_20_2"), _numeric(result, "bb_upper_20_2")
        )
    if "kc_position_20_1_5" in requested:
        result["kc_position_20_1_5"] = _position_between(
            close, _numeric(result, "kc_lower_20_1_5"), _numeric(result, "kc_upper_20_1_5")
        )
    if "donchian_position_20" in requested:
        result["donchian_position_20"] = _position_between(
            close, _numeric(result, "donchian_low_20"), _numeric(result, "donchian_high_20")
        )
    if "donchian_position_55" in requested:
        result["donchian_position_55"] = _position_between(
            close, _numeric(result, "donchian_low_55"), _numeric(result, "donchian_high_55")
        )

    if "cross_close_sma_20_code" in requested:
        result["cross_close_sma_20_code"] = _change_code(close, _numeric(result, "sma_20"))
    if "cross_close_ema_20_code" in requested:
        result["cross_close_ema_20_code"] = _change_code(close, _numeric(result, "ema_20"))
    if "cross_close_session_vwap_code" in requested:
        ensure_session_vwap()
        result["cross_close_session_vwap_code"] = _change_code(
            close, _numeric(result, "session_vwap")
        )
    if "cross_close_rolling_high_20_code" in requested:
        ensure_rolling_levels()
        result["cross_close_rolling_high_20_code"] = _change_code(
            close, _numeric(result, "rolling_high_20").shift(1)
        )
    if "cross_close_rolling_low_20_code" in requested:
        ensure_rolling_levels()
        result["cross_close_rolling_low_20_code"] = _change_code(
            close, _numeric(result, "rolling_low_20").shift(1)
        )
    if "macd_signal_cross_code" in requested:
        result["macd_signal_cross_code"] = _change_code(
            _numeric(result, "macd_12_26_9"), _numeric(result, "macd_signal_12_26_9")
        )
    if "ppo_signal_cross_code" in requested:
        result["ppo_signal_cross_code"] = _change_code(
            _numeric(result, "ppo_12_26_9"), _numeric(result, "ppo_signal_12_26_9")
        )
    if "trix_signal_cross_code" in requested:
        result["trix_signal_cross_code"] = _change_code(
            _numeric(result, "trix_30_9"), _numeric(result, "trix_signal_30_9")
        )
    if "kst_signal_cross_code" in requested:
        result["kst_signal_cross_code"] = _change_code(
            _numeric(result, "kst_10_15_20_30"), _numeric(result, "kst_signal_9")
        )

    if "close_change_1" in requested:
        result["close_change_1"] = close0.diff(1)
    if "close_slope_20" in requested:
        result["close_slope_20"] = _numeric(result, "close_slope_20")
    if "sma_20_slope_5" in requested:
        result["sma_20_slope_5"] = _slope(_numeric(result, "sma_20") - offset, length=5)
    if "ema_20_slope_5" in requested:
        result["ema_20_slope_5"] = _slope(_numeric(result, "ema_20") - offset, length=5)
    if "roc_10_change_1" in requested:
        result["roc_10_change_1"] = _numeric(result, "roc_10").diff(1)
    if "mom_10_change_1" in requested:
        result["mom_10_change_1"] = _numeric(result, "mom_10").diff(1)
    if "volume_change_1" in requested:
        result["volume_change_1"] = _numeric(result, "volume").diff(1)
        if is_continuous_front:
            same_epoch_prev = _numeric(result, "roll_epoch") == _numeric(
                result, "roll_epoch"
            ).shift(1)
            result.loc[~same_epoch_prev.fillna(False), "volume_change_1"] = pd.NA
    if "oi_change_1" in requested:
        result["oi_change_1"] = _numeric(result, "oi_change_1")

    if "rvol_20" in requested:
        result["rvol_20"] = _numeric(result, "rvol_20")
    if "volume_zscore_20" in requested:
        result["volume_zscore_20"] = _numeric(result, "volume_z_20")

    relationship_columns = {
        "price_volume_corr_20",
        "price_oi_corr_20",
        "volume_oi_corr_20",
    }
    requested_relationships = requested & relationship_columns
    if requested_relationships:
        if "price_volume_corr_20" in requested_relationships:
            result["price_volume_corr_20"] = (
                close0.diff(1)
                .rolling(window=20, min_periods=20)
                .corr(numeric_input("volume").diff(1))
            )
        if "price_oi_corr_20" in requested_relationships:
            result["price_oi_corr_20"] = (
                close0.diff(1).rolling(window=20, min_periods=20).corr(numeric_input("oi_change_1"))
            )
        if "volume_oi_corr_20" in requested_relationships:
            result["volume_oi_corr_20"] = (
                numeric_input("volume")
                .diff(1)
                .rolling(window=20, min_periods=20)
                .corr(numeric_input("oi_change_1"))
            )
        if is_continuous_front:
            cross_native_window = _roll_epoch_cross_window(result, window=20)
            for column in requested_relationships:
                result.loc[cross_native_window, column] = pd.NA

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
    requested_divergence_suffixes: set[str] = set()
    for output_suffix, column in divergence_sources.items():
        output_column = f"divergence_price_{output_suffix}_score"
        if output_column not in requested:
            continue
        requested_divergence_suffixes.add(output_suffix)
        divergence_columns[output_column] = _divergence_score(
            close0, _numeric(result, column), length=20
        )
    if is_continuous_front and requested_divergence_suffixes:
        cross_divergence_window = _roll_epoch_cross_window(result, window=21)
        for output_suffix in requested_divergence_suffixes & ROLL_BOUNDARY_DERIVED_SOURCE_COLUMNS:
            divergence_columns[f"divergence_price_{output_suffix}_score"].loc[
                cross_divergence_window
            ] = pd.NA
    result = _append_columns(result, divergence_columns)

    requested_mtf_columns = {column for column in requested if column.startswith("mtf_")}
    if requested_mtf_columns:
        result = _append_columns(
            result,
            _compute_mtf_overlay(
                frame=result,
                current_timeframe=current_timeframe,
                source_frames=source_frames,
                output_columns=requested_mtf_columns,
            ),
        )

    missing_columns: dict[str, pd.Series] = {}
    for column in profile.output_columns:
        if column not in result.columns:
            missing_columns[column] = pd.Series(
                [None] * len(result), index=result.index, dtype="object"
            )
    result = _append_columns(result, missing_columns)
    return result


def _compute_derived_frame(
    *,
    base_frame: pd.DataFrame,
    current_timeframe: str,
    source_frames: dict[str, pd.DataFrame],
    profile: DerivedIndicatorProfile,
) -> pd.DataFrame:
    assert_derived_bar_usage_policy_coverage(profile)
    if base_frame.empty or "bar_usage_flags" not in base_frame.columns:
        return _compute_derived_frame_unmasked(
            base_frame=base_frame,
            current_timeframe=current_timeframe,
            source_frames=source_frames,
            profile=profile,
        )
    result = base_frame.copy()
    for rule, output_columns in derived_bar_usage_groups_for_outputs(profile.output_columns):
        execution_groups = (
            _split_mtf_output_columns_by_source_flags(output_columns)
            if rule.group_id == "derived_mtf_projection"
            else ((0, output_columns),)
        )
        for source_required_flags, grouped_columns in execution_groups:
            eligible = _bar_usage_eligible_mask(base_frame, rule.required_flags)
            eligible_frame = base_frame.loc[eligible].copy()
            if eligible_frame.empty:
                computed = pd.DataFrame(index=eligible_frame.index)
            else:
                grouped_profile = _profile_for_output_columns(profile, set(grouped_columns))
                grouped_source_frames = (
                    _filter_mtf_source_frames_for_bar_usage(
                        source_frames, required_flags=source_required_flags
                    )
                    if source_required_flags
                    else source_frames
                )
                computed = _compute_derived_frame_unmasked(
                    base_frame=eligible_frame,
                    current_timeframe=current_timeframe,
                    source_frames=grouped_source_frames,
                    profile=grouped_profile,
                )
            result = _append_columns(
                result,
                _project_bar_usage_computed_columns(
                    frame=base_frame,
                    computed=computed,
                    output_columns=grouped_columns,
                    rule=rule,
                ),
            )
    return result


def _null_warmup_span(
    payload_rows: list[dict[str, object]], output_columns: tuple[str, ...]
) -> int:
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


def _profile_for_output_columns(
    profile: DerivedIndicatorProfile, columns: set[str]
) -> DerivedIndicatorProfile:
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
) -> bool:
    if not existing_rows:
        return False
    head = existing_rows[0]
    existing_output_columns_hash = str(head.get("output_columns_hash") or "")
    return (
        head.get("source_bars_hash") == source_bars_hash
        and head.get("source_indicators_hash") == source_indicators_hash
        and head.get("profile_version") == profile_version
        and int(head.get("row_count", -1) or -1) == row_count
        and existing_output_columns_hash == output_columns_hash
    )


def _source_bars_may_have_changed(
    *,
    source_bars_hash: str,
    existing_row_count: int,
    row_count: int,
    existing_source_dataset_bars_hash: str,
    current_dataset_bars_hash: str,
    table_exists: bool,
    legacy_table_covers_source_commit: bool,
) -> bool:
    return (
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
        )
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
    series_mode: str,
    profile: DerivedIndicatorProfile,
    local_series: list[ResearchBarView],
    local_indicator_rows: list[IndicatorFrameRow],
    source_frames: dict[str, pd.DataFrame],
    base_frame: pd.DataFrame | None = None,
    source_dataset_bars_hash: str = "",
    output_columns_hash: str | None = None,
    compute_profile: DerivedIndicatorProfile | None = None,
    existing_rows: list[DerivedIndicatorFrameRow] | None = None,
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
) -> list[DerivedIndicatorFrameRow]:
    if base_frame is None:
        base_frame = _prepare_base_frame(
            bars=local_series,
            indicators=local_indicator_rows,
            adjustment_ladder_rows=adjustment_ladder_rows,
        )
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
    source_indicator_profile_version = (
        local_indicator_rows[0].profile_version if local_indicator_rows else ""
    )
    source_indicator_output_columns_hash = (
        local_indicator_rows[0].output_columns_hash if local_indicator_rows else ""
    )
    output_columns_hash = output_columns_hash or derived_indicator_output_columns_hash(
        output_columns
    )
    prepared_values: list[
        tuple[ResearchBarView, dict[str, float | int | None], DerivedIndicatorFrameRow | None]
    ] = []
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
        series_key = _series_key_from_bar(original, series_mode=series_mode)
        rows.append(
            DerivedIndicatorFrameRow(
                dataset_version=dataset_version,
                contour_id=original.contour_id,
                series_mode=series_key.series_mode,
                series_id=series_key.series_id,
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
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
) -> list[DerivedIndicatorFrameRow]:
    profile = profile or current_derived_indicator_profile()
    grouped_bars = _group_bar_views(bar_views=bar_views, series_mode=series_mode)
    grouped_indicators = _group_indicator_rows(rows=indicator_rows, series_mode=series_mode)
    source_frames_by_family: dict[DerivedSeriesKey, dict[str, pd.DataFrame]] = {}
    for source_series_key, source_bars_by_timeframe in grouped_bars.items():
        family_key = _mtf_source_family_key(source_series_key)
        source_frames = source_frames_by_family.setdefault(family_key, {})
        source_indicators_by_timeframe = grouped_indicators.get(source_series_key, {})
        for timeframe, source_series in source_bars_by_timeframe.items():
            if timeframe in source_frames:
                raise ValueError(
                    "derived indicator MTF source family has multiple partitions for "
                    f"{family_key.instrument_id}|{family_key.series_mode}|{timeframe}"
                )
            source_frames[timeframe] = _prepare_base_frame(
                bars=source_series,
                indicators=source_indicators_by_timeframe.get(timeframe, []),
                adjustment_ladder_rows=adjustment_ladder_rows
                if series_mode == "continuous_front"
                else (),
            )

    rows: list[DerivedIndicatorFrameRow] = []
    for series_key, bars_by_timeframe in sorted(
        grouped_bars.items(), key=lambda item: (item[0].instrument_id, item[0].contract_id or "")
    ):
        indicators_by_timeframe = grouped_indicators.get(series_key, {})
        source_frames = source_frames_by_family.get(_mtf_source_family_key(series_key), {})
        for timeframe, local_series in sorted(bars_by_timeframe.items(), key=lambda item: item[0]):
            rows.extend(
                _build_partition_rows(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    derived_indicator_set_version=derived_indicator_set_version,
                    series_mode=series_mode,
                    profile=profile,
                    local_series=local_series,
                    local_indicator_rows=indicators_by_timeframe.get(timeframe, []),
                    source_frames=source_frames,
                    adjustment_ladder_rows=adjustment_ladder_rows
                    if series_mode == "continuous_front"
                    else (),
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
    contour_id: str = "native_tradable",
    profile: DerivedIndicatorProfile | None = None,
    profile_version: str | None = None,
) -> dict[str, object]:
    dataset_manifest = _load_dataset_manifest(
        output_dir=dataset_output_dir,
        dataset_version=dataset_version,
        contour_id=contour_id,
    )
    series_mode = str(dataset_manifest.get("series_mode", "contract"))
    adjustment_ladder_rows = (
        cf_input_projection.load_adjustment_ladder_rows(
            dataset_output_dir=dataset_output_dir, dataset_version=dataset_version
        )
        if series_mode == "continuous_front"
        else ()
    )
    current_dataset_bars_hash = str(dataset_manifest.get("bars_hash") or "")
    registry: DerivedIndicatorProfileRegistry = build_derived_indicator_profile_registry()
    resolved_profile = profile or registry.get(profile_version or "core_v1")
    required_source_indicator_columns = _source_indicator_columns_for_profile(resolved_profile)
    indicator_table_exists = (
        indicator_output_dir / "research_indicator_frames.delta" / "_delta_log"
    ).exists()
    available_indicator_columns = (
        set(existing_indicator_value_columns(output_dir=indicator_output_dir))
        if indicator_table_exists
        else set()
    )
    missing_required_indicator_columns = tuple(
        column
        for column in required_source_indicator_columns
        if column not in available_indicator_columns
    )
    if missing_required_indicator_columns:
        missing_joined = ", ".join(missing_required_indicator_columns)
        raise ValueError(
            f"derived indicator materialization requires source indicator columns: {missing_joined}"
        )

    source_frame_report = run_research_derived_source_frames_spark_job(
        bar_views_path=dataset_output_dir / "research_bar_views.delta",
        indicator_frames_path=indicator_output_dir / "research_indicator_frames.delta",
        output_dir=derived_indicator_output_dir,
        dataset_version=dataset_version,
        contour_id=contour_id,
        indicator_set_version=indicator_set_version,
        derived_profile_version=resolved_profile.version,
        source_indicator_columns=required_source_indicator_columns,
    )
    source_metadata = load_derived_source_frame_partition_metadata(
        output_dir=derived_indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        contour_id=contour_id,
    )
    partition_counts: dict[DerivedSeriesKey, dict[str, int]] = {}
    for row in source_metadata:
        series_key = _series_key_from_source_metadata(row, requested_series_mode=series_mode)
        timeframe = str(row["timeframe"])
        row_count = int(row.get("partition_row_count") or row.get("joined_row_count") or 0)
        partition_counts.setdefault(series_key, {})[timeframe] = row_count

    source_rows_cache: dict[
        tuple[DerivedSeriesKey, str],
        tuple[list[ResearchBarView], list[IndicatorFrameRow], pd.DataFrame],
    ] = {}

    def _load_source_inputs(
        series_key: DerivedSeriesKey, timeframe: str
    ) -> tuple[list[ResearchBarView], list[IndicatorFrameRow], pd.DataFrame]:
        cache_key = (series_key, timeframe)
        if cache_key not in source_rows_cache:
            rows = load_derived_source_frame_partition_rows(
                output_dir=derived_indicator_output_dir,
                partition=_source_frame_partition_key(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    series_key=series_key,
                    timeframe=timeframe,
                ),
                source_indicator_columns=required_source_indicator_columns,
            )
            if not rows:
                raise ValueError(
                    "derived indicator source-frame partition is empty for "
                    f"{series_key.instrument_id}|{series_key.series_id}|{timeframe}"
                )
            local_series, local_indicator_rows, base_frame = _source_frame_rows_to_inputs(
                rows=rows,
                source_indicator_columns=required_source_indicator_columns,
            )
            base_frame = _with_continuous_front_projection(
                base_frame,
                adjustment_ladder_rows=adjustment_ladder_rows
                if series_mode == "continuous_front"
                else (),
            )
            source_rows_cache[cache_key] = (local_series, local_indicator_rows, base_frame)
        return source_rows_cache[cache_key]

    table_exists = (
        derived_indicator_output_dir / "research_derived_indicator_frames.delta" / "_delta_log"
    ).exists()
    source_table_commit_ts = _latest_delta_commit_timestamp(
        derived_indicator_output_dir / "research_derived_source_frames.delta"
    )
    derived_table_commit_ts = (
        _latest_delta_commit_timestamp(
            derived_indicator_output_dir / "research_derived_indicator_frames.delta"
        )
        if table_exists
        else None
    )
    legacy_table_covers_source_commit = bool(
        source_table_commit_ts is not None
        and derived_table_commit_ts is not None
        and derived_table_commit_ts >= source_table_commit_ts
    )
    existing_metadata = (
        load_derived_indicator_partition_metadata(
            output_dir=derived_indicator_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            contour_id=contour_id,
        )
        if table_exists
        else []
    )
    existing_by_partition = _group_existing_partition_metadata(
        rows=existing_metadata, series_mode=series_mode
    )
    target_output_columns = resolved_profile.output_columns
    target_output_columns_hash = derived_indicator_output_columns_hash(target_output_columns)
    existing_output_columns = (
        set(existing_derived_indicator_value_columns(output_dir=derived_indicator_output_dir))
        if table_exists
        else set()
    )
    current_partitions: set[DerivedIndicatorFramePartitionKey] = set()
    replace_partitions: list[DerivedIndicatorFramePartitionKey] = []
    refresh_plan: list[
        tuple[
            DerivedSeriesKey,
            dict[str, pd.DataFrame],
            list[
                tuple[
                    str,
                    list[ResearchBarView],
                    list[IndicatorFrameRow],
                    pd.DataFrame,
                    DerivedIndicatorProfile,
                    list[DerivedIndicatorFrameRow] | None,
                ]
            ],
        ]
    ] = []
    reused_partitions = 0
    refreshed_partitions = 0
    extended_partitions = 0
    recomputed_partitions = 0
    source_frames_by_family: dict[DerivedSeriesKey, dict[str, pd.DataFrame]] = {}

    def _source_frames_for_family(series_key: DerivedSeriesKey) -> dict[str, pd.DataFrame]:
        family_key = _mtf_source_family_key(series_key)
        if family_key in source_frames_by_family:
            return source_frames_by_family[family_key]
        source_frames: dict[str, pd.DataFrame] = {}
        for candidate_key, candidate_counts in partition_counts.items():
            if _mtf_source_family_key(candidate_key) != family_key:
                continue
            for source_timeframe in candidate_counts:
                if source_timeframe in source_frames:
                    raise ValueError(
                        "derived indicator MTF source family has multiple partitions for "
                        f"{family_key.instrument_id}|{family_key.series_mode}|{source_timeframe}"
                    )
                source_frames[source_timeframe] = _load_source_inputs(
                    candidate_key, source_timeframe
                )[2]
        source_frames_by_family[family_key] = source_frames
        return source_frames

    for series_key, counts_by_timeframe in sorted(
        partition_counts.items(),
        key=lambda item: (item[0].instrument_id, item[0].contract_id or ""),
    ):
        refreshed_timeframes: list[
            tuple[
                str,
                list[ResearchBarView],
                list[IndicatorFrameRow],
                pd.DataFrame,
                DerivedIndicatorProfile,
                list[DerivedIndicatorFrameRow] | None,
            ]
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
            existing_output_columns_hash = (
                str(head.get("output_columns_hash") or "") if head else ""
            )
            partition_existing_columns = (
                existing_output_columns if existing_output_columns_hash else set()
            )
            partition_reusable_existing_columns = tuple(
                column for column in target_output_columns if column in partition_existing_columns
            )
            existing_row_count = int(head.get("row_count", -1) or -1) if head else -1
            existing_source_dataset_bars_hash = (
                str(head.get("source_dataset_bars_hash") or "") if head else ""
            )
            local_series, local_indicator_rows, source_base_frame = _load_source_inputs(
                series_key, timeframe
            )
            source_bars_hash = _bars_hash(local_series)
            source_indicators_hash = _indicator_hash(
                local_indicator_rows,
                value_columns=required_source_indicator_columns,
            )
            source_bars_may_have_changed = _source_bars_may_have_changed(
                source_bars_hash=source_bars_hash,
                existing_row_count=existing_row_count,
                row_count=row_count,
                existing_source_dataset_bars_hash=existing_source_dataset_bars_hash,
                current_dataset_bars_hash=current_dataset_bars_hash,
                table_exists=table_exists,
                legacy_table_covers_source_commit=legacy_table_covers_source_commit,
            )
            if source_bars_may_have_changed:
                source_bars_hash = _bars_hash(local_series)
            source_indicator_profile_version = (
                local_indicator_rows[0].profile_version if local_indicator_rows else ""
            )
            source_indicator_output_columns_hash = (
                local_indicator_rows[0].output_columns_hash if local_indicator_rows else ""
            )
            stored_indicator_metadata_matches = bool(head) and (
                head.get("source_indicator_profile_version") == source_indicator_profile_version
                and head.get("source_indicator_output_columns_hash")
                == source_indicator_output_columns_hash
            )
            if not stored_indicator_metadata_matches and head:
                source_indicators_hash = _indicator_hash(
                    local_indicator_rows,
                    value_columns=required_source_indicator_columns,
                )
            if _existing_partition_matches(
                existing_partition_metadata,
                source_bars_hash=source_bars_hash,
                source_indicators_hash=source_indicators_hash,
                profile_version=resolved_profile.version,
                row_count=row_count,
                output_columns_hash=target_output_columns_hash,
            ):
                reused_partitions += 1
                continue

            source_unchanged = bool(head) and (
                head.get("source_bars_hash") == source_bars_hash
                and head.get("source_indicators_hash") == source_indicators_hash
                and int(head.get("row_count", -1) or -1) == row_count
            )
            missing_columns = set(target_output_columns) - partition_existing_columns
            can_extend_from_existing = bool(
                source_unchanged and missing_columns and partition_reusable_existing_columns
            )
            if can_extend_from_existing:
                compute_profile = _profile_for_output_columns(resolved_profile, missing_columns)
                existing_rows = load_derived_indicator_partition_rows(
                    output_dir=derived_indicator_output_dir,
                    partition=partition_key,
                    value_columns=partition_reusable_existing_columns,
                )
                extended_partitions += 1
            else:
                compute_profile = resolved_profile
                existing_rows = None
                recomputed_partitions += 1

            replace_partitions.append(partition_key)
            refreshed_timeframes.append(
                (
                    timeframe,
                    local_series,
                    local_indicator_rows,
                    source_base_frame,
                    compute_profile,
                    existing_rows,
                )
            )
            refreshed_partitions += 1
        if refreshed_timeframes:
            refresh_plan.append(
                (series_key, _source_frames_for_family(series_key), refreshed_timeframes)
            )

    deleted_partitions = tuple(
        partition for partition in existing_by_partition if partition not in current_partitions
    )
    replace_partitions.extend(deleted_partitions)

    current_total_rows = sum(
        row_count
        for counts_by_timeframe in partition_counts.values()
        for row_count in counts_by_timeframe.values()
    )
    output_paths = {
        DERIVED_SOURCE_FRAME_TABLE: (
            derived_indicator_output_dir / "research_derived_source_frames.delta"
        ).as_posix(),
        "research_derived_indicator_frames": (
            derived_indicator_output_dir / "research_derived_indicator_frames.delta"
        ).as_posix(),
    }
    refreshed_row_count = 0
    batch_count = 0
    if replace_partitions or not table_exists:

        def _row_batches() -> Iterator[list[DerivedIndicatorFrameRow]]:
            for series_key, source_frames, refreshed_timeframes in refresh_plan:
                del series_key
                for (
                    timeframe,
                    local_series,
                    local_indicator_rows,
                    source_base_frame,
                    compute_profile,
                    existing_rows,
                ) in refreshed_timeframes:
                    yield _build_partition_rows(
                        dataset_version=dataset_version,
                        indicator_set_version=indicator_set_version,
                        derived_indicator_set_version=derived_indicator_set_version,
                        series_mode=series_mode,
                        profile=resolved_profile,
                        local_series=local_series,
                        local_indicator_rows=local_indicator_rows,
                        source_frames=source_frames,
                        base_frame=source_base_frame,
                        source_dataset_bars_hash=current_dataset_bars_hash,
                        output_columns_hash=target_output_columns_hash,
                        compute_profile=compute_profile,
                        existing_rows=existing_rows,
                        adjustment_ladder_rows=adjustment_ladder_rows
                        if series_mode == "continuous_front"
                        else (),
                    )

        derived_output_paths, refreshed_row_count, batch_count = (
            write_derived_indicator_frame_batches(
                output_dir=derived_indicator_output_dir,
                row_batches=_row_batches(),
                replace_partitions=tuple(replace_partitions),
                profile=resolved_profile,
            )
        )
        output_paths.update(derived_output_paths)
    source_rows_by_table = dict(source_frame_report.get("rows_by_table", {}))
    source_frame_row_count = int(
        source_rows_by_table.get(DERIVED_SOURCE_FRAME_TABLE, current_total_rows) or 0
    )
    return {
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "derived_indicator_set_version": derived_indicator_set_version,
        "profile_version": resolved_profile.version,
        "bar_usage_policy_id": BAR_USAGE_POLICY_ID,
        "indicator_usage_policy_id": INDICATOR_USAGE_POLICY_ID,
        "derived_indicator_row_count": current_total_rows,
        "source_frame_row_count": source_frame_row_count,
        "source_frame_report": source_frame_report,
        "refreshed_row_count": refreshed_row_count,
        "refreshed_partition_count": refreshed_partitions,
        "reused_partition_count": reused_partitions,
        "extended_partition_count": extended_partitions,
        "recomputed_partition_count": recomputed_partitions,
        "deleted_partition_count": len(deleted_partitions),
        "write_batch_count": batch_count,
        "output_columns_hash": target_output_columns_hash,
        "loaded_indicator_partition_count": 0,
        "loaded_source_frame_partition_count": len(source_rows_cache),
        "output_paths": output_paths,
        "delta_manifest": {
            **research_derived_source_frame_store_contract(
                source_indicator_columns=required_source_indicator_columns
            ),
            **research_derived_indicator_store_contract(profile=resolved_profile),
        },
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
