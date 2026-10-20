from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_table_columns,
    delta_table_identity,
    delta_table_version,
    has_delta_log,
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
    assert_derived_bar_usage_policy_coverage,
    derived_bar_usage_groups_for_outputs,
    has_required_bar_usage_flags,
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
    DerivedIndicatorProfile,
    DerivedIndicatorProfileRegistry,
    build_derived_indicator_profile_registry,
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.indicators import (
    IndicatorFramePartitionKey,
    IndicatorFrameRow,
)
from trading_advisor_3000.product_plane.research.indicators.store import (
    existing_indicator_value_columns,
)
from trading_advisor_3000.product_plane.runtime.stage_timings import (
    StageTimings,
    record_skipped_stage,  # noqa: F401 - frozen native compatibility owner
    record_stage_timing,
    stage_timer,
)
from trading_advisor_3000.spark_jobs import DEFAULT_SPARK_MASTER
from trading_advisor_3000.spark_jobs.research_derived_source_frames_job import (
    run_research_derived_source_frames_spark_job,
)

from .source_frames import (
    DERIVED_SOURCE_FRAME_TABLE,
    DerivedSourceFramePartitionKey,
    NativeRefreshScope,
    derived_source_indicator_columns_hash,
    load_derived_source_frame_partition_metadata,
    load_derived_source_frame_partition_rows,  # noqa: F401 - frozen native compatibility owner
    research_derived_source_frame_store_contract,
)
from .store import (
    DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS,
    DerivedIndicatorFramePartitionKey,
    DerivedIndicatorFrameRow,
    derived_indicator_output_columns_hash,
    existing_derived_indicator_value_columns,
    load_derived_indicator_frames,
    load_derived_indicator_partition_metadata,
    load_derived_indicator_partition_rows,
    research_derived_indicator_store_contract,
    write_derived_indicator_frame_batches,  # noqa: F401 - frozen native compatibility owner
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
            "values": {column: row.values.get(column) for column in value_columns},
        }
        for row in rows
    ]
    # Bars, count and local input content have separate guards. L1 provenance is not
    # a consumed indicator value (e.g. a VP-only refresh must not invalidate EMA).
    normalized = json.dumps(
        {"indicator_content_version": 2, "rows": payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
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
        projected = _fill_bar_usage_noncomputed_rows(projected, computed_index=computed.index)
        projected = projected.infer_objects(copy=False)
    elif rule.mode == STATE_UPDATE_RESET_SCOPE:
        scope_key = _bar_usage_scope_key(frame, rule.scope_id)
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
    computed_indices = set(computed_index)
    scope_values = scope_key.reindex(filled.index) if scope_key is not None else None
    scope_marker = object()
    last_values = {column: pd.NA for column in filled.columns}
    for position, index in enumerate(filled.index):
        if scope_values is not None:
            scope_value = scope_values.iloc[position]
            scope_token = None if pd.isna(scope_value) else scope_value
            if scope_token != scope_marker:
                last_values = {column: pd.NA for column in filled.columns}
                scope_marker = scope_token
        if index in computed_indices:
            for column_index, column in enumerate(filled.columns):
                last_values[column] = filled.iat[position, column_index]
        else:
            for column_index, column in enumerate(filled.columns):
                filled.iat[position, column_index] = last_values[column]
    return filled


def _compute_derived_frame_unmasked(
    *,
    base_frame: pd.DataFrame,
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
    profile: DerivedIndicatorProfile,
) -> pd.DataFrame:
    assert_derived_bar_usage_policy_coverage(profile)
    if base_frame.empty or "bar_usage_flags" not in base_frame.columns:
        return _compute_derived_frame_unmasked(
            base_frame=base_frame,
            profile=profile,
        )
    result = base_frame.copy()
    for rule, output_columns in derived_bar_usage_groups_for_outputs(profile.output_columns):
        eligible = _bar_usage_eligible_mask(base_frame, rule.required_flags)
        eligible_frame = base_frame.loc[eligible].copy()
        if eligible_frame.empty:
            computed = pd.DataFrame(index=eligible_frame.index)
        else:
            grouped_profile = _profile_for_output_columns(profile, set(output_columns))
            computed = _compute_derived_frame_unmasked(
                base_frame=eligible_frame,
                profile=grouped_profile,
            )
        result = _append_columns(
            result,
            _project_bar_usage_computed_columns(
                frame=base_frame,
                computed=computed,
                output_columns=output_columns,
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


def _derived_projection_contract_changed(
    *,
    table_path: Path,
    expected_hash: str,
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    dataset_instrument_ids: Sequence[str] = (),
    series_mode: str | None = None,
    scope: NativeRefreshScope = NativeRefreshScope(),
) -> bool:
    """Detect legacy/partial L2 publication before narrowing the target scope."""
    if not has_delta_log(table_path):
        return False
    columns = ("source_indicator_columns_hash",)
    available = set(delta_table_columns(table_path))
    if not set(columns) <= available:
        return True
    if {"source_snapshot_hash", "compute_contract_hash", "created_at"} <= available:
        owner_metadata = load_derived_indicator_partition_metadata(
            output_dir=table_path.parent,
            dataset_version=dataset_version,
            contour_id=contour_id,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
        )
        owner_metadata = [
            row
            for row in owner_metadata
            if (series_mode is None or row.get("series_mode") == series_mode)
            and (not dataset_instrument_ids or row["instrument_id"] in dataset_instrument_ids)
            and scope.matches(row["instrument_id"], row["timeframe"])
        ]
        if any(not row.get("_metadata_valid", True) for row in owner_metadata):
            return True
        if owner_metadata and all(
            row.get("source_snapshot_hash") and row.get("compute_contract_hash")
            for row in owner_metadata
        ):
            # A successful suffix carries complete partition metadata in its
            # latest row; retained prefix hashes remain historical provenance.
            return any(row.get(columns[0]) != expected_hash for row in owner_metadata)
    import pyarrow.compute as pc

    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
        ("indicator_set_version", "=", indicator_set_version),
        ("derived_indicator_set_version", "=", derived_indicator_set_version),
    ]
    if dataset_instrument_ids:
        filters.append(("instrument_id", "in", tuple(dataset_instrument_ids)))
    if series_mode is not None and "series_mode" in available:
        filters.append(("series_mode", "=", series_mode))
    partition_columns = ["instrument_id", "series_mode", "series_id", "contract_id", "timeframe"]
    partition_columns = [column for column in partition_columns if column in available]
    metadata = read_delta_table_arrow(
        table_path, columns=[*partition_columns, *columns], filters=filters
    )
    metadata = metadata.filter(
        pa.array(
            [
                scope.matches(row["instrument_id"], row["timeframe"])
                for row in metadata.select(["instrument_id", "timeframe"]).to_pylist()
            ],
            type=pa.bool_(),
        )
    )
    identities = pc.unique(metadata.column(columns[0]))
    if len(identities) == 0:
        return False
    if len(identities) != 1 or identities[0].as_py() != expected_hash:
        return True
    return False


from contextlib import nullcontext  # noqa: E402

import pyarrow.compute as pc  # noqa: E402

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (  # noqa: E402
    DeltaArrowReader,
)

from .fingerprints import columnar_fingerprint  # noqa: E402
from .read_cache import PartitionReadCache  # noqa: E402
from .source_frames import load_derived_source_frame_partition_arrow  # noqa: E402
from .store import (  # noqa: E402
    DerivedIndicatorPartitionUpdate,
    write_derived_indicator_partition_updates,
)
from .tails import suffix_start_index  # noqa: E402


def _source_indicator_row(
    row: dict[str, object], source_indicator_columns: tuple[str, ...], row_count: int
) -> IndicatorFrameRow:
    values: dict[str, float | int | None] = {}
    for column in source_indicator_columns:
        value = row.get(column)
        values[column] = None if value is None else float(value)
    return IndicatorFrameRow(
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
        row_count=int(row.get("indicator_row_count") or row_count),
        warmup_span=int(row.get("indicator_warmup_span") or 0),
        null_warmup_span=int(row.get("indicator_null_warmup_span") or 0),
        created_at=str(row.get("indicator_created_at") or ""),
        output_columns_hash=str(row.get("indicator_output_columns_hash") or ""),
    )


class _ColumnarBars(Sequence[ResearchBarView]):
    """Scalar Arrow inputs with timestamps/dates already normalized to ISO."""

    def __init__(self, table: pa.Table):
        self.table = table

    def __len__(self) -> int:
        return len(self.table)

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step != 1:
                raise ValueError("columnar bars require a contiguous slice")
            return [
                ResearchBarView.from_dict(row)
                for row in self.table.slice(start, max(stop - start, 0)).to_pylist()
            ]
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        row = self.table.slice(index, 1).to_pylist()[0]
        return ResearchBarView.from_dict(row)


def _source_frame_arrow_to_inputs(
    *, table: pa.Table, source_indicator_columns: tuple[str, ...]
) -> tuple[_ColumnarBars, list[IndicatorFrameRow], pd.DataFrame]:
    # Normalize once in Arrow; both pandas and canonical JSON reuse these strings.
    for index, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type):
            text = pc.strftime(
                table[field.name].cast(pa.timestamp("us", "UTC")),
                format="%Y-%m-%dT%H:%M:%SZ",
            )
            table = table.set_column(
                index, field.name, pc.replace_substring(text, pattern=".000000Z", replacement="Z")
            )
        elif pa.types.is_date(field.type):
            table = table.set_column(
                index, field.name, pc.strftime(table[field.name], format="%Y-%m-%d")
            )
    table = table.sort_by([("ts", "ascending")])
    frame = table.to_pandas(ignore_metadata=True)
    for field in table.schema:
        column = table[field.name]
        if column.null_count == len(table):
            # DataFrame(list[dict]) inferred all-None columns as object.
            frame[field.name] = pd.Series(
                np.full(len(frame), None, dtype=object), index=frame.index
            )
        elif pa.types.is_integer(field.type):
            frame[field.name] = frame[field.name].astype(
                "float64" if column.null_count else "int64"
            )
        elif pa.types.is_floating(field.type):
            frame[field.name] = frame[field.name].astype("float64")
        elif pa.types.is_dictionary(field.type):
            frame[field.name] = frame[field.name].astype(object)
    head = table.slice(0, 1).to_pylist()[0]
    return (
        _ColumnarBars(table),
        [_source_indicator_row(head, source_indicator_columns, len(table))],
        frame,
    )


def _fast_source_hashes(table: pa.Table, source_indicator_columns: tuple[str, ...]):
    bars = [name for name in ResearchBarView.__dataclass_fields__ if name in table.column_names]
    l1 = [
        name
        for name in ("contract_id", "instrument_id", "timeframe", "ts", *source_indicator_columns)
        if name in table.column_names
    ]
    return (
        columnar_fingerprint(table.select(bars), domain="research.derived_indicators.source-bars"),
        columnar_fingerprint(table.select(l1), domain="research.derived_indicators.source-l1"),
    )


@dataclass(frozen=True)
class PreparedSourceHashes:
    bars: str
    indicators: str
    inputs: str
    frame_identity: int
    profile_version: str
    output_columns: tuple[str, ...]

    def validate(self, frame: pd.DataFrame, profile: DerivedIndicatorProfile) -> None:
        if (
            self.frame_identity != id(frame)
            or self.profile_version != profile.version
            or self.output_columns != profile.output_columns
        ):
            raise ValueError("prepared source hashes belong to different inputs/profile")


def _columnar_source_hashes(table, source_indicator_columns):
    bars = [ResearchBarView.from_dict(row) for row in table.to_pylist()]
    indicators = [
        _source_indicator_row(row, source_indicator_columns, len(table))
        for row in table.to_pylist()
    ]
    return _bars_hash(bars), _indicator_hash(indicators, value_columns=source_indicator_columns)


def _build_prepared_partition_rows(
    *,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    series_mode: str,
    profile: DerivedIndicatorProfile,
    local_series: Sequence[ResearchBarView],
    local_indicator_rows: list[IndicatorFrameRow],
    base_frame: pd.DataFrame | None = None,
    source_dataset_bars_hash: str = "",
    output_columns_hash: str | None = None,
    compute_profile: DerivedIndicatorProfile | None = None,
    existing_rows: list[DerivedIndicatorFrameRow] | None = None,
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
    source_inputs_hash: str = "",
    prepared_hashes: PreparedSourceHashes | None = None,
    previous_created_at: object = None,
    tail_output_dir: Path | None = None,
    tail_compatible: bool = False,
    old_reader=None,
    source_snapshot_hash: str = "",
    compute_contract_hash: str = "",
) -> list[DerivedIndicatorFrameRow]:
    if base_frame is None:
        base_frame = _prepare_base_frame(
            bars=local_series,
            indicators=local_indicator_rows,
            adjustment_ladder_rows=adjustment_ladder_rows,
        )
    current_timeframe = local_series[0].timeframe
    compute_profile = compute_profile or profile
    if prepared_hashes is not None:
        prepared_hashes.validate(base_frame, profile)
    computed = _compute_derived_frame(
        base_frame=base_frame,
        profile=compute_profile,
    )
    if prepared_hashes is not None:
        source_inputs_hash = prepared_hashes.inputs
        source_bars_hash = prepared_hashes.bars
        source_indicators_hash = prepared_hashes.indicators
    else:
        source_inputs_hash = source_inputs_hash or _source_inputs_hash(
            profile=profile, base_frame=base_frame
        )
        source_bars_hash = _bars_hash(local_series)
        source_indicators_hash = _indicator_hash(
            local_indicator_rows, value_columns=_source_indicator_columns_for_profile(profile)
        )
    output_columns = profile.output_columns
    computed_columns = set(compute_profile.output_columns)
    existing_by_ts = {row.ts: row for row in existing_rows or []}
    source_indicator_profile_version = (
        local_indicator_rows[0].profile_version if local_indicator_rows else ""
    )
    source_indicator_output_columns_hash = (
        local_indicator_rows[0].output_columns_hash if local_indicator_rows else ""
    )
    output_columns_hash = output_columns_hash or derived_indicator_output_columns_hash(
        output_columns
    )
    values_frame = computed.reindex(columns=output_columns).copy()
    for column in output_columns:
        if column not in computed_columns:
            values_frame[column] = [
                existing_by_ts[ts].values.get(column) if ts in existing_by_ts else None
                for ts in base_frame["ts"]
            ]
    active = values_frame.columns[values_frame.notna().any()]
    complete = (
        values_frame[active].notna().all(axis=1)
        if len(active)
        else pd.Series(True, index=values_frame.index)
    )
    null_warmup_span = int(np.argmax(complete.to_numpy())) if complete.any() else len(values_frame)
    materialized_at = pd.Timestamp.now(tz="UTC")
    previous = pd.to_datetime(previous_created_at, utc=True, errors="coerce")
    if not pd.isna(previous):
        materialized_at = max(materialized_at, previous + pd.Timedelta(microseconds=1))
    materialized_at_text = materialized_at.isoformat().replace("+00:00", "Z")
    start = 0
    if tail_output_dir is not None:
        proof = values_frame.copy()
        proof["ts"] = base_frame["ts"].to_numpy()
        proof["contract_id"] = base_frame["contract_id"].to_numpy()
        proof["ts_close_year"] = pd.to_datetime(
            base_frame["bar_end_ts"], utc=True
        ).dt.year.to_numpy()
        proof["profile_version"] = profile.version
        proof["output_columns_hash"] = output_columns_hash
        proof["warmup_span"] = profile.warmup_bars
        proof["null_warmup_span"] = null_warmup_span
        start = suffix_start_index(
            output_dir=tail_output_dir,
            partition=_derived_partition_key(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                derived_indicator_set_version=derived_indicator_set_version,
                series_key=_series_key_from_bar(local_series[0], series_mode=series_mode),
                timeframe=current_timeframe,
            ),
            computed=proof,
            output_columns=output_columns,
            compatible=tail_compatible,
            reader=old_reader,
        )
    # Serialize only the proved changed suffix, not the full computed history.
    payload_rows = values_frame.iloc[start:].to_dict("records")
    rows: list[DerivedIndicatorFrameRow] = []
    for original, payload in zip(local_series[start:], payload_rows, strict=True):
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
                bar_end_ts=original.bar_end_ts,
                values={c: _derived_value(c, payload[c]) for c in output_columns},
                source_bars_hash=source_bars_hash,
                source_indicators_hash=source_indicators_hash,
                source_inputs_hash=source_inputs_hash,
                source_indicator_columns_hash=derived_source_indicator_columns_hash(
                    _source_indicator_columns_for_profile(profile)
                ),
                source_snapshot_hash=source_snapshot_hash,
                compute_contract_hash=compute_contract_hash,
                source_dataset_bars_hash=source_dataset_bars_hash,
                source_indicator_profile_version=source_indicator_profile_version,
                source_indicator_output_columns_hash=source_indicator_output_columns_hash,
                row_count=len(local_series),
                warmup_span=profile.warmup_bars,
                null_warmup_span=null_warmup_span,
                created_at=materialized_at_text,
                output_columns_hash=output_columns_hash,
            )
        )
    return rows


def _derived_partition_coverage_changed(
    *,
    table_path: Path,
    expected_counts: dict[DerivedIndicatorFramePartitionKey, int],
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    dataset_instrument_ids: Sequence[str],
    series_mode: str,
    previous_counts: dict[DerivedIndicatorFramePartitionKey, int] | None = None,
    requested_timeframes: Sequence[str] = (),
    scope: NativeRefreshScope = NativeRefreshScope(),
) -> bool:
    """Require repair unless a complete in-scope count transition is proven.

    Fast metadata reuse supplies no previous counts and remains strict against
    current source coverage. Only the slow path may prove valid prior coverage.
    """
    if not has_delta_log(table_path):
        return bool(expected_counts)
    available = set(delta_table_columns(table_path))
    keys = ["instrument_id", "contract_id", "timeframe", "series_mode", "series_id"]
    if not {"instrument_id", "contract_id", "timeframe"} <= available:
        return True
    keys = [key for key in keys if key in available]
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
        ("indicator_set_version", "=", indicator_set_version),
        ("derived_indicator_set_version", "=", derived_indicator_set_version),
    ]
    filters = [clause for clause in filters if clause[0] in available]
    if dataset_instrument_ids:
        filters.append(("instrument_id", "in", tuple(dataset_instrument_ids)))
    if "series_mode" in available:
        filters.append(("series_mode", "=", series_mode))
    metadata = read_delta_table_arrow(table_path, columns=keys, filters=filters)
    observed = metadata.group_by(keys).aggregate([("instrument_id", "count")])
    actual_counts: dict[DerivedIndicatorFramePartitionKey, int] = {}
    # Only native aggregated partition metadata crosses into Python, not business rows.
    for row in observed.to_pylist():
        if not scope.matches(row["instrument_id"], row["timeframe"]):
            continue
        key = _metadata_partition_key(
            {
                **row,
                "dataset_version": dataset_version,
                "contour_id": contour_id,
                "indicator_set_version": indicator_set_version,
                "derived_indicator_set_version": derived_indicator_set_version,
            },
            series_mode=series_mode,
        )
        actual_counts[key] = actual_counts.get(key, 0) + int(row["instrument_id_count"])
    if actual_counts == expected_counts:
        return False
    if (
        previous_counts is None
        or actual_counts != previous_counts
        or actual_counts.keys() != expected_counts.keys()
    ):
        return True
    requested = frozenset(
        str(value).strip() for value in requested_timeframes if str(value).strip()
    )
    return bool(
        requested
        and any(
            key.timeframe not in requested
            for key in expected_counts
            if actual_counts[key] != expected_counts[key]
        )
    )


def _source_inputs_hash(*, profile: DerivedIndicatorProfile, base_frame: pd.DataFrame) -> str:
    """Fingerprint this partition only, including its continuous-front projection."""
    digest = hashlib.sha256()
    digest.update(
        json.dumps(["derived-native-input-v1", profile.version, profile.output_columns]).encode()
    )
    semantic = (
        set(ResearchBarView.__dataclass_fields__)
        | set(_source_indicator_columns_for_profile(profile))
        | {"indicator_profile_version", "indicator_output_columns_hash"}
    )
    columns = sorted(c for c in base_frame.columns if c in semantic or c.startswith("_cf_"))
    digest.update(json.dumps([(c, str(base_frame[c].dtype)) for c in columns]).encode())
    digest.update(pd.util.hash_pandas_object(base_frame[columns], index=False).values.tobytes())
    return digest.hexdigest()


def _existing_partition_matches(
    existing_rows: list[dict[str, object]],
    *,
    source_bars_hash: str,
    source_indicators_hash: str,
    profile_version: str,
    row_count: int,
    output_columns_hash: str,
    source_inputs_hash: str,
    compute_contract_hash: str = "",
    warmup_span: int | None = None,
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
        and head.get("source_inputs_hash") == source_inputs_hash
        and bool(compute_contract_hash)
        and head.get("compute_contract_hash") == compute_contract_hash
        and (warmup_span is None or head.get("warmup_span") == warmup_span)
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
    base_frame: pd.DataFrame | None = None,
    source_dataset_bars_hash: str = "",
    output_columns_hash: str | None = None,
    compute_profile: DerivedIndicatorProfile | None = None,
    existing_rows: list[DerivedIndicatorFrameRow] | None = None,
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
    source_snapshot_hash: str = "",
    compute_contract_hash: str = "",
) -> list[DerivedIndicatorFrameRow]:
    if base_frame is None:
        base_frame = _prepare_base_frame(
            bars=local_series,
            indicators=local_indicator_rows,
            adjustment_ladder_rows=adjustment_ladder_rows,
        )
    compute_profile = compute_profile or profile
    computed = _compute_derived_frame(
        base_frame=base_frame,
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

    source_indicator_columns_hash = derived_source_indicator_columns_hash(
        _source_indicator_columns_for_profile(profile)
    )
    source_inputs_hash = _source_inputs_hash(profile=profile, base_frame=base_frame)
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
                bar_end_ts=original.bar_end_ts,
                values=values,
                source_bars_hash=source_bars_hash,
                source_dataset_bars_hash=source_dataset_bars_hash,
                source_indicators_hash=source_indicators_hash,
                source_indicator_profile_version=source_indicator_profile_version,
                source_indicator_output_columns_hash=source_indicator_output_columns_hash,
                source_indicator_columns_hash=source_indicator_columns_hash,
                source_inputs_hash=source_inputs_hash,
                source_snapshot_hash=source_snapshot_hash,
                compute_contract_hash=compute_contract_hash,
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
    rows: list[DerivedIndicatorFrameRow] = []
    for series_key, bars_by_timeframe in sorted(
        grouped_bars.items(), key=lambda item: (item[0].instrument_id, item[0].contract_id or "")
    ):
        indicators_by_timeframe = grouped_indicators.get(series_key, {})
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
                    adjustment_ladder_rows=adjustment_ladder_rows
                    if series_mode == "continuous_front"
                    else (),
                )
            )
    return rows


def _changed_partition_tail(
    rows: list[DerivedIndicatorFrameRow],
    existing: list[DerivedIndicatorFrameRow],
) -> tuple[str | None, list[DerivedIndicatorFrameRow]]:
    """Compare fully computed semantics; never truncate recursive computation."""
    if not rows or not existing:
        return None, rows
    if len({row.ts for row in existing}) != len(existing) or len({row.ts for row in rows}) != len(
        rows
    ):
        return None, rows
    if any(row.stored_close_year is None for row in existing) or any(
        not row.bar_end_ts for row in rows
    ):
        return None, rows
    fields = (
        "dataset_version",
        "contour_id",
        "series_mode",
        "series_id",
        "contract_id",
        "instrument_id",
        "timeframe",
        "indicator_set_version",
        "derived_indicator_set_version",
        "profile_version",
        "output_columns_hash",
        "warmup_span",
        "null_warmup_span",
    )
    previous = {row.ts: row for row in existing}
    current = {row.ts: row for row in rows}
    changed = set(previous) ^ set(current)
    for row in rows:
        old = previous.get(row.ts)
        year = pd.Timestamp(row.bar_end_ts).tz_convert("UTC").year
        if (
            old is None
            or row.values != old.values
            or year != old.stored_close_year
            or any(getattr(row, field) != getattr(old, field) for field in fields)
        ):
            changed.add(row.ts)
    start = min(min(changed), rows[-1].ts) if changed else rows[-1].ts
    tail = [row for row in rows if row.ts >= start]
    if len(tail) > DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS:
        return None, rows
    latest = max(pd.Timestamp(row.created_at) for row in existing)
    stamp = (
        max(pd.Timestamp.now(tz="UTC"), latest + pd.Timedelta(1, unit="us"))
        .isoformat()
        .replace("+00:00", "Z")
    )
    return start, [replace(row, created_at=stamp) for row in tail]


def _delta_source_refs(paths: Sequence[Path]) -> list[dict[str, object]]:
    return [{**delta_table_identity(path), "version": delta_table_version(path)} for path in paths]


def _derived_compute_contract_hash(ladder: tuple[dict[str, object], ...]) -> str:
    research = Path(__file__).parent.parent
    files = (
        Path(__file__),
        Path(__file__).with_name("registry.py"),
        Path(__file__).with_name("fingerprints.py"),
        Path(__file__).with_name("read_cache.py"),
        Path(__file__).with_name("tails.py"),
        Path(__file__).with_name("source_frames.py"),
        research / "bar_usage_policy.py",
        research / "continuous_front_indicators" / "input_projection.py",
        research / "continuous_front_indicators" / "rules.py",
        research.parent.parent / "spark_jobs" / "research_derived_source_frames_job.py",
    )
    payload = {
        "contract": "derived-native-v1",
        "implementation": [hashlib.sha256(path.read_bytes()).hexdigest() for path in files],
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "bar_usage": BAR_USAGE_POLICY_ID,
        "indicator_usage": INDICATOR_USAGE_POLICY_ID,
        "ladder": sorted(ladder, key=lambda row: json.dumps(row, sort_keys=True, default=str)),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _derived_source_snapshot_hash(
    *,
    input_refs: list[dict[str, object]],
    source_path: Path,
    compute_contract_hash: str,
    profile: DerivedIndicatorProfile,
    columns: tuple[str, ...],
    timeframes: Sequence[str],
    instruments: Sequence[str],
) -> str:
    if not has_delta_log(source_path):
        return ""
    payload = {
        "version": 1,
        "inputs": input_refs,
        "source_frame": _delta_source_refs([source_path]),
        "compute": compute_contract_hash,
        "profile": asdict(profile),
        "columns": columns,
        "timeframes": sorted(set(timeframes)),
        "instruments": sorted(set(instruments)),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _derived_metadata_fast_reuse(
    *,
    output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    contour_id: str,
    series_mode: str,
    profile: DerivedIndicatorProfile,
    snapshot_hash: str,
    compute_contract_hash: str,
    instruments: Sequence[str],
    timeframes: Sequence[str],
    requested_pairs: tuple[tuple[str, str], ...] = (),
) -> dict[str, object] | None:
    table_path = output_dir / "research_derived_indicator_frames.delta"
    if not snapshot_hash or not has_delta_log(table_path):
        return None
    scope = NativeRefreshScope(tuple(timeframes), tuple(instruments), requested_pairs)
    metadata = load_derived_indicator_partition_metadata(
        output_dir=output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        contour_id=contour_id,
    )
    scoped = [
        row
        for row in metadata
        if row.get("series_mode") == series_mode
        and scope.matches(row["instrument_id"], row["timeframe"])
    ]
    output_hash = derived_indicator_output_columns_hash(profile.output_columns)
    if not scoped or any(
        not row.get("_metadata_valid", True)
        or row.get("source_snapshot_hash") != snapshot_hash
        or row.get("compute_contract_hash") != compute_contract_hash
        or row.get("output_columns_hash") != output_hash
        or row.get("warmup_span") != profile.warmup_bars
        for row in scoped
    ):
        return None
    source_metadata = load_derived_source_frame_partition_metadata(
        output_dir=output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        contour_id=contour_id,
    )
    expected_counts = {}
    for row in source_metadata:
        if str(row.get("series_mode") or "contract") != series_mode:
            continue
        if not scope.matches(row["instrument_id"], row["timeframe"]):
            continue
        partition = _derived_partition_key(
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            series_key=_series_key_from_source_metadata(row, requested_series_mode=series_mode),
            timeframe=str(row["timeframe"]),
        )
        count = int(row.get("partition_row_count") or row.get("joined_row_count") or 0)
        expected_counts[partition] = expected_counts.get(partition, 0) + count
    if _derived_partition_coverage_changed(
        table_path=table_path,
        expected_counts=expected_counts,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        contour_id=contour_id,
        dataset_instrument_ids=instruments,
        series_mode=series_mode,
        scope=scope,
    ):
        return None
    selected = {
        key: count
        for key, count in expected_counts.items()
        if not timeframes or key.timeframe in timeframes
    }
    if not selected:
        return None
    return {
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "derived_indicator_set_version": derived_indicator_set_version,
        "profile_version": profile.version,
        "bar_usage_policy_id": BAR_USAGE_POLICY_ID,
        "indicator_usage_policy_id": INDICATOR_USAGE_POLICY_ID,
        "derived_indicator_row_count": sum(selected.values()),
        "source_frame_row_count": sum(expected_counts.values()),
        "source_frame_report": {"status": "PASS-NOOP", "mode": "exact_snapshot_reuse"},
        "source_projection_contract_changed": False,
        "derived_projection_contract_changed": False,
        "derived_partition_coverage_changed": False,
        "refreshed_row_count": 0,
        "refreshed_partition_count": 0,
        "reused_partition_count": len(selected),
        "extended_partition_count": 0,
        "recomputed_partition_count": 0,
        "deleted_partition_count": 0,
        "write_batch_count": 0,
        "output_columns_hash": output_hash,
        "loaded_indicator_partition_count": 0,
        "loaded_source_frame_partition_count": 0,
        "reuse_plan_mode": "metadata_fast_path",
        "output_paths": {
            DERIVED_SOURCE_FRAME_TABLE: (
                output_dir / "research_derived_source_frames.delta"
            ).as_posix(),
            "research_derived_indicator_frames": table_path.as_posix(),
        },
    }


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
    spark_master: str = "",
    timeframes: Sequence[str] = (),
    dataset_instrument_ids: Sequence[str] = (),
    refresh_windows: Sequence[Mapping[str, object]] = (),
    force_recompute: bool = False,
) -> dict[str, object]:
    requested_scope = NativeRefreshScope.from_request(
        timeframes=timeframes,
        instruments=dataset_instrument_ids,
        refresh_windows=refresh_windows,
    )
    timeframes = requested_scope.timeframes
    dataset_instrument_ids = requested_scope.instruments
    stage_timings: StageTimings = {}
    stage_started = stage_timer("load_dataset_manifest")
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
    input_paths = [
        dataset_output_dir / "research_bar_views.delta",
        indicator_output_dir / "research_indicator_frames.delta",
    ]
    input_refs = _delta_source_refs(input_paths)
    compute_contract_hash = _derived_compute_contract_hash(adjustment_ladder_rows)
    snapshot_args = {
        "input_refs": input_refs,
        "source_path": derived_indicator_output_dir / "research_derived_source_frames.delta",
        "compute_contract_hash": compute_contract_hash,
        "profile": resolved_profile,
        "columns": required_source_indicator_columns,
        "timeframes": timeframes,
        "instruments": dataset_instrument_ids,
    }
    record_stage_timing(
        stage_timings,
        "load_dataset_manifest",
        stage_started,
        series_mode=series_mode,
        profile_version=resolved_profile.version,
        adjustment_ladder_row_count=len(adjustment_ladder_rows),
    )

    stage_started = stage_timer("source_column_check")
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
    record_stage_timing(
        stage_timings,
        "source_column_check",
        stage_started,
        source_indicator_column_count=len(required_source_indicator_columns),
        available_indicator_column_count=len(available_indicator_columns),
    )

    stage_started = stage_timer("reuse_decision")
    initial_snapshot_hash = _derived_source_snapshot_hash(**snapshot_args)
    reused = _derived_metadata_fast_reuse(
        output_dir=derived_indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        contour_id=contour_id,
        series_mode=series_mode,
        profile=resolved_profile,
        snapshot_hash=initial_snapshot_hash,
        compute_contract_hash=compute_contract_hash,
        instruments=dataset_instrument_ids,
        timeframes=timeframes,
        requested_pairs=requested_scope.pairs,
    )
    if reused is not None and not force_recompute:
        from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
            finalize_research_delta_table,
        )

        path = derived_indicator_output_dir / "research_derived_indicator_frames.delta"
        finalize_research_delta_table(
            path,
            since_version=delta_table_version(path),
            partition_by=("ts_close_year",),
        )
        if (
            _delta_source_refs(input_paths) != input_refs
            or _derived_source_snapshot_hash(**snapshot_args) != initial_snapshot_hash
        ):
            raise ValueError("L2 inputs changed during snapshot reuse")
        record_stage_timing(stage_timings, "reuse_decision", stage_started, reused=True)
        return {
            **reused,
            "stage_timings": stage_timings,
            "refresh_window_count": len(tuple(refresh_windows)),
            "delta_manifest": {
                **research_derived_source_frame_store_contract(
                    source_indicator_columns=required_source_indicator_columns
                ),
                **research_derived_indicator_store_contract(profile=resolved_profile),
            },
        }
    record_stage_timing(stage_timings, "reuse_decision", stage_started, reused=False)
    stage_started = stage_timer("build_source_frames_spark")
    if requested_scope.bounded and _derived_projection_contract_changed(
        table_path=derived_indicator_output_dir / "research_derived_indicator_frames.delta",
        expected_hash=derived_source_indicator_columns_hash(required_source_indicator_columns),
        dataset_version=dataset_version,
        contour_id=contour_id,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        dataset_instrument_ids=dataset_instrument_ids,
        series_mode=series_mode,
        scope=requested_scope,
    ):
        raise ValueError("native derived metadata adaptation required before scoped refresh")
    source_frame_report = run_research_derived_source_frames_spark_job(
        bar_views_path=dataset_output_dir / "research_bar_views.delta",
        indicator_frames_path=indicator_output_dir / "research_indicator_frames.delta",
        output_dir=derived_indicator_output_dir,
        dataset_version=dataset_version,
        contour_id=contour_id,
        indicator_set_version=indicator_set_version,
        derived_profile_version=resolved_profile.version,
        source_indicator_columns=required_source_indicator_columns,
        spark_master=spark_master or DEFAULT_SPARK_MASTER,
        timeframes=timeframes,
        dataset_instrument_ids=dataset_instrument_ids,
        refresh_windows=refresh_windows,
    )
    if _delta_source_refs(input_paths) != input_refs:
        raise ValueError("L2 inputs changed during source-frame production")
    source_snapshot_hash = _derived_source_snapshot_hash(**snapshot_args)
    source_rows_by_table = dict(source_frame_report.get("rows_by_table", {}))
    record_stage_timing(
        stage_timings,
        "build_source_frames_spark",
        stage_started,
        row_count=int(source_rows_by_table.get(DERIVED_SOURCE_FRAME_TABLE, 0) or 0),
    )

    source_projection_contract_changed = bool(
        source_frame_report.get("source_projection_contract_changed", False)
    )
    derived_projection_contract_changed = _derived_projection_contract_changed(
        table_path=derived_indicator_output_dir / "research_derived_indicator_frames.delta",
        expected_hash=derived_source_indicator_columns_hash(required_source_indicator_columns),
        dataset_version=dataset_version,
        contour_id=contour_id,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        dataset_instrument_ids=dataset_instrument_ids,
        series_mode=series_mode,
        scope=requested_scope,
    )
    full_family_repair = source_projection_contract_changed or derived_projection_contract_changed
    if full_family_repair and requested_scope.bounded:
        raise ValueError("native derived metadata adaptation required before scoped refresh")
    effective_timeframes = () if full_family_repair else timeframes
    effective_instruments = dataset_instrument_ids
    scoped_timeframes = frozenset(
        str(item).strip() for item in effective_timeframes if str(item).strip()
    )
    scoped_instruments = frozenset(
        str(item).strip() for item in effective_instruments if str(item).strip()
    )

    def _partition_in_requested_scope(partition: DerivedIndicatorFramePartitionKey) -> bool:
        return (
            partition.series_mode == series_mode
            and (not scoped_timeframes or partition.timeframe in scoped_timeframes)
            and (not scoped_instruments or partition.instrument_id in scoped_instruments)
            and requested_scope.matches(partition.instrument_id, partition.timeframe)
        )

    stage_started = stage_timer("load_source_metadata")
    source_snapshot = DeltaArrowReader(
        derived_indicator_output_dir / "research_derived_source_frames.delta"
    )
    source_metadata = load_derived_source_frame_partition_metadata(
        output_dir=derived_indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        contour_id=contour_id,
        reader=source_snapshot,
    )
    partition_counts: dict[DerivedSeriesKey, dict[str, int]] = {}
    for row in source_metadata:
        if str(row.get("series_mode") or series_mode) != series_mode:
            continue
        series_key = _series_key_from_source_metadata(row, requested_series_mode=series_mode)
        timeframe = str(row["timeframe"])
        if not requested_scope.matches(series_key.instrument_id, timeframe):
            continue
        if series_key.series_mode != series_mode:
            continue
        if scoped_instruments and series_key.instrument_id not in scoped_instruments:
            continue
        if scoped_timeframes and timeframe not in scoped_timeframes:
            continue
        row_count = int(row.get("partition_row_count") or row.get("joined_row_count") or 0)
        counts_by_timeframe = partition_counts.setdefault(series_key, {})
        counts_by_timeframe[timeframe] = counts_by_timeframe.get(timeframe, 0) + row_count
    expected_counts = {
        _derived_partition_key(
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            series_key=series_key,
            timeframe=timeframe,
        ): row_count
        for series_key, counts in partition_counts.items()
        for timeframe, row_count in counts.items()
    }
    table_exists = (
        derived_indicator_output_dir / "research_derived_indicator_frames.delta" / "_delta_log"
    ).exists()
    old_snapshot = (
        DeltaArrowReader(derived_indicator_output_dir / "research_derived_indicator_frames.delta")
        if table_exists
        else None
    )
    existing_metadata = (
        load_derived_indicator_partition_metadata(
            output_dir=derived_indicator_output_dir,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            contour_id=contour_id,
            reader=old_snapshot,
        )
        if table_exists
        else []
    )
    existing_by_partition = _group_existing_partition_metadata(
        rows=[
            row
            for row in existing_metadata
            if str(row.get("series_mode") or "contract") == series_mode
        ],
        series_mode=series_mode,
    )
    # Only complete latest proof carriers can distinguish normal new source
    # counts from physically incomplete output. Unknown keys/proofs keep repair.
    previous_counts: dict[DerivedIndicatorFramePartitionKey, int] | None = {}
    proof_fields = (
        "source_bars_hash",
        "source_indicators_hash",
        "profile_version",
        "output_columns_hash",
        "source_inputs_hash",
        "compute_contract_hash",
        "source_snapshot_hash",
    )
    for partition, rows in existing_by_partition.items():
        if not requested_scope.matches(partition.instrument_id, partition.timeframe):
            continue
        head = rows[0] if rows else {}
        count = head.get("row_count")
        warmup = head.get("warmup_span")
        if (
            not head.get("_metadata_valid", True)
            or type(count) is not int
            or count <= 0
            or type(warmup) is not int
            or warmup < 0
            or any(
                str(head.get(field) or "").strip().casefold()
                in {"", "unknown", "unavailable", "none"}
                for field in proof_fields
            )
        ):
            previous_counts = None
            break
        previous_counts[partition] = count
    # This slow-path flag denotes conservative coverage repair, not whether
    # source gained rows. The fast-noop call above remains strictly unchanged.
    derived_partition_coverage_changed = _derived_partition_coverage_changed(
        table_path=derived_indicator_output_dir / "research_derived_indicator_frames.delta",
        expected_counts=expected_counts,
        dataset_version=dataset_version,
        contour_id=contour_id,
        indicator_set_version=indicator_set_version,
        derived_indicator_set_version=derived_indicator_set_version,
        dataset_instrument_ids=dataset_instrument_ids,
        series_mode=series_mode,
        scope=requested_scope,
        previous_counts=previous_counts,
        requested_timeframes=timeframes,
    )
    if derived_partition_coverage_changed and not requested_scope.bounded:
        full_family_repair = True
        scoped_timeframes = frozenset()
    record_stage_timing(
        stage_timings,
        "load_source_metadata",
        stage_started,
        partition_family_count=len(partition_counts),
        source_metadata_row_count=len(source_metadata),
    )

    source_rows_cache: dict[
        tuple[DerivedSeriesKey, str],
        tuple[Sequence[ResearchBarView], list[IndicatorFrameRow], pd.DataFrame],
    ] = {}
    loaded_source_partitions: set[tuple[DerivedSeriesKey, str]] = set()

    def _load_source_inputs(
        series_key: DerivedSeriesKey, timeframe: str
    ) -> tuple[Sequence[ResearchBarView], list[IndicatorFrameRow], pd.DataFrame]:
        cache_key = (series_key, timeframe)
        if cache_key not in source_rows_cache:
            source_started = stage_timer("partition.read_source")
            table = load_derived_source_frame_partition_arrow(
                output_dir=derived_indicator_output_dir,
                partition=_source_frame_partition_key(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    series_key=series_key,
                    timeframe=timeframe,
                ),
                source_indicator_columns=required_source_indicator_columns,
                reader=source_cache,
            )
            record_stage_timing(
                {},
                "partition.read_source",
                source_started,
                instrument_id=series_key.instrument_id,
                timeframe=timeframe,
                row_count=len(table),
            )
            if not len(table):
                raise ValueError(
                    "derived indicator source-frame partition is empty for "
                    f"{series_key.instrument_id}|{series_key.series_id}|{timeframe}"
                )
            source_started = stage_timer("partition.prepare_inputs")
            local_series, local_indicator_rows, base_frame = _source_frame_arrow_to_inputs(
                table=table,
                source_indicator_columns=required_source_indicator_columns,
            )
            base_frame = _with_continuous_front_projection(
                base_frame,
                adjustment_ladder_rows=adjustment_ladder_rows
                if series_mode == "continuous_front"
                else (),
            )
            source_rows_cache[cache_key] = (local_series, local_indicator_rows, base_frame)
            loaded_source_partitions.add(cache_key)
            record_stage_timing(
                {},
                "partition.prepare_inputs",
                source_started,
                instrument_id=series_key.instrument_id,
                timeframe=timeframe,
                row_count=len(table),
            )
        return source_rows_cache[cache_key]

    target_output_columns = resolved_profile.output_columns
    target_output_columns_hash = derived_indicator_output_columns_hash(target_output_columns)
    existing_output_columns = (
        set(
            existing_derived_indicator_value_columns(
                output_dir=derived_indicator_output_dir, reader=old_snapshot
            )
        )
        if table_exists
        else set()
    )
    scopes = [
        (
            series_key,
            timeframe,
            row_count,
            _derived_partition_key(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                derived_indicator_set_version=derived_indicator_set_version,
                series_key=series_key,
                timeframe=timeframe,
            ),
        )
        for series_key, counts in sorted(
            partition_counts.items(),
            key=lambda item: (item[0].instrument_id, item[0].contract_id or ""),
        )
        for timeframe, row_count in sorted(counts.items())
    ]
    current_partitions = {scope[3] for scope in scopes}
    source_filters = [
        ("series_mode", "=", series_mode),
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
        ("indicator_set_version", "=", indicator_set_version),
    ]
    if scoped_instruments:
        source_filters.append(("instrument_id", "in", sorted(scoped_instruments)))
    if scoped_timeframes:
        source_filters.append(("timeframe", "in", sorted(scoped_timeframes)))
    if requested_scope.pairs:
        source_filters = [
            [*source_filters, ("instrument_id", "=", instrument), ("timeframe", "=", timeframe)]
            for instrument, timeframe in sorted(requested_scope.pairs)
        ]

    def available_filters(filters, schema):
        groups = filters if filters and isinstance(filters[0], list) else [filters]
        result = [[item for item in group if item[0] in schema.names] for group in groups]
        return result if filters and isinstance(filters[0], list) else result[0]

    source_columns = research_derived_source_frame_store_contract(
        source_indicator_columns=required_source_indicator_columns
    )[DERIVED_SOURCE_FRAME_TABLE]["columns"]
    source_cache = PartitionReadCache(
        source_snapshot,
        columns=source_columns,
        filters=available_filters(source_filters, source_snapshot.schema),
        enabled=len(scopes) > 1,
    )
    old_cache = (
        PartitionReadCache(
            old_snapshot,
            columns={
                **research_derived_indicator_store_contract(profile=resolved_profile)[
                    "research_derived_indicator_frames"
                ]["columns"],
                "ts_close_year": "int",
            },
            filters=available_filters(
                [
                    [*group, ("derived_indicator_set_version", "=", derived_indicator_set_version)]
                    for group in (
                        source_filters
                        if source_filters and isinstance(source_filters[0], list)
                        else [source_filters]
                    )
                ],
                old_snapshot.schema,
            ),
            enabled=len(scopes) > 1,
        )
        if old_snapshot is not None
        else None
    )
    deleted_partitions = tuple(
        partition
        for partition in existing_by_partition
        if partition not in current_partitions and _partition_in_requested_scope(partition)
    )
    reused_partitions = refreshed_partitions = extended_partitions = recomputed_partitions = 0
    metadata_rebased_partitions = 0

    def assert_source_stable():
        if (
            _delta_source_refs(input_paths) != input_refs
            or _derived_source_snapshot_hash(**snapshot_args) != source_snapshot_hash
        ):
            raise ValueError("L2 inputs changed during partition refresh")

    def _partition_updates() -> Iterator[DerivedIndicatorPartitionUpdate]:
        nonlocal \
            reused_partitions, \
            refreshed_partitions, \
            extended_partitions, \
            recomputed_partitions, \
            metadata_rebased_partitions
        for series_key, timeframe, row_count, partition_key in scopes:
            local_series = local_indicator_rows = source_base_frame = None
            try:
                existing_partition_metadata = existing_by_partition.get(partition_key, [])
                head = existing_partition_metadata[0] if existing_partition_metadata else None
                existing_output_columns_hash = (
                    str(head.get("output_columns_hash") or "") if head else ""
                )
                partition_existing_columns = (
                    existing_output_columns if existing_output_columns_hash else set()
                )
                partition_reusable_existing_columns = tuple(
                    column
                    for column in target_output_columns
                    if column in partition_existing_columns
                )
                local_series, local_indicator_rows, source_base_frame = _load_source_inputs(
                    series_key, timeframe
                )
                partition_started = stage_timer("partition.hash_sources")
                source_bars_hash, source_indicators_hash = _fast_source_hashes(
                    local_series.table, required_source_indicator_columns
                )
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
                record_stage_timing(
                    {},
                    "partition.hash_sources",
                    partition_started,
                    instrument_id=series_key.instrument_id,
                    timeframe=timeframe,
                )
                source_inputs_hash = _source_inputs_hash(
                    profile=resolved_profile,
                    base_frame=source_base_frame,
                )
                prepared_hashes = PreparedSourceHashes(
                    source_bars_hash,
                    source_indicators_hash,
                    source_inputs_hash,
                    id(source_base_frame),
                    resolved_profile.version,
                    resolved_profile.output_columns,
                )
                if (
                    not full_family_repair
                    and not derived_partition_coverage_changed
                    and not force_recompute
                    and stored_indicator_metadata_matches
                    and head.get("_metadata_valid", True)
                    and _existing_partition_matches(
                        existing_partition_metadata,
                        source_bars_hash=source_bars_hash,
                        source_indicators_hash=source_indicators_hash,
                        profile_version=resolved_profile.version,
                        row_count=row_count,
                        output_columns_hash=target_output_columns_hash,
                        source_inputs_hash=source_inputs_hash,
                        compute_contract_hash=compute_contract_hash,
                        warmup_span=resolved_profile.warmup_bars,
                    )
                ):
                    reused_partitions += 1
                    assert_source_stable()
                    if head.get("source_snapshot_hash") != source_snapshot_hash:
                        last_bar = local_series[-1]
                        carrier = load_derived_indicator_partition_rows(
                            output_dir=derived_indicator_output_dir,
                            partition=partition_key,
                            value_columns=target_output_columns,
                            ts=last_bar.ts,
                            reader=old_cache,
                        )
                        if len(carrier) != 1:
                            raise ValueError("derived metadata carrier is missing or duplicated")
                        stamp = max(
                            pd.Timestamp.now(tz="UTC"),
                            pd.Timestamp(head["created_at"]) + pd.Timedelta(1, unit="us"),
                        )
                        metadata_rebased_partitions += 1
                        yield DerivedIndicatorPartitionUpdate(
                            partition_key,
                            "replace",
                            [
                                replace(
                                    carrier[0],
                                    source_snapshot_hash=source_snapshot_hash,
                                    source_dataset_bars_hash=current_dataset_bars_hash,
                                    source_indicator_profile_version=source_indicator_profile_version,
                                    source_indicator_output_columns_hash=source_indicator_output_columns_hash,
                                    bar_end_ts=last_bar.bar_end_ts,
                                    created_at=stamp.isoformat().replace("+00:00", "Z"),
                                )
                            ],
                            last_bar.ts,
                        )
                    else:
                        yield DerivedIndicatorPartitionUpdate(partition_key, "reuse", [])
                    continue

                source_unchanged = (
                    not full_family_repair
                    and not derived_partition_coverage_changed
                    and not force_recompute
                    and stored_indicator_metadata_matches
                    and head.get("compute_contract_hash") == compute_contract_hash
                    and bool(head)
                    and head.get("_metadata_valid", True)
                    and (
                        head.get("source_inputs_hash") == source_inputs_hash
                        and head.get("source_bars_hash") == source_bars_hash
                        and head.get("source_indicators_hash") == source_indicators_hash
                        and int(head.get("row_count", -1) or -1) == row_count
                    )
                )
                missing_columns = set(target_output_columns) - partition_existing_columns
                can_extend_from_existing = bool(
                    source_unchanged
                    and series_mode != "continuous_front"
                    and missing_columns
                    and partition_reusable_existing_columns
                )
                if can_extend_from_existing:
                    compute_profile = _profile_for_output_columns(resolved_profile, missing_columns)
                    extended_partitions += 1
                else:
                    compute_profile = resolved_profile
                    recomputed_partitions += 1
                refreshed_partitions += 1
                existing_rows = (
                    load_derived_indicator_partition_rows(
                        output_dir=derived_indicator_output_dir,
                        partition=_derived_partition_key(
                            dataset_version=dataset_version,
                            indicator_set_version=indicator_set_version,
                            derived_indicator_set_version=derived_indicator_set_version,
                            series_key=series_key,
                            timeframe=timeframe,
                        ),
                        value_columns=tuple(
                            column
                            for column in target_output_columns
                            if column in existing_output_columns
                        ),
                        reader=old_cache,
                    )
                    if can_extend_from_existing
                    else None
                )
                partition_key = _derived_partition_key(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    derived_indicator_set_version=derived_indicator_set_version,
                    series_key=series_key,
                    timeframe=timeframe,
                )
                previous_metadata = existing_by_partition.get(partition_key, [])
                previous_head = previous_metadata[0] if previous_metadata else {}
                partition_started = stage_timer("partition.compute")
                partition_rows = _build_prepared_partition_rows(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    derived_indicator_set_version=derived_indicator_set_version,
                    series_mode=series_mode,
                    profile=resolved_profile,
                    local_series=local_series,
                    local_indicator_rows=local_indicator_rows,
                    base_frame=source_base_frame,
                    prepared_hashes=prepared_hashes,
                    previous_created_at=previous_head.get("created_at"),
                    tail_output_dir=derived_indicator_output_dir,
                    old_reader=old_cache,
                    tail_compatible=(
                        not force_recompute
                        and not full_family_repair
                        and not derived_partition_coverage_changed
                        and previous_head.get("_metadata_valid", True)
                        and previous_head.get("profile_version") == resolved_profile.version
                        and previous_head.get("output_columns_hash") == target_output_columns_hash
                    ),
                    source_dataset_bars_hash=current_dataset_bars_hash,
                    source_snapshot_hash=source_snapshot_hash,
                    compute_contract_hash=compute_contract_hash,
                    output_columns_hash=target_output_columns_hash,
                    compute_profile=compute_profile,
                    existing_rows=existing_rows,
                    adjustment_ladder_rows=adjustment_ladder_rows
                    if series_mode == "continuous_front"
                    else (),
                )
                record_stage_timing(
                    {},
                    "partition.compute",
                    partition_started,
                    instrument_id=series_key.instrument_id,
                    timeframe=timeframe,
                    row_count=len(partition_rows),
                )
                start_ts = (
                    partition_rows[0].ts
                    if partition_rows and partition_rows[0].ts != local_series[0].ts
                    else None
                )
                assert_source_stable()
                yield DerivedIndicatorPartitionUpdate(
                    partition_key, "replace", partition_rows, start_ts
                )
                del partition_rows, existing_rows
            finally:
                source_rows_cache.clear()
                # Release before loading another native partition, including reuse/failure.
                del local_series, local_indicator_rows, source_base_frame
        assert_source_stable()
        for partition in deleted_partitions:
            yield DerivedIndicatorPartitionUpdate(partition, "delete", [])

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
    stage_started = stage_timer("process_derived_indicator_partitions")
    with source_cache, old_cache if old_cache is not None else nullcontext():
        derived_output_paths, refreshed_row_count, batch_count = (
            write_derived_indicator_partition_updates(
                output_dir=derived_indicator_output_dir,
                updates=_partition_updates(),
                expected_partitions=tuple(scope[3] for scope in scopes) + deleted_partitions,
                profile=resolved_profile,
            )
        )
    assert_source_stable()
    output_paths.update(derived_output_paths)
    record_stage_timing(
        stage_timings,
        "process_derived_indicator_partitions",
        stage_started,
        refreshed_partition_count=refreshed_partitions,
        refreshed_row_count=refreshed_row_count,
        write_batch_count=batch_count,
    )
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
        "source_projection_contract_changed": source_projection_contract_changed,
        "derived_projection_contract_changed": derived_projection_contract_changed,
        "derived_partition_coverage_changed": derived_partition_coverage_changed,
        "metadata_rebased_partition_count": metadata_rebased_partitions,
        "refreshed_row_count": refreshed_row_count,
        "refreshed_partition_count": refreshed_partitions,
        "reused_partition_count": reused_partitions,
        "extended_partition_count": extended_partitions,
        "recomputed_partition_count": recomputed_partitions,
        "deleted_partition_count": len(deleted_partitions),
        "write_batch_count": batch_count,
        "refresh_window_count": len(tuple(refresh_windows)),
        "output_columns_hash": target_output_columns_hash,
        "loaded_indicator_partition_count": 0,
        "loaded_source_frame_partition_count": len(loaded_source_partitions),
        "output_paths": output_paths,
        "stage_timings": stage_timings,
        "source_read_cache": source_cache.stats,
        "old_read_cache": old_cache.stats if old_cache is not None else None,
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
