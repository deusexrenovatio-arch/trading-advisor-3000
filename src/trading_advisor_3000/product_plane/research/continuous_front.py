from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
)


CONTINUOUS_FRONT_TABLES = (
    "continuous_front_bars",
    "continuous_front_roll_events",
    "continuous_front_adjustment_ladder",
    "continuous_front_qc_report",
)


def continuous_front_store_contract() -> dict[str, dict[str, object]]:
    return {
        "continuous_front_bars": {
            "format": "delta",
            "partition_by": ["dataset_version", "instrument_id", "timeframe"],
            "constraints": [
                "unique(dataset_version, roll_policy_version, adjustment_policy_version, instrument_id, timeframe, ts)"
            ],
            "columns": {
                "dataset_version": "string",
                "roll_policy_version": "string",
                "adjustment_policy_version": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "ts": "timestamp",
                "active_contract_id": "string",
                "previous_contract_id": "string",
                "candidate_contract_id": "string",
                "roll_epoch": "int",
                "roll_event_id": "string",
                "is_roll_bar": "boolean",
                "is_first_bar_after_roll": "boolean",
                "bars_since_roll": "int",
                "native_open": "double",
                "native_high": "double",
                "native_low": "double",
                "native_close": "double",
                "native_volume": "bigint",
                "native_open_interest": "bigint",
                "continuous_open": "double",
                "continuous_high": "double",
                "continuous_low": "double",
                "continuous_close": "double",
                "adjustment_mode": "string",
                "cumulative_additive_offset": "double",
                "ratio_factor": "double",
                "price_space": "string",
                "causality_watermark_ts": "timestamp",
                "input_row_count": "bigint",
                "created_at": "timestamp",
            },
        },
        "continuous_front_roll_events": {
            "format": "delta",
            "partition_by": ["dataset_version", "instrument_id", "timeframe"],
            "constraints": ["unique(dataset_version, instrument_id, timeframe, roll_event_id)"],
            "columns": {
                "dataset_version": "string",
                "roll_policy_version": "string",
                "adjustment_policy_version": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "roll_event_id": "string",
                "roll_sequence": "int",
                "old_contract_id": "string",
                "new_contract_id": "string",
                "decision_ts": "timestamp",
                "effective_ts": "timestamp",
                "first_new_bar_ts": "timestamp",
                "last_old_bar_ts": "timestamp",
                "old_reference_price": "double",
                "new_reference_price": "double",
                "additive_gap": "double",
                "ratio_gap": "double",
                "old_reference_source": "string",
                "new_reference_source": "string",
                "roll_reason": "string",
                "causality_watermark_ts": "timestamp",
                "created_at": "timestamp",
            },
        },
        "continuous_front_adjustment_ladder": {
            "format": "delta",
            "partition_by": ["dataset_version", "instrument_id", "timeframe"],
            "constraints": ["unique(dataset_version, instrument_id, timeframe, roll_sequence)"],
            "columns": {
                "dataset_version": "string",
                "roll_policy_version": "string",
                "adjustment_policy_version": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "roll_event_id": "string",
                "roll_sequence": "int",
                "effective_ts": "timestamp",
                "additive_gap": "double",
                "cumulative_offset_before": "double",
                "cumulative_offset_after": "double",
                "ratio_gap": "double",
                "ratio_factor_before": "double",
                "ratio_factor_after": "double",
                "created_at": "timestamp",
            },
        },
        "continuous_front_qc_report": {
            "format": "delta",
            "partition_by": ["dataset_version", "instrument_id", "timeframe"],
            "constraints": ["unique(dataset_version, instrument_id, timeframe, run_id)"],
            "columns": {
                "dataset_version": "string",
                "roll_policy_version": "string",
                "adjustment_policy_version": "string",
                "instrument_id": "string",
                "timeframe": "string",
                "run_id": "string",
                "started_at": "timestamp",
                "completed_at": "timestamp",
                "input_row_count": "bigint",
                "output_row_count": "bigint",
                "roll_event_count": "bigint",
                "missing_active_bar_count": "bigint",
                "duplicate_key_count": "bigint",
                "timeline_error_count": "bigint",
                "ohlc_error_count": "bigint",
                "negative_volume_oi_count": "bigint",
                "missing_reference_price_count": "bigint",
                "future_causality_violation_count": "bigint",
                "gap_abs_max": "double",
                "gap_abs_mean": "double",
                "blocked_reason": "string",
                "status": "string",
            },
        },
    }


@dataclass(frozen=True)
class ContinuousFrontResearchBar:
    contract_id: str
    instrument_id: str
    timeframe: object
    ts: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: int
    series_id: str
    series_mode: str
    active_contract_id: str
    previous_contract_id: str | None
    candidate_contract_id: str | None
    roll_epoch: int
    roll_event_id: str | None
    is_roll_bar: bool
    is_first_bar_after_roll: bool
    bars_since_roll: int
    native_open: float
    native_high: float
    native_low: float
    native_close: float
    continuous_open: float
    continuous_high: float
    continuous_low: float
    continuous_close: float
    adjustment_mode: str
    cumulative_additive_offset: float
    ratio_factor: float | None
    price_space: str


def _session_date(ts: str) -> str:
    return ts[:10]


def _continuous_front_delta_filters(
    *,
    dataset_version: str | None = None,
    instrument_ids: Sequence[str] = (),
    timeframes: Sequence[str] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> list[tuple[str, str, object]] | None:
    filters: list[tuple[str, str, object]] = []
    if dataset_version:
        filters.append(("dataset_version", "=", dataset_version))
    instruments = sorted({str(item) for item in instrument_ids if str(item)})
    if instruments:
        filters.append(("instrument_id", "in", instruments))
    timeframe_values = sorted({str(item) for item in timeframes if str(item)})
    if timeframe_values:
        filters.append(("timeframe", "in", timeframe_values))
    if start_ts:
        filters.append(("ts", ">=", start_ts))
    if end_ts:
        filters.append(("ts", "<=", end_ts))
    return filters or None


def load_continuous_front_as_research_context(
    *,
    continuous_front_output_dir: Path,
    dataset_version: str,
    instrument_ids: Sequence[str] = (),
    timeframes: Sequence[str] = (),
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> tuple[list[ContinuousFrontResearchBar], list[RollMapEntry]]:
    table_path = continuous_front_output_dir / "continuous_front_bars.delta"
    if not has_delta_log(table_path):
        raise RuntimeError(f"missing continuous_front_bars delta table: {table_path.as_posix()}")
    instrument_filter = {str(item) for item in instrument_ids}
    timeframe_filter = {str(item) for item in timeframes}
    row_filters = _continuous_front_delta_filters(
        dataset_version=dataset_version,
        instrument_ids=tuple(instrument_filter),
        timeframes=tuple(timeframe_filter),
        start_ts=start_ts,
        end_ts=end_ts,
    )
    bars: list[ContinuousFrontResearchBar] = []
    roll_map_by_session_contract: dict[tuple[str, str, str], RollMapEntry] = {}
    for row in read_delta_table_rows(table_path, filters=row_filters):
        if str(row.get("dataset_version")) != dataset_version:
            continue
        instrument_id = str(row["instrument_id"])
        timeframe = str(row["timeframe"])
        if instrument_filter and instrument_id not in instrument_filter:
            continue
        if timeframe_filter and timeframe not in timeframe_filter:
            continue
        active_contract_id = str(row["active_contract_id"])
        ts = str(row["ts"]).replace("+00:00", "Z")
        canonical_bar = CanonicalBar.from_dict(
            {
                "contract_id": active_contract_id,
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "ts": ts,
                "open": float(row["continuous_open"]),
                "high": float(row["continuous_high"]),
                "low": float(row["continuous_low"]),
                "close": float(row["continuous_close"]),
                "volume": int(row["native_volume"]),
                "open_interest": int(row["native_open_interest"]),
            }
        )
        bars.append(
            ContinuousFrontResearchBar(
                contract_id=canonical_bar.contract_id,
                instrument_id=canonical_bar.instrument_id,
                timeframe=canonical_bar.timeframe,
                ts=canonical_bar.ts,
                open=canonical_bar.open,
                high=canonical_bar.high,
                low=canonical_bar.low,
                close=canonical_bar.close,
                volume=canonical_bar.volume,
                open_interest=canonical_bar.open_interest,
                series_id=f"{instrument_id}|{timeframe}|continuous_front",
                series_mode="continuous_front",
                active_contract_id=active_contract_id,
                previous_contract_id=None if row.get("previous_contract_id") is None else str(row["previous_contract_id"]),
                candidate_contract_id=None if row.get("candidate_contract_id") is None else str(row["candidate_contract_id"]),
                roll_epoch=int(row.get("roll_epoch") or 0),
                roll_event_id=None if row.get("roll_event_id") is None else str(row["roll_event_id"]),
                is_roll_bar=bool(row.get("is_roll_bar")),
                is_first_bar_after_roll=bool(row.get("is_first_bar_after_roll")),
                bars_since_roll=int(row.get("bars_since_roll") or 0),
                native_open=float(row["native_open"]),
                native_high=float(row["native_high"]),
                native_low=float(row["native_low"]),
                native_close=float(row["native_close"]),
                continuous_open=float(row["continuous_open"]),
                continuous_high=float(row["continuous_high"]),
                continuous_low=float(row["continuous_low"]),
                continuous_close=float(row["continuous_close"]),
                adjustment_mode=str(row.get("adjustment_mode") or ""),
                cumulative_additive_offset=float(row.get("cumulative_additive_offset") or 0.0),
                ratio_factor=None if row.get("ratio_factor") is None else float(row["ratio_factor"]),
                price_space=str(row.get("price_space") or "continuous_adjusted"),
            )
        )
        session_key = (instrument_id, _session_date(ts), active_contract_id)
        roll_map_by_session_contract.setdefault(
            session_key,
            RollMapEntry(
                instrument_id=instrument_id,
                session_date=_session_date(ts),
                active_contract_id=active_contract_id,
                reason="continuous_front_bars",
            ),
        )
    if not bars:
        raise RuntimeError(f"continuous_front_bars has no rows for dataset_version `{dataset_version}`")
    return sorted(bars, key=lambda row: (row.instrument_id, row.contract_id, row.timeframe.value, row.ts)), sorted(
        roll_map_by_session_contract.values(),
        key=lambda row: (row.instrument_id, row.session_date, row.active_contract_id),
    )
