from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView

from .contracts import continuous_front_indicator_store_contract


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(payload: dict[str, object]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _series_ladder_rows(
    ladder_rows: Sequence[dict[str, object]],
    *,
    instrument_id: str,
    timeframe: str,
) -> tuple[dict[str, object], ...]:
    return tuple(
        sorted(
            (
                row
                for row in ladder_rows
                if str(row.get("instrument_id")) == instrument_id and str(row.get("timeframe")) == timeframe
            ),
            key=lambda row: int(row["roll_sequence"]),
        )
    )


def _offset_by_roll_sequence(ladder_rows: Sequence[dict[str, object]]) -> dict[int, float]:
    offsets: dict[int, float] = {0: 0.0}
    running = 0.0
    for row in sorted(ladder_rows, key=lambda item: int(item["roll_sequence"])):
        sequence = int(row["roll_sequence"])
        running += float(row["additive_gap"])
        offsets[sequence] = running
    return {
        sequence: offset
        for sequence, offset in offsets.items()
    }


def _normal_float(value: object) -> float:
    return float(value or 0.0)


def _timeframe_delta(timeframe: str) -> timedelta:
    if timeframe.endswith("m"):
        return timedelta(minutes=int(timeframe[:-1]))
    if timeframe.endswith("h"):
        return timedelta(hours=int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return timedelta(days=int(timeframe[:-1]))
    raise ValueError(f"unsupported timeframe token: {timeframe}")


def _bar_close_ts(ts: str, timeframe: str) -> str:
    opened = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    closed = opened + _timeframe_delta(timeframe)
    return closed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _input_row_hash(row: dict[str, object]) -> str:
    payload = {key: value for key, value in row.items() if key not in {"created_at_utc", "input_front_row_hash"}}
    return _stable_hash(payload)


def build_cf_indicator_input_rows(
    *,
    bar_views: list[ResearchBarView],
    adjustment_ladder_rows: Sequence[dict[str, object]] = (),
    dataset_version: str,
    source_canonical_version: str = "",
    roll_policy_version: str = "",
    adjustment_policy_version: str = "",
    require_ladder_for_rolls: bool = True,
    created_at_utc: str | None = None,
) -> list[dict[str, object]]:
    created_at = created_at_utc or _utc_now_iso()
    ordered = sorted(bar_views, key=lambda row: (row.instrument_id, row.timeframe, row.ts, row.contract_id))
    grouped: dict[tuple[str, str], list[ResearchBarView]] = {}
    for row in ordered:
        grouped.setdefault((row.instrument_id, row.timeframe), []).append(row)

    rows: list[dict[str, object]] = []
    for (instrument_id, timeframe), series in sorted(grouped.items()):
        ladder = _series_ladder_rows(
            adjustment_ladder_rows,
            instrument_id=instrument_id,
            timeframe=timeframe,
        )
        max_roll_epoch = max((int(row.roll_epoch or 0) for row in series), default=0)
        if require_ladder_for_rolls and max_roll_epoch > 0 and not ladder:
            raise ValueError(
                "cf_indicator_input_frame requires adjustment ladder rows for rolled series "
                f"{instrument_id}|{timeframe}"
            )
        ladder_sequences = {int(row["roll_sequence"]) for row in ladder}
        missing_sequences = [sequence for sequence in range(1, max_roll_epoch + 1) if sequence not in ladder_sequences]
        if missing_sequences:
            joined = ", ".join(str(sequence) for sequence in missing_sequences)
            raise ValueError(
                "cf_indicator_input_frame missing adjustment ladder roll_sequence "
                f"{joined} for {instrument_id}|{timeframe}"
            )
        offsets = _offset_by_roll_sequence(ladder)

        prev_close0: float | None = None
        for source in series:
            ts_close = _bar_close_ts(source.ts, timeframe)
            roll_seq = int(source.roll_epoch or 0)
            offset = offsets.get(roll_seq, float(source.cumulative_additive_offset or 0.0))
            native_open = _normal_float(source.native_open if source.native_open is not None else source.open)
            native_high = _normal_float(source.native_high if source.native_high is not None else source.high)
            native_low = _normal_float(source.native_low if source.native_low is not None else source.low)
            native_close = _normal_float(source.native_close if source.native_close is not None else source.close)
            open0 = native_open - offset
            high0 = native_high - offset
            low0 = native_low - offset
            close0 = native_close - offset
            true_range0 = max(
                high0 - low0,
                abs(high0 - prev_close0) if prev_close0 is not None and math.isfinite(prev_close0) else high0 - low0,
                abs(low0 - prev_close0) if prev_close0 is not None and math.isfinite(prev_close0) else high0 - low0,
            )
            payload = {
                "dataset_version": dataset_version,
                "source_canonical_version": source_canonical_version,
                "roll_policy_version": roll_policy_version,
                "adjustment_policy_version": adjustment_policy_version,
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "ts": source.ts,
                "ts_close": ts_close,
                "session_date": source.session_date,
                "session_open_ts": source.session_open_ts,
                "session_close_ts": source.session_close_ts,
                "active_contract_id": source.active_contract_id,
                "roll_epoch_id": f"{instrument_id}|{timeframe}|{roll_seq}",
                "roll_seq": roll_seq,
                "is_roll_bar": source.is_roll_bar,
                "is_first_bar_after_roll": source.is_first_bar_after_roll,
                "bars_since_roll": int(source.bars_since_roll or 0),
                "native_open": native_open,
                "native_high": native_high,
                "native_low": native_low,
                "native_close": native_close,
                "native_volume": int(source.volume),
                "native_open_interest": int(source.open_interest),
                "cumulative_additive_offset": offset,
                "open0": open0,
                "high0": high0,
                "low0": low0,
                "close0": close0,
                "hl2_0": (high0 + low0) / 2.0,
                "hlc3_0": (high0 + low0 + close0) / 3.0,
                "true_range0": true_range0,
                "price_space_native": "contract_native",
                "price_space_normalized": "causal_zero_anchor",
                "causality_watermark_ts": ts_close,
                "created_at_utc": created_at,
            }
            payload["input_front_row_hash"] = _input_row_hash(payload)
            rows.append(payload)
            prev_close0 = close0
    return rows


def load_research_bar_views(*, dataset_output_dir: Path, dataset_version: str) -> list[ResearchBarView]:
    rows = read_delta_table_rows(
        dataset_output_dir / "research_bar_views.delta",
        filters=[("dataset_version", "=", dataset_version)],
    )
    return [ResearchBarView.from_dict(row) for row in rows]


def load_adjustment_ladder_rows(*, dataset_output_dir: Path, dataset_version: str) -> tuple[dict[str, object], ...]:
    table_path = dataset_output_dir / "continuous_front_adjustment_ladder.delta"
    if not (table_path / "_delta_log").exists():
        return ()
    rows = read_delta_table_rows(table_path, filters=[("dataset_version", "=", dataset_version)])
    return tuple(dict(row) for row in rows)


def materialize_cf_indicator_input_frame(
    *,
    dataset_output_dir: Path,
    output_dir: Path,
    dataset_version: str,
    source_canonical_version: str = "",
    roll_policy_version: str = "",
    adjustment_policy_version: str = "",
) -> dict[str, object]:
    rows = build_cf_indicator_input_rows(
        bar_views=load_research_bar_views(dataset_output_dir=dataset_output_dir, dataset_version=dataset_version),
        adjustment_ladder_rows=load_adjustment_ladder_rows(
            dataset_output_dir=dataset_output_dir,
            dataset_version=dataset_version,
        ),
        dataset_version=dataset_version,
        source_canonical_version=source_canonical_version,
        roll_policy_version=roll_policy_version,
        adjustment_policy_version=adjustment_policy_version,
    )
    contract = continuous_front_indicator_store_contract()["cf_indicator_input_frame"]
    table_path = output_dir / "cf_indicator_input_frame.delta"
    write_delta_table_rows(table_path=table_path, rows=rows, columns=dict(contract["columns"]))
    return {
        "dataset_version": dataset_version,
        "row_count": len(rows),
        "output_path": table_path.as_posix(),
    }
