from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Literal

import pyarrow as pa
from deltalake import DeltaTable

from trading_advisor_3000.product_plane.common.table_layout import (
    MAX_COMPRESSED_FILE_BYTES,
    YEAR_PARTITION_COLUMN,
    default_file_policy,
    validate_file_actions,
    with_arrow_close_year,
)
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    _normalize_loaded_rows,
    delete_delta_table_rows,
    delta_table_columns,
    ensure_delta_table_columns,
    has_delta_log,
    read_delta_table_arrow,
    read_delta_table_rows,
    write_delta_table_row_batches,
    write_delta_table_rows,
    write_delta_table_scoped_batches,
)
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    DerivedIndicatorProfile,
    current_derived_indicator_profile,
)

DEFAULT_DERIVED_INDICATOR_SET_VERSION = "derived-v1"
DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS = 100_000
DERIVED_INDICATOR_PARTITION_METADATA_COLUMNS = (
    "ts",
    "null_warmup_span",
    "dataset_version",
    "contour_id",
    "series_mode",
    "series_id",
    "indicator_set_version",
    "derived_indicator_set_version",
    "profile_version",
    "contract_id",
    "instrument_id",
    "timeframe",
    "source_bars_hash",
    "source_dataset_bars_hash",
    "source_indicators_hash",
    "source_indicator_profile_version",
    "source_indicator_output_columns_hash",
    "source_indicator_columns_hash",
    "source_inputs_hash",
    "source_snapshot_hash",
    "compute_contract_hash",
    "row_count",
    "warmup_span",
    "output_columns_hash",
    "created_at",
)
DERIVED_INDICATOR_RESERVED_COLUMNS = {
    "ts_close_year",
    "dataset_version",
    "contour_id",
    "series_mode",
    "series_id",
    "indicator_set_version",
    "derived_indicator_set_version",
    "profile_version",
    "contract_id",
    "instrument_id",
    "timeframe",
    "ts",
    "source_bars_hash",
    "source_dataset_bars_hash",
    "source_indicators_hash",
    "source_indicator_profile_version",
    "source_indicator_output_columns_hash",
    "source_indicator_columns_hash",
    "source_inputs_hash",
    "source_snapshot_hash",
    "compute_contract_hash",
    "row_count",
    "warmup_span",
    "null_warmup_span",
    "created_at",
    "output_columns_hash",
    "source_mtf_hash",
    "mtf_source_family_hash",
}
DERIVED_INDICATOR_FRAME_LOGICAL_KEY = (
    "dataset_version",
    "contour_id",
    "series_mode",
    "series_id",
    "indicator_set_version",
    "derived_indicator_set_version",
    "timeframe",
    "ts",
)


def _derived_indicator_value_type(column: str) -> str:
    if column.endswith("_code") or column.endswith("_flag"):
        return "int"
    return "double"


def _derived_indicator_value_columns(
    profile: DerivedIndicatorProfile | None = None,
) -> dict[str, str]:
    profile = profile or current_derived_indicator_profile()
    if any(c.startswith("mtf_") or c == "source_mtf_hash" for c in profile.output_columns):
        raise ValueError("retired derived projections cannot be written")
    int_columns = {
        column
        for column in profile.output_columns
        if column.endswith("_code") or column.endswith("_flag")
    }
    return {
        column: "int" if column in int_columns else "double" for column in profile.output_columns
    }


def _derived_indicator_frame_columns(
    profile: DerivedIndicatorProfile | None = None,
) -> dict[str, str]:
    return {
        "dataset_version": "string",
        "contour_id": "string",
        "series_mode": "string",
        "series_id": "string",
        "indicator_set_version": "string",
        "derived_indicator_set_version": "string",
        "profile_version": "string",
        "contract_id": "string",
        "instrument_id": "string",
        "timeframe": "string",
        "ts": "timestamp",
        **_derived_indicator_value_columns(profile=profile),
        "source_bars_hash": "string",
        "source_dataset_bars_hash": "string",
        "source_indicators_hash": "string",
        "source_indicator_profile_version": "string",
        "source_indicator_output_columns_hash": "string",
        "source_indicator_columns_hash": "string",
        "source_inputs_hash": "string",
        "source_snapshot_hash": "string",
        "compute_contract_hash": "string",
        "row_count": "int",
        "warmup_span": "int",
        "null_warmup_span": "int",
        "created_at": "timestamp",
        "output_columns_hash": "string",
    }


@dataclass(frozen=True)
class DerivedIndicatorFramePartitionKey:
    dataset_version: str
    indicator_set_version: str
    derived_indicator_set_version: str
    timeframe: str
    instrument_id: str
    contract_id: str | None = None
    contour_id: str = "native_tradable"
    series_mode: str = "contract"
    series_id: str = ""

    def partition_path(self) -> str:
        contract_token = self.contract_id or "continuous-front"
        return (
            f"dataset_version={self.dataset_version}/"
            f"contour_id={self.contour_id}/"
            f"series_mode={self.series_mode}/"
            f"series_id={self.series_id or 'contract'}/"
            f"indicator_set_version={self.indicator_set_version}/"
            f"derived_indicator_set_version={self.derived_indicator_set_version}/"
            f"instrument_id={self.instrument_id}/"
            f"contract_id={contract_token}/"
            f"timeframe={self.timeframe}"
        )

    def matches_row(self, row: dict[str, object]) -> bool:
        if str(row.get("dataset_version")) != self.dataset_version:
            return False
        if str(row.get("contour_id", "native_tradable")) != self.contour_id:
            return False
        if str(row.get("series_mode", "contract")) != self.series_mode:
            return False
        if self.series_id and str(row.get("series_id", "")) != self.series_id:
            return False
        if str(row.get("indicator_set_version")) != self.indicator_set_version:
            return False
        if str(row.get("derived_indicator_set_version")) != self.derived_indicator_set_version:
            return False
        if str(row.get("instrument_id")) != self.instrument_id:
            return False
        if str(row.get("timeframe")) != self.timeframe:
            return False
        if self.contract_id is None:
            return True
        return str(row.get("contract_id")) == self.contract_id


@dataclass(frozen=True)
class DerivedIndicatorFrameRow:
    dataset_version: str
    indicator_set_version: str
    derived_indicator_set_version: str
    profile_version: str
    contract_id: str
    instrument_id: str
    timeframe: str
    ts: str
    values: dict[str, float | int | None]
    source_bars_hash: str
    source_indicators_hash: str
    row_count: int
    warmup_span: int
    null_warmup_span: int
    created_at: str
    output_columns_hash: str = ""
    source_dataset_bars_hash: str = ""
    source_indicator_profile_version: str = ""
    source_indicator_output_columns_hash: str = ""
    source_indicator_columns_hash: str = ""
    source_inputs_hash: str = ""
    source_snapshot_hash: str = ""
    compute_contract_hash: str = ""
    contour_id: str = "native_tradable"
    series_mode: str = "contract"
    series_id: str = ""
    # Physical layout input only: excluded from business payloads and hashes.
    bar_end_ts: str | None = None
    stored_close_year: int | None = field(default=None, compare=False, repr=False)

    def partition_key(self, *, series_mode: str) -> DerivedIndicatorFramePartitionKey:
        return DerivedIndicatorFramePartitionKey(
            dataset_version=self.dataset_version,
            indicator_set_version=self.indicator_set_version,
            derived_indicator_set_version=self.derived_indicator_set_version,
            timeframe=self.timeframe,
            instrument_id=self.instrument_id,
            contract_id=None if series_mode == "continuous_front" else self.contract_id,
            contour_id=self.contour_id,
            series_mode=self.series_mode,
            series_id=self.series_id,
        )

    def to_dict(self) -> dict[str, object]:
        if any(c.startswith("mtf_") or c == "source_mtf_hash" for c in self.values):
            raise ValueError("retired derived projections cannot be written")
        return {
            "dataset_version": self.dataset_version,
            "contour_id": self.contour_id,
            "series_mode": self.series_mode,
            "series_id": self.series_id,
            "indicator_set_version": self.indicator_set_version,
            "derived_indicator_set_version": self.derived_indicator_set_version,
            "profile_version": self.profile_version,
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "ts": self.ts,
            **self.values,
            "source_bars_hash": self.source_bars_hash,
            "source_dataset_bars_hash": self.source_dataset_bars_hash,
            "source_indicators_hash": self.source_indicators_hash,
            "source_indicator_profile_version": self.source_indicator_profile_version,
            "source_indicator_output_columns_hash": self.source_indicator_output_columns_hash,
            "source_indicator_columns_hash": self.source_indicator_columns_hash,
            "source_inputs_hash": self.source_inputs_hash,
            "source_snapshot_hash": self.source_snapshot_hash,
            "compute_contract_hash": self.compute_contract_hash,
            "row_count": self.row_count,
            "warmup_span": self.warmup_span,
            "null_warmup_span": self.null_warmup_span,
            "created_at": self.created_at,
            "output_columns_hash": self.output_columns_hash,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "DerivedIndicatorFrameRow":
        reserved = DERIVED_INDICATOR_RESERVED_COLUMNS
        values: dict[str, float | int | None] = {}
        for key, value in payload.items():
            if key in reserved or key.startswith("mtf_"):
                continue
            if value is None:
                values[key] = None
                continue
            if _derived_indicator_value_type(key) == "int":
                values[key] = int(value)
                continue
            values[key] = float(value)
        return cls(
            dataset_version=str(payload["dataset_version"]),
            contour_id=str(payload.get("contour_id") or "native_tradable"),
            series_mode=str(payload.get("series_mode") or "contract"),
            series_id=str(payload.get("series_id") or payload.get("contract_id") or ""),
            indicator_set_version=str(payload["indicator_set_version"]),
            derived_indicator_set_version=str(
                payload.get("derived_indicator_set_version", DEFAULT_DERIVED_INDICATOR_SET_VERSION)
            ),
            profile_version=str(payload["profile_version"]),
            contract_id=str(payload["contract_id"]),
            instrument_id=str(payload["instrument_id"]),
            timeframe=str(payload["timeframe"]),
            ts=str(payload["ts"]),
            values=values,
            source_bars_hash=str(payload["source_bars_hash"]),
            source_dataset_bars_hash=str(payload.get("source_dataset_bars_hash") or ""),
            source_indicators_hash=str(payload["source_indicators_hash"]),
            source_indicator_profile_version=str(
                payload.get("source_indicator_profile_version") or ""
            ),
            source_indicator_output_columns_hash=str(
                payload.get("source_indicator_output_columns_hash") or ""
            ),
            source_indicator_columns_hash=str(payload.get("source_indicator_columns_hash") or ""),
            source_inputs_hash=str(payload.get("source_inputs_hash") or ""),
            source_snapshot_hash=str(payload.get("source_snapshot_hash") or ""),
            compute_contract_hash=str(payload.get("compute_contract_hash") or ""),
            row_count=int(payload["row_count"]),
            warmup_span=int(payload["warmup_span"]),
            null_warmup_span=int(payload["null_warmup_span"]),
            created_at=str(payload["created_at"]),
            output_columns_hash=str(payload.get("output_columns_hash") or ""),
            stored_close_year=int(payload[YEAR_PARTITION_COLUMN])
            if payload.get(YEAR_PARTITION_COLUMN) is not None
            else None,
        )


def derived_indicator_output_columns_hash(output_columns: tuple[str, ...]) -> str:
    normalized = json.dumps(tuple(output_columns), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def research_derived_indicator_store_contract(
    profile: DerivedIndicatorProfile | None = None,
) -> dict[str, dict[str, object]]:
    return {
        "research_derived_indicator_frames": {
            "format": "delta",
            "partition_by": [YEAR_PARTITION_COLUMN],
            "physical_columns": {YEAR_PARTITION_COLUMN: "int"},
            "partition_close_column": "bar_end_ts",
            "file_policy": default_file_policy(),
            "constraints": [
                "unique(dataset_version, contour_id, series_mode, series_id, "
                "indicator_set_version, derived_indicator_set_version, timeframe, ts)"
            ],
            "columns": _derived_indicator_frame_columns(profile=profile),
        }
    }


def write_derived_indicator_frames(
    *,
    output_dir: Path,
    rows: list[DerivedIndicatorFrameRow],
    replace_partitions: tuple[DerivedIndicatorFramePartitionKey, ...],
    profile: DerivedIndicatorProfile | None = None,
) -> dict[str, str]:
    paths, _, _ = write_derived_indicator_frame_batches(
        output_dir=output_dir,
        row_batches=(rows,),
        replace_partitions=replace_partitions,
        profile=profile,
    )
    return paths


def _quote_delta_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _partition_delete_predicate(partition: DerivedIndicatorFramePartitionKey) -> str:
    clauses = [
        f"dataset_version = {_quote_delta_sql_string(partition.dataset_version)}",
        f"contour_id = {_quote_delta_sql_string(partition.contour_id)}",
        f"series_mode = {_quote_delta_sql_string(partition.series_mode)}",
        f"series_id = {_quote_delta_sql_string(partition.series_id)}",
        f"indicator_set_version = {_quote_delta_sql_string(partition.indicator_set_version)}",
        "derived_indicator_set_version = "
        f"{_quote_delta_sql_string(partition.derived_indicator_set_version)}",
        f"instrument_id = {_quote_delta_sql_string(partition.instrument_id)}",
        f"timeframe = {_quote_delta_sql_string(partition.timeframe)}",
    ]
    if partition.contract_id is not None:
        clauses.append(f"contract_id = {_quote_delta_sql_string(partition.contract_id)}")
    return "(" + " AND ".join(clauses) + ")"


def _replace_partitions_predicate(
    partitions: tuple[DerivedIndicatorFramePartitionKey, ...],
) -> str | None:
    if not partitions:
        return None
    unique_partitions = tuple(
        sorted(set(partitions), key=lambda partition: partition.partition_path())
    )
    return " OR ".join(_partition_delete_predicate(partition) for partition in unique_partitions)


def _assert_unique_write_rows(
    rows: list[dict[str, object]],
    *,
    table_name: str,
    key_columns: tuple[str, ...],
    seen_keys: set[tuple[object, ...]],
) -> None:
    for row in rows:
        key = tuple(row.get(column) for column in key_columns)
        if key not in seen_keys:
            seen_keys.add(key)
            continue
        sample = ", ".join(f"{column}={value}" for column, value in zip(key_columns, key))
        raise ValueError(
            f"{table_name} duplicate logical key in write batch on "
            f"{', '.join(key_columns)}: {sample}"
        )


def write_derived_indicator_frame_batches(
    *,
    output_dir: Path,
    row_batches: Iterable[list[DerivedIndicatorFrameRow]],
    max_rows_per_delta_write: int = DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS,
    replace_partitions: tuple[DerivedIndicatorFramePartitionKey, ...] | None = None,
    profile: DerivedIndicatorProfile | None = None,
    atomic_partition_batches: bool = False,
    partition_start_ts: Mapping[DerivedIndicatorFramePartitionKey, str] | None = None,
) -> tuple[dict[str, str], int, int]:
    if max_rows_per_delta_write <= 0:
        raise ValueError("max_rows_per_delta_write must be > 0")
    if atomic_partition_batches:
        if replace_partitions is None:
            raise ValueError("atomic derived writes require explicit replacement partitions")
        allowed = set(replace_partitions)
        written = set()

        def updates():
            for batch in row_batches:
                if not batch:
                    continue
                partition = batch[0].partition_key(series_mode=batch[0].series_mode)
                if partition not in allowed or partition in written:
                    raise ValueError(
                        "derived atomic stream has an unexpected or repeated partition"
                    )
                written.add(partition)
                yield DerivedIndicatorPartitionUpdate(
                    partition,
                    "replace",
                    batch,
                    partition_start_ts.get(partition) if partition_start_ts is not None else None,
                )
            for partition in allowed - written:
                yield DerivedIndicatorPartitionUpdate(partition, "delete", [])

        return write_derived_indicator_partition_updates(
            output_dir=output_dir,
            updates=updates(),
            expected_partitions=replace_partitions,
            profile=profile,
        )
    contract = research_derived_indicator_store_contract(profile=profile)
    path = output_dir / "research_derived_indicator_frames.delta"
    table_contract = contract["research_derived_indicator_frames"]
    columns = {**table_contract["columns"], **table_contract["physical_columns"]}
    original_id = None
    if has_delta_log(path):
        table = DeltaTable(str(path))
        original_id = table.metadata().id
        if table.metadata().partition_columns != table_contract["partition_by"]:
            raise ValueError("derived frames require an isolated layout migration before mutation")
        fields = {
            field["name"]: field["type"] for field in json.loads(table.schema().to_json())["fields"]
        }
        if fields.get(YEAR_PARTITION_COLUMN) not in {"integer", "long"}:
            raise ValueError(
                "derived frames require an integer physical close-year carrier before mutation"
            )
        columns[YEAR_PARTITION_COLUMN] = (
            "bigint" if fields[YEAR_PARTITION_COLUMN] == "long" else "int"
        )
    replace_predicate = (
        _replace_partitions_predicate(replace_partitions)
        if replace_partitions is not None
        else None
    )
    seen_keys: set[tuple[object, ...]] = set()

    def _validated_row_batches() -> Iterable[list[dict[str, object]]]:
        for batch in row_batches:
            rows = [row.to_dict() for row in batch]
            closes = []
            for row in batch:
                if row.bar_end_ts is None:
                    raise ValueError(
                        "derived frames require authoritative bar_end_ts for physical layout"
                    )
                close = datetime.fromisoformat(row.bar_end_ts.replace("Z", "+00:00"))
                if close.tzinfo is None or close.utcoffset() is None:
                    raise ValueError(
                        "derived physical close timestamps require timezone-aware instants"
                    )
                closes.append(close)
            physical = with_arrow_close_year(
                pa.table({"ts_close": pa.array(closes, type=pa.timestamp("us", tz="UTC"))})
            )
            for row, year in zip(rows, physical.column(YEAR_PARTITION_COLUMN).to_pylist()):
                row[YEAR_PARTITION_COLUMN] = year
            _assert_unique_write_rows(
                rows,
                table_name="research_derived_indicator_frames",
                key_columns=DERIVED_INDICATOR_FRAME_LOGICAL_KEY,
                seen_keys=seen_keys,
            )
            yield rows

    # Keep streaming bounded; validate the first payload before any schema/delete write.
    validated = iter(_validated_row_batches())
    first = next(validated, [])
    if has_delta_log(path):
        ensure_delta_table_columns(table_path=path, columns=dict(columns))
    if partition_start_ts is None:
        row_count, batch_count = write_delta_table_row_batches(
            table_path=path,
            row_batches=chain((first,), validated),
            columns=columns,
            max_rows_per_delta_write=max_rows_per_delta_write,
            replace_predicate=replace_predicate,
            preserve_existing_table=replace_partitions is not None,
            partition_by=(YEAR_PARTITION_COLUMN,),
        )
    else:
        # Consume one computed partition at a time. A bounded suffix is one
        # Delta overwrite transaction; unsupported/large partitions stay full.
        row_count = batch_count = 0
        written: set[DerivedIndicatorFramePartitionKey] = set()
        for rows in chain((first,), validated):
            if not rows:
                continue
            matches = [part for part in replace_partitions or () if part.matches_row(rows[0])]
            if len(matches) != 1 or matches[0] in written:
                raise ValueError("derived suffix writer requires one unique declared partition")
            partition = matches[0]
            if not all(partition.matches_row(row) for row in rows):
                raise ValueError("derived suffix writer received mixed partition rows")
            written.add(partition)
            predicate = _partition_delete_predicate(partition)
            start = partition_start_ts.get(partition)
            if start is not None:
                if len(rows) > max_rows_per_delta_write:
                    raise ValueError("atomic derived suffix exceeds max_rows_per_delta_write")
                predicate = f"({predicate}) AND ts >= {_quote_delta_sql_string(start)}"
                write_delta_table_rows(
                    table_path=path,
                    rows=rows,
                    columns=columns,
                    replace_predicate=predicate,
                    partition_by=(YEAR_PARTITION_COLUMN,),
                    compact=False,
                )
                count, batches = len(rows), 1
            else:
                count, batches = write_delta_table_row_batches(
                    table_path=path,
                    row_batches=(rows,),
                    columns=columns,
                    max_rows_per_delta_write=max_rows_per_delta_write,
                    replace_predicate=predicate,
                    preserve_existing_table=True,
                    partition_by=(YEAR_PARTITION_COLUMN,),
                )
            row_count += count
            batch_count += batches
        for partition in replace_partitions or ():
            if partition not in written:
                delete_delta_table_rows(
                    table_path=path, predicate=_partition_delete_predicate(partition)
                )

    table = DeltaTable(str(path))
    if original_id is not None and table.metadata().id != original_id:
        raise ValueError("derived frame Delta identity changed during update")
    table.optimize.compact(target_size=MAX_COMPRESSED_FILE_BYTES, max_concurrent_tasks=1)
    table.update_incremental()
    validate_file_actions(
        table.get_add_actions(flatten=True).to_pylist(), max_file_bytes=MAX_COMPRESSED_FILE_BYTES
    )

    return {"research_derived_indicator_frames": path.as_posix()}, row_count, batch_count


def load_derived_indicator_partition_metadata(
    *,
    output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    contour_id: str = "native_tradable",
    reader=None,
) -> list[dict[str, object]]:
    path = output_dir / "research_derived_indicator_frames.delta"
    existing_columns = set(reader.schema.names if reader is not None else delta_table_columns(path))
    read_columns = [
        column
        for column in DERIVED_INDICATOR_PARTITION_METADATA_COLUMNS
        if column in existing_columns
    ]
    metadata_by_partition: dict[tuple[object, ...], dict[str, object]] = {}
    filters = _filters_for_existing_columns(
        path,
        [
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", contour_id),
            ("indicator_set_version", "=", indicator_set_version),
            ("derived_indicator_set_version", "=", derived_indicator_set_version),
        ],
        reader=reader,
    )
    # Native reduction collapses repeated per-bar provenance before Python selection.
    table = (
        reader.read(columns=read_columns, filters=filters)
        if reader is not None
        else read_delta_table_arrow(path, columns=read_columns, filters=filters)
    )
    group_columns = [name for name in read_columns if name != "ts"]
    reduced = table.group_by(group_columns).aggregate([("ts", "max")])
    import pandas as pd

    signatures: dict[tuple[object, ...], set[tuple[object, ...]]] = {}
    for row in reduced.to_pylist():
        row["ts"] = row.pop("ts_max")
        for column in DERIVED_INDICATOR_PARTITION_METADATA_COLUMNS:
            row.setdefault(column, None)
        key = (
            row.get("dataset_version"),
            row.get("contour_id") or "native_tradable",
            row.get("series_mode") or "contract",
            row.get("series_id") or "",
            row.get("indicator_set_version"),
            row.get("derived_indicator_set_version"),
            None if row.get("series_mode") == "continuous_front" else row.get("contract_id"),
            row.get("instrument_id"),
            row.get("timeframe"),
        )
        stamp = pd.to_datetime(row.get("created_at"), utc=True, errors="coerce")
        row["_metadata_valid"] = not pd.isna(stamp)
        order = pd.Timestamp.min.tz_localize("UTC") if pd.isna(stamp) else stamp
        previous = metadata_by_partition.get(key)
        previous_order = previous.get("_carrier_order") if previous else None
        signature = tuple(
            row.get(c) for c in group_columns if c not in {"contract_id", "created_at"}
        )
        if previous_order is None or order > previous_order:
            row["_carrier_order"] = order
            metadata_by_partition[key] = row
            signatures[key] = {signature}
        elif order == previous_order:
            signatures[key].add(signature)
    for key, row in metadata_by_partition.items():
        row["_metadata_valid"] = bool(row["_metadata_valid"]) and len(signatures[key]) == 1
        row.pop("_carrier_order", None)
    return list(metadata_by_partition.values())


def _filters_for_existing_columns(
    path: Path,
    filters: list[tuple[str, str, object]],
    reader=None,
) -> list[tuple[str, str, object]]:
    existing_columns = set(reader.schema.names if reader is not None else delta_table_columns(path))
    return [item for item in filters if item[0] in existing_columns]


def _read_rows_with_existing_columns(
    *,
    path: Path,
    requested_columns: tuple[str, ...],
    filters: list[tuple[str, str, object]],
    reader=None,
) -> list[dict[str, object]]:
    existing_columns = set(reader.schema.names if reader is not None else delta_table_columns(path))
    read_columns = [column for column in requested_columns if column in existing_columns]
    safe_filters = [item for item in filters if item[0] in existing_columns]
    rows = (
        _normalize_loaded_rows(reader.read(columns=read_columns, filters=safe_filters).to_pylist())
        if reader is not None
        else read_delta_table_rows(path, columns=read_columns, filters=safe_filters)
    )
    for row in rows:
        for column in requested_columns:
            row.setdefault(column, None)
    return rows


def load_derived_indicator_partition_rows(
    *,
    output_dir: Path,
    partition: DerivedIndicatorFramePartitionKey,
    value_columns: tuple[str, ...],
    ts: str | None = None,
    reader=None,
) -> list[DerivedIndicatorFrameRow]:
    path = output_dir / "research_derived_indicator_frames.delta"
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", partition.dataset_version),
        ("contour_id", "=", partition.contour_id),
        ("series_mode", "=", partition.series_mode),
        ("series_id", "=", partition.series_id),
        ("indicator_set_version", "=", partition.indicator_set_version),
        ("derived_indicator_set_version", "=", partition.derived_indicator_set_version),
        ("instrument_id", "=", partition.instrument_id),
        ("timeframe", "=", partition.timeframe),
    ]
    if partition.contract_id is not None:
        filters.append(("contract_id", "=", partition.contract_id))
    if ts is not None:
        filters.append(("ts", "=", ts))
    requested_columns = tuple(
        dict.fromkeys(
            (
                "dataset_version",
                "contour_id",
                "series_mode",
                "series_id",
                "indicator_set_version",
                "derived_indicator_set_version",
                "profile_version",
                "contract_id",
                "instrument_id",
                "timeframe",
                "ts",
                YEAR_PARTITION_COLUMN,
                *value_columns,
                "source_bars_hash",
                "source_dataset_bars_hash",
                "source_indicators_hash",
                "source_indicator_profile_version",
                "source_indicator_output_columns_hash",
                "source_indicator_columns_hash",
                "source_inputs_hash",
                "source_snapshot_hash",
                "compute_contract_hash",
                "row_count",
                "warmup_span",
                "null_warmup_span",
                "created_at",
                "output_columns_hash",
            )
        )
    )
    rows = _read_rows_with_existing_columns(
        path=path, requested_columns=requested_columns, filters=filters, reader=reader
    )
    return sorted((DerivedIndicatorFrameRow.from_dict(row) for row in rows), key=lambda row: row.ts)


def load_derived_indicator_frames(
    *,
    output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    contour_id: str = "native_tradable",
    value_columns: tuple[str, ...] | None = None,
) -> list[DerivedIndicatorFrameRow]:
    path = output_dir / "research_derived_indicator_frames.delta"
    filters = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
        ("indicator_set_version", "=", indicator_set_version),
        ("derived_indicator_set_version", "=", derived_indicator_set_version),
    ]
    if value_columns is None:
        rows = read_delta_table_rows(
            path,
            filters=_filters_for_existing_columns(path, filters),
        )
    else:
        requested_columns = tuple(
            dict.fromkeys(
                (
                    "dataset_version",
                    "contour_id",
                    "series_mode",
                    "series_id",
                    "indicator_set_version",
                    "derived_indicator_set_version",
                    "profile_version",
                    "contract_id",
                    "instrument_id",
                    "timeframe",
                    "ts",
                    YEAR_PARTITION_COLUMN,
                    *value_columns,
                    "source_bars_hash",
                    "source_dataset_bars_hash",
                    "source_indicators_hash",
                    "source_indicator_profile_version",
                    "source_indicator_output_columns_hash",
                    "source_indicator_columns_hash",
                    "source_inputs_hash",
                    "source_snapshot_hash",
                    "compute_contract_hash",
                    "row_count",
                    "warmup_span",
                    "null_warmup_span",
                    "created_at",
                    "output_columns_hash",
                )
            )
        )
        rows = _read_rows_with_existing_columns(
            path=path, requested_columns=requested_columns, filters=filters
        )
    return [DerivedIndicatorFrameRow.from_dict(row) for row in rows]


def existing_derived_indicator_value_columns(*, output_dir: Path, reader=None) -> tuple[str, ...]:
    path = output_dir / "research_derived_indicator_frames.delta"
    if not (path / "_delta_log").exists():
        return ()
    return tuple(
        column
        for column in (reader.schema.names if reader is not None else delta_table_columns(path))
        if column not in DERIVED_INDICATOR_RESERVED_COLUMNS
    )


def _validated_partition_rows(
    partition: DerivedIndicatorFramePartitionKey,
    batch: list[DerivedIndicatorFrameRow],
    seen_prefixes: set[tuple[object, ...]],
) -> list[dict[str, object]]:
    reserved = DERIVED_INDICATOR_RESERVED_COLUMNS | (
        set(DerivedIndicatorFrameRow.__dataclass_fields__) - {"values"}
    )
    if any(
        row.partition_key(series_mode=row.series_mode) != partition
        or reserved.intersection(row.values)
        for row in batch
    ):
        raise ValueError("derived replacement mixes scopes or reserved columns")
    rows = [row.to_dict() for row in batch]
    _assert_unique_write_rows(
        rows,
        table_name="research_derived_indicator_frames",
        key_columns=DERIVED_INDICATOR_FRAME_LOGICAL_KEY,
        seen_keys=set(),
    )
    prefixes = {
        tuple(row[column] for column in DERIVED_INDICATOR_FRAME_LOGICAL_KEY[:-1]) for row in rows
    }
    if len(prefixes) > 1 or prefixes & seen_prefixes:
        raise ValueError("research_derived_indicator_frames duplicate logical key")
    seen_prefixes.update(prefixes)
    return _derived_physical_rows(batch, rows)


@dataclass(frozen=True)
class DerivedIndicatorPartitionUpdate:
    """One explicit decision per expected scope; omission never means delete."""

    partition: DerivedIndicatorFramePartitionKey
    action: Literal["reuse", "replace", "delete"]
    rows: list[DerivedIndicatorFrameRow]
    start_ts: str | None = None


def write_derived_indicator_partition_updates(
    *,
    output_dir: Path,
    updates: Iterable[DerivedIndicatorPartitionUpdate],
    expected_partitions: tuple[DerivedIndicatorFramePartitionKey, ...],
    profile: DerivedIndicatorProfile | None = None,
) -> tuple[dict[str, str], int, int]:
    path = output_dir / "research_derived_indicator_frames.delta"
    columns = _derived_write_columns(path=path, profile=profile)
    expected = set(expected_partitions)
    if len(expected) != len(expected_partitions):
        raise ValueError("derived expected partitions contain duplicates")
    seen: set[DerivedIndicatorFramePartitionKey] = set()
    prefixes: set[tuple[object, ...]] = set()

    def scoped_batches():
        for update in updates:
            partition = update.partition
            if partition not in expected or partition in seen:
                raise ValueError("derived update has an unexpected or repeated partition")
            seen.add(partition)
            if update.action in {"reuse", "delete"}:
                if update.rows or update.start_ts is not None:
                    raise ValueError("derived reuse/delete must have no rows or suffix")
                if update.action == "delete":
                    yield _partition_delete_predicate(partition), []
                continue
            if update.action != "replace" or not update.rows:
                raise ValueError("derived replacement requires non-empty rows")
            rows = _validated_partition_rows(partition, update.rows, prefixes)
            predicate = _partition_delete_predicate(partition)
            if update.start_ts is not None:
                if update.start_ts != update.rows[0].ts:
                    raise ValueError("derived suffix must begin at the first emitted timestamp")
                predicate += f" AND ts >= {_quote_delta_sql_string(update.start_ts)}"
            yield predicate, rows
        if seen != expected:
            raise ValueError("derived update stream omitted an expected partition")

    row_count, batch_count = write_delta_table_scoped_batches(
        table_path=path,
        scoped_batches=scoped_batches(),
        columns=columns,
        partition_by=(YEAR_PARTITION_COLUMN,),
    )
    return {"research_derived_indicator_frames": path.as_posix()}, row_count, batch_count


def _derived_write_columns(*, path: Path, profile):
    contract = research_derived_indicator_store_contract(profile=profile)
    table_contract = contract["research_derived_indicator_frames"]
    columns = {**table_contract["columns"], **table_contract["physical_columns"]}
    if has_delta_log(path):
        table = DeltaTable(str(path))
        if table.metadata().partition_columns != table_contract["partition_by"]:
            raise ValueError("derived frames require an isolated layout migration before mutation")
        fields = {
            field["name"]: field["type"] for field in json.loads(table.schema().to_json())["fields"]
        }
        if fields.get(YEAR_PARTITION_COLUMN) not in {"integer", "long"}:
            raise ValueError(
                "derived frames require an integer physical close-year carrier before mutation"
            )
        columns[YEAR_PARTITION_COLUMN] = (
            "bigint" if fields[YEAR_PARTITION_COLUMN] == "long" else "int"
        )
    return columns


def _derived_physical_rows(batch, rows):
    closes = []
    for row in batch:
        if row.bar_end_ts is None:
            raise ValueError("derived frames require authoritative bar_end_ts for physical layout")
        close = datetime.fromisoformat(row.bar_end_ts.replace("Z", "+00:00"))
        if close.tzinfo is None or close.utcoffset() is None:
            raise ValueError("derived physical close timestamps require timezone-aware instants")
        closes.append(close)
    physical = with_arrow_close_year(
        pa.table({"ts_close": pa.array(closes, type=pa.timestamp("us", tz="UTC"))})
    )
    for row, year in zip(rows, physical.column(YEAR_PARTITION_COLUMN).to_pylist()):
        row[YEAR_PARTITION_COLUMN] = year
    return rows
