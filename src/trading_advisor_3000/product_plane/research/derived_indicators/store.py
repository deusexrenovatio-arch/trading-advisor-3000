from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_table_columns,
    iter_delta_table_row_batches,
    read_delta_table_rows,
    write_delta_table_row_batches,
)
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    DerivedIndicatorProfile,
    current_derived_indicator_profile,
)


DEFAULT_DERIVED_INDICATOR_SET_VERSION = "derived-v1"
DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS = 100_000
DERIVED_INDICATOR_PARTITION_METADATA_COLUMNS = (
    "dataset_version",
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
    "row_count",
    "output_columns_hash",
)
DERIVED_INDICATOR_RESERVED_COLUMNS = {
    "dataset_version",
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
    "row_count",
    "warmup_span",
    "null_warmup_span",
    "created_at",
    "output_columns_hash",
}


def _derived_indicator_value_type(column: str) -> str:
    return "int" if column.endswith("_code") or column.endswith("_flag") else "double"


def _derived_indicator_value_columns(profile: DerivedIndicatorProfile | None = None) -> dict[str, str]:
    profile = profile or current_derived_indicator_profile()
    int_columns = {
        column
        for column in profile.output_columns
        if column.endswith("_code") or column.endswith("_flag")
    }
    return {
        column: "int" if column in int_columns else "double"
        for column in profile.output_columns
    }


def _derived_indicator_frame_columns(profile: DerivedIndicatorProfile | None = None) -> dict[str, str]:
    return {
        "dataset_version": "string",
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

    def partition_path(self) -> str:
        contract_token = self.contract_id or "continuous-front"
        return (
            f"dataset_version={self.dataset_version}/"
            f"indicator_set_version={self.indicator_set_version}/"
            f"derived_indicator_set_version={self.derived_indicator_set_version}/"
            f"instrument_id={self.instrument_id}/"
            f"contract_id={contract_token}/"
            f"timeframe={self.timeframe}"
        )

    def matches_row(self, row: dict[str, object]) -> bool:
        if str(row.get("dataset_version")) != self.dataset_version:
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

    def partition_key(self, *, series_mode: str) -> DerivedIndicatorFramePartitionKey:
        return DerivedIndicatorFramePartitionKey(
            dataset_version=self.dataset_version,
            indicator_set_version=self.indicator_set_version,
            derived_indicator_set_version=self.derived_indicator_set_version,
            timeframe=self.timeframe,
            instrument_id=self.instrument_id,
            contract_id=None if series_mode == "continuous_front" else self.contract_id,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_version": self.dataset_version,
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
            "row_count": self.row_count,
            "warmup_span": self.warmup_span,
            "null_warmup_span": self.null_warmup_span,
            "created_at": self.created_at,
            "output_columns_hash": self.output_columns_hash,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "DerivedIndicatorFrameRow":
        reserved = {
            "dataset_version",
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
            "row_count",
            "warmup_span",
            "null_warmup_span",
            "created_at",
            "output_columns_hash",
        }
        values: dict[str, float | int | None] = {}
        for key, value in payload.items():
            if key in reserved:
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
            source_indicator_profile_version=str(payload.get("source_indicator_profile_version") or ""),
            source_indicator_output_columns_hash=str(payload.get("source_indicator_output_columns_hash") or ""),
            row_count=int(payload["row_count"]),
            warmup_span=int(payload["warmup_span"]),
            null_warmup_span=int(payload["null_warmup_span"]),
            created_at=str(payload["created_at"]),
            output_columns_hash=str(payload.get("output_columns_hash") or ""),
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
            "partition_by": [
                "dataset_version",
                "indicator_set_version",
                "derived_indicator_set_version",
                "instrument_id",
                "timeframe",
            ],
            "constraints": [
                "unique(dataset_version, indicator_set_version, derived_indicator_set_version, contract_id, timeframe, ts)"
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
        f"indicator_set_version = {_quote_delta_sql_string(partition.indicator_set_version)}",
        f"derived_indicator_set_version = {_quote_delta_sql_string(partition.derived_indicator_set_version)}",
        f"instrument_id = {_quote_delta_sql_string(partition.instrument_id)}",
        f"timeframe = {_quote_delta_sql_string(partition.timeframe)}",
    ]
    if partition.contract_id is not None:
        clauses.append(f"contract_id = {_quote_delta_sql_string(partition.contract_id)}")
    return "(" + " AND ".join(clauses) + ")"


def _replace_partitions_predicate(partitions: tuple[DerivedIndicatorFramePartitionKey, ...]) -> str | None:
    if not partitions:
        return None
    unique_partitions = tuple(sorted(set(partitions), key=lambda partition: partition.partition_path()))
    return " OR ".join(_partition_delete_predicate(partition) for partition in unique_partitions)


def write_derived_indicator_frame_batches(
    *,
    output_dir: Path,
    row_batches: Iterable[list[DerivedIndicatorFrameRow]],
    max_rows_per_delta_write: int = DEFAULT_DERIVED_INDICATOR_WRITE_BATCH_ROWS,
    replace_partitions: tuple[DerivedIndicatorFramePartitionKey, ...] | None = None,
    profile: DerivedIndicatorProfile | None = None,
) -> tuple[dict[str, str], int, int]:
    contract = research_derived_indicator_store_contract(profile=profile)
    path = output_dir / "research_derived_indicator_frames.delta"
    columns = contract["research_derived_indicator_frames"]["columns"]
    replace_predicate = (
        _replace_partitions_predicate(replace_partitions)
        if replace_partitions is not None
        else None
    )
    row_count, batch_count = write_delta_table_row_batches(
        table_path=path,
        row_batches=([row.to_dict() for row in batch] for batch in row_batches),
        columns=columns,
        max_rows_per_delta_write=max_rows_per_delta_write,
        replace_predicate=replace_predicate,
        preserve_existing_table=replace_partitions is not None,
    )

    return {"research_derived_indicator_frames": path.as_posix()}, row_count, batch_count


def load_derived_indicator_partition_metadata(
    *,
    output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
) -> list[dict[str, object]]:
    path = output_dir / "research_derived_indicator_frames.delta"
    existing_columns = set(delta_table_columns(path))
    read_columns = [column for column in DERIVED_INDICATOR_PARTITION_METADATA_COLUMNS if column in existing_columns]
    metadata_by_partition: dict[tuple[object, ...], dict[str, object]] = {}
    for batch in iter_delta_table_row_batches(path, columns=read_columns):
        for row in batch:
            if (
                row.get("dataset_version") != dataset_version
                or row.get("indicator_set_version") != indicator_set_version
                or row.get("derived_indicator_set_version") != derived_indicator_set_version
            ):
                continue
            for column in DERIVED_INDICATOR_PARTITION_METADATA_COLUMNS:
                row.setdefault(column, None)
            key = (
                row.get("dataset_version"),
                row.get("indicator_set_version"),
                row.get("derived_indicator_set_version"),
                row.get("contract_id"),
                row.get("instrument_id"),
                row.get("timeframe"),
            )
            metadata_by_partition.setdefault(key, row)
    return list(metadata_by_partition.values())


def _read_rows_with_existing_columns(
    *,
    path: Path,
    requested_columns: tuple[str, ...],
    filters: list[tuple[str, str, object]],
) -> list[dict[str, object]]:
    existing_columns = set(delta_table_columns(path))
    read_columns = [column for column in requested_columns if column in existing_columns]
    rows = read_delta_table_rows(path, columns=read_columns, filters=filters)
    for row in rows:
        for column in requested_columns:
            row.setdefault(column, None)
    return rows


def load_derived_indicator_partition_rows(
    *,
    output_dir: Path,
    partition: DerivedIndicatorFramePartitionKey,
    value_columns: tuple[str, ...],
) -> list[DerivedIndicatorFrameRow]:
    path = output_dir / "research_derived_indicator_frames.delta"
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", partition.dataset_version),
        ("indicator_set_version", "=", partition.indicator_set_version),
        ("derived_indicator_set_version", "=", partition.derived_indicator_set_version),
        ("instrument_id", "=", partition.instrument_id),
        ("timeframe", "=", partition.timeframe),
    ]
    if partition.contract_id is not None:
        filters.append(("contract_id", "=", partition.contract_id))
    requested_columns = tuple(
        dict.fromkeys(
            (
                "dataset_version",
                "indicator_set_version",
                "derived_indicator_set_version",
                "profile_version",
                "contract_id",
                "instrument_id",
                "timeframe",
                "ts",
                *value_columns,
                "source_bars_hash",
                "source_dataset_bars_hash",
                "source_indicators_hash",
                "source_indicator_profile_version",
                "source_indicator_output_columns_hash",
                "row_count",
                "warmup_span",
                "null_warmup_span",
                "created_at",
                "output_columns_hash",
            )
        )
    )
    rows = _read_rows_with_existing_columns(path=path, requested_columns=requested_columns, filters=filters)
    return sorted((DerivedIndicatorFrameRow.from_dict(row) for row in rows), key=lambda row: row.ts)


def load_derived_indicator_frames(
    *,
    output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    derived_indicator_set_version: str = DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    value_columns: tuple[str, ...] | None = None,
) -> list[DerivedIndicatorFrameRow]:
    path = output_dir / "research_derived_indicator_frames.delta"
    filters = [
        ("dataset_version", "=", dataset_version),
        ("indicator_set_version", "=", indicator_set_version),
        ("derived_indicator_set_version", "=", derived_indicator_set_version),
    ]
    if value_columns is None:
        rows = read_delta_table_rows(path, filters=filters)
    else:
        requested_columns = tuple(
            dict.fromkeys(
                (
                    "dataset_version",
                    "indicator_set_version",
                    "derived_indicator_set_version",
                    "profile_version",
                    "contract_id",
                    "instrument_id",
                    "timeframe",
                    "ts",
                    *value_columns,
                    "source_bars_hash",
                    "source_dataset_bars_hash",
                    "source_indicators_hash",
                    "source_indicator_profile_version",
                    "source_indicator_output_columns_hash",
                    "row_count",
                    "warmup_span",
                    "null_warmup_span",
                    "created_at",
                    "output_columns_hash",
                )
            )
        )
        rows = _read_rows_with_existing_columns(path=path, requested_columns=requested_columns, filters=filters)
    return [
        DerivedIndicatorFrameRow.from_dict(row)
        for row in rows
    ]


def existing_derived_indicator_value_columns(*, output_dir: Path) -> tuple[str, ...]:
    path = output_dir / "research_derived_indicator_frames.delta"
    if not (path / "_delta_log").exists():
        return ()
    return tuple(column for column in delta_table_columns(path) if column not in DERIVED_INDICATOR_RESERVED_COLUMNS)
