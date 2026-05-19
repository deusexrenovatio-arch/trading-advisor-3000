from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_table_columns,
    read_delta_table_arrow,
    read_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import (
    research_dataset_store_contract,
)

DERIVED_SOURCE_FRAME_TABLE = "research_derived_source_frames"
DERIVED_SOURCE_FRAME_DELTA = f"{DERIVED_SOURCE_FRAME_TABLE}.delta"

DERIVED_SOURCE_FRAME_METADATA_COLUMNS: tuple[str, ...] = (
    "dataset_version",
    "contour_id",
    "series_mode",
    "series_id",
    "indicator_set_version",
    "derived_profile_version",
    "contract_id",
    "instrument_id",
    "timeframe",
    "source_indicator_columns_hash",
    "source_l0_delta_version",
    "source_l1_delta_version",
    "source_l0_delta_hash",
    "source_l1_delta_hash",
    "l0_row_count",
    "l1_row_count",
    "joined_row_count",
    "duplicate_indicator_key_count",
    "missing_indicator_key_count",
    "source_bars_hash",
    "source_indicators_hash",
    "indicator_profile_version",
    "indicator_source_dataset_bars_hash",
    "indicator_output_columns_hash",
    "source_frame_created_at",
)


@dataclass(frozen=True)
class DerivedSourceFramePartitionKey:
    dataset_version: str
    indicator_set_version: str
    timeframe: str
    instrument_id: str
    contract_id: str | None = None
    contour_id: str = "native_tradable"
    series_mode: str = "contract"
    series_id: str = ""

    def partition_path(self) -> str:
        contract = self.contract_id or ""
        return (
            f"{self.dataset_version}/{self.contour_id}/{self.series_mode}/"
            f"{self.series_id}/{self.indicator_set_version}/{contract}/"
            f"{self.instrument_id}/{self.timeframe}"
        )


def derived_source_indicator_columns_hash(columns: tuple[str, ...]) -> str:
    payload = "\n".join(sorted(dict.fromkeys(columns)))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()


def research_derived_source_frame_store_contract(
    *, source_indicator_columns: tuple[str, ...] = ()
) -> dict[str, dict[str, object]]:
    bar_columns = research_dataset_store_contract()["research_bar_views"]["columns"]
    columns = {
        **bar_columns,
        "indicator_set_version": "string",
        "derived_profile_version": "string",
        **{column: "double" for column in source_indicator_columns},
        "indicator_profile_version": "string",
        "indicator_source_bars_hash": "string",
        "indicator_source_dataset_bars_hash": "string",
        "indicator_row_count": "int",
        "indicator_warmup_span": "int",
        "indicator_null_warmup_span": "int",
        "indicator_created_at": "timestamp",
        "indicator_output_columns_hash": "string",
        "source_indicator_columns_hash": "string",
        "source_l0_delta_version": "bigint",
        "source_l1_delta_version": "bigint",
        "source_l0_delta_hash": "string",
        "source_l1_delta_hash": "string",
        "l0_row_count": "bigint",
        "l1_row_count": "bigint",
        "joined_row_count": "bigint",
        "duplicate_indicator_key_count": "bigint",
        "missing_indicator_key_count": "bigint",
        "source_bars_hash": "string",
        "source_indicators_hash": "string",
        "source_frame_created_at": "timestamp",
    }
    return {
        DERIVED_SOURCE_FRAME_TABLE: {
            "format": "delta",
            "partition_by": [
                "dataset_version",
                "contour_id",
                "indicator_set_version",
                "instrument_id",
                "timeframe",
            ],
            "constraints": [
                "unique(dataset_version, contour_id, series_mode, series_id, "
                "indicator_set_version, timeframe, ts)"
            ],
            "columns": columns,
        }
    }


def _filters_for_existing_columns(
    path: Path, filters: list[tuple[str, str, object]]
) -> list[tuple[str, str, object]]:
    existing = set(delta_table_columns(path))
    return [item for item in filters if item[0] in existing]


def load_derived_source_frame_partition_metadata(
    *,
    output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    contour_id: str = "native_tradable",
) -> list[dict[str, object]]:
    path = output_dir / DERIVED_SOURCE_FRAME_DELTA
    existing_columns = set(delta_table_columns(path))
    read_columns = [
        column for column in DERIVED_SOURCE_FRAME_METADATA_COLUMNS if column in existing_columns
    ]
    if "ts" in existing_columns:
        read_columns.append("ts")
    group_columns = [
        column
        for column in (
            "dataset_version",
            "contour_id",
            "series_mode",
            "series_id",
            "indicator_set_version",
            "contract_id",
            "instrument_id",
            "timeframe",
        )
        if column in read_columns
    ]
    if not group_columns:
        return []
    aggregate_columns = [
        column for column in read_columns if column not in group_columns and column != "ts"
    ]
    filters = _filters_for_existing_columns(
        path,
        [
            ("dataset_version", "=", dataset_version),
            ("contour_id", "=", contour_id),
            ("indicator_set_version", "=", indicator_set_version),
        ],
    )
    table = read_delta_table_arrow(path, columns=read_columns, filters=filters)
    if table.num_rows == 0:
        return []
    count_aggregates = [("ts", "count")] if "ts" in read_columns else []
    grouped = table.group_by(group_columns).aggregate(
        [(column, "max") for column in aggregate_columns] + count_aggregates
    )
    rows: list[dict[str, object]] = []
    for grouped_row in grouped.to_pylist():
        row = {column: grouped_row.get(column) for column in group_columns}
        for column in aggregate_columns:
            row[column] = grouped_row.get(f"{column}_max")
        row["partition_row_count"] = (
            grouped_row.get("ts_count")
            if "ts" in read_columns
            else grouped_row.get("joined_row_count_max")
        )
        for column in DERIVED_SOURCE_FRAME_METADATA_COLUMNS:
            row.setdefault(column, None)
        rows.append(row)
    return rows


def load_derived_source_frame_partition_rows(
    *,
    output_dir: Path,
    partition: DerivedSourceFramePartitionKey,
    source_indicator_columns: tuple[str, ...],
) -> list[dict[str, object]]:
    path = output_dir / DERIVED_SOURCE_FRAME_DELTA
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", partition.dataset_version),
        ("contour_id", "=", partition.contour_id),
        ("series_mode", "=", partition.series_mode),
        ("series_id", "=", partition.series_id),
        ("indicator_set_version", "=", partition.indicator_set_version),
        ("instrument_id", "=", partition.instrument_id),
        ("timeframe", "=", partition.timeframe),
    ]
    if partition.contract_id is not None:
        filters.append(("contract_id", "=", partition.contract_id))
    requested_columns = tuple(
        dict.fromkeys(
            (
                *research_derived_source_frame_store_contract(
                    source_indicator_columns=source_indicator_columns
                )[DERIVED_SOURCE_FRAME_TABLE]["columns"].keys(),
            )
        )
    )
    existing_columns = set(delta_table_columns(path))
    read_columns = [column for column in requested_columns if column in existing_columns]
    rows = read_delta_table_rows(
        path,
        columns=read_columns,
        filters=[item for item in filters if item[0] in existing_columns],
    )
    for row in rows:
        for column in requested_columns:
            row.setdefault(column, None)
    return sorted(rows, key=lambda row: str(row["ts"]))


def iter_derived_source_frame_partition_rows(
    *,
    output_dir: Path,
    partition: DerivedSourceFramePartitionKey,
    source_indicator_columns: tuple[str, ...],
) -> list[dict[str, object]]:
    return load_derived_source_frame_partition_rows(
        output_dir=output_dir,
        partition=partition,
        source_indicator_columns=source_indicator_columns,
    )
