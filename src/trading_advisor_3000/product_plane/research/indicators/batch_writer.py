"""Bounded transaction adapter; the existing indicator store owns DTO and layout."""

from collections.abc import Iterable, Mapping
from pathlib import Path

from trading_advisor_3000.product_plane.common.table_layout import YEAR_PARTITION_COLUMN
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    write_delta_table_scoped_batches,
)

from .registry import IndicatorProfile
from .store import (
    INDICATOR_FRAME_LOGICAL_KEY,
    IndicatorFramePartitionKey,
    IndicatorFrameRow,
    _assert_unique_write_rows,
    _indicator_physical_rows,
    _indicator_write_columns,
    _partition_delete_predicate,
    _quote_delta_sql_string,
)


def write_indicator_frame_scoped_batches(
    *,
    output_dir: Path,
    partition_row_batches: Iterable[tuple[IndicatorFramePartitionKey, list[IndicatorFrameRow]]],
    delete_partitions: tuple[IndicatorFramePartitionKey, ...] = (),
    profile: IndicatorProfile | None = None,
    partition_start_ts: Mapping[IndicatorFramePartitionKey, str],
) -> tuple[dict[str, str], int, int]:
    path = output_dir / "research_indicator_frames.delta"
    columns = _indicator_write_columns(path=path, profile=profile)

    def scoped_batches():
        seen = set()
        logical_prefixes = set()
        reserved = {YEAR_PARTITION_COLUMN} | (
            set(IndicatorFrameRow.__dataclass_fields__) - {"values"}
        )
        for partition, batch in partition_row_batches:
            if partition in seen:
                raise ValueError("research_indicator_frames duplicate logical key in stream")
            seen.add(partition)
            if any(reserved.intersection(row.values) for row in batch):
                raise ValueError("indicator values cannot override reserved columns")
            if any(
                row.series_mode != partition.series_mode
                or row.partition_key(series_mode=partition.series_mode) != partition
                for row in batch
            ):
                raise ValueError("research_indicator_frames row does not match partition identity")
            rows = [row.to_dict() for row in batch]
            _assert_unique_write_rows(
                rows,
                table_name="research_indicator_frames",
                key_columns=INDICATOR_FRAME_LOGICAL_KEY,
                seen_keys=set(),
            )
            prefixes = {
                tuple(row[column] for column in INDICATOR_FRAME_LOGICAL_KEY[:-1]) for row in rows
            }
            if len(prefixes) > 1 or prefixes & logical_prefixes:
                raise ValueError("research_indicator_frames duplicate logical key in stream")
            logical_prefixes.update(prefixes)
            predicate = _partition_delete_predicate(partition)
            if partition not in partition_start_ts:
                raise ValueError("daily indicator batch requires an explicit suffix start")
            predicate += f" AND ts >= {_quote_delta_sql_string(partition_start_ts[partition])}"
            yield predicate, _indicator_physical_rows(batch, rows)
        for partition in delete_partitions:
            if partition in seen:
                raise ValueError("indicator scope is both replaced and deleted")
            seen.add(partition)
            yield _partition_delete_predicate(partition), []

    row_count, batch_count = write_delta_table_scoped_batches(
        table_path=path,
        scoped_batches=scoped_batches(),
        columns=dict(columns),
        partition_by=(YEAR_PARTITION_COLUMN,),
    )
    return {"research_indicator_frames": path.as_posix()}, row_count, batch_count
