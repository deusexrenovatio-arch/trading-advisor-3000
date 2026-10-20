"""Exact persisted-value suffix proof; no bounded indicator recomputation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_arrow,
)

from .store import DerivedIndicatorFramePartitionKey


def partition_filters(partition: DerivedIndicatorFramePartitionKey):
    return [(name, "=", value) for name, value in vars(partition).items() if value is not None]


def changed_suffix_index(old: pa.Table, new: pa.Table) -> int:
    """Sorted unique timestamps; always retain a final metadata carrier.

    Ordinal comparison finds an insertion/deletion at its first shifted key.
    Rewriting everything after that key also removes deleted timestamps.
    """
    for table in (old, new):
        if table["ts"].null_count or pc.count_distinct(table["ts"]).as_py() != len(table):
            raise ValueError("derived suffix proof requires unique non-null timestamps")
        if len(table) > 1 and not pc.all(pc.less(table["ts"][:-1], table["ts"][1:])).as_py():
            raise ValueError("derived suffix proof requires sorted timestamps")
    if not len(new):
        return 0
    size = min(len(old), len(new))
    equal = pa.array([True] * size, type=pa.bool_())
    for name in new.column_names:
        left = old[name].slice(0, size).cast(new[name].type)
        right = new[name].slice(0, size)
        same = pc.or_(
            pc.fill_null(pc.equal(left, right), False), pc.and_(pc.is_null(left), pc.is_null(right))
        )
        equal = pc.and_(equal, same)
    mismatch = pc.index(equal, False).as_py()
    if mismatch >= 0:
        # A removed timestamp can precede the new timestamp at this ordinal.
        start = min(old["ts"][mismatch].as_py(), new["ts"][mismatch].as_py())
        index = pc.sum(pc.less(new["ts"], pa.scalar(start, new["ts"].type))).as_py()
        # A deletion can fall in a gap before the first emitted surviving key.
        # Include its predecessor; index zero requires a full partition predicate.
        if old["ts"][mismatch].as_py() < new["ts"][mismatch].as_py():
            index = max(0, index - 1)
        return min(index, len(new) - 1)
    # Identical values or a deletion after the surviving end still needs carrier.
    return min(size, len(new) - 1)


def suffix_start_index(
    *,
    output_dir: Path,
    partition: DerivedIndicatorFramePartitionKey,
    computed: pd.DataFrame,
    output_columns: tuple[str, ...],
    compatible: bool,
    reader=None,
) -> int:
    path = output_dir / "research_derived_indicator_frames.delta"
    if not compatible or not has_delta_log(path):
        return 0
    names = [
        "ts",
        "contract_id",
        "ts_close_year",
        "warmup_span",
        "null_warmup_span",
        "profile_version",
        "output_columns_hash",
        *output_columns,
    ]
    old = (
        reader.read(columns=names, filters=partition_filters(partition))
        if reader is not None
        else read_delta_table_arrow(path, columns=names, filters=partition_filters(partition))
    )
    if set(old.column_names) != set(names):
        return 0
    old = old.sort_by([("ts", "ascending")])
    arrays = [
        pa.Array.from_pandas(pd.to_datetime(computed["ts"], utc=True)).cast(
            pa.timestamp("us", "UTC")
        ),
        pa.Array.from_pandas(computed["contract_id"]).cast(pa.string()),
    ]
    for name in ("ts_close_year", "warmup_span", "null_warmup_span"):
        arrays.append(pa.Array.from_pandas(computed[name]).cast(pa.int32()))
    for name in ("profile_version", "output_columns_hash"):
        arrays.append(pa.Array.from_pandas(computed[name]).cast(pa.string()))
    for name in output_columns:
        dtype = pa.int32() if name.endswith(("_flag", "_code")) else pa.float64()
        # from_pandas maps NaN to null exactly like _derived_value.
        values = pa.Array.from_pandas(computed[name])
        if pa.types.is_integer(dtype) and pa.types.is_floating(values.type):
            values = pc.trunc(values)
        # Never retain a prefix by wrapping invalid flags/codes into int32.
        arrays.append(values.cast(dtype, safe=True))
    new = pa.Table.from_arrays(arrays, names=names)
    return changed_suffix_index(old, new)
