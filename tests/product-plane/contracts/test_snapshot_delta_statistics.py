"""Native Arrow memory-filesystem fixture; no Delta table or disk data writes."""

from types import SimpleNamespace

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    _filtered_delta_dataset,
    delta_arrow_dataset,
)


def test_missing_delta_bounds_do_not_hide_real_rows_in_old_or_pinned_entry():
    filesystem = fs._MockFileSystem()
    filesystem.create_dir("source")
    table = pa.table({"legacy_series_id": ["source-A"], "value": pa.array([7], type=pa.int64())})
    pq.write_table(table, "source/part.parquet", filesystem=filesystem)
    format_ = ds.ParquetFileFormat()
    # Reproduce a false all-NULL constraint from missing Delta statistics.
    fragment = format_.make_fragment(
        "source/part.parquet",
        filesystem,
        partition_expression=ds.field("legacy_series_id").is_null(),
    )
    buggy = ds.FileSystemDataset([fragment], table.schema, format_, filesystem)
    predicate = ds.field("legacy_series_id") == "source-A"
    assert buggy.to_table(filter=predicate).num_rows == 0
    actions = pa.Table.from_pylist(
        [
            {
                "path": "source/part.parquet",
                "min": {"legacy_series_id": None},
                "max": {"legacy_series_id": None},
            }
        ]
    )
    pinned = SimpleNamespace(
        to_pyarrow_dataset=lambda: buggy,
        metadata=lambda: SimpleNamespace(partition_columns=[]),
        get_add_actions=lambda: actions,
    )
    fresh = delta_arrow_dataset(pinned, predicate_columns=["legacy_series_id"])
    old = _filtered_delta_dataset(pinned, [("legacy_series_id", "=", "source-A")])
    for dataset in [fresh, old]:
        result = dataset.scanner(columns=["value"], filter=predicate).head(2)
        assert result["value"].equals(pa.chunked_array([[7]], type=pa.int64()))
