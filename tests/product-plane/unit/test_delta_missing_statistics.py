from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.data_plane import delta_runtime

READERS = ("count", "arrow", "frame", "rows", "limited_rows", "batches")


@pytest.fixture
def missing_bounds_table(tmp_path):
    path = tmp_path / "spark_stats.delta"
    write_deltalake(
        str(path),
        pa.table(
            {
                "year": [2025, 2025, 2026, None],
                "id": [1, 2, 3, 4],
                "flag": [True, False, None, True],
                "amount": [2, 1, 2, 3],
            }
        ),
        partition_by=["year"],
    )
    # Spark omits Boolean bounds and may omit columns beyond its statistics limit.
    log = path / "_delta_log/00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "add" in action:
            stats = json.loads(action["add"]["stats"])
            for bound in ("minValues", "maxValues"):
                for field in ("flag", "amount"):
                    stats[bound].pop(field, None)
            action["add"]["stats"] = json.dumps(stats)
    log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    return path


def _assert_read(reader, path, filters, expected):
    if reader == "count":
        assert delta_runtime.count_delta_table_rows(path, filters=filters) == len(expected)
        return
    if reader == "arrow":
        result = delta_runtime.read_delta_table_arrow(
            path, columns=["id", "absent_projection"], filters=filters
        )
        assert result.column_names == ["id"]
        ids = result["id"].to_pylist()
    elif reader == "frame":
        result = delta_runtime.read_delta_table_frame(
            path, columns=["id", "absent_projection"], filters=filters
        )
        assert list(result.columns) == ["id"]
        ids = result["id"].tolist()
    elif reader == "batches":
        batches = list(
            delta_runtime.iter_delta_table_row_batches(
                path, columns=["id"], filters=filters, batch_size=1
            )
        )
        assert all(len(batch) <= 1 for batch in batches)
        ids = [row["id"] for batch in batches for row in batch]
    else:
        kwargs = {"limit": len(expected)} if reader == "limited_rows" else {}
        ids = [
            row["id"]
            for row in delta_runtime.read_delta_table_rows(
                path, columns=["id"], filters=filters, **kwargs
            )
        ]
    assert sorted(ids) == expected


@pytest.mark.parametrize("reader", READERS)
@pytest.mark.parametrize("operator", ["=", "=="])
@pytest.mark.parametrize("field,value", [("flag", True), ("amount", 2)])
def test_missing_bounds_match_native_parquet_oracle(
    missing_bounds_table, reader, operator, field, value
):
    path = missing_bounds_table
    oracle = (
        DeltaTable(str(path))
        .to_pyarrow_table()
        .filter((pc.field("year") == 2025) & (pc.field(field) == value))
    )
    assert oracle["id"].to_pylist() == [1]
    _assert_read(reader, path, [("year", operator, 2025), (field, operator, value)], [1])


@pytest.mark.parametrize("reader", READERS)
@pytest.mark.parametrize(
    "filters,expected",
    [
        (
            [
                [("year", "=", 2025), ("flag", "=", False)],
                [("year", "=", 2026), ("amount", "in", [2])],
            ],
            [2, 3],
        ),
        ([("flag", "in", [None])], [3]),
        ([("year", "in", [None]), ("flag", "=", True)], [4]),
    ],
)
def test_missing_bounds_preserve_dnf_nulls_and_partition_values(
    missing_bounds_table, reader, filters, expected
):
    _assert_read(reader, missing_bounds_table, filters, expected)


@pytest.mark.parametrize("reader", READERS)
def test_missing_bounds_use_only_active_files_at_pinned_version(
    missing_bounds_table, reader, monkeypatch
):
    path = missing_bounds_table
    pinned = DeltaTable(str(path), version=0)
    write_deltalake(
        str(path),
        pa.table({"year": [2025], "id": [99], "flag": [True], "amount": [2]}),
        mode="overwrite",
        partition_by=["year"],
    )
    filters = [("year", "=", 2025), ("flag", "=", True)]
    _assert_read(reader, path, filters, [99])
    monkeypatch.setattr(delta_runtime, "DeltaTable", lambda _: pinned)
    _assert_read(reader, path, filters, [1])
