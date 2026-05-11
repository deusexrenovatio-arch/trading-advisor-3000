from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.data_plane import delta_runtime as delta_runtime_module
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    delta_table_columns,
    ensure_delta_table_columns,
    read_delta_table_rows,
    read_filtered_delta_table_rows,
    read_small_delta_table_rows,
    write_delta_table_row_batches,
    write_delta_table_rows,
)


def test_read_delta_table_rows_normalizes_temporal_filter_strings(tmp_path) -> None:
    table_path = tmp_path / "temporal_filters.delta"
    columns = {
        "id": "string",
        "ts": "timestamp",
        "session_date": "date",
    }
    write_delta_table_rows(
        table_path=table_path,
        columns=columns,
        rows=[
            {"id": "a", "ts": "2026-04-01T10:00:00Z", "session_date": "2026-04-01"},
            {"id": "b", "ts": "2026-04-02T10:00:00Z", "session_date": "2026-04-02"},
            {"id": "c", "ts": "2026-04-03T10:00:00Z", "session_date": "2026-04-03"},
        ],
    )

    rows = read_delta_table_rows(
        table_path,
        filters=[
            [("id", "=", "a"), ("ts", ">=", "2026-04-01T00:00:00Z")],
            [("id", "=", "b"), ("session_date", "=", "2026-04-02")],
        ],
    )

    assert [row["id"] for row in sorted(rows, key=lambda item: str(item["id"]))] == ["a", "b"]


def test_count_delta_table_rows_accepts_filters(tmp_path) -> None:
    table_path = tmp_path / "filtered_count.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"dataset_version": "string", "indicator_set_version": "string", "value": "int"},
        rows=[
            {"dataset_version": "dataset-v1", "indicator_set_version": "indicators-v1", "value": 1},
            {"dataset_version": "dataset-v1", "indicator_set_version": "indicators-v2", "value": 2},
            {"dataset_version": "dataset-v2", "indicator_set_version": "indicators-v1", "value": 3},
        ],
    )

    assert (
        count_delta_table_rows(
            table_path,
            filters=[
                ("dataset_version", "=", "dataset-v1"),
                ("indicator_set_version", "=", "indicators-v1"),
            ],
        )
        == 1
    )


def test_read_filtered_delta_table_rows_requires_filters(tmp_path) -> None:
    table_path = tmp_path / "filtered_read.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"dataset_version": "string", "value": "int"},
        rows=[
            {"dataset_version": "dataset-v1", "value": 1},
            {"dataset_version": "dataset-v2", "value": 2},
        ],
    )

    with pytest.raises(ValueError, match="filters are required"):
        read_filtered_delta_table_rows(table_path, filters=[])

    rows = read_filtered_delta_table_rows(
        table_path,
        filters=[("dataset_version", "=", "dataset-v1")],
        limit=1,
    )

    assert rows == [{"dataset_version": "dataset-v1", "value": 1}]


def test_read_delta_table_rows_rejects_unbounded_hot_tables(tmp_path) -> None:
    table_path = tmp_path / "canonical_bars.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"id": "string", "value": "int"},
        rows=[{"id": "a", "value": 1}],
    )

    with pytest.raises(ValueError, match="hot Delta tables"):
        read_delta_table_rows(table_path)
    with pytest.raises(ValueError, match="hot Delta tables"):
        read_delta_table_rows(table_path, filters=None)
    with pytest.raises(ValueError, match="hot Delta tables"):
        read_delta_table_rows(table_path, filters=[])
    with pytest.raises(ValueError, match="hot Delta tables"):
        read_delta_table_rows(table_path, filters=[[]])
    with pytest.raises(ValueError, match="hot Delta tables"):
        read_delta_table_rows(table_path, filters=[("id", "in", [])])
    with pytest.raises(ValueError, match="hot Delta tables"):
        read_delta_table_rows(table_path, filters=[("id", "=", "a"), [("id", "=", "b")]])

    assert read_delta_table_rows(table_path, filters=[("id", "=", "a")]) == [
        {"id": "a", "value": 1}
    ]
    assert read_delta_table_rows(table_path, limit=1) == [{"id": "a", "value": 1}]


def test_read_delta_table_rows_applies_limit_before_full_materialization(
    tmp_path, monkeypatch
) -> None:
    table_path = tmp_path / "canonical_bars.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"id": "string", "value": "int"},
        rows=[{"id": "a", "value": 1}, {"id": "b", "value": 2}],
    )

    def fail_full_materialization(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("limit read used full table materialization")

    monkeypatch.setattr(
        delta_runtime_module.DeltaTable,
        "to_pyarrow_table",
        fail_full_materialization,
    )

    rows = read_delta_table_rows(table_path, limit=1)

    assert len(rows) == 1
    assert rows[0] == {"id": "a", "value": 1}


def test_read_small_delta_table_rows_rejects_hot_tables(tmp_path) -> None:
    table_path = tmp_path / "research_datasets.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"dataset_version": "string"},
        rows=[{"dataset_version": "dataset-v1"}],
    )

    assert read_small_delta_table_rows(table_path) == [{"dataset_version": "dataset-v1"}]
    with pytest.raises(ValueError, match="hot Delta tables"):
        read_small_delta_table_rows(tmp_path / "canonical_bars.delta")


def test_write_delta_table_rows_overwrite_replaces_schema(tmp_path) -> None:
    table_path = tmp_path / "schema_overwrite.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"id": "string"},
        rows=[{"id": "old"}],
    )

    write_delta_table_rows(
        table_path=table_path,
        columns={"id": "string", "derived_indicator_set_version": "string"},
        rows=[{"id": "new", "derived_indicator_set_version": "derived-v1"}],
    )

    assert read_delta_table_rows(table_path) == [
        {"id": "new", "derived_indicator_set_version": "derived-v1"}
    ]


def test_write_delta_table_rows_append_merges_new_columns(tmp_path) -> None:
    table_path = tmp_path / "schema_append.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"id": "string"},
        rows=[{"id": "old"}],
    )

    write_delta_table_rows(
        table_path=table_path,
        columns={"id": "string", "new_indicator": "double"},
        rows=[{"id": "new", "new_indicator": 1.5}],
        mode="append",
    )

    rows = sorted(read_delta_table_rows(table_path), key=lambda item: str(item["id"]))
    assert "new_indicator" in delta_table_columns(table_path)
    assert rows == [
        {"id": "new", "new_indicator": 1.5},
        {"id": "old", "new_indicator": None},
    ]


def test_ensure_delta_table_columns_merges_missing_columns_without_rows(tmp_path) -> None:
    table_path = tmp_path / "schema_ensure.delta"
    write_delta_table_rows(
        table_path=table_path,
        columns={"id": "string"},
        rows=[{"id": "old"}],
    )

    changed = ensure_delta_table_columns(
        table_path=table_path,
        columns={"id": "string", "output_columns_hash": "string"},
    )
    unchanged = ensure_delta_table_columns(
        table_path=table_path,
        columns={"id": "string", "output_columns_hash": "string"},
    )

    assert changed is True
    assert unchanged is False
    assert delta_table_columns(table_path) == ("id", "output_columns_hash")
    assert read_delta_table_rows(table_path) == [{"id": "old", "output_columns_hash": None}]


def test_write_delta_table_row_batches_replaces_matching_rows_only(tmp_path) -> None:
    table_path = tmp_path / "partitioned.delta"
    columns = {
        "dataset_version": "string",
        "instrument_id": "string",
        "value": "int",
    }
    write_delta_table_rows(
        table_path=table_path,
        columns=columns,
        rows=[
            {"dataset_version": "dataset-v1", "instrument_id": "BR", "value": 1},
            {"dataset_version": "dataset-v1", "instrument_id": "Si", "value": 2},
        ],
    )

    row_count, batch_count = write_delta_table_row_batches(
        table_path=table_path,
        columns=columns,
        row_batches=iter([[{"dataset_version": "dataset-v1", "instrument_id": "BR", "value": 10}]]),
        max_rows_per_delta_write=10,
        replace_predicate="dataset_version = 'dataset-v1' AND instrument_id = 'BR'",
        preserve_existing_table=True,
    )

    rows = sorted(read_delta_table_rows(table_path), key=lambda item: str(item["instrument_id"]))
    assert row_count == 1
    assert batch_count == 1
    assert rows == [
        {"dataset_version": "dataset-v1", "instrument_id": "BR", "value": 10},
        {"dataset_version": "dataset-v1", "instrument_id": "Si", "value": 2},
    ]
