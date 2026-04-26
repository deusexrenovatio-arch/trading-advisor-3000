from __future__ import annotations

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
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
