from __future__ import annotations

from collections.abc import Iterator
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


DeltaReadFilters = list[tuple[str, str, object]] | list[list[tuple[str, str, object]]] | None


def _arrow_type(type_name: str) -> pa.DataType:
    normalized = type_name.strip().lower()
    if normalized in {"string", "json", "timestamp", "date"}:
        return pa.string()
    if normalized in {"double", "float"}:
        return pa.float64()
    if normalized in {"int", "integer"}:
        return pa.int32()
    if normalized in {"bigint", "long"}:
        return pa.int64()
    if normalized.startswith("array<") and normalized.endswith(">"):
        inner = normalized[6:-1].strip()
        if inner in {"int", "integer"}:
            return pa.list_(pa.int32())
        if inner in {"bigint", "long"}:
            return pa.list_(pa.int64())
        if inner in {"double", "float"}:
            return pa.list_(pa.float64())
        return pa.list_(pa.string())
    return pa.string()


def _normalize_value(value: Any, type_name: str) -> Any:
    if value is None:
        return None
    normalized = type_name.strip().lower()
    if normalized in {"string", "timestamp", "date"}:
        return str(value)
    if normalized == "json":
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if normalized in {"double", "float"}:
        return float(value)
    if normalized in {"int", "integer", "bigint", "long"}:
        return int(value)
    if normalized.startswith("array<") and normalized.endswith(">"):
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"expected list/tuple for `{type_name}`, got {type(value).__name__}")
        inner = normalized[6:-1].strip()
        return [_normalize_value(item, inner) for item in value]
    return value


def _build_schema(columns: dict[str, str]) -> pa.Schema:
    return pa.schema([pa.field(name, _arrow_type(type_name)) for name, type_name in columns.items()])


def _normalize_rows(rows: list[dict[str, object]], columns: dict[str, str]) -> list[dict[str, object]]:
    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        normalized: dict[str, object] = {}
        for column_name, type_name in columns.items():
            normalized[column_name] = _normalize_value(row.get(column_name), type_name)
        normalized_rows.append(normalized)
    return normalized_rows


def _normalize_loaded_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_normalize_loaded_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_loaded_value(item) for key, item in value.items()}
    return value


def has_delta_log(path: Path) -> bool:
    return (path / "_delta_log").exists()


def write_delta_table_rows(
    *,
    table_path: Path,
    rows: list[dict[str, object]],
    columns: dict[str, str],
    mode: str = "overwrite",
) -> None:
    table_path.parent.mkdir(parents=True, exist_ok=True)
    schema = _build_schema(columns)
    normalized_rows = _normalize_rows(rows, columns)

    if normalized_rows:
        arrow_table = pa.Table.from_pylist(normalized_rows, schema=schema)
    else:
        arrays = [pa.array([], type=field.type) for field in schema]
        arrow_table = pa.Table.from_arrays(arrays=arrays, schema=schema)

    write_deltalake(str(table_path), arrow_table, mode=mode)


def append_delta_table_rows(
    *,
    table_path: Path,
    rows: list[dict[str, object]],
    columns: dict[str, str],
) -> None:
    if not rows:
        return
    mode = "append" if has_delta_log(table_path) else "overwrite"
    write_delta_table_rows(table_path=table_path, rows=rows, columns=columns, mode=mode)


def delete_delta_table_rows(
    *,
    table_path: Path,
    predicate: str,
) -> None:
    if not has_delta_log(table_path):
        return
    normalized_predicate = str(predicate).strip()
    if not normalized_predicate:
        raise ValueError("delta delete predicate must be non-empty")
    table = DeltaTable(str(table_path))
    table.delete(normalized_predicate)


def count_delta_table_rows(table_path: Path) -> int:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = DeltaTable(str(table_path))
    return int(table.to_pyarrow_dataset().count_rows())


def iter_delta_table_row_batches(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    batch_size: int = 65_536,
) -> Iterator[list[dict[str, object]]]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    table = DeltaTable(str(table_path))
    dataset = table.to_pyarrow_dataset()
    for batch in dataset.to_batches(columns=columns, batch_size=batch_size):
        rows = batch.to_pylist()
        yield [
            {str(key): _normalize_loaded_value(value) for key, value in row.items()}
            for row in rows
            if isinstance(row, dict)
        ]


def read_delta_table_rows(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
) -> list[dict[str, object]]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = DeltaTable(str(table_path))
    rows = table.to_pyarrow_table(columns=columns, filters=filters).to_pylist()
    return [
        {str(key): _normalize_loaded_value(value) for key, value in row.items()}
        for row in rows
        if isinstance(row, dict)
    ]
