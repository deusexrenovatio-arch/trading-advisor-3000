from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import DeltaTable, write_deltalake

DeltaReadFilters = list[tuple[str, str, object]] | list[list[tuple[str, str, object]]] | None


def _arrow_type(type_name: str) -> pa.DataType:
    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return pa.string()
    if normalized == "timestamp":
        return pa.timestamp("us", tz="UTC")
    if normalized == "date":
        return pa.date32()
    if normalized in {"bool", "boolean"}:
        return pa.bool_()
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


def _normalize_timestamp_value(value: Any, type_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min, tzinfo=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise TypeError(f"expected ISO timestamp for `{type_name}`")
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise TypeError(f"expected ISO timestamp for `{type_name}`") from exc
    else:
        raise TypeError(f"expected timestamp-compatible value for `{type_name}`")

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_date_value(value: Any, type_name: str) -> date:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.date()
        return value.astimezone(timezone.utc).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise TypeError(f"expected ISO date for `{type_name}`")
        try:
            return date.fromisoformat(text)
        except ValueError:
            return _normalize_timestamp_value(text, type_name).date()
    raise TypeError(f"expected date-compatible value for `{type_name}`")


def _normalize_value(value: Any, type_name: str) -> Any:
    if value is None:
        return None
    normalized = type_name.strip().lower()
    if normalized == "string":
        return str(value)
    if normalized == "timestamp":
        return _normalize_timestamp_value(value, type_name)
    if normalized == "date":
        return _normalize_date_value(value, type_name)
    if normalized in {"bool", "boolean"}:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "false"}:
                return text == "true"
        raise TypeError(f"expected bool-compatible value for `{type_name}`")
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
    return pa.schema(
        [pa.field(name, _arrow_type(type_name)) for name, type_name in columns.items()]
    )


def _normalize_rows(
    rows: list[dict[str, object]], columns: dict[str, str]
) -> list[dict[str, object]]:
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


def _normalize_filter_value_for_field(value: Any, field: pa.Field) -> Any:
    if pa.types.is_timestamp(field.type):
        return _normalize_timestamp_value(value, field.name)
    if pa.types.is_date(field.type):
        return _normalize_date_value(value, field.name)
    return value


def _normalize_filter_clause(
    clause: tuple[str, str, object],
    fields_by_name: dict[str, pa.Field],
) -> tuple[str, str, object]:
    if len(clause) != 3:
        return clause
    column_name, operator, value = clause
    field = fields_by_name.get(str(column_name))
    if field is None:
        return clause
    normalized_operator = str(operator).strip().lower()
    if normalized_operator in {"in", "not in"} and isinstance(value, (list, tuple, set)):
        return (
            column_name,
            operator,
            [_normalize_filter_value_for_field(item, field) for item in value],
        )
    return column_name, operator, _normalize_filter_value_for_field(value, field)


def _normalize_filters_for_schema(
    filters: DeltaReadFilters,
    schema: pa.Schema,
) -> DeltaReadFilters:
    if filters is None:
        return None
    fields_by_name = {field.name: field for field in schema}
    if not filters:
        return None

    first = filters[0]
    if isinstance(first, tuple):
        return [
            _normalize_filter_clause(clause, fields_by_name)
            for clause in filters
            if isinstance(clause, tuple)
        ]

    normalized_groups: list[list[tuple[str, str, object]]] = []
    for group in filters:
        if not isinstance(group, list):
            continue
        normalized_groups.append(
            [
                _normalize_filter_clause(clause, fields_by_name)
                for clause in group
                if isinstance(clause, tuple)
            ]
        )
    return normalized_groups


def _filter_clause_expression(clause: tuple[str, str, object]) -> ds.Expression:
    column_name, operator, value = clause
    field = ds.field(str(column_name))
    normalized_operator = str(operator).strip().lower()
    if normalized_operator == "=":
        return field == value
    if normalized_operator in {"!=", "<>"}:
        return field != value
    if normalized_operator == ">":
        return field > value
    if normalized_operator == ">=":
        return field >= value
    if normalized_operator == "<":
        return field < value
    if normalized_operator == "<=":
        return field <= value
    if normalized_operator == "in" and isinstance(value, (list, tuple, set)):
        return field.isin(list(value))
    if normalized_operator == "not in" and isinstance(value, (list, tuple, set)):
        return ~field.isin(list(value))
    raise ValueError(f"unsupported delta dataset scanner filter operator: {operator}")


def _filters_to_dataset_expression(filters: DeltaReadFilters) -> ds.Expression | None:
    if filters is None:
        return None
    if not filters:
        return None
    first = filters[0]
    if isinstance(first, tuple):
        expressions = [
            _filter_clause_expression(clause) for clause in filters if isinstance(clause, tuple)
        ]
        if not expressions:
            return None
        expression = expressions[0]
        for item in expressions[1:]:
            expression = expression & item
        return expression

    group_expressions: list[ds.Expression] = []
    for group in filters:
        if not isinstance(group, list):
            continue
        expressions = [
            _filter_clause_expression(clause) for clause in group if isinstance(clause, tuple)
        ]
        if not expressions:
            continue
        expression = expressions[0]
        for item in expressions[1:]:
            expression = expression & item
        group_expressions.append(expression)
    if not group_expressions:
        return None
    expression = group_expressions[0]
    for item in group_expressions[1:]:
        expression = expression | item
    return expression


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

    schema_mode = "overwrite" if mode == "overwrite" else "merge"
    write_deltalake(str(table_path), arrow_table, mode=mode, schema_mode=schema_mode)


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


def write_delta_table_row_batches(
    *,
    table_path: Path,
    row_batches: Iterable[list[dict[str, object]]],
    columns: dict[str, str],
    max_rows_per_delta_write: int,
    replace_predicate: str | None = None,
    preserve_existing_table: bool = False,
) -> tuple[int, int]:
    if max_rows_per_delta_write <= 0:
        raise ValueError("max_rows_per_delta_write must be > 0")

    table_exists = has_delta_log(table_path)
    if table_exists and replace_predicate:
        delete_delta_table_rows(table_path=table_path, predicate=replace_predicate)

    row_count = 0
    batch_count = 0
    wrote_table = table_exists and preserve_existing_table
    buffered_rows: list[dict[str, object]] = []

    def flush_buffer() -> None:
        nonlocal batch_count, buffered_rows, wrote_table
        if not buffered_rows:
            return
        if wrote_table:
            append_delta_table_rows(table_path=table_path, rows=buffered_rows, columns=columns)
        else:
            write_delta_table_rows(table_path=table_path, rows=buffered_rows, columns=columns)
            wrote_table = True
        batch_count += 1
        buffered_rows = []

    for batch in row_batches:
        if not batch:
            continue
        buffered_rows.extend(batch)
        row_count += len(batch)
        if len(buffered_rows) >= max_rows_per_delta_write:
            flush_buffer()

    flush_buffer()

    if not wrote_table and not table_exists:
        write_delta_table_rows(table_path=table_path, rows=[], columns=columns)

    return row_count, batch_count


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


def _delta_literal(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def delta_equals_predicate(values: dict[str, object]) -> str:
    clauses = [f"{column} = {_delta_literal(value)}" for column, value in values.items()]
    if not clauses:
        raise ValueError("delta predicate requires at least one equality")
    return " AND ".join(clauses)


def replace_delta_table_rows(
    *,
    table_path: Path,
    rows: list[dict[str, object]],
    columns: dict[str, str],
    predicate: str,
) -> None:
    normalized_predicate = str(predicate).strip()
    if not normalized_predicate:
        raise ValueError("delta replace predicate must be non-empty")
    if has_delta_log(table_path):
        delete_delta_table_rows(table_path=table_path, predicate=normalized_predicate)
    if rows:
        append_delta_table_rows(table_path=table_path, rows=rows, columns=columns)
        return
    if not has_delta_log(table_path):
        write_delta_table_rows(table_path=table_path, rows=[], columns=columns)


def count_delta_table_rows(table_path: Path, *, filters: DeltaReadFilters = None) -> int:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = DeltaTable(str(table_path))
    if filters is None:
        return int(table.to_pyarrow_dataset().count_rows())
    normalized_filters = _normalize_filters_for_schema(filters, table.to_pyarrow_dataset().schema)
    return int(table.to_pyarrow_table(columns=[], filters=normalized_filters).num_rows)


def delta_table_columns(table_path: Path) -> tuple[str, ...]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = DeltaTable(str(table_path))
    return tuple(str(field.name) for field in table.to_pyarrow_dataset().schema)


def ensure_delta_table_columns(*, table_path: Path, columns: dict[str, str]) -> bool:
    existing_columns = set(delta_table_columns(table_path))
    missing_columns = [column for column in columns if column not in existing_columns]
    if not missing_columns:
        return False
    write_delta_table_rows(table_path=table_path, rows=[], columns=columns, mode="append")
    return True


def iter_delta_table_row_batches(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
    batch_size: int = 65_536,
) -> Iterator[list[dict[str, object]]]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    table = DeltaTable(str(table_path))
    dataset = table.to_pyarrow_dataset()
    normalized_filters = _normalize_filters_for_schema(filters, dataset.schema)
    scanner = dataset.scanner(
        columns=columns,
        filter=_filters_to_dataset_expression(normalized_filters),
        batch_size=batch_size,
    )
    for batch in scanner.to_batches():
        rows = batch.to_pylist()
        yield [
            {str(key): _normalize_loaded_value(value) for key, value in row.items()}
            for row in rows
            if isinstance(row, dict)
        ]


def read_delta_table_arrow(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
) -> pa.Table:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = DeltaTable(str(table_path))
    dataset = table.to_pyarrow_dataset()
    normalized_filters = _normalize_filters_for_schema(filters, dataset.schema)
    selected_columns = columns
    if selected_columns is not None:
        available = set(dataset.schema.names)
        selected_columns = [column for column in selected_columns if column in available]
    return table.to_pyarrow_table(columns=selected_columns, filters=normalized_filters)


def read_delta_table_rows(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
) -> list[dict[str, object]]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = DeltaTable(str(table_path))
    normalized_filters = _normalize_filters_for_schema(filters, table.to_pyarrow_dataset().schema)
    rows = table.to_pyarrow_table(columns=columns, filters=normalized_filters).to_pylist()
    return [
        {str(key): _normalize_loaded_value(value) for key, value in row.items()}
        for row in rows
        if isinstance(row, dict)
    ]


def read_delta_table_frame(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
) -> pd.DataFrame:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = DeltaTable(str(table_path))
    dataset = table.to_pyarrow_dataset()
    normalized_filters = _normalize_filters_for_schema(filters, dataset.schema)
    selected_columns = columns
    if selected_columns is not None:
        available = set(dataset.schema.names)
        selected_columns = [column for column in selected_columns if column in available]
    arrow_table = table.to_pyarrow_table(columns=selected_columns, filters=normalized_filters)
    return arrow_table.to_pandas()
