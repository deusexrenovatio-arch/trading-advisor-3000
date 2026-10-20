from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common.data_snapshots import open_read_table
from trading_advisor_3000.product_plane.common.table_layout import (
    MAX_COMPRESSED_FILE_BYTES,
    YEAR_PARTITION_COLUMN,
    assert_default_table_layout,
    assert_fresh_arrow_candidate,
    compact_default_table,
    default_partition_columns,
    validate_file_actions,
    write_layout_arrow_table,
)

from .hot_delta_tables import is_hot_delta_table_path

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
        if any(not isinstance(item, tuple) for item in filters):
            return None
        normalized_clauses = [
            _normalize_filter_clause(clause, fields_by_name)
            for clause in filters
            if isinstance(clause, tuple)
        ]
        return normalized_clauses or None

    if any(not isinstance(item, list) for item in filters):
        return None
    normalized_groups: list[list[tuple[str, str, object]]] = []
    for group in filters:
        if not isinstance(group, list):
            continue
        normalized_group = [
            _normalize_filter_clause(clause, fields_by_name)
            for clause in group
            if isinstance(clause, tuple)
        ]
        if normalized_group:
            normalized_groups.append(normalized_group)
    return normalized_groups or None


def _filter_clause_expression(clause: tuple[str, str, object]) -> ds.Expression:
    column_name, operator, value = clause
    field = ds.field(str(column_name))
    normalized_operator = str(operator).strip().lower()
    if normalized_operator in {"=", "=="}:
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


def _filtered_delta_dataset(table: DeltaTable, filters: DeltaReadFilters) -> ds.Dataset:
    dataset = table.to_pyarrow_dataset()
    if not filters:
        return dataset
    groups = [filters] if isinstance(filters[0], tuple) else filters
    predicate_columns = {clause[0] for group in groups for clause in group}
    partition_columns = set(table.metadata().partition_columns)
    data_columns = predicate_columns - partition_columns
    if not data_columns:
        return dataset
    actions = table.get_add_actions().to_pylist()
    if not any(
        (action.get(bound) or {}).get(column) is None
        for action in actions
        for column in data_columns
        for bound in ("min", "max")
    ):
        return dataset

    # delta-rs 0.25 fills absent bounds with NULL, then treats them as constraints.
    # Keep the pinned active file set and actual partition values; Parquet still
    # supplies row-group statistics and applies the original predicate exactly.
    fragments = []
    for action in actions:
        partition_expression = ds.scalar(True)
        for column in partition_columns:
            value = (action.get("partition_values") or {}).get(column)
            expression = (
                ds.field(column).is_null()
                if value is None
                else (ds.field(column) == pa.scalar(value).cast(dataset.schema.field(column).type))
            )
            partition_expression = partition_expression & expression
        fragments.append(
            dataset.format.make_fragment(
                action["path"], dataset.filesystem, partition_expression=partition_expression
            )
        )
    return ds.FileSystemDataset(fragments, dataset.schema, dataset.format, dataset.filesystem)


def _normalize_loaded_rows(rows: list[object]) -> list[dict[str, object]]:
    return [
        {str(key): _normalize_loaded_value(value) for key, value in row.items()}
        for row in rows
        if isinstance(row, dict)
    ]


def _has_effective_filter_value(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_has_effective_filter_value(item) for item in value)
    if isinstance(value, dict):
        return bool(value)
    return True


def _filters_have_effective_clause(filters: DeltaReadFilters) -> bool:
    if not filters:
        return False
    first = filters[0]
    if isinstance(first, tuple):
        if any(not isinstance(item, tuple) for item in filters):
            return False
        for item in filters:
            if len(item) != 3:
                continue
            column_name, operator, value = item
            if (
                str(column_name).strip()
                and str(operator).strip()
                and _has_effective_filter_value(value)
            ):
                return True
        return False
    if isinstance(first, list):
        if any(not isinstance(item, list) for item in filters):
            return False
        return any(_filters_have_effective_clause(item) for item in filters if item)
    return False


def _has_delta_read_bound(*, filters: DeltaReadFilters, limit: int | None = None) -> bool:
    return limit is not None or _filters_have_effective_clause(filters)


def _enforce_hot_delta_read_bound(
    table_path: Path,
    *,
    filters: DeltaReadFilters,
    limit: int | None = None,
) -> None:
    if is_hot_delta_table_path(table_path) and not _has_delta_read_bound(
        filters=filters, limit=limit
    ):
        raise ValueError(
            "hot Delta tables require filters, limit, or Spark/Delta-native operations"
        )


def _enforce_hot_delta_batch_bound(
    table_path: Path,
    *,
    filters: DeltaReadFilters,
) -> None:
    if is_hot_delta_table_path(table_path) and not _filters_have_effective_clause(filters):
        raise ValueError(
            "hot Delta table batch iteration requires filters or Spark/Delta-native operations"
        )


def has_delta_log(path: Path) -> bool:
    return (path / "_delta_log").exists()


def delta_table_version(table_path: Path) -> int:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    return int(open_read_table(table_path, unscoped_open=DeltaTable).version())


def delta_table_identity(table_path: Path) -> dict[str, str]:
    """Stable native metadata identity; the selected read pin owns this lookup."""
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = open_read_table(table_path)
    schema = json.loads(table.schema().to_json())
    schema_bytes = json.dumps(
        schema, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return {
        "table_id": str(table.metadata().id),
        "schema_hash": hashlib.sha256(schema_bytes).hexdigest(),
    }


def write_delta_table_rows(
    *,
    table_path: Path,
    rows: list[dict[str, object]],
    columns: dict[str, str],
    mode: str = "overwrite",
    compact: bool = True,
    family: str | None = None,
    candidate_id: str | None = None,
    replace_predicate: str | None = None,
    partition_by: tuple[str, ...] | None = None,
) -> None:
    table_path.parent.mkdir(parents=True, exist_ok=True)
    schema = _build_schema(columns)
    normalized_rows = _normalize_rows(rows, columns)

    if normalized_rows:
        arrow_table = pa.Table.from_pylist(normalized_rows, schema=schema)
    else:
        arrays = [pa.array([], type=field.type) for field in schema]
        arrow_table = pa.Table.from_arrays(arrays=arrays, schema=schema)

    write_layout_arrow_table(
        table_path=table_path,
        table=arrow_table,
        mode=mode,
        compact=compact,
        family=family,
        candidate_id=candidate_id,
        replace_predicate=replace_predicate,
        partition_by=partition_by,
    )


def append_delta_table_rows(
    *,
    table_path: Path,
    rows: list[dict[str, object]],
    columns: dict[str, str],
    compact: bool = True,
    candidate_id: str | None = None,
    partition_by: tuple[str, ...] | None = None,
) -> None:
    if not rows:
        return
    mode = "append" if has_delta_log(table_path) else "overwrite"
    write_delta_table_rows(
        table_path=table_path,
        rows=rows,
        columns=columns,
        mode=mode,
        compact=compact,
        candidate_id=candidate_id,
        partition_by=partition_by,
    )


def write_delta_table_row_batches(
    *,
    table_path: Path,
    row_batches: Iterable[list[dict[str, object]]],
    columns: dict[str, str],
    max_rows_per_delta_write: int,
    replace_predicate: str | None = None,
    preserve_existing_table: bool = False,
    partition_by: tuple[str, ...] | None = None,
) -> tuple[int, int]:
    if max_rows_per_delta_write <= 0:
        raise ValueError("max_rows_per_delta_write must be > 0")

    assert_fresh_arrow_candidate(table_path)
    table_exists = has_delta_log(table_path)
    if table_exists and partition_by is not None:
        if tuple(DeltaTable(str(table_path)).metadata().partition_columns) != partition_by:
            raise ValueError("explicit partitions require an isolated layout migration")
    if table_exists and replace_predicate:
        delete_delta_table_rows(table_path=table_path, predicate=replace_predicate)

    row_count = 0
    batch_count = 0
    wrote_table = table_exists and preserve_existing_table
    buffered_rows: list[dict[str, object]] = []
    candidate_id: str | None = None

    def flush_buffer() -> None:
        nonlocal batch_count, buffered_rows, wrote_table, candidate_id
        if not buffered_rows:
            return
        if wrote_table:
            append_delta_table_rows(
                table_path=table_path,
                rows=buffered_rows,
                columns=columns,
                compact=False,
                candidate_id=candidate_id,
                partition_by=partition_by,
            )
        else:
            write_delta_table_rows(
                table_path=table_path,
                rows=buffered_rows,
                columns=columns,
                compact=False,
                partition_by=partition_by,
            )
            wrote_table = True
            if default_partition_columns(table_path) is not None:
                candidate_id = DeltaTable(str(table_path)).metadata().id
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
        write_delta_table_rows(
            table_path=table_path,
            rows=[],
            columns=columns,
            compact=False,
            partition_by=partition_by,
        )
        if default_partition_columns(table_path) is not None:
            candidate_id = DeltaTable(str(table_path)).metadata().id

    compact_default_table(table_path, candidate_id=candidate_id)
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
    assert_fresh_arrow_candidate(table_path)
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
    assert_fresh_arrow_candidate(table_path)
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
    table = open_read_table(table_path, unscoped_open=DeltaTable)
    if filters is None:
        return int(table.to_pyarrow_dataset().count_rows())
    normalized_filters = _normalize_filters_for_schema(filters, table.to_pyarrow_dataset().schema)
    dataset = _filtered_delta_dataset(table, normalized_filters)
    return int(dataset.count_rows(filter=_filters_to_dataset_expression(normalized_filters)))


def delta_table_columns(table_path: Path) -> tuple[str, ...]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = open_read_table(table_path, unscoped_open=DeltaTable)
    return tuple(str(field.name) for field in table.to_pyarrow_dataset().schema)


def delta_table_column_value_counts(table_path: Path, *, column: str) -> dict[str, int]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = open_read_table(table_path, unscoped_open=DeltaTable)
    dataset = table.to_pyarrow_dataset()
    if column not in dataset.schema.names:
        return {}

    counts: Counter[str] = Counter()
    scanner = dataset.scanner(columns=[column])
    for batch in scanner.to_batches():
        for value in batch.column(0).to_pylist():
            counts["<null>" if value is None else str(value)] += 1
    return dict(sorted(counts.items()))


def ensure_delta_table_columns(
    *,
    table_path: Path,
    columns: dict[str, str],
    compact: bool = True,
    partition_by: tuple[str, ...] | None = None,
) -> bool:
    assert_default_table_layout(table_path)
    existing_columns = set(delta_table_columns(table_path))
    missing_columns = [column for column in columns if column not in existing_columns]
    if not missing_columns:
        return False
    write_delta_table_rows(
        table_path=table_path,
        rows=[],
        columns=columns,
        mode="append",
        compact=compact,
        partition_by=partition_by,
    )
    return True


def iter_delta_table_row_batches(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
    batch_size: int = 65_536,
) -> Iterator[list[dict[str, object]]]:
    reader = DeltaArrowReader(table_path).batches(
        columns=columns, filters=filters, batch_size=batch_size
    )
    for batch in reader:
        yield _normalize_loaded_rows(batch.to_pylist())


def read_delta_table_arrow(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
) -> pa.Table:
    return DeltaArrowReader(table_path).read(columns=columns, filters=filters)


def read_delta_table_rows(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    if limit is not None and limit < 0:
        raise ValueError("limit must be >= 0")
    table = open_read_table(table_path, unscoped_open=DeltaTable)
    dataset = table.to_pyarrow_dataset()
    normalized_filters = _normalize_filters_for_schema(filters, dataset.schema)
    _enforce_hot_delta_read_bound(table_path, filters=normalized_filters, limit=limit)
    if limit == 0:
        return []
    dataset = _filtered_delta_dataset(table, normalized_filters)
    if limit is not None:
        scanner = dataset.scanner(
            columns=columns,
            filter=_filters_to_dataset_expression(normalized_filters),
        )
        rows: list[dict[str, object]] = []
        for batch in scanner.to_batches():
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            rows.extend(_normalize_loaded_rows(batch.to_pylist())[:remaining])
        return rows
    arrow_table = dataset.to_table(
        columns=columns, filter=_filters_to_dataset_expression(normalized_filters)
    )
    return _normalize_loaded_rows(arrow_table.to_pylist())


def read_delta_table_frame(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    filters: DeltaReadFilters = None,
) -> pd.DataFrame:
    if not has_delta_log(table_path):
        raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
    table = open_read_table(table_path, unscoped_open=DeltaTable)
    dataset = table.to_pyarrow_dataset()
    normalized_filters = _normalize_filters_for_schema(filters, dataset.schema)
    _enforce_hot_delta_read_bound(table_path, filters=normalized_filters)
    dataset = _filtered_delta_dataset(table, normalized_filters)
    selected_columns = columns
    if selected_columns is not None:
        available = set(dataset.schema.names)
        selected_columns = [column for column in selected_columns if column in available]
    arrow_table = dataset.to_table(
        columns=selected_columns, filter=_filters_to_dataset_expression(normalized_filters)
    )
    return arrow_table.to_pandas()


def read_filtered_delta_table_rows(
    table_path: Path,
    *,
    filters: DeltaReadFilters,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    if not _filters_have_effective_clause(filters):
        raise ValueError("filters are required for filtered Delta table reads")
    return read_delta_table_rows(table_path, columns=columns, filters=filters, limit=limit)


def read_small_delta_table_rows(
    table_path: Path,
    *,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    if is_hot_delta_table_path(table_path):
        raise ValueError("hot Delta tables require filtered reads or Spark/Delta-native operations")
    return read_delta_table_rows(table_path, columns=columns, limit=limit)


def write_delta_table_scoped_batches(
    *,
    table_path: Path,
    scoped_batches: Iterable[tuple[str, list[dict[str, object]]]],
    columns: dict[str, str],
    partition_by: tuple[str, ...] | None = None,
    max_batch_bytes: int = 64 * 1024 * 1024,
    max_batch_scopes: int = 32,
) -> tuple[int, int]:
    """Atomically replace bounded packs; consume/validate before mutating each pack.

    One oversized logical scope is written alone. Earlier packs can be committed
    if a later generator fails; the owning asset must not publish that failed run.
    This API deliberately does not use the delete-before-read compatibility API.
    """
    if max_batch_bytes <= 0 or max_batch_scopes <= 0:
        raise ValueError("Delta pack limits must be > 0")
    # Research writers supply authoritative physical carriers. Raw publication
    # remains owned by write_layout_arrow_table and its fresh-candidate guard.
    if table_path.stem == "raw_moex_history":
        raise ValueError("scoped packs do not own raw history publication")
    if partition_by is None:
        raise ValueError("scoped packs require explicit physical partitions")
    if has_delta_log(table_path):
        actual = DeltaTable(str(table_path)).metadata().partition_columns
        if actual != list(partition_by):
            raise ValueError("scoped packs require an isolated layout migration")
    before_version = DeltaTable(str(table_path)).version() if has_delta_log(table_path) else -1
    tables: list[pa.Table] = []
    predicates: list[str] = []
    buffered_bytes = row_count = batch_count = 0

    def flush() -> None:
        nonlocal buffered_bytes, row_count, batch_count
        if not predicates:
            return
        reader = pa.RecordBatchReader.from_batches(
            tables[0].schema,
            (batch for table in tables for batch in table.to_batches()),
        )
        partitions = partition_by
        options = {} if partitions is None else {"partition_by": list(partitions)}
        write_deltalake(
            str(table_path),
            reader,
            mode="overwrite",
            schema_mode="merge",
            predicate=" OR ".join(f"({item})" for item in predicates),
            target_file_size=MAX_COMPRESSED_FILE_BYTES,
            **options,
        )
        row_count += sum(table.num_rows for table in tables)
        batch_count += 1
        tables.clear()
        predicates.clear()
        buffered_bytes = 0

    for predicate, rows in scoped_batches:
        if not str(predicate).strip():
            raise ValueError("Delta pack requires a non-empty replacement predicate")
        # Normalize the next complete scope before committing the previous pack.
        # In particular, invalid timestamps cannot follow a destructive delete.
        table = pa.Table.from_pylist(_normalize_rows(rows, columns), schema=_build_schema(columns))
        if predicates and (
            len(predicates) >= max_batch_scopes or buffered_bytes + table.nbytes > max_batch_bytes
        ):
            flush()
        tables.append(table)
        predicates.append(predicate)
        buffered_bytes += table.nbytes
        if buffered_bytes >= max_batch_bytes or len(predicates) >= max_batch_scopes:
            flush()
    flush()
    if not has_delta_log(table_path):
        write_delta_table_rows(
            table_path=table_path,
            rows=[],
            columns=columns,
            compact=False,
            partition_by=partition_by,
        )
    elif not batch_count:
        ensure_delta_table_columns(
            table_path=table_path, columns=columns, compact=False, partition_by=partition_by
        )
    # Also repairs a previous successful write followed by failed finalization.
    finalize_research_delta_table(
        table_path, since_version=before_version, partition_by=partition_by
    )
    return row_count, batch_count


def finalize_research_delta_table(
    table_path: Path, *, since_version: int, partition_by: tuple[str, ...]
) -> None:
    """Compact changed close-years plus existing debt once; healthy no-op is read-only."""
    if not has_delta_log(table_path):
        return
    if partition_by != (YEAR_PARTITION_COLUMN,):
        raise ValueError("research finalization requires authoritative close-year layout")
    table = DeltaTable(str(table_path))
    if table.metadata().partition_columns != list(partition_by):
        raise ValueError("research finalization requires an isolated layout migration")

    def files(snapshot):
        return {
            row["path"]: (str(row[f"partition.{YEAR_PARTITION_COLUMN}"]), int(row["size_bytes"]))
            for row in snapshot.get_add_actions(flatten=True).to_pylist()
        }

    current = files(table)
    previous = (
        files(DeltaTable(str(table_path), version=since_version)) if since_version >= 0 else {}
    )
    if any(size > MAX_COMPRESSED_FILE_BYTES for _, size in current.values()):
        raise ValueError("research compressed file exceeds 512 MiB; isolated rebuild required")
    years = {
        source[path][0]
        for source, other in ((previous, current), (current, previous))
        for path in source.keys() - other.keys()
    }
    by_year: dict[str, list[int]] = {}
    for year, size in current.values():
        by_year.setdefault(year, []).append(size)
    years.update(
        year
        for year, sizes in by_year.items()
        if len(sizes) > 1 and sum(sorted(sizes)[:2]) <= MAX_COMPRESSED_FILE_BYTES
    )
    if years and current:
        table.optimize.compact(
            target_size=MAX_COMPRESSED_FILE_BYTES,
            max_concurrent_tasks=1,
            partition_filters=[(YEAR_PARTITION_COLUMN, "in", sorted(years))],
        )
        table.update_incremental()
    validate_file_actions(
        table.get_add_actions(flatten=True).to_pylist(), max_file_bytes=MAX_COMPRESSED_FILE_BYTES
    )


class DeltaArrowReader:
    """One existing snapshot handle for related schema/metadata/value reads."""

    def __init__(self, table_path: Path):
        if not has_delta_log(table_path):
            raise FileNotFoundError(f"delta table is missing `_delta_log`: {table_path.as_posix()}")
        self.path = table_path
        self._table = open_read_table(table_path, unscoped_open=DeltaTable)
        self.schema = self._table.to_pyarrow_dataset().schema
        self.version = self._table.version()

    def _dataset(self, filters: DeltaReadFilters, *, batch: bool):
        normalized = _normalize_filters_for_schema(filters, self.schema)
        guard = _enforce_hot_delta_batch_bound if batch else _enforce_hot_delta_read_bound
        guard(self.path, filters=normalized)
        return _filtered_delta_dataset(self._table, normalized), normalized

    def read(self, *, columns=None, filters: DeltaReadFilters = None) -> pa.Table:
        dataset, normalized = self._dataset(filters, batch=False)
        selected = None if columns is None else [c for c in columns if c in self.schema.names]
        return dataset.to_table(columns=selected, filter=_filters_to_dataset_expression(normalized))

    def batches(
        self, *, columns=None, filters: DeltaReadFilters = None, batch_size: int = 32_768
    ) -> pa.RecordBatchReader:
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        dataset, normalized = self._dataset(filters, batch=True)
        return dataset.scanner(
            columns=columns,
            filter=_filters_to_dataset_expression(normalized),
            batch_size=batch_size,
            batch_readahead=1,
            fragment_readahead=1,
        ).to_reader()
