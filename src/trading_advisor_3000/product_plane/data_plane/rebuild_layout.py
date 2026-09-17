from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Mapping

import yaml

REBUILD_LAYOUT_SCHEMA_VERSION = "ta3000_table_layout.v1"
REBUILD_YEAR_PARTITION_COLUMN = "ts_close_year"
REBUILD_LAYOUT_LAYERS = frozenset({"raw", "canonical", "research"})


@dataclass(frozen=True)
class RebuildTableLayout:
    name: str
    layer: str
    relative_path: PurePosixPath
    scope: str
    required: bool
    partition_by: tuple[str, ...]
    partition_source_column: str | None
    sort_columns: tuple[str, ...]
    provisional_files: int


@dataclass(frozen=True)
class RebuildStorageLayout:
    schema_version: str
    manifest_path: Path
    manifest_sha256: str
    rebuild_id: str
    dataset_version: str
    compute_window_years: tuple[int, ...]
    target_window_years: tuple[int, ...]
    max_compressed_file_bytes: int
    files_per_nonempty_partition: int
    split_only_when_file_exceeds_limit: bool
    no_empty_partitions: bool
    tables: tuple[RebuildTableLayout, ...]

    def table(self, name: str) -> RebuildTableLayout:
        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(f"unknown rebuild table: {name}")

    def table_path(self, root: Path, name: str) -> Path:
        return Path(root).joinpath(*self.table(name).relative_path.parts)


def _required_mapping(payload: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"rebuild layout `{key}` must be a mapping")
    return value


def _required_text(payload: Mapping[str, object], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"rebuild layout `{key}` must be non-empty")
    return value


def _positive_int(payload: Mapping[str, object], key: str) -> int:
    value = int(payload.get(key) or 0)
    if value <= 0:
        raise ValueError(f"rebuild layout `{key}` must be > 0")
    return value


def _required_bool(payload: Mapping[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"rebuild layout {key} must be a boolean")
    return value


def _year_sequence(payload: Mapping[str, object], key: str) -> tuple[int, ...]:
    raw = payload.get(key)
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"rebuild layout `{key}` must be a non-empty year list")
    years = tuple(int(item) for item in raw)
    if years != tuple(sorted(set(years))):
        raise ValueError(f"rebuild layout `{key}` must contain unique ascending years")
    return years


def _relative_table_path(value: object) -> PurePosixPath:
    text = str(value or "").strip().replace("\\", "/")
    path = PurePosixPath(text)
    if not text or path.is_absolute() or ".." in path.parts or ":" in text:
        raise ValueError(f"rebuild layout requires a safe relative table path: {text!r}")
    if path.suffix != ".delta":
        raise ValueError(f"rebuild layout table path must end with .delta: {text!r}")
    return path


def _partition_source_column(layer: str, partition_by: tuple[str, ...]) -> str | None:
    if not partition_by:
        return None
    return "ts_close" if layer == "raw" else "ts"


def _parse_table(
    raw: Mapping[str, object],
    *,
    sort_columns_by_layer: Mapping[str, tuple[str, ...]],
) -> RebuildTableLayout:
    name = _required_text(raw, "name")
    layer = _required_text(raw, "layer")
    if layer not in REBUILD_LAYOUT_LAYERS:
        raise ValueError(f"unsupported rebuild table layer for {name}: {layer}")
    raw_partition_by = raw.get("partition_by")
    if not isinstance(raw_partition_by, list):
        raise ValueError(f"rebuild table {name} partition_by must be a list")
    partition_by = tuple(str(item).strip() for item in raw_partition_by)
    if partition_by not in {(), (REBUILD_YEAR_PARTITION_COLUMN,)}:
        raise ValueError(
            f"rebuild table {name} may partition only by {REBUILD_YEAR_PARTITION_COLUMN}"
        )
    sort_key = "raw_bars" if layer == "raw" else layer
    return RebuildTableLayout(
        name=name,
        layer=layer,
        relative_path=_relative_table_path(raw.get("path")),
        scope=_required_text(raw, "scope"),
        required=_required_bool(raw, "required"),
        partition_by=partition_by,
        partition_source_column=_partition_source_column(layer, partition_by),
        sort_columns=sort_columns_by_layer.get(sort_key, ()),
        provisional_files=_positive_int(raw, "provisional_files"),
    )


def load_rebuild_storage_layout(path: Path) -> RebuildStorageLayout:
    manifest_path = Path(path).resolve()
    raw_bytes = manifest_path.read_bytes()
    payload = yaml.safe_load(raw_bytes.decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("rebuild layout manifest must be a mapping")
    schema_version = _required_text(payload, "schema_version")
    if schema_version != REBUILD_LAYOUT_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported rebuild layout schema_version={schema_version!r}; "
            f"expected {REBUILD_LAYOUT_SCHEMA_VERSION!r}"
        )

    scope = _required_mapping(payload, "scope")
    file_policy = _required_mapping(payload, "file_policy")
    partition_contract = _required_mapping(file_policy, "partition_column_contract")
    if _required_text(partition_contract, "column") != REBUILD_YEAR_PARTITION_COLUMN:
        raise ValueError(f"rebuild layout partition column must be {REBUILD_YEAR_PARTITION_COLUMN}")
    if _required_text(partition_contract, "type") != "int":
        raise ValueError("rebuild layout ts_close_year type must be int")

    raw_sort_columns = _required_mapping(file_policy, "sort_within_file")
    sort_columns_by_layer: dict[str, tuple[str, ...]] = {}
    for key in ("raw_bars", "canonical", "research"):
        values = raw_sort_columns.get(key)
        if not isinstance(values, list):
            raise ValueError(f"rebuild layout sort_within_file.{key} must be a list")
        sort_columns_by_layer[key] = tuple(str(item).strip() for item in values)

    raw_tables = payload.get("tables")
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ValueError("rebuild layout tables must be a non-empty list")
    tables = tuple(
        _parse_table(
            _required_mapping({"table": raw}, "table"), sort_columns_by_layer=sort_columns_by_layer
        )
        for raw in raw_tables
    )
    names = [table.name for table in tables]
    paths = [table.relative_path for table in tables]
    if len(names) != len(set(names)):
        raise ValueError("rebuild layout table names must be unique")
    if len(paths) != len(set(paths)):
        raise ValueError("rebuild layout table paths must be unique")
    if any(left in right.parents or right in left.parents for left in paths for right in paths):
        raise ValueError("rebuild layout table paths must not overlap")

    totals = _required_mapping(payload, "totals")
    required_tables = _positive_int(totals, "required_tables")
    provisional_files = _positive_int(totals, "provisional_files")
    if required_tables != len(tables):
        raise ValueError(
            f"rebuild layout required_tables={required_tables} does not match tables={len(tables)}"
        )
    actual_provisional_files = sum(table.provisional_files for table in tables)
    if provisional_files != actual_provisional_files:
        raise ValueError(
            "rebuild layout provisional_files does not match table-level provisional file counts"
        )

    files_per_partition = _positive_int(file_policy, "files_per_nonempty_partition")
    if files_per_partition != 1:
        raise ValueError("rebuild layout strict minimum requires one file per non-empty partition")
    max_compressed_file_mib = _positive_int(file_policy, "max_compressed_file_mib")
    return RebuildStorageLayout(
        schema_version=schema_version,
        manifest_path=manifest_path,
        manifest_sha256=sha256(raw_bytes).hexdigest(),
        rebuild_id=_required_text(scope, "rebuild_id"),
        dataset_version=_required_text(scope, "dataset_version"),
        compute_window_years=_year_sequence(scope, "compute_window_years_provisional"),
        target_window_years=_year_sequence(scope, "target_window_years_provisional"),
        max_compressed_file_bytes=max_compressed_file_mib * 1024 * 1024,
        files_per_nonempty_partition=files_per_partition,
        split_only_when_file_exceeds_limit=_required_bool(
            file_policy, "split_only_when_file_exceeds_limit"
        ),
        no_empty_partitions=_required_bool(file_policy, "no_empty_partitions"),
        tables=tables,
    )


__all__ = [
    "REBUILD_LAYOUT_SCHEMA_VERSION",
    "REBUILD_YEAR_PARTITION_COLUMN",
    "RebuildStorageLayout",
    "RebuildTableLayout",
    "load_rebuild_storage_layout",
]
