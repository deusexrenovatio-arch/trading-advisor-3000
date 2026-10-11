"""Immutable, table-identity-aware snapshots over native Delta versions."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import uuid4

from deltalake import DeltaTable

SCHEMA_VERSION = "data_snapshot.v1"


class SnapshotUnavailableError(RuntimeError):
    """Pinned evidence is missing, expired, or belongs to another table."""


def _canonical(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(payload: object) -> str:
    return hashlib.sha256(_canonical(payload).encode()).hexdigest()


def _path(path: str | Path) -> str:
    return os.path.normcase(str(Path(path).resolve()))


def _file_path(uri: str) -> Path:
    if uri.startswith("file:"):
        parsed = urlparse(uri)
        text = unquote(parsed.path)
        if os.name == "nt" and len(text) > 2 and text[0] == "/" and text[2] == ":":
            text = text[1:]
        if parsed.netloc:
            text = "//" + parsed.netloc + text
        return Path(text)
    return Path(unquote(uri))


def _active_files(table: DeltaTable) -> list[dict[str, object]]:
    """Bind Delta's selected add actions, without scanning Parquet rows."""
    actions = table.get_add_actions()
    paths = actions.column("path").to_pylist()
    sizes = actions.column("size_bytes").to_pylist()
    uris = {str(_file_path(uri).resolve()): _file_path(uri) for uri in table.file_uris()}
    root = _file_path(table.table_uri)
    files = []
    for relative, expected_size in zip(paths, sizes, strict=True):
        file = (root / unquote(relative)).resolve()
        if str(file) not in uris or not file.is_file():
            raise SnapshotUnavailableError(f"snapshot data file missing: {file}")
        if file.stat().st_size != expected_size:
            raise SnapshotUnavailableError(f"snapshot data file size changed: {file}")
        files.append({"path": relative, "size_bytes": expected_size})
    return sorted(files, key=lambda item: str(item["path"]))


def _validate_hex(value: object, length: int, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"invalid {label}")


def _validate_reference(reference: Mapping[str, Any]) -> None:
    path = reference.get("table_path")
    if not isinstance(path, str) or not path.strip() or not Path(path).is_absolute():
        raise ValueError("snapshot requires an absolute local table path")
    version = reference.get("delta_version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ValueError("snapshot Delta version must be a nonnegative integer")
    if not isinstance(reference.get("table_id"), str) or not reference["table_id"].strip():
        raise ValueError("snapshot requires table identity")
    _validate_hex(reference.get("schema_hash"), 64, "snapshot schema hash")
    for key in ("layout_version", "active_files_sha256"):
        if key in reference:
            _validate_hex(reference[key], 64, f"snapshot {key}")
    if "partition_columns" in reference:
        columns = reference["partition_columns"]
        if (
            not isinstance(columns, (list, tuple))
            or any(not isinstance(column, str) or not column for column in columns)
            or len(columns) != len(set(columns))
        ):
            raise ValueError("invalid snapshot partition columns")
    for key in ("row_count", "file_count"):
        if key in reference:
            value = reference[key]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"invalid snapshot {key}")


def _schema_hash(table: DeltaTable) -> str:
    return _hash(json.loads(table.schema().to_json()))


def capture_table(path: Path, *, version: int | None = None) -> dict[str, object]:
    """Capture metadata/statistics only; row transforms remain in Spark/Arrow."""
    if _READS.get() is not None:
        raise ValueError("capture is forbidden inside an accepted snapshot read scope")
    if version is not None and (
        isinstance(version, bool) or not isinstance(version, int) or version < 0
    ):
        raise ValueError("snapshot Delta version must be a nonnegative integer")
    table = DeltaTable(str(path.resolve()), version=version)
    files = _active_files(table)
    actions = table.get_add_actions()
    records = actions.column("num_records") if "num_records" in actions.schema.names else None
    row_count = (
        sum(records.to_pylist())
        if records is not None and not records.null_count
        else table.to_pyarrow_dataset().count_rows()
    )
    partitions = list(table.metadata().partition_columns)
    coverage = None
    flat = table.get_add_actions(flatten=True)
    for column in ("ts", "ts_close", "end_utc"):
        low, high = f"min.{column}", f"max.{column}"
        if low in flat.schema.names and high in flat.schema.names:
            minimum = [value for value in flat.column(low).to_pylist() if value is not None]
            maximum = [value for value in flat.column(high).to_pylist() if value is not None]
            if minimum and maximum:
                coverage = {
                    "column": column,
                    "minimum": str(min(minimum)),
                    "maximum": str(max(maximum)),
                    "source": "delta_file_statistics",
                }
                break
    return {
        "table_path": path.resolve().as_posix(),
        "table_id": table.metadata().id,
        "delta_version": table.version(),
        "schema_hash": _schema_hash(table),
        "layout_version": _hash({"partition_columns": partitions}),
        "partition_columns": partitions,
        "row_count": row_count,
        "file_count": len(files),
        "active_files_sha256": _hash(files),
        "coverage": coverage,
    }


def open_pinned_table(reference: Mapping[str, Any]) -> DeltaTable:
    _validate_reference(reference)
    path = Path(str(reference["table_path"]))
    version = reference["delta_version"]
    try:
        current = DeltaTable(str(path))
        if current.metadata().id != reference["table_id"]:
            raise SnapshotUnavailableError(f"snapshot table identity changed: {path}")
        table = DeltaTable(str(path), version=version)
        if table.version() != version:
            raise SnapshotUnavailableError(f"snapshot exact version unavailable: {path}@{version}")
        if table.metadata().id != reference["table_id"]:
            raise SnapshotUnavailableError(f"snapshot table identity mismatch: {path}")
        if _schema_hash(table) != reference["schema_hash"]:
            raise SnapshotUnavailableError(f"snapshot schema changed: {path}")
        partitions = list(table.metadata().partition_columns)
        if "partition_columns" in reference and list(reference["partition_columns"]) != partitions:
            raise SnapshotUnavailableError(f"snapshot partition layout changed: {path}")
        if "layout_version" in reference and reference["layout_version"] != _hash(
            {"partition_columns": partitions}
        ):
            raise SnapshotUnavailableError(f"snapshot layout identity changed: {path}")
        files = _active_files(table)
        if "file_count" in reference and reference["file_count"] != len(files):
            raise SnapshotUnavailableError(f"snapshot active file count changed: {path}")
        actions = table.get_add_actions()
        if "row_count" in reference and "num_records" in actions.schema.names:
            records = actions.column("num_records")
            if not records.null_count and sum(records.to_pylist()) != reference["row_count"]:
                raise SnapshotUnavailableError(f"snapshot declared row count changed: {path}")
        # Accepted legacy v1 references omit this optional evidence. Never recapture them.
        if "active_files_sha256" in reference and reference["active_files_sha256"] != _hash(files):
            raise SnapshotUnavailableError(f"snapshot active file set changed: {path}")
        return table
    except SnapshotUnavailableError:
        raise
    except Exception as exc:
        raise SnapshotUnavailableError(f"snapshot version unavailable: {path}@{version}") from exc


def assert_tables_unchanged(tables: Mapping[str, Mapping[str, Any]]) -> None:
    for reference in tables.values():
        table = open_pinned_table(reference)
        if DeltaTable(str(reference["table_path"])).version() != table.version():
            raise SnapshotUnavailableError(
                f"snapshot input changed during calculation: {reference['table_path']}"
            )


def create_snapshot(
    *,
    tables: Mapping[str, Mapping[str, Any]],
    required_tables: Sequence[str],
    dataset_version: str,
    run_id: str,
    code_version: str,
    quality_checks: Mapping[str, str],
    parameters: Mapping[str, object] | None = None,
    window: Mapping[str, object] | None = None,
    parents: Sequence[str] = (),
) -> dict[str, Any]:
    if not required_tables or set(required_tables) - set(tables):
        raise ValueError("snapshot is missing required tables")
    if not all((dataset_version.strip(), run_id.strip(), code_version.strip())):
        raise ValueError("snapshot dataset/run/code identity must be nonempty")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": uuid4().hex,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "dataset_version": dataset_version,
        "run_id": run_id,
        "code_version": code_version,
        "parameters": dict(parameters or {}),
        "window": dict(window or {}),
        "parents": list(parents),
        "required_tables": list(required_tables),
        "quality_checks": dict(quality_checks),
        "tables": {name: dict(ref) for name, ref in sorted(tables.items())},
    }
    manifest = {**payload, "manifest_sha256": _hash(payload)}
    _validate(manifest)
    return manifest


def _validate(manifest: Mapping[str, Any]) -> None:
    if not isinstance(manifest, Mapping):
        raise ValueError("snapshot manifest must be an object")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported data snapshot schema")
    payload = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if _hash(payload) != manifest.get("manifest_sha256"):
        raise ValueError("snapshot manifest hash mismatch")
    _validate_hex(manifest.get("snapshot_id"), 32, "snapshot ID")
    for key in ("dataset_version", "run_id", "code_version"):
        if not isinstance(manifest.get(key), str) or not manifest[key].strip():
            raise ValueError(f"snapshot requires {key}")
    required = manifest.get("required_tables")
    tables = manifest.get("tables")
    if (
        not isinstance(tables, Mapping)
        or not tables
        or any(
            not isinstance(name, str) or not name or not isinstance(ref, Mapping)
            for name, ref in tables.items()
        )
    ):
        raise ValueError("invalid snapshot tables")
    if (
        not isinstance(required, list)
        or not required
        or any(not isinstance(name, str) or not name for name in required)
        or len(set(required)) != len(required)
        or set(required) - set(tables)
    ):
        raise ValueError("snapshot missing required tables")
    if not isinstance(manifest.get("quality_checks"), Mapping) or any(
        not isinstance(name, str) or not name or not isinstance(status, str)
        for name, status in manifest["quality_checks"].items()
    ):
        raise ValueError("invalid snapshot quality checks")
    for ref in tables.values():
        _validate_reference(ref)
    paths = [_path(ref["table_path"]) for ref in tables.values()]
    if len(paths) != len(set(paths)):
        raise ValueError("snapshot contains duplicate table paths")


def _unlink_owned(path: Path, primary_error: BaseException | None) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as cleanup_error:
        if primary_error is None:
            raise
        primary_error.add_note(f"cleanup failed for {path}: {cleanup_error}")


def atomic_json(path: Path, payload: Mapping[str, Any], *, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = _canonical(payload) + "\n"
    # Repeating a 64-character checkpoint name can exceed Windows path limits.
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    primary_error = None
    owned = False
    try:
        stream = temporary.open("x", encoding="utf-8", newline="\n")
        owned = True
        with stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if immutable:
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_text(encoding="utf-8") != content:
                    raise ValueError(
                        "immutable snapshot already exists with different contents"
                    ) from None
        else:
            os.replace(temporary, path)
    except BaseException as error:
        primary_error = error
        raise
    finally:
        if owned:
            _unlink_owned(temporary, primary_error)


def save_snapshot(root: Path, manifest: Mapping[str, Any]) -> Path:
    _validate(manifest)
    path = root / "manifests" / f"{manifest['snapshot_id']}.json"
    atomic_json(path, manifest, immutable=True)
    return path


def load_snapshot(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    _validate(manifest)
    return manifest


def current_snapshot(root: Path) -> dict[str, Any] | None:
    pointer = root / "published_current.json"
    if not pointer.exists():
        return None
    publication = json.loads(pointer.read_text(encoding="utf-8"))
    if publication.get("schema_version") != "data_snapshot_publication.v1":
        raise ValueError("unsupported published snapshot schema")
    identifier = publication.get("snapshot_id")
    _validate_hex(identifier, 32, "published snapshot ID")
    manifest = load_snapshot(root / "manifests" / f"{identifier}.json")
    if manifest["snapshot_id"] != identifier:
        raise ValueError("published snapshot manifest ID mismatch")
    if publication["manifest_sha256"] != manifest["manifest_sha256"]:
        raise ValueError("published snapshot hash mismatch")
    return manifest


def publish_snapshot(root: Path, path: Path, *, expected_current_id: str | None) -> None:
    manifest = load_snapshot(path)
    if path.resolve() != (root / "manifests" / f"{manifest['snapshot_id']}.json").resolve():
        raise ValueError("publication requires a manifest saved in the snapshot registry")
    checks = manifest["quality_checks"]
    if not checks or any(status not in {"PASS", "PASS-NOOP"} for status in checks.values()):
        raise ValueError("snapshot publication requires PASS QC")
    if expected_current_id is not None:
        _validate_hex(expected_current_id, 32, "expected current snapshot ID")
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".publication.lock"
    try:
        stream = lock.open("x")
    except FileExistsError:
        raise ValueError("snapshot publication already running") from None
    primary_error = None
    try:
        with stream:
            stream.write(str(os.getpid()))
        locked_manifest = load_snapshot(path)
        if locked_manifest["manifest_sha256"] != manifest["manifest_sha256"]:
            raise ValueError("snapshot manifest changed before publication")
        manifest = locked_manifest
        current = current_snapshot(root)
        actual = current["snapshot_id"] if current else None
        if actual != expected_current_id:
            raise ValueError("snapshot publication changed; retry against the accepted snapshot")
        for reference in manifest["tables"].values():
            open_pinned_table(reference)
        atomic_json(
            root / "published_current.json",
            {
                "schema_version": "data_snapshot_publication.v1",
                "snapshot_id": manifest["snapshot_id"],
                "manifest_sha256": manifest["manifest_sha256"],
            },
        )
    except BaseException as error:
        primary_error = error
        raise
    finally:
        _unlink_owned(lock, primary_error)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class SnapshotReads:
    tables: Mapping[str, Mapping[str, Any]]

    def reference(self, path: Path) -> dict[str, Any]:
        key = _path(path)
        matches = [ref for ref in self.tables.values() if _path(ref["table_path"]) == key]
        if matches:
            return _thaw(matches[0])
        raise SnapshotUnavailableError(f"table not pinned in snapshot: {path}")


_READS: ContextVar[SnapshotReads | None] = ContextVar("data_snapshot_reads", default=None)


@contextmanager
def snapshot_read_scope(
    manifest: Mapping[str, Any],
) -> Iterator[SnapshotReads]:
    _validate(manifest)
    tables = {name: _freeze(deepcopy(dict(ref))) for name, ref in manifest["tables"].items()}
    scope = SnapshotReads(tables=MappingProxyType(tables))
    token = _READS.set(scope)
    try:
        yield scope
    finally:
        _READS.reset(token)


def snapshot_table_reference(path: Path) -> dict[str, Any] | None:
    scope = _READS.get()
    return scope.reference(path) if scope is not None else None


def open_read_table(
    path: Path, *, unscoped_open: Callable[[str], DeltaTable] = DeltaTable
) -> DeltaTable:
    reference = snapshot_table_reference(path)
    return open_pinned_table(reference) if reference is not None else unscoped_open(str(path))


def read_spark_delta(spark: Any, path: str | Path) -> Any:
    reference = snapshot_table_reference(Path(path))
    reader = spark.read.format("delta")
    if reference is not None:
        open_pinned_table(reference)
        reader = reader.option("versionAsOf", reference["delta_version"])
    return reader.load(reference["table_path"] if reference is not None else str(path))
