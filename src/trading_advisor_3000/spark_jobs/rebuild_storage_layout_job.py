from __future__ import annotations

import json
import math
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from deltalake import DeltaTable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log
from trading_advisor_3000.product_plane.data_plane.rebuild_layout import (
    RebuildStorageLayout,
    RebuildTableLayout,
    load_rebuild_storage_layout,
)

from .canonical_bars_job import DEFAULT_SPARK_MASTER, _create_spark_session


class RebuildTableMaterializer(Protocol):
    def materialize(
        self,
        *,
        source_path: Path,
        staged_path: Path,
        table: RebuildTableLayout,
    ) -> dict[str, object]: ...


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    resolved = path.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(resolved)
    return resolved


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _ensure_distinct_roots(source_root: Path, target_root: Path) -> tuple[Path, Path]:
    source = source_root.resolve()
    target = target_root.resolve()
    if source == target or _is_relative_to(source, target) or _is_relative_to(target, source):
        raise ValueError("rebuild layout source_root and target_root must be different roots")
    return source, target


def _ensure_outside_published_current(
    path: Path,
    published_current_roots: tuple[Path, ...],
    *,
    label: str,
) -> tuple[Path, ...]:
    resolved_path = Path(path).resolve()
    resolved_roots = tuple(Path(root).resolve() for root in published_current_roots)
    for root in resolved_roots:
        if (
            resolved_path == root
            or _is_relative_to(resolved_path, root)
            or _is_relative_to(root, resolved_path)
        ):
            raise ValueError(
                f"rebuild layout {label} must stay outside published current: "
                f"{resolved_path.as_posix()}"
            )
    return resolved_roots


def _proof_blockers(
    *,
    layout: RebuildStorageLayout,
    table: RebuildTableLayout,
    proof: dict[str, object],
) -> list[str]:
    blockers: list[str] = []
    if str(proof.get("status") or "") != "PASS":
        blockers.append("materializer_status")
    if not bool(proof.get("delta_log")):
        blockers.append("missing_delta_log")
    actual_partitions = tuple(str(item) for item in list(proof.get("partition_columns") or []))
    if actual_partitions != table.partition_by:
        blockers.append("partition_columns")
    row_count = int(proof.get("row_count") or 0)
    parquet_files = int(proof.get("parquet_files") or 0)
    if row_count > 0 and parquet_files <= 0:
        blockers.append("missing_parquet_files")
    files_by_partition = proof.get("files_by_partition")
    if not isinstance(files_by_partition, dict):
        blockers.append("files_by_partition")
    elif row_count > 0:
        raw_size_splits = proof.get("size_split_partitions")
        size_splits = raw_size_splits if isinstance(raw_size_splits, dict) else {}
        invalid_counts = {
            str(key): int(value)
            for key, value in files_by_partition.items()
            if (
                int(value) != layout.files_per_nonempty_partition
                and not (
                    layout.split_only_when_file_exceeds_limit
                    and isinstance(size_splits.get(str(key)), dict)
                    and int(size_splits[str(key)].get("original_max_file_bytes") or 0)
                    > layout.max_compressed_file_bytes
                    and int(size_splits[str(key)].get("required_files") or 0) == int(value)
                    and int(size_splits[str(key)].get("required_files") or 0)
                    == math.ceil(
                        int(size_splits[str(key)].get("original_max_file_bytes") or 0)
                        / layout.max_compressed_file_bytes
                    )
                )
            )
        }
        if invalid_counts:
            blockers.append("strict_minimum_files")
    max_file_bytes = int(proof.get("max_file_bytes") or 0)
    if max_file_bytes > layout.max_compressed_file_bytes:
        blockers.append("max_compressed_file_size")
    return blockers


def finalize_rebuild_storage_layout(
    *,
    layout: RebuildStorageLayout,
    source_root: Path,
    target_root: Path,
    report_path: Path,
    run_id: str,
    materializer: RebuildTableMaterializer,
    published_current_roots: tuple[Path, ...] = (),
) -> dict[str, object]:
    resolved_run_id = run_id.strip()
    if not resolved_run_id:
        raise ValueError("rebuild layout run_id must be non-empty")
    source, target = _ensure_distinct_roots(source_root, target_root)
    current_roots = _ensure_outside_published_current(
        source, published_current_roots, label="source_root"
    )
    _ensure_outside_published_current(target, current_roots, label="target_root")
    _ensure_outside_published_current(report_path, current_roots, label="report_path")
    if target.exists():
        raise FileExistsError(
            "rebuild layout target_root already exists; use a fresh isolated root: "
            f"{target.as_posix()}"
        )

    table_reports: list[dict[str, object]] = []
    blockers: list[str] = []
    for table in layout.tables:
        source_path = layout.table_path(source, table.name)
        staged_path = layout.table_path(target, table.name)
        if not has_delta_log(source_path):
            table_blockers = ["missing_source_delta_log"]
            proof: dict[str, object] = {
                "status": "BLOCKED",
                "row_count": 0,
                "parquet_files": 0,
                "partition_columns": [],
                "files_by_partition": {},
                "max_file_bytes": 0,
                "delta_log": False,
            }
        else:
            proof = dict(
                materializer.materialize(
                    source_path=source_path,
                    staged_path=staged_path,
                    table=table,
                )
            )
            table_blockers = _proof_blockers(layout=layout, table=table, proof=proof)
        table_status = "PASS" if not table_blockers else "BLOCKED"
        if table_blockers:
            blockers.extend(f"{table.name}:{item}" for item in table_blockers)
        table_reports.append(
            {
                "name": table.name,
                "layer": table.layer,
                "source_path": source_path.as_posix(),
                "target_path": staged_path.as_posix(),
                "expected": {
                    "partition_by": list(table.partition_by),
                    "partition_source_column": table.partition_source_column,
                    "sort_columns": list(table.sort_columns),
                    "files_per_nonempty_partition": layout.files_per_nonempty_partition,
                    "max_compressed_file_bytes": layout.max_compressed_file_bytes,
                },
                "proof": proof,
                "blockers": table_blockers,
                "status": table_status,
            }
        )

    report: dict[str, object] = {
        "schema_version": "ta3000_rebuild_storage_layout_report.v1",
        "status": "PASS" if not blockers else "BLOCKED",
        "run_id": resolved_run_id,
        "rebuild_id": layout.rebuild_id,
        "dataset_version": layout.dataset_version,
        "runtime_owner": "spark_delta",
        "operation": "isolated_compute_to_final_layout",
        "source_root": source.as_posix(),
        "target_root": target.as_posix(),
        "published_current_roots": [path.as_posix() for path in current_roots],
        "layout_manifest_path": layout.manifest_path.as_posix(),
        "layout_manifest_sha256": layout.manifest_sha256,
        "table_count": len(table_reports),
        "row_count": sum(int(item["proof"].get("row_count") or 0) for item in table_reports),
        "parquet_file_count": sum(
            int(item["proof"].get("parquet_files") or 0) for item in table_reports
        ),
        "tables": table_reports,
        "blockers": blockers,
        "current_mutation": False,
        "built_at_utc": _utc_now_iso(),
    }
    report["report_path"] = report_path.resolve().as_posix()
    _write_json(report_path, report)
    if blockers:
        if any(item.endswith(":max_compressed_file_size") for item in blockers):
            limit_mib = layout.max_compressed_file_bytes // (1024 * 1024)
            raise RuntimeError(
                f"rebuild layout blocked: a physical file exceeds {limit_mib} MiB; "
                "split that year before retry"
            )
        raise RuntimeError(f"rebuild layout blocked: {', '.join(blockers)}")
    return report


def _parquet_file_profile(table_path: Path) -> dict[str, object]:
    files = sorted(
        path
        for path in table_path.rglob("*.parquet")
        if "_delta_log" not in path.relative_to(table_path).parts
    )
    files_by_partition: dict[str, int] = {}
    max_file_bytes_by_partition: dict[str, int] = {}
    for path in files:
        relative_parent = path.parent.relative_to(table_path).as_posix()
        partition = relative_parent if relative_parent != "." else "__unpartitioned__"
        files_by_partition[partition] = files_by_partition.get(partition, 0) + 1
        max_file_bytes_by_partition[partition] = max(
            max_file_bytes_by_partition.get(partition, 0),
            path.stat().st_size,
        )
    sizes = [path.stat().st_size for path in files]
    return {
        "parquet_files": len(files),
        "files_by_partition": files_by_partition,
        "max_file_bytes_by_partition": max_file_bytes_by_partition,
        "total_bytes": sum(sizes),
        "max_file_bytes": max(sizes, default=0),
    }


class SparkDeltaTableMaterializer:
    def __init__(self, spark: object, *, max_compressed_file_bytes: int) -> None:
        self._spark = spark
        self._max_compressed_file_bytes = max_compressed_file_bytes

    @staticmethod
    def _partition_key(table: RebuildTableLayout, value: object | None = None) -> str:
        if not table.partition_by:
            return "__unpartitioned__"
        return f"{table.partition_by[0]}={value}"

    def _write(
        self,
        *,
        dataframe,
        staged_path: Path,
        table: RebuildTableLayout,
        mode: str,
        max_rows_per_file: int | None = None,
    ) -> None:
        writer = dataframe.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if max_rows_per_file is not None:
            writer = writer.option("maxRecordsPerFile", str(max_rows_per_file))
        if table.partition_by:
            writer = writer.partitionBy(*table.partition_by)
        writer.save(str(staged_path))

    def _rewrite_with_size_splits(
        self,
        *,
        source,
        staged_path: Path,
        table: RebuildTableLayout,
        sort_columns: list[str],
        initial_profile: dict[str, object],
    ) -> tuple[dict[str, object], dict[str, dict[str, int]]]:
        from pyspark.sql import functions as F  # type: ignore[import-not-found]

        if table.partition_by:
            partition_column = table.partition_by[0]
            partition_rows = {
                self._partition_key(table, row[partition_column]): int(row["count"])
                for row in source.groupBy(partition_column).count().collect()
            }
        else:
            partition_rows = {"__unpartitioned__": int(source.count())}
        original_max_by_partition = {
            str(key): int(value)
            for key, value in dict(initial_profile.get("max_file_bytes_by_partition") or {}).items()
        }
        required_files = {
            key: max(1, math.ceil(size / self._max_compressed_file_bytes))
            for key, size in original_max_by_partition.items()
        }

        profile = initial_profile
        for _ in range(3):
            if staged_path.exists():
                shutil.rmtree(staged_path)
            first_write = True
            for partition_key in sorted(partition_rows):
                row_count = partition_rows[partition_key]
                if table.partition_by:
                    raw_value = partition_key.split("=", 1)[1]
                    frame = source.where(F.col(table.partition_by[0]) == F.lit(int(raw_value)))
                else:
                    frame = source
                prepared = frame.coalesce(1)
                if sort_columns:
                    prepared = prepared.sortWithinPartitions(*sort_columns)
                file_count = required_files.get(partition_key, 1)
                max_rows_per_file = max(1, math.ceil(row_count / file_count))
                self._write(
                    dataframe=prepared,
                    staged_path=staged_path,
                    table=table,
                    mode="overwrite" if first_write else "append",
                    max_rows_per_file=max_rows_per_file,
                )
                first_write = False
            profile = _parquet_file_profile(staged_path)
            max_by_partition = {
                str(key): int(value)
                for key, value in dict(profile.get("max_file_bytes_by_partition") or {}).items()
            }
            oversized = {
                key: size
                for key, size in max_by_partition.items()
                if size > self._max_compressed_file_bytes
            }
            if not oversized:
                break
            for key, size in oversized.items():
                current = required_files.get(key, 1)
                required_files[key] = max(
                    current + 1,
                    math.ceil(current * size / self._max_compressed_file_bytes),
                )

        size_split_partitions = {
            key: {
                "original_max_file_bytes": original_max_by_partition[key],
                "required_files": file_count,
            }
            for key, file_count in required_files.items()
            if file_count > 1
        }
        return profile, size_split_partitions

    def materialize(
        self,
        *,
        source_path: Path,
        staged_path: Path,
        table: RebuildTableLayout,
    ) -> dict[str, object]:
        from pyspark.sql import functions as F  # type: ignore[import-not-found]

        source = self._spark.read.format("delta").load(str(source_path))
        source_columns = set(source.columns)
        if table.partition_source_column is not None:
            if table.partition_source_column not in source_columns:
                raise RuntimeError(
                    f"{table.name} missing partition source column {table.partition_source_column}"
                )
            if source.where(F.col(table.partition_source_column).isNull()).limit(1).count():
                raise RuntimeError(
                    f"{table.name} contains null {table.partition_source_column}; "
                    "year partition would be ambiguous"
                )
            source = source.withColumn(
                "ts_close_year",
                F.year(F.col(table.partition_source_column).cast("timestamp")).cast("int"),
            )

        source_row_count = int(source.count())
        sort_columns = [column for column in table.sort_columns if column in source.columns]
        if table.partition_by:
            prepared = source.repartition(*table.partition_by)
        else:
            prepared = source.coalesce(1)
        if sort_columns:
            prepared = prepared.sortWithinPartitions(*sort_columns)

        staged_path.parent.mkdir(parents=True, exist_ok=True)
        self._write(
            dataframe=prepared,
            staged_path=staged_path,
            table=table,
            mode="overwrite",
        )

        file_profile = _parquet_file_profile(staged_path)
        size_split_partitions: dict[str, dict[str, int]] = {}
        if int(file_profile["max_file_bytes"]) > self._max_compressed_file_bytes:
            file_profile, size_split_partitions = self._rewrite_with_size_splits(
                source=source,
                staged_path=staged_path,
                table=table,
                sort_columns=sort_columns,
                initial_profile=file_profile,
            )
        staged_row_count = int(self._spark.read.format("delta").load(str(staged_path)).count())
        partition_columns = tuple(
            str(item) for item in DeltaTable(str(staged_path)).metadata().partition_columns
        )
        return {
            "status": (
                "PASS"
                if source_row_count == staged_row_count
                and partition_columns == table.partition_by
                and has_delta_log(staged_path)
                else "BLOCKED"
            ),
            "source_row_count": source_row_count,
            "row_count": staged_row_count,
            "partition_columns": list(partition_columns),
            "delta_log": has_delta_log(staged_path),
            "size_split_partitions": size_split_partitions,
            **file_profile,
        }


def run_rebuild_storage_layout_spark_job(
    *,
    layout_manifest_path: Path,
    source_root: Path,
    target_root: Path,
    report_path: Path,
    run_id: str,
    published_current_root: Path,
    spark_master: str = DEFAULT_SPARK_MASTER,
    spark_session_factory=None,
) -> dict[str, object]:
    layout = load_rebuild_storage_layout(layout_manifest_path)
    factory = spark_session_factory or _create_spark_session
    spark = factory("ta3000-rebuild-storage-layout", spark_master)
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        return finalize_rebuild_storage_layout(
            layout=layout,
            source_root=source_root,
            target_root=target_root,
            report_path=report_path,
            run_id=run_id,
            materializer=SparkDeltaTableMaterializer(
                spark,
                max_compressed_file_bytes=layout.max_compressed_file_bytes,
            ),
            published_current_roots=(published_current_root,),
        )
    finally:
        try:
            spark.stop()
        except Exception:
            pass


__all__ = [
    "RebuildTableMaterializer",
    "SparkDeltaTableMaterializer",
    "finalize_rebuild_storage_layout",
    "run_rebuild_storage_layout_spark_job",
]
