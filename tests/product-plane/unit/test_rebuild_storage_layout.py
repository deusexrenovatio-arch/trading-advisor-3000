from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest
import yaml


def _layout_manifest(path: Path) -> Path:
    payload = {
        "schema_version": "ta3000_table_layout.v1",
        "status": "planned_requires_implementation",
        "scope": {
            "rebuild_id": "test-rebuild",
            "dataset_version": "test-dataset",
            "compute_window_years_provisional": [2025, 2026],
            "target_window_years_provisional": [2025, 2026],
        },
        "file_policy": {
            "max_compressed_file_mib": 512,
            "files_per_nonempty_partition": 1,
            "split_only_when_file_exceeds_limit": True,
            "no_empty_partitions": True,
            "partition_year_semantics": "year_of_utc_bar_close",
            "partition_column_contract": {
                "column": "ts_close_year",
                "type": "int",
            },
            "physical_vs_logical": {
                "physical_partitioning": "governed_by_this_layout",
                "compute_and_replace_keys": "remain_table_business_keys",
            },
            "sort_within_file": {
                "raw_bars": ["timeframe", "internal_id", "moex_secid", "ts_close"],
                "canonical": ["timeframe", "instrument_id", "contract_id", "ts"],
                "research": ["contour_id", "timeframe", "instrument_id", "ts"],
            },
        },
        "totals": {
            "required_tables": 2,
            "provisional_files": 3,
        },
        "tables": [
            {
                "name": "canonical_bars",
                "layer": "canonical",
                "path": "canonical/moex/canonical_bars.delta",
                "scope": "compute_window",
                "required": True,
                "partition_by": ["ts_close_year"],
                "provisional_files": 2,
            },
            {
                "name": "canonical_session_calendar",
                "layer": "canonical",
                "path": "canonical/moex/canonical_session_calendar.delta",
                "scope": "compute_window",
                "required": True,
                "partition_by": [],
                "provisional_files": 1,
            },
        ],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _load_layout_builder():
    data_plane = importlib.import_module("trading_advisor_3000.product_plane.data_plane")
    builder = getattr(data_plane, "load_rebuild_storage_layout", None)
    assert callable(builder), "data plane must expose the governed rebuild layout loader"
    return builder


def _load_layout_runner():
    spark_jobs = importlib.import_module("trading_advisor_3000.spark_jobs")
    runner = getattr(spark_jobs, "finalize_rebuild_storage_layout", None)
    assert callable(runner), "Spark jobs must expose the isolated rebuild layout finalizer"
    return runner


def test_rebuild_layout_loader_separates_physical_layout_from_business_keys(
    tmp_path: Path,
) -> None:
    layout = _load_layout_builder()(_layout_manifest(tmp_path / "layout.yaml"))

    bars = layout.table("canonical_bars")
    calendar = layout.table("canonical_session_calendar")

    assert bars.partition_by == ("ts_close_year",)
    assert bars.partition_source_column == "ts"
    assert bars.sort_columns == ("timeframe", "instrument_id", "contract_id", "ts")
    assert calendar.partition_by == ()
    assert layout.files_per_nonempty_partition == 1
    assert layout.max_compressed_file_bytes == 512 * 1024 * 1024
    assert layout.table_path(tmp_path / "root", "canonical_bars") == (
        tmp_path / "root" / "canonical" / "moex" / "canonical_bars.delta"
    )


def test_rebuild_layout_loader_rejects_non_year_and_unsafe_paths(tmp_path: Path) -> None:
    manifest_path = _layout_manifest(tmp_path / "layout.yaml")
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    payload["tables"][0]["partition_by"] = ["ts_close_month"]
    payload["tables"][1]["path"] = "../published-current.delta"
    manifest_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="ts_close_year|relative table path"):
        _load_layout_builder()(manifest_path)


def test_rebuild_layout_loader_rejects_string_boolean_policy_values(tmp_path: Path) -> None:
    manifest_path = _layout_manifest(tmp_path / "layout.yaml")
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    payload["file_policy"]["no_empty_partitions"] = "false"
    manifest_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="no_empty_partitions must be a boolean"):
        _load_layout_builder()(manifest_path)


class _RecordingMaterializer:
    def __init__(
        self,
        *,
        oversize: bool = False,
        proven_size_split: bool = False,
        excessive_size_split: bool = False,
    ) -> None:
        self.oversize = oversize
        self.proven_size_split = proven_size_split
        self.excessive_size_split = excessive_size_split
        self.calls: list[tuple[Path, Path, str]] = []

    def materialize(self, *, source_path: Path, staged_path: Path, table) -> dict[str, object]:
        self.calls.append((source_path, staged_path, table.name))
        staged_path.mkdir(parents=True)
        (staged_path / "_delta_log").mkdir()
        if table.partition_by:
            files_by_partition = {
                "ts_close_year=2025": (
                    3 if self.excessive_size_split else 2 if self.proven_size_split else 1
                ),
                "ts_close_year=2026": 1,
            }
            parquet_files = sum(files_by_partition.values())
            partition_columns = ["ts_close_year"]
            row_count = 4
        else:
            files_by_partition = {"__unpartitioned__": 1}
            parquet_files = 1
            partition_columns = []
            row_count = 2
        max_file_bytes = 513 * 1024 * 1024 if self.oversize else 1024
        proof = {
            "status": "PASS",
            "row_count": row_count,
            "partition_columns": partition_columns,
            "parquet_files": parquet_files,
            "files_by_partition": files_by_partition,
            "max_file_bytes": max_file_bytes,
            "delta_log": True,
        }
        if table.partition_by and (self.proven_size_split or self.excessive_size_split):
            proof["size_split_partitions"] = {
                "ts_close_year=2025": {
                    "original_max_file_bytes": 600 * 1024 * 1024,
                    "required_files": 3 if self.excessive_size_split else 2,
                }
            }
        return proof


def _source_tables(root: Path) -> None:
    for relative in (
        Path("canonical/moex/canonical_bars.delta"),
        Path("canonical/moex/canonical_session_calendar.delta"),
    ):
        (root / relative / "_delta_log").mkdir(parents=True)


def test_layout_finalizer_materializes_every_table_into_isolated_root(tmp_path: Path) -> None:
    layout = _load_layout_builder()(_layout_manifest(tmp_path / "layout.yaml"))
    source_root = tmp_path / "compute"
    target_root = tmp_path / "final"
    report_path = tmp_path / "reports" / "layout.json"
    _source_tables(source_root)
    materializer = _RecordingMaterializer()

    report = _load_layout_runner()(
        layout=layout,
        source_root=source_root,
        target_root=target_root,
        report_path=report_path,
        run_id="layout-test",
        materializer=materializer,
    )

    assert report["status"] == "PASS"
    assert report["table_count"] == 2
    assert report["row_count"] == 6
    assert report["parquet_file_count"] == 3
    assert [call[2] for call in materializer.calls] == [
        "canonical_bars",
        "canonical_session_calendar",
    ]
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["status"] == "PASS"
    assert persisted["report_path"] == report_path.resolve().as_posix()


def test_layout_finalizer_blocks_overlap_and_oversized_files(tmp_path: Path) -> None:
    layout = _load_layout_builder()(_layout_manifest(tmp_path / "layout.yaml"))
    source_root = tmp_path / "compute"
    _source_tables(source_root)
    runner = _load_layout_runner()

    with pytest.raises(ValueError, match="different roots"):
        runner(
            layout=layout,
            source_root=source_root,
            target_root=source_root,
            report_path=tmp_path / "overlap.json",
            run_id="layout-test",
            materializer=_RecordingMaterializer(),
        )

    with pytest.raises(RuntimeError, match="512 MiB"):
        runner(
            layout=layout,
            source_root=source_root,
            target_root=tmp_path / "final",
            report_path=tmp_path / "oversize.json",
            run_id="layout-test",
            materializer=_RecordingMaterializer(oversize=True),
        )


def test_layout_finalizer_rejects_any_published_current_overlap(tmp_path: Path) -> None:
    layout = _load_layout_builder()(_layout_manifest(tmp_path / "layout.yaml"))
    source_root = tmp_path / "compute"
    published_current = tmp_path / "published-current"
    published_current.mkdir()
    _source_tables(source_root)
    runner = _load_layout_runner()

    with pytest.raises(ValueError, match="target_root.*published current"):
        runner(
            layout=layout,
            source_root=source_root,
            target_root=published_current / "new-rebuild",
            report_path=tmp_path / "forbidden-root.json",
            run_id="layout-test",
            materializer=_RecordingMaterializer(),
            published_current_roots=(published_current,),
        )

    with pytest.raises(ValueError, match="source_root.*published current"):
        runner(
            layout=layout,
            source_root=published_current,
            target_root=tmp_path / "final-from-current",
            report_path=tmp_path / "forbidden-source.json",
            run_id="layout-test",
            materializer=_RecordingMaterializer(),
            published_current_roots=(published_current,),
        )

    with pytest.raises(ValueError, match="report_path.*published current"):
        runner(
            layout=layout,
            source_root=source_root,
            target_root=tmp_path / "final-with-current-report",
            report_path=published_current / "forbidden-report.json",
            run_id="layout-test",
            materializer=_RecordingMaterializer(),
            published_current_roots=(published_current,),
        )


def test_layout_finalizer_allows_only_proven_size_driven_extra_files(tmp_path: Path) -> None:
    layout = _load_layout_builder()(_layout_manifest(tmp_path / "layout.yaml"))
    source_root = tmp_path / "compute"
    _source_tables(source_root)

    report = _load_layout_runner()(
        layout=layout,
        source_root=source_root,
        target_root=tmp_path / "final",
        report_path=tmp_path / "size-split.json",
        run_id="layout-test",
        materializer=_RecordingMaterializer(proven_size_split=True),
    )

    assert report["status"] == "PASS"
    assert report["parquet_file_count"] == 4


def test_layout_finalizer_rejects_more_than_minimum_size_split(tmp_path: Path) -> None:
    layout = _load_layout_builder()(_layout_manifest(tmp_path / "layout.yaml"))
    source_root = tmp_path / "compute"
    _source_tables(source_root)

    with pytest.raises(RuntimeError, match="strict_minimum_files"):
        _load_layout_runner()(
            layout=layout,
            source_root=source_root,
            target_root=tmp_path / "final",
            report_path=tmp_path / "excessive-size-split.json",
            run_id="layout-test",
            materializer=_RecordingMaterializer(excessive_size_split=True),
        )


def test_layout_finalizer_has_governed_dagster_job_binding(tmp_path: Path) -> None:
    dagster_defs = importlib.import_module("trading_advisor_3000.dagster_defs")
    build_config = getattr(dagster_defs, "build_rebuild_storage_layout_run_config", None)
    assert callable(build_config), "Dagster must expose the rebuild layout run-config builder"

    config = build_config(
        layout_manifest_path=tmp_path / "layout.yaml",
        source_root=tmp_path / "compute",
        target_root=tmp_path / "final",
        published_current_root=tmp_path / "published-current",
        report_path=tmp_path / "layout-report.json",
        run_id="layout-test",
        spark_master="local[2]",
    )
    assert config["ops"]["rebuild_storage_layout"]["config"]["run_id"] == "layout-test"

    product_defs = importlib.import_module(
        "trading_advisor_3000.dagster_defs.product_plane_definitions"
    ).product_plane_definitions
    job = product_defs.get_repository_def().get_job("rebuild_storage_layout_job")
    assert set(job.graph.node_dict) == {"rebuild_storage_layout"}
