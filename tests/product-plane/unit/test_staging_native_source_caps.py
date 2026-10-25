"""Synthetic Delta metadata only; no Spark work or acceptance proof."""

from pathlib import PurePosixPath

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common import staging_execution, staging_source_scope
from trading_advisor_3000.product_plane.common.data_snapshots import capture_table
from trading_advisor_3000.product_plane.data_plane.rebuild_layout import (
    RebuildStorageLayout,
    RebuildTableLayout,
)
from trading_advisor_3000.spark_jobs import moex_contract_economics_job as economics
from trading_advisor_3000.spark_jobs import rebuild_storage_layout_job as rebuild


def _delta(path, values=(1,)):
    write_deltalake(str(path), pa.table({"value": values}), mode="overwrite")
    return sum(DeltaTable(str(path)).get_add_actions().column("size_bytes").to_pylist())


def _no_spark(*args, **kwargs):
    pytest.fail("source cap did not stop work before Spark session")


@pytest.fixture
def cap(monkeypatch):
    plan = {"scope": {"max_source_bytes": 0}}
    # Quota/run identity has its own tests; retain the real native byte-cap owner.
    monkeypatch.setattr(staging_execution, "require_staging_compute", lambda **kwargs: None)
    monkeypatch.setattr(staging_source_scope, "_plan", lambda output: plan)
    return plan["scope"]


def _economics_call(source, output, **kwargs):
    economics.run_moex_contract_economics_spark_job(
        raw_contract_specs_path=source,
        raw_fx_rates_path=source,
        raw_rms_limits_path=source,
        raw_rms_staticparams_path=source,
        output_dir=output,
        spark_session_factory=_no_spark,
        **kwargs,
    )


@pytest.mark.parametrize(
    "name",
    ("canonical_fx_rates", "canonical_asset_risk_parameters", "canonical_contract_economics"),
)
def test_economics_counts_each_existing_merge_target_before_spark(tmp_path, cap, name):
    source, output = tmp_path / "raw.delta", tmp_path / "canonical"
    raw_size = _delta(source)
    target_size = _delta(output / f"{name}.delta")
    cap["max_source_bytes"] = raw_size + target_size - 1
    with pytest.raises(staging_execution.StagingExecutionError, match="source bytes"):
        _economics_call(source, output)


def test_economics_counts_previous_pinned_version_not_only_current(tmp_path, cap):
    source, output = tmp_path / "raw.delta", tmp_path / "canonical"
    raw_size = _delta(source)
    previous_path = output / "canonical_contract_economics.delta"
    old_size = _delta(previous_path, [f"{i:064x}" * 4 for i in range(500)])
    previous = capture_table(previous_path)
    current_size = _delta(previous_path, ["small"])
    assert old_size > current_size
    cap["max_source_bytes"] = raw_size + current_size
    with pytest.raises(staging_execution.StagingExecutionError, match="source bytes"):
        _economics_call(source, output, previous_economics_reference=previous)


def test_rebuild_counts_all_manifest_sources_before_spark(tmp_path, monkeypatch, cap):
    source = tmp_path / "source"
    table_paths = ("canonical/a.delta", "research/b.delta")
    sizes = [_delta(source / relative) for relative in table_paths]
    tables = tuple(
        RebuildTableLayout(
            name=str(index),
            layer=relative.split("/")[0],
            relative_path=PurePosixPath(relative),
            scope="compute_window",
            required=True,
            partition_by=(),
            partition_source_column=None,
            sort_columns=(),
            provisional_files=1,
        )
        for index, relative in enumerate(table_paths)
    )
    layout = RebuildStorageLayout(
        schema_version="ta3000_table_layout.v1",
        manifest_path=tmp_path / "layout.yaml",
        manifest_sha256="a" * 64,
        rebuild_id="fixture",
        dataset_version="fixture",
        compute_window_years=(2026,),
        target_window_years=(2026,),
        max_compressed_file_bytes=1024,
        files_per_nonempty_partition=1,
        split_only_when_file_exceeds_limit=True,
        no_empty_partitions=True,
        tables=tables,
    )
    monkeypatch.setattr(rebuild, "load_rebuild_storage_layout", lambda path: layout)
    cap["max_source_bytes"] = sum(sizes) - 1
    assert all(size < cap["max_source_bytes"] for size in sizes)
    with pytest.raises(staging_execution.StagingExecutionError, match="source bytes"):
        rebuild.run_rebuild_storage_layout_spark_job(
            layout_manifest_path=layout.manifest_path,
            source_root=source,
            target_root=tmp_path / "target",
            report_path=tmp_path / "report.json",
            run_id="metadata-fixture",
            published_current_root=tmp_path / "published",
            spark_session_factory=_no_spark,
        )
