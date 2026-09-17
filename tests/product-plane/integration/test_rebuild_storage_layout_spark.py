from __future__ import annotations

from pathlib import Path, PurePosixPath

import pytest
from deltalake import DeltaTable
from support.spark_runtime import require_configured_spark_delta_profile

from trading_advisor_3000.product_plane.data_plane.rebuild_layout import (
    RebuildStorageLayout,
    RebuildTableLayout,
)
from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session
from trading_advisor_3000.spark_jobs.rebuild_storage_layout_job import (
    SparkDeltaTableMaterializer,
    finalize_rebuild_storage_layout,
)


@pytest.fixture(scope="module")
def spark():
    require_configured_spark_delta_profile()
    session = _create_spark_session("rebuild-layout-regression", "local[2]")
    yield session
    session.stop()


def _table(partitioned: bool = True) -> RebuildTableLayout:
    return RebuildTableLayout(
        name="canonical_bars",
        layer="canonical",
        relative_path=PurePosixPath("canonical/bars.delta"),
        scope="compute_window",
        required=True,
        partition_by=("ts_close_year",) if partitioned else (),
        partition_source_column="ts" if partitioned else None,
        sort_columns=("ts", "id"),
        provisional_files=1,
    )


@pytest.mark.parametrize("partitioned", [True, False])
@pytest.mark.parametrize("limit", [1024 * 1024, 512 * 1024 * 1024])
def test_spark_finalizer_preserves_rows_and_enforces_file_layout(
    tmp_path: Path, spark, partitioned: bool, limit: int
) -> None:
    table = _table(partitioned)
    source_root = tmp_path / "compute"
    target_root = tmp_path / "final"
    source = source_root / table.relative_path
    frame = spark.range(30000).selectExpr(
        "id",
        "cast('2025-12-31 23:59:59' as timestamp) as ts",
        "concat(sha2(cast(id as string), 512), sha2(cast(id + 30000 as string), 512)) as payload",
    )
    frame.write.format("delta").save(str(source))
    manifest = tmp_path / "layout.yaml"
    manifest.write_text("isolated integration fixture", encoding="utf-8")
    layout = RebuildStorageLayout(
        schema_version="ta3000_table_layout.v1",
        manifest_path=manifest,
        manifest_sha256="integration-fixture",
        rebuild_id="integration",
        dataset_version="integration",
        compute_window_years=(2025,),
        target_window_years=(2025,),
        max_compressed_file_bytes=limit,
        files_per_nonempty_partition=1,
        split_only_when_file_exceeds_limit=True,
        no_empty_partitions=True,
        tables=(table,),
    )
    # Ambient Spark file limits must not silently defeat the manifest policy.
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "100")
    report = finalize_rebuild_storage_layout(
        layout=layout,
        source_root=source_root,
        target_root=target_root,
        report_path=tmp_path / "report.json",
        run_id="isolated-spark",
        materializer=SparkDeltaTableMaterializer(spark, max_compressed_file_bytes=limit),
        published_current_roots=(tmp_path / "published",),
    )
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "0")
    proof = report["tables"][0]["proof"]
    assert report["status"] == "PASS"
    assert proof["source_delta_version"] == 0
    assert proof["row_count"] == 30000
    assert proof["max_file_bytes"] <= limit
    assert (proof["parquet_files"] > 1) == (limit == 1024 * 1024)
    written = spark.read.format("delta").load(str(target_root / table.relative_path))
    assert written.select(*frame.columns).exceptAll(frame).count() == 0
    assert frame.exceptAll(written.select(*frame.columns)).count() == 0
    if partitioned:
        assert [row[0] for row in written.select("ts_close_year").distinct().collect()] == [2025]


@pytest.mark.parametrize("timestamp", [None, "not-a-timestamp"])
def test_spark_finalizer_rejects_invalid_year_before_output(
    tmp_path: Path, spark, timestamp
) -> None:
    source = tmp_path / "source.delta"
    target = tmp_path / "target.delta"
    spark.createDataFrame([(1, timestamp)], "id long, ts string").write.format("delta").save(
        str(source)
    )
    spark.conf.set("spark.sql.ansi.enabled", "false")
    with pytest.raises(RuntimeError, match="null|invalid year"):
        SparkDeltaTableMaterializer(spark, max_compressed_file_bytes=1024 * 1024).materialize(
            source_path=source, staged_path=target, table=_table()
        )
    assert not target.exists()


def test_spark_finalizer_pins_source_version_during_write(
    tmp_path: Path, spark, monkeypatch
) -> None:
    source = tmp_path / "source.delta"
    target = tmp_path / "target.delta"
    frame = spark.createDataFrame([(1, "2025-01-01")], "id long, ts string")
    frame.write.format("delta").save(str(source))
    materializer = SparkDeltaTableMaterializer(spark, max_compressed_file_bytes=1024 * 1024)
    write = materializer._write

    def append_then_write(**kwargs):
        frame.write.format("delta").mode("append").save(str(source))
        write(**kwargs)

    monkeypatch.setattr(materializer, "_write", append_then_write)
    proof = materializer.materialize(source_path=source, staged_path=target, table=_table())
    assert DeltaTable(str(source)).version() == 1
    assert proof["source_delta_version"] == 0
    assert proof["source_row_count"] == proof["row_count"] == 1
    assert proof["status"] == "PASS"
