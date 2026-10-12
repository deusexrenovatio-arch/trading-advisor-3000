from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest


def _windows_hadoop_nativeio_unavailable() -> bool:
    if os.name != "nt":
        return False
    hadoop_home = os.environ.get("HADOOP_HOME")
    return not hadoop_home or not (Path(hadoop_home) / "bin" / "hadoop.dll").exists()


@pytest.mark.skipif(
    _windows_hadoop_nativeio_unavailable(),
    reason="local Windows Spark/Delta requires Hadoop NativeIO; use Docker/Linux native proof",
)
def test_r2_year_writers_preserve_identity_history_and_business_values(tmp_path: Path) -> None:
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    from pyspark.sql import functions as F

    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        DerivedIndicatorFrameRow,
        research_derived_indicator_store_contract,
        write_derived_indicator_frames,
    )
    from trading_advisor_3000.spark_jobs import research_derived_source_frames_job as owner

    spark = owner._create_spark_session("research-r2-close-year-proof", "local[2]")
    proof = {"scope": "isolated R2 physical writer fixture", "tables": [], "legacy_guards": []}
    opened_and_closed = (
        (datetime(2025, 12, 31, 22, 15, tzinfo=UTC), datetime(2025, 12, 31, 22, 30, tzinfo=UTC)),
        (datetime(2025, 12, 31, 23, 45, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
    )
    expected_years = {opened.timestamp(): closed.year for opened, closed in opened_and_closed}

    def read_rows(table):
        return sorted(table.to_pyarrow_table().to_pylist(), key=lambda row: row["ts"])

    def assert_updated(path: Path, initial, value_column: str) -> None:
        initial_rows = read_rows(initial)
        current = DeltaTable(str(path))
        assert current.metadata().id == initial.metadata().id
        assert current.version() > initial.version()
        assert current.metadata().partition_columns == ["ts_close_year"]
        assert pa.types.is_int32(current.schema().to_pyarrow().field("ts_close_year").type)
        assert read_rows(DeltaTable(str(path), version=0)) == initial_rows
        assert all(row[value_column] == 1.25 for row in initial_rows)
        assert read_rows(current) == [{**row, value_column: 2.5} for row in initial_rows]
        assert {
            row["ts"].timestamp(): row["ts_close_year"] for row in read_rows(current)
        } == expected_years
        actions = current.get_add_actions(flatten=True).to_pylist()
        assert len(actions) == 2
        assert {int(item["partition.ts_close_year"]) for item in actions} == {2025, 2026}
        assert all(
            0 < item["size_bytes"] <= 512 * 1024 * 1024 and item["num_records"] > 0
            for item in actions
        )
        proof["tables"].append(
            {
                "family": path.name,
                "table_id": current.metadata().id,
                "old_pin": 0,
                "new_version": current.version(),
                "physical_type": "int",
                "rows": len(initial_rows),
                "active_files": [
                    {
                        key: item[key]
                        for key in ("path", "size_bytes", "num_records", "partition.ts_close_year")
                    }
                    for item in actions
                ],
            }
        )

    def assert_legacy_unchanged(path: Path, initial) -> None:
        current = DeltaTable(str(path))
        assert current.metadata().id == initial.metadata().id
        assert current.version() == initial.version()
        assert read_rows(current) == read_rows(initial)
        proof["legacy_guards"].append(
            {"family": path.name, "table_id": current.metadata().id, "version": current.version()}
        )

    try:
        spark.conf.set("spark.sql.session.timeZone", "Europe/Moscow")
        proof["actual_jvm_max_heap_bytes"] = int(
            spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()
        )
        frame = spark.createDataFrame(
            [
                (
                    "fixture",
                    "native_tradable",
                    "contract",
                    "BR-3.26",
                    "BR-3.26",
                    "FUT_BR",
                    "15m",
                    opened,
                    closed,
                    1.25,
                    2030,
                )
                for opened, closed in opened_and_closed
            ],
            "dataset_version string, contour_id string, series_mode string, series_id string, "
            "contract_id string, instrument_id string, timeframe string, ts timestamp, "
            "bar_end_ts timestamp, adx_14 double, ts_close_year int",
        )
        source_path = tmp_path / "source" / "research_derived_source_frames.delta"
        source_arguments = {
            "table_path": source_path,
            "replace_scope": [("dataset_version", "=", "fixture")],
            "source_indicator_columns": ("adx_14",),
        }
        owner._write_source_frame_table(dataframe=frame, **source_arguments)
        original_source = DeltaTable(str(source_path), version=0)
        owner._write_source_frame_table(
            dataframe=frame.withColumn("adx_14", F.lit(2.5)), **source_arguments
        )
        assert_updated(source_path, original_source, "adx_14")
        assert {
            row["ts"].timestamp(): row["bar_end_ts"].timestamp()
            for row in read_rows(DeltaTable(str(source_path)))
        } == {opened.timestamp(): closed.timestamp() for opened, closed in opened_and_closed}
        assert (
            spark.read.format("delta")
            .load(str(source_path))
            .schema["ts_close_year"]
            .dataType.simpleString()
            == "int"
        )

        value_column = next(
            name
            for name, kind in research_derived_indicator_store_contract()[
                "research_derived_indicator_frames"
            ]["columns"].items()
            if kind == "double"
        )
        derived_rows = [
            DerivedIndicatorFrameRow(
                dataset_version="fixture",
                indicator_set_version="core-fixture",
                derived_indicator_set_version="derived-fixture",
                profile_version="fixture",
                contract_id="BR-3.26",
                instrument_id="FUT_BR",
                timeframe="15m",
                ts=opened.isoformat(),
                bar_end_ts=closed.isoformat(),
                values={value_column: 1.25},
                source_bars_hash="bars-fixture",
                source_indicators_hash="indicators-fixture",
                row_count=2,
                warmup_span=0,
                null_warmup_span=0,
                created_at="2026-01-01T00:00:00Z",
                series_id="BR-3.26",
            )
            for opened, closed in opened_and_closed
        ]
        derived_dir = tmp_path / "derived"
        derived_path = derived_dir / "research_derived_indicator_frames.delta"
        write_derived_indicator_frames(
            output_dir=derived_dir, rows=derived_rows, replace_partitions=()
        )
        original_derived = DeltaTable(str(derived_path), version=0)
        updated_rows = [replace(row, values={value_column: 2.5}) for row in derived_rows]
        partitions = (derived_rows[0].partition_key(series_mode="contract"),)
        write_derived_indicator_frames(
            output_dir=derived_dir, rows=updated_rows, replace_partitions=partitions
        )
        assert_updated(derived_path, original_derived, value_column)

        legacy_source = tmp_path / "legacy-source" / source_path.name
        spark.read.format("delta").load(str(source_path)).drop("ts_close_year").write.format(
            "delta"
        ).save(str(legacy_source))
        legacy_source_before = DeltaTable(str(legacy_source))
        with pytest.raises(ValueError, match="isolated layout migration"):
            owner._write_source_frame_table(
                dataframe=frame,
                table_path=legacy_source,
                replace_scope=[("dataset_version", "=", "fixture")],
                source_indicator_columns=("adx_14",),
            )
        assert_legacy_unchanged(legacy_source, legacy_source_before)

        legacy_derived_dir = tmp_path / "legacy-derived"
        legacy_derived_path = legacy_derived_dir / derived_path.name
        write_deltalake(
            str(legacy_derived_path), original_derived.to_pyarrow_table().drop(["ts_close_year"])
        )
        legacy_derived_before = DeltaTable(str(legacy_derived_path))
        with pytest.raises(ValueError, match="isolated layout migration"):
            write_derived_indicator_frames(
                output_dir=legacy_derived_dir, rows=updated_rows, replace_partitions=partitions
            )
        assert_legacy_unchanged(legacy_derived_path, legacy_derived_before)

        assert spark.conf.get("spark.sql.session.timeZone") == "Europe/Moscow"
        if os.environ.get("TA3000_RESEARCH_CARRIER_PROOF"):
            Path(os.environ["TA3000_RESEARCH_CARRIER_PROOF"]).write_text(
                json.dumps(proof, indent=2), encoding="utf-8"
            )
    finally:
        spark.stop()
