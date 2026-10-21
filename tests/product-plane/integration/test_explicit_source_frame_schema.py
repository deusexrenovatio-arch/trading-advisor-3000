"""Exercise the actual Spark source-frame producer and native pinned consumer."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest
from deltalake import Schema, write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as s
from trading_advisor_3000.product_plane.research.compute_contract import semantic_compute_contract
from trading_advisor_3000.product_plane.research.derived_indicators.materialize import (
    _source_indicator_columns_for_profile,
)
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    current_derived_indicator_profile,
)
from trading_advisor_3000.spark_jobs.research_derived_source_frames_job import (
    _cast_to_source_contract,
)


def test_real_spark_source_frame_matches_semantic_and_physical_manifest(tmp_path):
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("explicit-schema-proof")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        source = spark.range(1).select(
            F.lit("2026-10-20T21:00:00Z").cast("timestamp").alias("bar_end_ts")
        )
        frame = _cast_to_source_contract(
            source,
            source_indicator_columns=_source_indicator_columns_for_profile(
                current_derived_indicator_profile()
            ),
        )
        expected = semantic_compute_contract()["schemas"]["research_derived_source_frames"]
        assert set(frame.columns) == set(expected) | {"ts_close_year"}
        assert "adx_14" not in frame.columns
        native_schema = json.loads(frame.schema.json())
        schema = Schema.from_json(frame.schema.json()).to_pyarrow()
        rows = [row.asDict() for row in frame.collect()]
        arrow = pa.Table.from_pylist(rows, schema=schema)
        path = tmp_path / "produced.delta"
        write_deltalake(str(path), arrow, partition_by=["ts_close_year"])
        value = s.create_snapshot(
            tables={"source": s.capture_table(path)},
            required_tables=["source"],
            dataset_version="spark-proof",
            run_id="producer",
            code_version="current",
            quality_checks={"producer": "PASS"},
        )
        assert value["physical_schemas"]["source"]["schema"] == native_schema
        with s.snapshot_read_scope(value):
            table = s.open_read_table(path)
            assert table.version() == 0
            assert table.to_pyarrow_table(columns=["ts_close_year"])[
                "ts_close_year"
            ].to_pylist() == [2026]
    finally:
        spark.stop()
