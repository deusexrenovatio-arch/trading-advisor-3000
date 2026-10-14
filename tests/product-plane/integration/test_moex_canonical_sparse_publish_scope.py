from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from trading_advisor_3000.spark_jobs import moex_canonical_publish_job as publish_job


@pytest.fixture(scope="module")
def spark():
    if os.name == "nt":
        pytest.skip("Docker/Linux native Spark proof avoids Windows Hadoop NativeIO")
    session = publish_job._create_spark_session("sparse-publish-scope-proof", "local[2]")
    yield session
    session.stop()


@pytest.mark.parametrize("explicit_scope", [True, False])
def test_sparse_windows_preserve_gap_when_explicit_scope_is_given(
    tmp_path: Path, spark, explicit_scope: bool
) -> None:
    from pyspark.sql import functions as F

    dates = ["2026-04-02", "2026-04-04", "2026-04-06"]
    bars = spark.createDataFrame(
        [("BRM6@MOEX", "FUT_BR", "5m", f"{date}T10:00:00Z") for date in dates],
        "contract_id string, instrument_id string, timeframe string, ts string",
    ).withColumn("ts", F.to_timestamp("ts"))
    provenance = (
        spark.createDataFrame(
            [
                ("BRM6@MOEX", "FUT_BR", "5m", f"{date}T10:00:00Z", f"{date}T10:04:59Z")
                for date in [dates[0], dates[-1]]
            ],
            "contract_id string, instrument_id string, timeframe string, "
            "source_ts_open_first string, source_ts_close_last string",
        )
        .withColumn("source_ts_open_first", F.to_timestamp("source_ts_open_first"))
        .withColumn("source_ts_close_last", F.to_timestamp("source_ts_close_last"))
    )
    path = tmp_path / "publish-scope.jsonl" if explicit_scope else None
    if path:
        path.write_text(
            "\n".join(
                json.dumps(
                    {
                        "instrument_id": "FUT_BR",
                        "timeframe": "5m",
                        "target_minutes": 5,
                        "window_start_utc": f"{date}T10:00:00Z",
                        "window_end_utc": f"{date}T10:04:59Z",
                    }
                )
                for date in [dates[0], dates[-1]]
            )
            + "\n",
            encoding="utf-8",
        )
    keys = publish_job._impacted_scope_keys(
        spark=spark,
        publish_scope_path=path,
        target_bars_df=bars,
        staged_provenance_df=provenance,
        functions=F,
    )
    actual = {row.ts.date().isoformat() for row in keys.collect()}
    expected = {dates[0], dates[-1]} if explicit_scope else set(dates)
    assert actual == expected
