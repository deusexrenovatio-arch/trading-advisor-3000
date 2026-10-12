from datetime import UTC, date, datetime, timedelta

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.schemas import (
    historical_data_delta_schema_manifest,
)
from trading_advisor_3000.spark_jobs import moex_canonical_publish_job as publish_job
from trading_advisor_3000.spark_jobs import moex_canonicalization_job as job
from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    session = _create_spark_session("closed-session-regressions", "local[2]")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()


@pytest.mark.parametrize(
    "case", ["weekly", "unknown_date", "all_closed", "daily_closed", "exclusive_end"]
)
def test_weekly_sidecar_keeps_closed_anchor_and_covers_trading_period(spark, tmp_path, case):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Monday is a holiday; MOEX end labels Wednesday as an included date.
    _, _, intervals, closed = _inputs(spark, tmp_path)
    manifest = historical_data_delta_schema_manifest()
    write_delta_table_rows(
        table_path=intervals,
        rows=[
            {
                "instrument_id": "FUT_BR",
                "session_date": day,
                "interval_id": day,
                "interval_seq": 1,
                "expected_open_ts": day + "T07:00:00Z",
                "expected_close_ts": day + "T15:45:00Z",
                "session_class": "regular",
                "interval_type": "regular",
                "policy_id": "fixture-policy",
                "source_id": "fixture-source",
                "source_document_hash": "fixture-hash",
            }
            for day in (["2020-02-03"] if case == "all_closed" else ["2020-01-28", "2020-01-29"])
        ],
        columns=manifest["canonical_session_intervals"]["columns"],
    )
    write_delta_table_rows(
        table_path=closed,
        rows=[
            {
                "instrument_id": "FUT_BR",
                "session_date": day,
                "policy_id": "fixture-policy",
                "source_id": "fixture-source",
                "source_document_hash": "fixture-hash",
            }
            for day in (
                ["2020-01-27", "2020-01-28", "2020-01-29"]
                if case == "all_closed"
                else ["2020-01-27"]
            )
        ],
        columns={
            "instrument_id": "string",
            "session_date": "date",
            "policy_id": "string",
            "source_id": "string",
            "source_document_hash": "string",
        },
    )
    key = {
        "contract_id": "BRG0@MOEX",
        "instrument_id": "FUT_BR",
        "timeframe": "1d" if case == "daily_closed" else "1w",
        "ts": datetime(2020, 1, 27, tzinfo=UTC),
    }
    bars = spark.createDataFrame([{**key, "open_interest": 1}])
    provenance = spark.createDataFrame(
        [
            {
                **key,
                "source_provider": "fixture" if case == "exclusive_end" else "moex_iss",
                "source_timeframe": "1w",
                "source_interval": 10080,
                "source_ts_open_first": datetime(2020, 1, 26, 21, tzinfo=UTC),
                "source_ts_close_last": datetime(2020, 1, 28, 21, tzinfo=UTC),
            }
        ]
    )
    kwargs = dict(
        spark=spark,
        bars_df=bars,
        provenance_df=provenance,
        affected_sessions_df=spark.createDataFrame([], "instrument_id STRING, session_date DATE"),
        session_intervals_path=intervals,
        sidecars_exist=False,
        window=Window,
        functions=F,
        closed_session_dates_path=None if case == "unknown_date" else closed,
    )
    if case in {"unknown_date", "all_closed", "daily_closed"}:
        match = "unknown_period_dates=1" if case == "unknown_date" else "resolved_session_rows=0"
        with pytest.raises(ValueError, match=match):
            publish_job._sidecar_frames(**kwargs)
        return
    calendar, roll, _ = publish_job._sidecar_frames(**kwargs)
    row = calendar.first().asDict()
    assert row["session_date"] == date(2020, 1, 27)
    assert row["session_open_ts"] == datetime(2020, 1, 28, 7)
    assert row["session_close_ts"] == datetime(
        2020, 1, 28 if case == "exclusive_end" else 29, 15, 45
    )
    assert roll.first()["session_date"] == date(2020, 1, 27)


def _inputs(
    spark,
    tmp_path,
    *,
    missing=False,
    conflict=False,
    bad_hash=False,
    duplicate=False,
    all_closed=False,
):
    start = date(2020, 1, 27)
    intervals = []
    for offset in [7] if all_closed else range(5):
        day = (start + timedelta(days=offset)).isoformat()
        intervals.append(
            {
                "instrument_id": "FUT_BR",
                "session_date": day,
                "interval_id": day,
                "interval_seq": 1,
                "expected_open_ts": day + "T07:00:00Z",
                "expected_close_ts": day + "T15:45:00Z",
                "session_class": "regular",
                "interval_type": "regular",
                "policy_id": "fixture-policy",
                "source_id": "fixture-source",
                "source_document_hash": "fixture-hash",
            }
        )
    interval_path = tmp_path / "intervals.delta"
    write_delta_table_rows(
        table_path=interval_path,
        rows=intervals,
        columns=historical_data_delta_schema_manifest()["canonical_session_intervals"]["columns"],
    )
    closed = []
    for offset in (
        range(7) if all_closed else ([0, 5, 6] if conflict else ([5] if missing else [5, 6]))
    ):
        closed.append(
            {
                "instrument_id": "FUT_BR",
                "session_date": (start + timedelta(days=offset)).isoformat(),
                "policy_id": "fixture-policy",
                "source_id": "fixture-source",
                "source_document_hash": "" if bad_hash else "fixture-hash",
            }
        )
    if duplicate:
        closed.append(dict(closed[-1]))
    closed_path = tmp_path / "closed.delta"
    write_delta_table_rows(
        table_path=closed_path,
        rows=closed,
        columns={
            "instrument_id": "string",
            "session_date": "date",
            "policy_id": "string",
            "source_id": "string",
            "source_document_hash": "string",
        },
    )
    source = spark.createDataFrame(
        [
            {
                "contract_id": "BRG0@MOEX",
                "instrument_id": "FUT_BR",
                "source_timeframe": "1w",
                "source_interval": 10080,
                "ts_open": datetime(2020, 1, 26, 21, tzinfo=UTC),
                "ts_close": datetime(2020, 2, 2, 21, tzinfo=UTC),
                "open": 10.0,
                "high": 12.0,
                "low": 9.0,
                "close": 11.0,
                "volume": 7,
                "open_interest": 1,
                "open_interest_imputed": False,
                "source_provider": "fixture",
                "source_run_id": "raw-fixture",
                "source_ingest_run_id": "raw-fixture",
            }
        ]
    )
    selected = spark.createDataFrame(
        [
            {
                "contract_id": "BRG0@MOEX",
                "instrument_id": "FUT_BR",
                "source_interval": 10080,
                "target_minutes": 10080,
                "timeframe": "1w",
            }
        ]
    )
    return source, selected, interval_path, closed_path


def _build(spark, tmp_path, **options):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source, selected, intervals, closed = _inputs(spark, tmp_path, **options)
    try:
        return job._build_session_bounded_outputs(
            spark=spark,
            source_df=source,
            selected_df=selected,
            session_intervals_path=intervals,
            closed_session_dates_path=closed,
            build_run_id="closed-regression",
            built_at_utc="2020-02-03T00:00:00Z",
            functions=F,
            window=Window,
        )
    finally:
        spark.catalog.clearCache()


def test_weekly_candle_accepts_explicit_closed_weekend(spark, tmp_path):
    bars, provenance, report = _build(spark, tmp_path)
    assert bars.count() == provenance.count() == 1
    assert report["missing_official_coverage_rows"] == 0
    assert report["closed_session_dates_used"] == 2
    assert report["admitted_source_rows"] == 1


@pytest.mark.parametrize(
    "option,error",
    [
        ({"missing": True}, "missing_official_coverage_rows=1"),
        ({"conflict": True}, "conflicting_open_closed_dates=1"),
        ({"bad_hash": True}, "invalid_closed_date_rows=2"),
        ({"duplicate": True}, "duplicate_closed_dates=1"),
        ({"all_closed": True}, "closed_only_source_spans=1"),
    ],
)
def test_closed_dates_fail_closed(spark, tmp_path, option, error):
    with pytest.raises(ValueError, match=error):
        _build(spark, tmp_path, **option)


def test_missing_calendar_does_not_mean_closed(spark, tmp_path):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source, selected, intervals, _ = _inputs(spark, tmp_path)
    with pytest.raises(ValueError, match="missing_official_coverage_rows=2"):
        job._build_session_bounded_outputs(
            spark=spark,
            source_df=source,
            selected_df=selected,
            session_intervals_path=intervals,
            build_run_id="missing",
            built_at_utc="2020-02-03T00:00:00Z",
            functions=F,
            window=Window,
        )
    spark.catalog.clearCache()


def test_daily_candle_on_explicit_closed_day_still_fails(spark, tmp_path):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source, selected, intervals, closed = _inputs(spark, tmp_path)
    source = (
        source.withColumn("source_interval", F.lit(1440))
        .withColumn("source_timeframe", F.lit("1d"))
        .withColumn("ts_open", F.to_timestamp(F.lit("2020-01-31T21:00:00Z")))
        .withColumn("ts_close", F.to_timestamp(F.lit("2020-02-01T21:00:00Z")))
    )
    selected = (
        selected.withColumn("source_interval", F.lit(1440))
        .withColumn("target_minutes", F.lit(1440))
        .withColumn("timeframe", F.lit("1d"))
    )
    try:
        with pytest.raises(ValueError, match="missing_official_coverage_rows=1"):
            job._build_session_bounded_outputs(
                spark=spark,
                source_df=source,
                selected_df=selected,
                session_intervals_path=intervals,
                closed_session_dates_path=closed,
                build_run_id="closed-daily",
                built_at_utc="2020-02-03T00:00:00Z",
                functions=F,
                window=Window,
            )
    finally:
        spark.catalog.clearCache()


def test_moex_weekly_midnight_end_includes_last_trading_date(spark, tmp_path):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source, selected, intervals, closed = _inputs(spark, tmp_path)
    # A midnight MOEX end label includes the following open Monday, after a closed Sunday.
    source = (
        source.withColumn("source_provider", F.lit("moex_iss"))
        .withColumn("ts_open", F.to_timestamp(F.lit("2020-02-01T21:00:00Z")))
        .withColumn("ts_close", F.to_timestamp(F.lit("2020-02-02T21:00:00Z")))
    )
    columns = historical_data_delta_schema_manifest()["canonical_session_intervals"]["columns"]
    row = {
        "instrument_id": "FUT_BR",
        "session_date": "2020-02-03",
        "interval_id": "2020-02-03",
        "interval_seq": 1,
        "expected_open_ts": "2020-02-03T07:00:00Z",
        "expected_close_ts": "2020-02-03T15:45:00Z",
        "session_class": "regular",
        "interval_type": "regular",
        "policy_id": "fixture-policy",
        "source_id": "fixture-source",
        "source_document_hash": "fixture-hash",
    }
    write_delta_table_rows(table_path=intervals, rows=[row], columns=columns, mode="append")
    try:
        bars, provenance, report = job._build_session_bounded_outputs(
            spark=spark,
            source_df=source,
            selected_df=selected,
            session_intervals_path=intervals,
            closed_session_dates_path=closed,
            build_run_id="moex-weekly-end",
            built_at_utc="2020-02-04T00:00:00Z",
            functions=F,
            window=Window,
        )
        assert bars.count() == provenance.count() == 1
        assert report["admitted_source_rows"] == 1
        assert report["closed_session_dates_used"] == 1
        # Raw source timestamp remains a provenance label; do not rewrite it.
        assert provenance.first()["source_ts_close_last"].isoformat() == "2020-02-02T21:00:00"
    finally:
        spark.catalog.clearCache()


@pytest.mark.parametrize(
    "provider,timeframe,interval,expected",
    [
        ("moex_iss", "1w", 10080, date(2020, 2, 3)),
        ("other", "1w", 10080, date(2020, 2, 2)),
        ("moex_iss", "1d", 1440, date(2020, 2, 2)),
    ],
)
def test_end_date_policy_is_provider_and_timeframe_specific(
    spark, provider, timeframe, interval, expected
):
    from pyspark.sql import functions as F

    source = spark.createDataFrame(
        [(provider, timeframe, interval, datetime(2020, 2, 2, 21, tzinfo=UTC))],
        "source_provider STRING, source_timeframe STRING, source_interval INT, ts_close TIMESTAMP",
    )
    assert source.select(job._source_session_end_date(F).alias("day")).first()["day"] == expected
