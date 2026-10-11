import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from trading_advisor_3000.spark_jobs import moex_canonicalization_job as job
from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    session = _create_spark_session("clock-grid-regressions", "local[2]")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()


def _source(hour, *, source_interval=60, end_minute=59, end_second=59):
    return dict(
        contract_id="BRM6@MOEX",
        instrument_id="FUT_BR",
        source_timeframe="1h" if source_interval == 60 else "1m",
        source_interval=source_interval,
        ts_open=datetime(2026, 3, 24, hour, tzinfo=UTC),
        ts_close=datetime(2026, 3, 24, hour, end_minute, end_second, tzinfo=UTC),
        open=100.0 + hour,
        high=102.0 + hour,
        low=99.0 + hour,
        close=101.0 + hour,
        volume=10,
        open_interest=1,
        open_interest_imputed=False,
        source_provider="moex_iss",
        source_run_id="raw",
        source_ingest_run_id="raw",
    )


def test_clock_grid_preserves_raw_hours_and_partial_session_edges(spark, tmp_path):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    rows = [
        _source(h, end_minute={15: 44, 20: 49}.get(h, 59)) for h in [5, 6, 7, 8, 15, 16, 17, 20]
    ]
    for hour, minute in [(16, 5), (16, 14), (16, 15), (20, 49)]:
        row = _source(hour, source_interval=1, end_minute=minute)
        row["ts_open"] = row["ts_open"].replace(minute=minute)
        rows.append(row)
    intervals = []
    for seq, (start, end) in enumerate(
        [
            ("05:50:00", "07:00:00"),
            ("07:00:00", "15:45:00"),
            ("16:05:00", "20:50:00"),
        ],
        1,
    ):
        intervals.append(
            dict(
                instrument_id="FUT_BR",
                session_date="2026-03-24",
                interval_id=f"session-{seq}",
                interval_seq=seq,
                expected_open_ts=f"2026-03-24T{start}Z",
                expected_close_ts=f"2026-03-24T{end}Z",
                session_class="regular",
                interval_type="regular_trading",
                policy_id="test",
                source_id="test",
                source_document_hash="test-hash",
            )
        )
    path = tmp_path / "sessions.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in intervals) + "\n")
    selected = spark.createDataFrame(
        [
            dict(
                contract_id="BRM6@MOEX",
                instrument_id="FUT_BR",
                source_interval=1 if tf == "15m" else 60,
                timeframe=tf,
                target_minutes=minutes,
            )
            for tf, minutes in [("15m", 15), ("1h", 60), ("4h", 240)]
        ]
    )
    bars, provenance, report = job._build_session_bounded_outputs(
        spark=spark,
        source_df=spark.createDataFrame(rows),
        selected_df=selected,
        session_intervals_path=path,
        build_run_id="clock",
        built_at_utc="2026-03-25T00:00:00Z",
        functions=F,
        window=Window,
    )
    try:
        bar_rows = bars.collect()
        actual = {(r.timeframe, r.ts.hour): r.asDict() for r in bar_rows if r.timeframe != "15m"}
        quarter_hours = {r.ts.strftime("%H:%M"): r.volume for r in bar_rows if r.timeframe == "15m"}
        assert quarter_hours == {"16:00": 20, "16:15": 10, "20:45": 10}
        assert len(actual) == 11
        assert report["admitted_source_rows"] == len(rows)
        assert report["rejected_out_of_session_rows"] == 0
        for raw in [r for r in rows if r["source_interval"] == 60]:
            bar = actual[("1h", raw["ts_open"].hour)]
            assert bar["ts"].minute == 0
            for field in ["open", "high", "low", "close", "volume", "open_interest"]:
                assert bar[field] == raw[field]
        assert actual[("4h", 5)]["volume"] == 40
        assert actual[("4h", 13)]["volume"] == 20
        assert actual[("4h", 17)]["volume"] == 20
        spans = {(r.timeframe, r.ts.hour): r.asDict() for r in provenance.collect()}
        for key, row in spans.items():
            minutes = {"15m": 15, "1h": 60, "4h": 240}[key[0]]
            assert row["ts"] <= row["bar_start_ts"] <= row["bar_end_ts"]
            assert row["bar_end_ts"] <= row["ts"] + timedelta(minutes=minutes)
        assert spans[("4h", 5)]["source_row_count"] == 4
        assert spans[("4h", 5)]["session_interval_id"] == "session-2"
        assert spans[("4h", 13)]["session_interval_id"] == "session-3"
    finally:
        bars.unpersist()
        provenance.unpersist()


def test_clock_grid_rejects_source_spanning_target_bucket(spark):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    row = _source(7)
    row.update(timeframe="15m", target_minutes=15, interval_id="session-1")
    joined = spark.createDataFrame([row])
    with pytest.raises(ValueError, match="source_outside_target_bucket_rows=1"):
        job._aggregate_joined_source_outputs(
            joined=joined,
            bucket_seconds=F.unix_timestamp(F.col("ts_open")),
            build_run_id="invalid",
            built_at_utc="2026-03-25T00:00:00Z",
            functions=F,
            window=Window,
            enforce_session_bounds=False,
        )


def test_research_marks_partial_and_cross_session_clock_bars(spark):
    from trading_advisor_3000.spark_jobs.research_bar_views_job import (
        BarUsageContextFrames,
        _with_bar_usage_contract,
    )

    def stamp(hour, minute=0, second=0):
        return datetime(2026, 3, 24, hour, minute, second, tzinfo=UTC)

    frame = spark.createDataFrame(
        [
            dict(
                instrument_id="FUT_BR",
                contract_id="BRM6@MOEX",
                timeframe=tf,
                ts=ts,
                session_date=ts.date(),
                bar_start_ts=start,
                bar_end_ts=end,
                session_interval_id="evening",
                calendar_session_class="regular",
                provenance_boundary_bar=False,
                provenance_shortened_bar=False,
            )
            for tf, ts, start, end in [
                ("15m", stamp(20, 45), stamp(20, 45), stamp(20, 49, 12)),
                ("4h", stamp(13), stamp(15), stamp(16, 50)),
                ("1h", stamp(17), stamp(17, 15), stamp(17, 45)),
            ]
        ]
    )
    intervals = spark.createDataFrame(
        [
            dict(
                interval_instrument_id="FUT_BR",
                interval_session_date=stamp(16).date(),
                interval_id="evening",
                interval_open_ts=stamp(16, 5),
                interval_close_ts=stamp(20, 50),
                interval_session_class="regular",
                interval_shortened_bar=False,
            )
        ]
    )
    context = BarUsageContextFrames(
        intervals=intervals,
        day_meta=spark.createDataFrame(
            [], "day_instrument_id string, day_session_date date, day_session_class string"
        ),
        expected_weekly=spark.createDataFrame(
            [],
            "weekly_expected_instrument_id string, weekly_expected_week_start date, "
            "expected_traded_sessions long, weekly_partial_count long, "
            "weekly_special_count long, weekly_shortened_count long",
        ),
        actual_daily=spark.createDataFrame(
            [],
            "daily_contract_id string, daily_instrument_id string, "
            "week_start date, actual_daily_sessions long",
        ),
    )
    result = _with_bar_usage_contract(
        spark=spark,
        dataframe=frame,
        canonical_bars_path=Path("unused"),
        canonical_session_intervals_path=Path("unused"),
        usage_context=context,
    )
    try:
        assert {r.timeframe: r.bar_usage_profile for r in result.collect()} == {
            "15m": "boundary_risk",
            "4h": "boundary_risk",
            "1h": "regular_trading",
        }
    finally:
        result.unpersist()


def test_clock_grid_delta_readback_keeps_utc_close_year_at_moscow_new_year(spark, tmp_path):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
        read_delta_table_rows,
        write_delta_table_rows,
    )
    from trading_advisor_3000.product_plane.data_plane.moex import canonicalization as canonical
    from trading_advisor_3000.product_plane.data_plane.schemas import (
        historical_data_delta_schema_manifest,
    )

    source_rows = []
    for stamp in [
        datetime(2026, 12, 31, 22, 59, tzinfo=UTC),
        datetime(2027, 1, 1, 0, 0, tzinfo=UTC),
    ]:
        row = _source(stamp.hour, source_interval=1)
        row.update(ts_open=stamp, ts_close=stamp + timedelta(seconds=59))
        source_rows.append(row)
    interval = dict(
        instrument_id="FUT_BR",
        session_date="2027-01-01",
        interval_id="new-year-session",
        interval_seq=1,
        expected_open_ts="2026-12-31T21:00:00Z",
        expected_close_ts="2027-01-01T01:00:00Z",
        session_class="regular",
        interval_type="regular_trading",
        policy_id="test",
        source_id="test",
        source_document_hash="test-hash",
    )
    interval_path = tmp_path / "canonical_session_intervals.delta"
    write_delta_table_rows(
        table_path=interval_path,
        rows=[interval],
        columns=historical_data_delta_schema_manifest()["canonical_session_intervals"]["columns"],
    )
    selected = spark.createDataFrame(
        [
            dict(
                contract_id="BRM6@MOEX",
                instrument_id="FUT_BR",
                source_interval=1,
                timeframe=timeframe,
                target_minutes=minutes,
            )
            for timeframe, minutes in [("1h", 60), ("4h", 240)]
        ]
    )
    bars, provenance, report = job._build_session_bounded_outputs(
        spark=spark,
        source_df=spark.createDataFrame(source_rows),
        selected_df=selected,
        session_intervals_path=interval_path,
        build_run_id="clock-new-year",
        built_at_utc="2027-01-02T00:00:00Z",
        functions=F,
        window=Window,
    )
    bar_path = tmp_path / "canonical_bars.delta"
    provenance_path = tmp_path / "canonical_bar_provenance.delta"
    try:
        assert report["admitted_source_rows"] == 2
        assert report["rejected_out_of_session_rows"] == 0
        for dataframe, table_path, manifest in [
            (bars, bar_path, job.CANONICAL_BAR_MANIFEST),
            (provenance, provenance_path, job.CANONICAL_PROVENANCE_MANIFEST),
        ]:
            job._write_delta_dataframe(
                dataframe=dataframe,
                table_path=table_path,
                manifest_entry=dict(manifest),
            )
            assert (table_path / "_delta_log").is_dir()
        persisted_bars = read_delta_table_rows(bar_path, limit=4)
        persisted_provenance = read_delta_table_rows(provenance_path, limit=4)
        assert len(persisted_bars) == len(persisted_provenance) == 3

        def key(row):
            stamp = canonical._parse_iso_utc(str(row["ts"]))
            return (
                row["contract_id"],
                row["instrument_id"],
                row["timeframe"],
                stamp.isoformat(timespec="seconds").replace("+00:00", "Z"),
            )

        physical_keys = {key(row) for row in persisted_bars}
        assert physical_keys == {key(row) for row in persisted_provenance}
        raw_candles = [
            canonical.RawCandle(
                **{
                    **row,
                    "ts_open": row["ts_open"].isoformat(),
                    "ts_close": row["ts_close"].isoformat(),
                }
            )
            for row in source_rows
        ]
        affected_keys = canonical._compute_affected_canonical_keys(
            scoped_rows=raw_candles,
            selected_source_intervals={
                ("BRM6@MOEX", "FUT_BR", timeframe): 1 for timeframe in ["1h", "4h"]
            },
            session_intervals_path=interval_path,
        )
        assert (
            physical_keys
            == affected_keys
            == {
                ("BRM6@MOEX", "FUT_BR", "1h", "2026-12-31T22:00:00Z"),
                ("BRM6@MOEX", "FUT_BR", "1h", "2027-01-01T00:00:00Z"),
                ("BRM6@MOEX", "FUT_BR", "4h", "2026-12-31T21:00:00Z"),
            }
        )
        joined = (
            spark.read.format("delta")
            .load(str(bar_path))
            .join(
                spark.read.format("delta").load(str(provenance_path)),
                ["contract_id", "instrument_id", "timeframe", "ts"],
            )
            .select(
                "timeframe",
                F.date_format("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("bucket_utc"),
                F.date_format("bar_end_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("close_utc"),
                F.year("bar_end_ts").alias("utc_bar_close_year"),
                F.year(F.from_utc_timestamp("bar_end_ts", "Europe/Moscow")).alias(
                    "moscow_bar_close_year"
                ),
                "volume",
                "source_row_count",
            )
            .collect()
        )
        assert len(joined) == 3
        actual = {(row.timeframe, row.bucket_utc): row.asDict() for row in joined}
        first_hour = actual[("1h", "2026-12-31T22:00:00Z")]
        assert first_hour["utc_bar_close_year"] == 2026
        assert first_hour["moscow_bar_close_year"] == 2027
        last_hour = actual[("1h", "2027-01-01T00:00:00Z")]
        assert last_hour["utc_bar_close_year"] == 2027
        aggregate = actual[("4h", "2026-12-31T21:00:00Z")]
        assert aggregate["close_utc"] == "2027-01-01T00:00:59Z"
        assert aggregate["utc_bar_close_year"] == aggregate["moscow_bar_close_year"] == 2027
        assert aggregate["volume"] == 20
        assert aggregate["source_row_count"] == 2
        print(json.dumps({"clock_delta_fixture": list(actual.values())}, sort_keys=True))
    finally:
        bars.unpersist()
        provenance.unpersist()


def test_research_weekly_session_date_follows_moscow_provenance_start(spark):
    from pyspark.sql import functions as F

    from trading_advisor_3000.spark_jobs.research_bar_views_job import _bar_session_date

    monday = datetime(2027, 1, 4, tzinfo=UTC)
    frame = spark.createDataFrame(
        [
            dict(
                case=case,
                timeframe=timeframe,
                ts=monday,
                bar_start_ts=start,
            )
            for case, timeframe, start in [
                ("weekly-monday", "1w", monday - timedelta(hours=3)),
                ("weekly-after-closed-monday", "1w", monday + timedelta(days=1, hours=7)),
                ("daily-monday", "1d", monday - timedelta(hours=3)),
                ("intraday-date-retained", "1h", monday - timedelta(hours=3)),
            ]
        ]
    )
    actual = {
        row.case: row.session_date
        for row in frame.select(
            "case", F.date_format(_bar_session_date(), "yyyy-MM-dd").alias("session_date")
        ).collect()
    }
    assert actual == {
        "weekly-monday": "2027-01-04",
        "weekly-after-closed-monday": "2027-01-05",
        "daily-monday": "2027-01-04",
        "intraday-date-retained": "2027-01-03",
    }
