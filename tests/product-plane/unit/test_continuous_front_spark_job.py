from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.research.continuous_front import CONTINUOUS_FRONT_TABLES
from trading_advisor_3000.product_plane.research.datasets import (
    CALENDAR_EXPIRY_CONTINUOUS_FRONT_POLICY,
    ContinuousFrontPolicy,
)
from trading_advisor_3000.spark_jobs import continuous_front_job as job


class _FakeDataFrame:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = rows or []

    def select(self, *_columns: object) -> "_FakeDataFrame":
        return self

    def count(self) -> int:
        return len(self.rows) or 1

    def toLocalIterator(self):
        class _Row(dict):
            def asDict(self, recursive: bool = True):  # noqa: ARG002
                return dict(self)

        return iter(_Row(row) for row in self.rows)


class _FakeReader:
    def __init__(self) -> None:
        self.loaded_paths: list[str] = []

    def format(self, _format: str) -> "_FakeReader":
        return self

    def load(self, path: str) -> _FakeDataFrame:
        self.loaded_paths.append(path)
        return _FakeDataFrame()


class _FakeSpark:
    def __init__(self) -> None:
        self.read = _FakeReader()
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


def test_continuous_front_spark_job_uses_native_spark_contour(
    tmp_path: Path,
    monkeypatch,
) -> None:
    canonical_bars_path = tmp_path / "canonical_bars.delta"
    calendar_path = tmp_path / "canonical_session_calendar.delta"
    roll_map_path = tmp_path / "canonical_roll_map.delta"
    for table_path in (canonical_bars_path, calendar_path, roll_map_path):
        (table_path / "_delta_log").mkdir(parents=True)

    fake_spark = _FakeSpark()
    written_roots: list[Path] = []
    native_builder_calls: list[str] = []
    qc_rows = [
        {
            "instrument_id": "FUT_BR",
            "timeframe": "15m",
            "status": "PASS",
            "blocked_reason": None,
        }
    ]
    fake_tables = {table_name: _FakeDataFrame(qc_rows if table_name == "continuous_front_qc_report" else []) for table_name in CONTINUOUS_FRONT_TABLES}

    def _native_builder(**kwargs: object) -> dict[str, _FakeDataFrame]:
        native_builder_calls.append(str(kwargs["dataset_version"]))
        return fake_tables

    def _write_tables(**kwargs: object) -> dict[str, str]:
        output_dir = Path(str(kwargs["output_dir"]))
        written_roots.append(output_dir)
        return {table_name: (output_dir / f"{table_name}.delta").as_posix() for table_name in CONTINUOUS_FRONT_TABLES}

    monkeypatch.setattr(job, "_build_spark_native_tables", _native_builder)
    monkeypatch.setattr(job, "_write_spark_dataframe_tables", _write_tables)
    monkeypatch.setattr(job, "_validate_spark_promoted_contracts", lambda _paths: [])
    monkeypatch.setattr(job, "count_delta_table_rows", lambda _path: 1)

    report = job.run_continuous_front_spark_job(
        canonical_bars_path=canonical_bars_path,
        canonical_session_calendar_path=calendar_path,
        canonical_roll_map_path=roll_map_path,
        output_dir=tmp_path / "continuous-front",
        dataset_version="spark-cf-v1",
        run_id="spark-run",
        spark_session_factory=lambda _app_name, _master: fake_spark,
    )

    assert native_builder_calls == ["spark-cf-v1"]
    assert report["status"] == "PASS"
    assert report["spark_profile"]["delta_reader"] == "spark"
    assert report["spark_profile"]["delta_writer"] == "spark"
    assert report["spark_profile"]["causal_roll_engine"] == "spark-native-window-batch"
    assert set(report["spark_profile"]) == {
        "app_name",
        "master",
        "delta_reader",
        "delta_writer",
        "causal_roll_engine",
        "sql_plan",
    }
    assert not hasattr(job, "_build_tables_from_spark_stream")
    assert not hasattr(job, "_iter_canonical_bars_from_spark")
    assert not hasattr(job, "_iter_roll_map_from_spark")
    assert not hasattr(job, "build_continuous_front_tables")
    assert any(path.name == "spark-run" for path in written_roots)
    assert (tmp_path / "continuous-front") in written_roots
    assert str(calendar_path) in fake_spark.read.loaded_paths
    assert str(roll_map_path) in fake_spark.read.loaded_paths
    assert fake_spark.stopped is True
    assert "materialize_continuous_front" not in job.run_continuous_front_spark_job.__code__.co_names
    assert "build_continuous_front_tables" not in job.run_continuous_front_spark_job.__code__.co_names
    assert "toLocalIterator" not in job.run_continuous_front_spark_job.__code__.co_names
    assert "collect" not in job.run_continuous_front_spark_job.__code__.co_names


def test_continuous_front_spark_job_rejects_unsupported_reference_policy(
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="reference_price_policy=settlement_price"):
        job.run_continuous_front_spark_job(
            canonical_bars_path=tmp_path / "canonical_bars.delta",
            canonical_session_calendar_path=tmp_path / "canonical_session_calendar.delta",
            canonical_roll_map_path=tmp_path / "canonical_roll_map.delta",
            output_dir=tmp_path / "continuous-front",
            dataset_version="spark-cf-v1",
            policy=ContinuousFrontPolicy(reference_price_policy="settlement_price"),
            spark_session_factory=lambda _app_name, _master: _FakeSpark(),
        )


def test_spark_native_calendar_expiry_uses_roll_map_active_contracts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    monkeypatch.setenv("HADOOP_HOME", (Path.cwd() / ".tmp" / "hadoop-winutils").as_posix())
    monkeypatch.setenv("TA3000_SPARK_RUNTIME_ROOT", (tmp_path / "spark-runtime").as_posix())
    spark = job._create_spark_session(  # type: ignore[attr-defined]
        "ta3000-continuous-front-calendar-test",
        "local[2]",
    )
    try:
        bars = spark.sql(
            """
            SELECT
              contract_id,
              instrument_id,
              timeframe,
              CAST(ts AS TIMESTAMP) AS ts,
              CAST(open_price AS DOUBLE) AS open,
              CAST(high_price AS DOUBLE) AS high,
              CAST(low_price AS DOUBLE) AS low,
              CAST(close_price AS DOUBLE) AS close,
              CAST(volume AS BIGINT) AS volume,
              CAST(open_interest AS BIGINT) AS open_interest
            FROM VALUES
              ('BRK2@MOEX', 'FUT_BR', '15m', '2022-03-21 10:00:00', 99.0, 101.0, 98.0, 100.0, 1000, 300),
              ('BRM2@MOEX', 'FUT_BR', '15m', '2022-03-21 10:00:00', 109.0, 111.0, 108.0, 110.0, 900, 100),
              ('BRK2@MOEX', 'FUT_BR', '15m', '2022-03-21 10:15:00', 100.0, 102.0, 99.0, 101.0, 1000, 120),
              ('BRM2@MOEX', 'FUT_BR', '15m', '2022-03-21 10:15:00', 110.0, 112.0, 109.0, 111.0, 900, 420),
              ('BRK2@MOEX', 'FUT_BR', '15m', '2022-03-22 10:00:00', 101.0, 103.0, 100.0, 102.0, 1000, 100),
              ('BRM2@MOEX', 'FUT_BR', '15m', '2022-03-22 10:00:00', 111.0, 113.0, 110.0, 112.0, 900, 430)
            AS t(
              contract_id,
              instrument_id,
              timeframe,
              ts,
              open_price,
              high_price,
              low_price,
              close_price,
              volume,
              open_interest
            )
            """
        )
        session_calendar = spark.sql(
            """
            SELECT
              instrument_id,
              timeframe,
              session_date,
              CAST(session_open_ts AS TIMESTAMP) AS session_open_ts,
              CAST(session_close_ts AS TIMESTAMP) AS session_close_ts
            FROM VALUES
              ('FUT_BR', '15m', '2022-03-21', '2022-03-21 09:00:00', '2022-03-21 23:50:00'),
              ('FUT_BR', '15m', '2022-03-22', '2022-03-22 09:00:00', '2022-03-22 23:50:00')
            AS t(instrument_id, timeframe, session_date, session_open_ts, session_close_ts)
            """
        )
        roll_map = spark.sql(
            """
            SELECT instrument_id, session_date, active_contract_id
            FROM VALUES
              ('FUT_BR', '2022-03-21', 'BRK2@MOEX'),
              ('FUT_BR', '2022-03-22', 'BRM2@MOEX')
            AS t(instrument_id, session_date, active_contract_id)
            """
        )
        monkeypatch.setattr(job, "_load_filtered_bars", lambda **_kwargs: bars)
        monkeypatch.setattr(job, "_load_session_calendar", lambda **_kwargs: session_calendar)
        monkeypatch.setattr(job, "_load_roll_map", lambda **_kwargs: roll_map)

        tables = job._build_spark_native_tables(  # type: ignore[attr-defined]
            spark=spark,
            canonical_bars_path=tmp_path / "canonical_bars.delta",
            canonical_session_calendar_path=tmp_path / "canonical_session_calendar.delta",
            canonical_roll_map_path=tmp_path / "canonical_roll_map.delta",
            dataset_version="spark-calendar-v1",
            policy=ContinuousFrontPolicy.from_config(CALENDAR_EXPIRY_CONTINUOUS_FRONT_POLICY),
            run_id="spark-calendar",
            instrument_ids=(),
            timeframes=("15m",),
            start_ts=None,
            end_ts=None,
        )

        front = [
            row.asDict(recursive=True)
            for row in tables["continuous_front_bars"].orderBy("ts").toLocalIterator()
        ]
        event = next(tables["continuous_front_roll_events"].toLocalIterator()).asDict(recursive=True)
        qc = next(tables["continuous_front_qc_report"].toLocalIterator()).asDict(recursive=True)

        assert [row["active_contract_id"] for row in front] == ["BRK2@MOEX", "BRK2@MOEX", "BRM2@MOEX"]
        assert [row["candidate_contract_id"] for row in front] == ["BRK2@MOEX", "BRK2@MOEX", "BRM2@MOEX"]
        assert [row["input_row_count"] for row in front] == [6, 6, 6]
        assert front[-1]["is_roll_bar"] is True
        assert front[-1]["causality_watermark_ts"] == front[-1]["ts"]
        assert event["decision_ts"] == front[1]["ts"]
        assert event["effective_ts"] == front[-1]["ts"]
        assert event["old_reference_price"] == pytest.approx(101.0)
        assert event["new_reference_price"] == pytest.approx(112.0)
        assert event["additive_gap"] == pytest.approx(11.0)
        assert event["causality_watermark_ts"] == event["effective_ts"]
        assert qc["status"] == "PASS"
        assert qc["missing_active_bar_count"] == 0
    finally:
        spark.stop()


def test_spark_native_adjustment_uses_backward_current_anchor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pytest.importorskip("pyspark.sql")
    monkeypatch.setenv("HADOOP_HOME", (Path.cwd() / ".tmp" / "hadoop-winutils").as_posix())
    monkeypatch.setenv("TA3000_SPARK_RUNTIME_ROOT", (tmp_path / "spark-runtime").as_posix())
    spark = job._create_spark_session(  # type: ignore[attr-defined]
        "ta3000-continuous-front-current-anchor-test",
        "local[2]",
    )
    try:
        bars = spark.sql(
            """
            SELECT
              contract_id,
              instrument_id,
              timeframe,
              CAST(ts AS TIMESTAMP) AS ts,
              CAST(open_price AS DOUBLE) AS open,
              CAST(high_price AS DOUBLE) AS high,
              CAST(low_price AS DOUBLE) AS low,
              CAST(close_price AS DOUBLE) AS close,
              CAST(volume AS BIGINT) AS volume,
              CAST(open_interest AS BIGINT) AS open_interest
            FROM VALUES
              ('BRK2@MOEX', 'BR', '15m', '2022-03-21 10:00:00', 99.0, 101.0, 98.0, 100.0, 1000, 300),
              ('BRM2@MOEX', 'BR', '15m', '2022-03-21 10:00:00', 109.0, 111.0, 108.0, 110.0, 1000, 100),
              ('BRK2@MOEX', 'BR', '15m', '2022-03-21 10:15:00', 100.0, 102.0, 99.0, 101.0, 1000, 120),
              ('BRM2@MOEX', 'BR', '15m', '2022-03-21 10:15:00', 110.0, 112.0, 109.0, 111.0, 1000, 420),
              ('BRK2@MOEX', 'BR', '15m', '2022-03-21 10:30:00', 101.0, 103.0, 100.0, 102.0, 1000, 100),
              ('BRM2@MOEX', 'BR', '15m', '2022-03-21 10:30:00', 111.0, 113.0, 110.0, 112.0, 1000, 430),
              ('BRM2@MOEX', 'BR', '15m', '2022-03-21 10:45:00', 112.0, 114.0, 111.0, 113.0, 1000, 440)
            AS t(
              contract_id,
              instrument_id,
              timeframe,
              ts,
              open_price,
              high_price,
              low_price,
              close_price,
              volume,
              open_interest
            )
            """
        )
        monkeypatch.setattr(job, "_load_filtered_bars", lambda **_kwargs: bars)

        tables = job._build_spark_native_tables(  # type: ignore[attr-defined]
            spark=spark,
            canonical_bars_path=tmp_path / "canonical_bars.delta",
            canonical_session_calendar_path=tmp_path / "canonical_session_calendar.delta",
            canonical_roll_map_path=tmp_path / "canonical_roll_map.delta",
            dataset_version="spark-current-anchor-v1",
            policy=ContinuousFrontPolicy(confirmation_bars=1),
            run_id="spark-current-anchor",
            instrument_ids=(),
            timeframes=("15m",),
            start_ts=None,
            end_ts=None,
        )

        front = [
            row.asDict(recursive=True)
            for row in tables["continuous_front_bars"].orderBy("ts").toLocalIterator()
        ]
        event = next(tables["continuous_front_roll_events"].toLocalIterator()).asDict(recursive=True)
        ladder = next(tables["continuous_front_adjustment_ladder"].toLocalIterator()).asDict(recursive=True)

        assert [row["active_contract_id"] for row in front] == [
            "BRK2@MOEX",
            "BRK2@MOEX",
            "BRM2@MOEX",
            "BRM2@MOEX",
        ]
        assert [row["continuous_close"] for row in front] == pytest.approx([100.0, 101.0, 112.0, 113.0])
        assert [row["native_close"] for row in front] == pytest.approx([100.0, 101.0, 112.0, 113.0])
        assert [row["cumulative_additive_offset"] for row in front] == pytest.approx([0.0, 0.0, 0.0, 0.0])
        assert event["old_reference_price"] == pytest.approx(101.0)
        assert event["new_reference_price"] == pytest.approx(111.0)
        assert event["additive_gap"] == pytest.approx(10.0)
        assert ladder["cumulative_offset_before"] == pytest.approx(10.0)
        assert ladder["cumulative_offset_after"] == pytest.approx(0.0)
    finally:
        spark.stop()
