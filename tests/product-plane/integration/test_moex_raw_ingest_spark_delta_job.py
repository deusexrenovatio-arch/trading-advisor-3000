from __future__ import annotations

# ruff: noqa: E501
import json
import os
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS
from trading_advisor_3000.spark_jobs import moex_raw_ingest_job as raw_ingest_job
from trading_advisor_3000.spark_jobs.moex_raw_ingest_job import (
    run_moex_raw_ingest_spark_delta_job,
)


def _raw_row(
    *,
    ts_open: str,
    ts_close: str,
    close: float,
    run_id: str = "seed",
    moex_board: str = "RFUD",
    provenance_json: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "internal_id": "FUT_BR",
        "finam_symbol": "BRQ6",
        "moex_engine": "futures",
        "moex_market": "forts",
        "moex_board": moex_board,
        "moex_secid": "BRQ6",
        "asset_group": "commodity",
        "timeframe": "1m",
        "source_interval": 1,
        "ts_open": ts_open,
        "ts_close": ts_close,
        "open": 100.0 if close < 101 else 100.5,
        "high": 101.0 if close < 101 else 101.5,
        "low": 99.5 if close < 101 else 100.4,
        "close": close,
        "volume": 50 if close < 101 else 75,
        "open_interest": None,
        "ingest_run_id": run_id,
        "ingested_at_utc": "2026-04-01T07:20:00Z",
        "provenance_json": provenance_json or {"source_provider": "moex_iss", "run_id": run_id},
    }


def _read_test_rows(raw_table_path: Path) -> list[dict[str, object]]:
    return read_delta_table_rows(
        raw_table_path,
        filters=[
            ("internal_id", "=", "FUT_BR"),
            ("timeframe", "=", "1m"),
            ("source_interval", "=", 1),
            ("moex_secid", "=", "BRQ6"),
        ],
        limit=20,
    )


def test_raw_ingest_spark_delta_job_fails_closed_without_windows_hadoop_home(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(raw_ingest_job.os, "name", "nt", raising=False)
    monkeypatch.delenv("HADOOP_HOME", raising=False)

    with pytest.raises(RuntimeError, match="HADOOP_HOME"):
        run_moex_raw_ingest_spark_delta_job(
            table_path=tmp_path / "raw_moex_history.delta",
            source_rows=[],
            window_scopes=[],
            initial_watermarks={},
            run_id="missing-hadoop",
            ingest_till_utc="2026-04-01T08:00:00Z",
            refresh_overlap_minutes=20,
            progress_path=tmp_path / "raw-ingest-progress.jsonl",
            progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
            error_path=tmp_path / "raw-ingest-errors.jsonl",
            error_latest_path=tmp_path / "raw-ingest-error.latest.json",
        )


def test_raw_ingest_spark_delta_job_reads_source_rows_path_and_fails_closed_on_unmatched_scope(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; Docker/Linux proof profile runs this path"
        )

    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    source_rows_path = tmp_path / "raw-source-rows.jsonl"
    staged_row = _raw_row(
        ts_open="2026-04-01T07:00:00Z",
        ts_close="2026-04-01T07:09:59Z",
        close=100.5,
        run_id="spark-source-path",
    )
    staged_row["provenance_json"] = json.dumps(staged_row["provenance_json"], sort_keys=True)
    staged_row["_source_order"] = 1
    source_rows_path.write_text(json.dumps(staged_row, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="raw source rows did not match declared window scopes"):
        run_moex_raw_ingest_spark_delta_job(
            table_path=raw_table_path,
            source_rows=[],
            source_rows_path=source_rows_path,
            window_scopes=[
                {
                    "internal_id": "FUT_BR",
                    "timeframe": "1m",
                    "source_interval": 1,
                    "moex_secid": "BRQ6",
                    "window_start_utc": "2026-04-01T08:00:00Z",
                    "window_end_utc": "2026-04-01T08:30:00Z",
                    "watermark_utc": "",
                }
            ],
            initial_watermarks={},
            run_id="spark-source-path",
            ingest_till_utc="2026-04-01T08:30:00Z",
            refresh_overlap_minutes=20,
            progress_path=tmp_path / "raw-ingest-progress.jsonl",
            progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
            error_path=tmp_path / "raw-ingest-errors.jsonl",
            error_latest_path=tmp_path / "raw-ingest-error.latest.json",
        )


def test_raw_ingest_spark_delta_job_merges_corrections_without_python_table_rewrite(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; Docker/Linux proof profile runs this path"
        )

    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(ts_open="2026-04-01T07:00:00Z", ts_close="2026-04-01T07:09:59Z", close=100.5),
            _raw_row(ts_open="2026-04-01T07:10:00Z", ts_close="2026-04-01T07:19:59Z", close=101.2),
        ],
        columns=RAW_COLUMNS,
    )

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=raw_table_path,
        source_rows=[
            _raw_row(
                ts_open="2026-04-01T07:00:00Z",
                ts_close="2026-04-01T07:09:59Z",
                close=99.75,
                run_id="spark-run",
            ),
            _raw_row(
                ts_open="2026-04-01T07:10:00Z",
                ts_close="2026-04-01T07:19:59Z",
                close=101.2,
                run_id="spark-run",
            ),
            _raw_row(
                ts_open="2026-04-01T07:20:00Z",
                ts_close="2026-04-01T07:29:59Z",
                close=101.8,
                run_id="spark-run",
            ),
        ],
        window_scopes=[
            {
                "internal_id": "FUT_BR",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRQ6",
                "window_start_utc": "2026-04-01T06:59:59Z",
                "window_end_utc": "2026-04-01T08:00:00Z",
                "watermark_utc": "2026-04-01T07:19:59Z",
            }
        ],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"},
        run_id="spark-raw-ingest",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_overlap_minutes=20,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    rows = _read_test_rows(raw_table_path)
    assert report["source_rows"] == 3
    assert report["incremental_rows"] == 2
    assert report["deduplicated_rows"] == 1
    assert report["changed_windows"] == [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRQ6",
            "window_start_utc": "2026-04-01T06:59:59Z",
            "window_end_utc": "2026-04-01T08:00:00Z",
            "incremental_rows": 2,
        }
    ]
    assert {row["ts_close"] for row in rows} == {
        "2026-04-01T07:09:59Z",
        "2026-04-01T07:19:59Z",
        "2026-04-01T07:29:59Z",
    }
    assert {row["close"] for row in rows if row["ts_close"] == "2026-04-01T07:09:59Z"} == {99.75}
    assert (raw_table_path / "_delta_log").exists()
    assert (tmp_path / "raw-ingest-progress.latest.json").exists()


def test_raw_ingest_spark_delta_job_deletes_target_rows_missing_from_refresh_window(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; Docker/Linux proof profile runs this path"
        )

    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(ts_open="2026-04-01T07:00:00Z", ts_close="2026-04-01T07:09:59Z", close=100.5),
            _raw_row(ts_open="2026-04-01T07:10:00Z", ts_close="2026-04-01T07:19:59Z", close=101.2),
            _raw_row(ts_open="2026-04-01T07:20:00Z", ts_close="2026-04-01T07:29:59Z", close=101.8),
        ],
        columns=RAW_COLUMNS,
    )

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=raw_table_path,
        source_rows=[
            _raw_row(
                ts_open="2026-04-01T07:00:00Z",
                ts_close="2026-04-01T07:09:59Z",
                close=100.5,
                run_id="spark-run",
            ),
            _raw_row(
                ts_open="2026-04-01T07:20:00Z",
                ts_close="2026-04-01T07:29:59Z",
                close=101.8,
                run_id="spark-run",
            ),
        ],
        window_scopes=[
            {
                "internal_id": "FUT_BR",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRQ6",
                "window_start_utc": "2026-04-01T06:59:59Z",
                "window_end_utc": "2026-04-01T08:00:00Z",
                "watermark_utc": "2026-04-01T07:29:59Z",
            }
        ],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:29:59Z"},
        run_id="spark-raw-delete",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_overlap_minutes=60,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    rows = _read_test_rows(raw_table_path)
    assert report["incremental_rows"] == 1
    assert {row["ts_close"] for row in rows} == {
        "2026-04-01T07:09:59Z",
        "2026-04-01T07:29:59Z",
    }


def test_raw_ingest_spark_delta_job_reconciles_multiple_scopes_without_window_predicates(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; Docker/Linux proof profile runs this path"
        )

    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(ts_open="2026-04-01T07:00:00Z", ts_close="2026-04-01T07:09:59Z", close=100.5),
            _raw_row(ts_open="2026-04-01T07:10:00Z", ts_close="2026-04-01T07:19:59Z", close=101.2),
            _raw_row(ts_open="2026-04-01T08:00:00Z", ts_close="2026-04-01T08:09:59Z", close=102.5),
            _raw_row(ts_open="2026-04-01T08:10:00Z", ts_close="2026-04-01T08:19:59Z", close=103.2),
        ],
        columns=RAW_COLUMNS,
    )

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=raw_table_path,
        source_rows=[
            _raw_row(
                ts_open="2026-04-01T07:00:00Z",
                ts_close="2026-04-01T07:09:59Z",
                close=99.75,
                run_id="spark-multi-scope",
            ),
            _raw_row(
                ts_open="2026-04-01T08:00:00Z",
                ts_close="2026-04-01T08:09:59Z",
                close=104.25,
                run_id="spark-multi-scope",
            ),
        ],
        window_scopes=[
            {
                "internal_id": "FUT_BR",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRQ6",
                "window_start_utc": "2026-04-01T06:59:59Z",
                "window_end_utc": "2026-04-01T07:20:00Z",
                "watermark_utc": "2026-04-01T07:19:59Z",
            },
            {
                "internal_id": "FUT_BR",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRQ6",
                "window_start_utc": "2026-04-01T07:59:59Z",
                "window_end_utc": "2026-04-01T08:20:00Z",
                "watermark_utc": "2026-04-01T08:19:59Z",
            },
        ],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T08:19:59Z"},
        run_id="spark-multi-scope",
        ingest_till_utc="2026-04-01T08:30:00Z",
        refresh_overlap_minutes=90,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    rows = _read_test_rows(raw_table_path)
    rows_by_ts_close = {str(row["ts_close"]): float(row["close"]) for row in rows}

    assert report["incremental_rows"] == 4
    assert rows_by_ts_close == {
        "2026-04-01T07:09:59Z": 99.75,
        "2026-04-01T08:09:59Z": 104.25,
    }


def test_raw_ingest_spark_delta_job_fingerprint_detects_provider_metadata_change(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; Docker/Linux proof profile runs this path"
        )

    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(
                ts_open="2026-04-01T07:00:00Z",
                ts_close="2026-04-01T07:09:59Z",
                close=100.5,
                moex_board="RFUD",
                provenance_json={"source_provider": "moex_iss", "revision": "old"},
            ),
        ],
        columns=RAW_COLUMNS,
    )

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=raw_table_path,
        source_rows=[
            _raw_row(
                ts_open="2026-04-01T07:00:00Z",
                ts_close="2026-04-01T07:09:59Z",
                close=100.5,
                run_id="spark-run",
                moex_board="RFUD-CORR",
                provenance_json={"source_provider": "moex_iss", "revision": "corrected"},
            ),
        ],
        window_scopes=[
            {
                "internal_id": "FUT_BR",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRQ6",
                "window_start_utc": "2026-04-01T06:59:59Z",
                "window_end_utc": "2026-04-01T07:10:00Z",
                "watermark_utc": "2026-04-01T07:09:59Z",
            }
        ],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:09:59Z"},
        run_id="spark-raw-fingerprint",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_overlap_minutes=60,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    rows = _read_test_rows(raw_table_path)
    provenance = rows[0]["provenance_json"]
    provenance_payload = json.loads(provenance) if isinstance(provenance, str) else provenance
    assert report["incremental_rows"] == 1
    assert rows[0]["moex_board"] == "RFUD-CORR"
    assert provenance_payload["revision"] == "corrected"


def test_raw_ingest_spark_delta_job_ignores_pipeline_provenance_only_changes(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; Docker/Linux proof profile runs this path"
        )

    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    existing = _raw_row(
        ts_open="2026-04-01T07:10:00Z",
        ts_close="2026-04-01T07:19:59Z",
        close=101.2,
        run_id="old-run",
        provenance_json={
            "source_provider": "moex_iss",
            "source_interval": 1,
            "source_timeframe": "1m",
            "requested_target_timeframes": "5m,15m",
            "run_id": "old-run",
            "window_start_utc": "2026-04-01T04:19:59Z",
            "window_end_utc": "2026-04-01T07:19:59Z",
            "stability_lag_minutes": 20,
            "refresh_overlap_minutes": 180,
            "discovery_url": "https://iss.moex.com/old/candleborders.json",
        },
    )
    write_delta_table_rows(table_path=raw_table_path, rows=[existing], columns=RAW_COLUMNS)
    version_before = sorted((raw_table_path / "_delta_log").glob("*.json"))[-1].name

    source = dict(existing)
    source["ingest_run_id"] = "new-run"
    source["ingested_at_utc"] = "2026-04-01T08:00:00Z"
    source["provenance_json"] = {
        "source_provider": "moex_iss",
        "source_interval": 1,
        "source_timeframe": "1m",
        "requested_target_timeframes": "1m,5m,15m",
        "run_id": "new-run",
        "window_start_utc": "2026-04-01T04:19:59Z",
        "window_end_utc": "2026-04-01T07:19:59Z",
        "stability_lag_minutes": 0,
        "refresh_overlap_minutes": 180,
        "discovery_url": "local-tail://canonical-roll-map/FUT_BR/1/BRQ6/2026-04-01",
    }

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=raw_table_path,
        source_rows=[source],
        window_scopes=[
            {
                "internal_id": "FUT_BR",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRQ6",
                "window_start_utc": "2026-04-01T07:10:00Z",
                "window_end_utc": "2026-04-01T07:19:59Z",
                "watermark_utc": "2026-04-01T07:09:59Z",
            }
        ],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:09:59Z"},
        run_id="spark-provenance-only",
        ingest_till_utc="2026-04-01T07:19:59Z",
        refresh_overlap_minutes=180,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )
    version_after = sorted((raw_table_path / "_delta_log").glob("*.json"))[-1].name

    assert report["status"] == "PASS-NOOP"
    assert report["source_rows"] == 1
    assert report["incremental_rows"] == 0
    assert report["deduplicated_rows"] == 1
    assert report["changed_windows"] == []
    assert version_after == version_before


def test_raw_close_year_uses_utc_for_borrowed_moscow_ltz_and_rejects_ntz() -> None:
    from support.spark_runtime import require_configured_spark_delta_profile

    from trading_advisor_3000.product_plane.common.table_layout import with_spark_close_year

    require_configured_spark_delta_profile()
    spark = raw_ingest_job._create_spark_session("raw-close-year-timezones", "local[2]")
    _, _, functions, _ = raw_ingest_job._load_spark_modules()
    try:
        spark.conf.set("spark.sql.session.timeZone", "Europe/Moscow")
        source = spark.createDataFrame(
            [("2026-01-01T00:00:00Z", 1999), ("2026-01-01T01:00:00+03:00", 1999)],
            "close_text string, ts_close_year int",
        ).withColumn("ts_close", functions.to_timestamp("close_text"))
        output = with_spark_close_year(source, functions=functions).orderBy("close_text")
        assert [row.ts_close_year for row in output.select("ts_close_year").collect()] == [
            2026,
            2025,
        ]
        assert output.select("ts_close").exceptAll(source.select("ts_close")).count() == 0
        ntz = spark.sql(
            "SELECT TIMESTAMP_NTZ '2026-01-01 00:00:00' AS ts_close, 1999 AS ts_close_year"
        )
        with pytest.raises(ValueError, match="timestamp_ntz"):
            with_spark_close_year(ntz, functions=functions)
        assert spark.conf.get("spark.sql.session.timeZone") == "Europe/Moscow"
    finally:
        spark.stop()


def test_raw_stable_owner_preserves_pins_through_append_correction_delete_noop(
    tmp_path: Path,
) -> None:
    from deltalake import DeltaTable
    from support.spark_runtime import require_configured_spark_delta_profile

    from trading_advisor_3000.product_plane.common.table_layout import MAX_COMPRESSED_FILE_BYTES

    require_configured_spark_delta_profile()
    target = tmp_path / "raw_moex_history.delta"
    paths = {
        "progress_path": tmp_path / "progress.jsonl",
        "progress_latest_path": tmp_path / "progress.json",
        "error_path": tmp_path / "errors.jsonl",
        "error_latest_path": tmp_path / "errors.json",
    }
    first = _raw_row(ts_open="2025-12-31T23:59:00Z", ts_close="2026-01-01T00:00:00Z", close=100.0)
    second = _raw_row(ts_open="2026-01-01T00:00:00Z", ts_close="2026-01-01T00:01:00Z", close=101.0)
    scope = {
        "internal_id": "FUT_BR",
        "timeframe": "1m",
        "source_interval": 1,
        "moex_secid": "BRQ6",
        "window_start_utc": "2025-12-31T23:58:00Z",
        "window_end_utc": "2026-01-01T00:01:00Z",
        "watermark_utc": None,
    }

    def run(rows, run_id):
        return run_moex_raw_ingest_spark_delta_job(
            table_path=target,
            source_rows=rows,
            window_scopes=[scope],
            initial_watermarks={},
            run_id=run_id,
            ingest_till_utc="2026-01-01T00:01:00Z",
            refresh_overlap_minutes=60,
            **paths,
        )

    assert run([first], "create")["status"] == "PASS"
    initial = DeltaTable(str(target))
    identity, pin = initial.metadata().id, initial.version()
    assert run([first, second], "append")["status"] == "PASS"
    revised = {**first, "close": 99.75}
    assert run([revised, second], "correct")["status"] == "PASS"
    assert run([revised], "delete")["status"] == "PASS"
    current = DeltaTable(str(target))
    version = current.version()
    assert run([revised], "noop")["status"] == "PASS-NOOP"
    current = DeltaTable(str(target))
    assert current.metadata().id == identity
    assert current.version() == version
    assert current.metadata().partition_columns == ["ts_close_year"]
    assert current.to_pyarrow_table().column("close").to_pylist() == [99.75]
    assert current.to_pyarrow_table().column("ts_close_year").to_pylist() == [2026]
    assert DeltaTable(str(target), version=pin).to_pyarrow_table().column("close").to_pylist() == [
        100.0
    ]
    actions = current.get_add_actions(flatten=True).to_pylist()
    assert len(actions) == 1
    assert 0 < actions[0]["size_bytes"] <= MAX_COMPRESSED_FILE_BYTES
    assert actions[0]["num_records"] == 1


@pytest.mark.parametrize("ntz_source", [False, True], ids=["moscow-ltz", "reject-native-ntz"])
def test_raw_migration_owns_utc_year_or_rejects_ntz_before_target_write(
    tmp_path: Path,
    ntz_source: bool,
) -> None:
    from deltalake import DeltaTable
    from support.spark_runtime import require_configured_spark_delta_profile

    from trading_advisor_3000.product_plane.common.table_layout import validate_default_table_files
    from trading_advisor_3000.spark_jobs import moex_raw_layout_migration_job as migration

    require_configured_spark_delta_profile()
    source_path = tmp_path / "source.delta"
    target_path = tmp_path / "raw_moex_history.layout-staged.delta"
    spark = raw_ingest_job._create_spark_session("raw-migration-native-close", "local[2]")
    _, _, functions, _ = raw_ingest_job._load_spark_modules()
    from pyspark.sql import types as spark_types

    spark.conf.set("spark.sql.session.timeZone", "Europe/Moscow")
    rows = [
        _raw_row(ts_open="2025-12-31T23:59:00Z", ts_close="2026-01-01T00:00:00Z", close=100.0),
        _raw_row(ts_open="2025-12-31T20:59:00Z", ts_close="2026-01-01T01:00:00+03:00", close=101.0),
    ]
    try:
        source = spark.createDataFrame(
            [raw_ingest_job._normalize_raw_row(row, index) for index, row in enumerate(rows)],
            schema=raw_ingest_job._raw_schema(spark_types),
        ).withColumn("ts_close_year", functions.lit(1999))
        if ntz_source:
            source = source.withColumn("ts_close", functions.col("ts_close").cast("timestamp_ntz"))
        source.write.format("delta").mode("overwrite").save(str(source_path))
        original = DeltaTable(str(source_path))
        identity, pin = original.metadata().id, original.version()
        report_path = tmp_path / "migration-report.json"
        args = dict(
            source_table_path=source_path,
            staged_table_path=target_path,
            report_path=report_path,
            run_id="native-migration-close",
            spark_session_factory=lambda *_: spark,
        )
        if ntz_source:
            with pytest.raises(ValueError, match="timestamp_ntz"):
                migration.run_moex_raw_layout_migration_spark_job(**args)
            assert not target_path.exists()
            assert not report_path.exists()
        else:
            report = migration.run_moex_raw_layout_migration_spark_job(**args)
            assert report["status"] == "PASS"
            assert report["storage_file_policy_passes"] is True
            current = DeltaTable(str(target_path))
            assert current.metadata().partition_columns == ["ts_close_year"]
            output = current.to_pyarrow_table().to_pylist()
            assert sorted(row["ts_close_year"] for row in output) == [2025, 2026]
            assert sorted(row["close"] for row in output) == [100.0, 101.0]
            expected = (
                DeltaTable(str(source_path), version=pin)
                .to_pyarrow_table()
                .select(list(RAW_COLUMNS))
            )
            actual = current.to_pyarrow_table().select(list(RAW_COLUMNS))
            assert sorted(actual.to_pylist(), key=lambda row: row["ts_close"]) == sorted(
                expected.to_pylist(), key=lambda row: row["ts_close"]
            )
            validate_default_table_files(target_path)
        preserved = DeltaTable(str(source_path))
        assert (preserved.metadata().id, preserved.version()) == (identity, pin)
    finally:
        spark.stop()


def test_raw_stable_owner_empty_bootstrap_and_empty_retry_are_readonly(tmp_path: Path) -> None:
    from deltalake import DeltaTable
    from support.spark_runtime import require_configured_spark_delta_profile

    require_configured_spark_delta_profile()
    target = tmp_path / "raw_moex_history.delta"
    args = dict(
        table_path=target,
        source_rows=[],
        window_scopes=[],
        initial_watermarks={},
        ingest_till_utc="2026-01-01T00:00:00Z",
        refresh_overlap_minutes=0,
        progress_path=tmp_path / "progress.jsonl",
        progress_latest_path=tmp_path / "progress.json",
        error_path=tmp_path / "errors.jsonl",
        error_latest_path=tmp_path / "errors.json",
    )
    assert (
        run_moex_raw_ingest_spark_delta_job(run_id="empty-create", **args)["status"] == "PASS-NOOP"
    )
    original = DeltaTable(str(target))
    identity, pin = original.metadata().id, original.version()
    assert original.metadata().partition_columns == ["ts_close_year"]
    assert original.get_add_actions(flatten=True).num_rows == 0
    assert (
        run_moex_raw_ingest_spark_delta_job(run_id="empty-retry", **args)["status"] == "PASS-NOOP"
    )
    current = DeltaTable(str(target))
    assert (current.metadata().id, current.version()) == (identity, pin)
    assert current.get_add_actions(flatten=True).num_rows == 0
