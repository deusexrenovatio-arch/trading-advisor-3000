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
        initial_watermarks={("FUT_BR", "1m", 1, "BRQ6"): "2026-04-01T07:19:59Z"},
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
        initial_watermarks={("FUT_BR", "1m", 1, "BRQ6"): "2026-04-01T07:29:59Z"},
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
        initial_watermarks={("FUT_BR", "1m", 1, "BRQ6"): "2026-04-01T08:19:59Z"},
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
        initial_watermarks={("FUT_BR", "1m", 1, "BRQ6"): "2026-04-01T07:09:59Z"},
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
