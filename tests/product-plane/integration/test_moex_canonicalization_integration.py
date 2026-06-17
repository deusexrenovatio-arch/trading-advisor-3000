from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    iter_delta_table_row_batches,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex import build_raw_ingest_run_report_v2
from trading_advisor_3000.product_plane.data_plane.moex import (
    canonicalization as canonicalization_module,
)
from trading_advisor_3000.product_plane.data_plane.moex.canonicalization import (
    run_moex_canonicalization,
)
from trading_advisor_3000.spark_jobs.moex_canonicalization_job import (
    run_moex_canonicalization_spark_delta_job,
    run_moex_canonicalization_spark_job,
)

RAW_COLUMNS: dict[str, str] = {
    "internal_id": "string",
    "finam_symbol": "string",
    "moex_secid": "string",
    "timeframe": "string",
    "source_interval": "int",
    "ts_open": "timestamp",
    "ts_close": "timestamp",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "bigint",
    "open_interest": "bigint",
    "ingest_run_id": "string",
    "ingested_at_utc": "timestamp",
    "provenance_json": "json",
}

SESSION_INTERVAL_COLUMNS: dict[str, str] = {
    "instrument_id": "string",
    "session_date": "date",
    "interval_id": "string",
    "interval_seq": "int",
    "expected_open_ts": "timestamp",
    "expected_close_ts": "timestamp",
    "session_class": "string",
    "interval_type": "string",
    "policy_id": "string",
    "source_id": "string",
    "source_document_hash": "string",
}


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_batched_delta_rows(table_path: Path) -> list[dict[str, object]]:
    return [
        row
        for batch in iter_delta_table_row_batches(
            table_path,
            filters=[("timeframe", "in", ["1m", "5m", "15m", "1h", "4h", "1d", "1w"])],
        )
        for row in batch
    ]


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )


def _raw_rows(*, with_source_provider: bool) -> list[dict[str, object]]:
    provider = "moex_iss" if with_source_provider else ""
    start = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for minute in range(20):
        ts_open = start + timedelta(minutes=minute)
        rows.append(
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6@MOEX",
                "moex_secid": "BRM6",
                "timeframe": "1m",
                "source_interval": 1,
                "ts_open": _iso(ts_open),
                "ts_close": _iso(ts_open + timedelta(minutes=1)),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10 + minute,
                "open_interest": None,
                "ingest_run_id": "raw-ingest-pass1",
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
                "provenance_json": {
                    "source_provider": provider,
                    "source_interval": 1,
                    "source_timeframe": "1m",
                    "run_id": "raw-ingest-pass1",
                    "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json",
                },
            }
        )
    return rows


def _raw_rows_with_daily_only_contract(*, with_source_provider: bool) -> list[dict[str, object]]:
    rows = _raw_rows(with_source_provider=with_source_provider)
    provider = "moex_iss" if with_source_provider else ""
    start = datetime(2026, 3, 28, 21, 0, tzinfo=UTC)
    for day in range(3):
        ts_open = start + timedelta(days=day)
        rows.append(
            {
                "internal_id": "FUT_WHEAT",
                "finam_symbol": "W4J6@MOEX",
                "moex_secid": "W4J6",
                "timeframe": "1d",
                "source_interval": 24,
                "ts_open": _iso(ts_open),
                "ts_close": _iso(ts_open + timedelta(days=1)),
                "open": 200.0 + day,
                "high": 205.0 + day,
                "low": 195.0 + day,
                "close": 202.0 + day,
                "volume": 200 + day,
                "open_interest": None,
                "ingest_run_id": "raw-ingest-pass1",
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
                "provenance_json": {
                    "source_provider": provider,
                    "source_interval": 24,
                    "source_timeframe": "1d",
                    "run_id": "raw-ingest-pass1",
                    "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/W4J6/candleborders.json",
                },
            }
        )
    return rows


def _write_raw_table(path: Path, rows: list[dict[str, object]]) -> None:
    write_delta_table_rows(table_path=path, rows=rows, columns=RAW_COLUMNS)


def _session_intervals_for_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    sessions = sorted(
        {
            (str(row["internal_id"]), str(row["ts_open"])[:10])
            for row in rows
            if str(row["timeframe"]) == "1m"
        }
    )
    return [
        {
            "instrument_id": instrument_id,
            "session_date": session_date,
            "interval_id": f"{instrument_id}-{session_date}-regular-1",
            "interval_seq": 1,
            "expected_open_ts": f"{session_date}T10:00:00Z",
            "expected_close_ts": f"{session_date}T18:45:00Z",
            "session_class": "regular",
            "interval_type": "regular_trading",
            "policy_id": "moex-official-session-v1",
            "source_id": "moex-official-schedule-fixture",
            "source_document_hash": "sha256:fixture",
        }
        for instrument_id, session_date in sessions
    ]


def _write_session_intervals(path: Path, rows: list[dict[str, object]]) -> Path:
    write_delta_table_rows(table_path=path, rows=rows, columns=SESSION_INTERVAL_COLUMNS)
    return path


def _build_raw_ingest_report_for_rows(
    *,
    rows: list[dict[str, object]],
    run_id: str,
) -> dict[str, object]:
    windows_by_key: dict[tuple[str, str, int, str], dict[str, object]] = {}
    watermarks: dict[str, str] = {}
    for row in rows:
        internal_id = str(row["internal_id"])
        timeframe = str(row["timeframe"])
        source_interval = int(row["source_interval"])
        moex_secid = str(row.get("moex_secid") or row["finam_symbol"])
        ts_open = str(row["ts_open"])
        ts_close = str(row["ts_close"])
        key = (internal_id, timeframe, source_interval, moex_secid)
        bucket = windows_by_key.get(key)
        if bucket is None:
            windows_by_key[key] = {
                "internal_id": internal_id,
                "source_timeframe": timeframe,
                "source_interval": source_interval,
                "moex_secid": moex_secid,
                "window_start_utc": ts_open,
                "window_end_utc": ts_close,
                "incremental_rows": 1,
            }
        else:
            bucket["window_start_utc"] = min(str(bucket["window_start_utc"]), ts_open)
            bucket["window_end_utc"] = max(str(bucket["window_end_utc"]), ts_close)
            bucket["incremental_rows"] = int(bucket["incremental_rows"]) + 1
        watermark_key = f"{internal_id}|{timeframe}|{moex_secid}"
        current = watermarks.get(watermark_key, "")
        if ts_close > current:
            watermarks[watermark_key] = ts_close

    changed_windows = list(windows_by_key.values())
    return build_raw_ingest_run_report_v2(
        run_id=run_id,
        ingest_till_utc=max(str(row["ts_close"]) for row in rows),
        source_rows=len(rows),
        incremental_rows=len(rows),
        deduplicated_rows=0,
        stale_rows=0,
        watermark_by_key=watermarks,
        raw_table_path="raw-ingest/raw_moex_history.delta",
        raw_ingest_progress_path="raw-ingest/raw-ingest-progress.jsonl",
        raw_ingest_error_path="raw-ingest/raw-ingest-errors.jsonl",
        raw_ingest_error_latest_path="raw-ingest/raw-ingest-error.latest.json",
        changed_windows=changed_windows,
    )


def _build_raw_ingest_report_noop(*, run_id: str) -> dict[str, object]:
    return build_raw_ingest_run_report_v2(
        run_id=run_id,
        ingest_till_utc="2026-04-02T10:20:00Z",
        source_rows=0,
        incremental_rows=0,
        deduplicated_rows=0,
        stale_rows=0,
        watermark_by_key={},
        raw_table_path="raw-ingest/raw_moex_history.delta",
        raw_ingest_progress_path="raw-ingest/raw-ingest-progress.jsonl",
        raw_ingest_error_path="raw-ingest/raw-ingest-errors.jsonl",
        raw_ingest_error_latest_path="raw-ingest/raw-ingest-error.latest.json",
        changed_windows=[],
    )


def test_canonicalization_generates_resampling_outputs_and_reports(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization",
        run_id="canonicalization-int-pass",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
        session_intervals_path=session_intervals_path,
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["canonicalization_engine"] == "spark"
    assert report["qc_report"]["status"] == "PASS"
    assert report["contract_compatibility_report"]["status"] == "PASS"
    assert report["runtime_decoupling_proof"]["status"] == "PASS"
    assert report["output_paths"]["canonical_bars"]
    assert report["output_paths"]["canonical_bar_provenance"]
    assert report["spark_execution_report"]["engine"] == "spark"
    assert report["spark_execution_report"]["proof_profile"] in {"docker-linux", "local-spark"}
    assert report["real_bindings"]

    bars = _read_batched_delta_rows(Path(str(report["output_paths"]["canonical_bars"])))
    assert bars
    assert {row["timeframe"] for row in bars} == {"1m", "5m", "15m", "1h", "4h", "1d", "1w"}
    first_m5 = next(
        row
        for row in bars
        if row["contract_id"] == "BRM6@MOEX"
        and row["instrument_id"] == "FUT_BR"
        and row["timeframe"] == "5m"
        and row["ts"] == "2026-04-02T10:00:00Z"
    )
    assert first_m5["open"] == 100.0
    assert first_m5["close"] == 104.5
    assert first_m5["high"] == 105.0
    assert first_m5["low"] == 99.0
    assert first_m5["volume"] == sum(10 + i for i in range(5))
    assert first_m5["open_interest"] == 0
    for artifact_path in report["artifact_paths"].values():
        assert Path(str(artifact_path)).exists()


def test_canonicalization_is_fail_closed_when_qc_fails(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=False)
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-blocked",
        run_id="canonicalization-int-fail",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
        session_intervals_path=session_intervals_path,
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert report["canonicalization_engine"] == "spark"
    assert report["qc_report"]["status"] == "FAIL"
    assert "provenance_completeness" in report["qc_report"]["failed_gates"]
    assert report["spark_execution_report"]["engine"] == "spark"
    assert report["output_paths"] == {}
    qc_report_path = Path(str(report["artifact_paths"]["qc_report"]))
    payload = json.loads(qc_report_path.read_text(encoding="utf-8"))
    assert payload["publish_decision"] == "blocked"


def test_canonicalization_blocks_without_official_session_intervals(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-missing-sessions",
        run_id="canonicalization-missing-sessions",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert report["session_intervals_path"] == ""
    assert report["session_intervals_mode"] == "manual_session_intervals_missing_blocked"
    assert report["session_admission_gate"]["failed_gates"] == ["official_schedule_missing_input"]
    assert report["spark_execution_report"] is None


def test_canonicalization_rejects_raw_minutes_outside_official_intervals(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        [
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-02",
                "interval_id": "FUT_BR-2026-04-02-regular-1",
                "interval_seq": 1,
                "expected_open_ts": "2026-04-02T10:00:00Z",
                "expected_close_ts": "2026-04-02T10:10:00Z",
                "session_class": "partial_or_gap",
                "interval_type": "regular_trading",
                "policy_id": "moex-official-session-v1",
                "source_id": "moex-official-schedule-fixture",
                "source_document_hash": "sha256:fixture",
            }
        ],
    )

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-session-bounded",
        run_id="canonicalization-session-bounded",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
        session_intervals_path=session_intervals_path,
    )

    admission = report["spark_execution_report"]["session_admission_report"]
    assert admission["admitted_source_rows"] == 10
    assert admission["rejected_out_of_session_rows"] == 10
    assert report["publish_decision"] == "publish"
    assert report["session_admission_gate"]["failed_gates"] == []


def test_spark_delta_canonicalization_admits_direct_daily_source_with_official_coverage(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    row = {
        "internal_id": "FUT_BR",
        "finam_symbol": "BRM6@MOEX",
        "moex_secid": "BRM6",
        "timeframe": "1d",
        "source_interval": 24,
        "ts_open": "2026-04-01T21:00:00Z",
        "ts_close": "2026-04-02T21:00:00Z",
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 102.0,
        "volume": 10,
        "open_interest": None,
        "ingest_run_id": "raw-ingest-daily",
        "ingested_at_utc": "2026-04-02T21:01:00Z",
        "provenance_json": {
            "source_provider": "moex_iss",
            "source_interval": 24,
            "source_timeframe": "1d",
            "run_id": "raw-ingest-daily",
            "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json",
        },
    }
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[row],
        columns={**RAW_COLUMNS, "moex_secid": "string"},
    )
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        [
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-02",
                "interval_id": "FUT_BR-2026-04-02-regular-1",
                "interval_seq": 1,
                "expected_open_ts": "2026-04-02T10:00:00Z",
                "expected_close_ts": "2026-04-02T18:45:00Z",
                "session_class": "regular",
                "interval_type": "regular_trading",
                "policy_id": "moex-official-session-v1",
                "source_id": "moex-official-schedule-fixture",
                "source_document_hash": "sha256:fixture",
            }
        ],
    )
    changed_windows_path = tmp_path / "spark" / "changed-windows.jsonl"
    selected_intervals_path = tmp_path / "spark" / "selected-source-intervals.jsonl"
    changed_windows_path.parent.mkdir(parents=True, exist_ok=True)
    changed_windows_path.write_text(
        json.dumps(
            {
                "internal_id": "FUT_BR",
                "source_timeframe": "1d",
                "source_interval": 1440,
                "moex_secid": "BRM6",
                "window_start_utc": "2026-04-01T21:00:00Z",
                "window_end_utc": "2026-04-02T21:00:00Z",
                "incremental_rows": 1,
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    selected_intervals_path.write_text(
        json.dumps(
            {
                "contract_id": "BRM6",
                "moex_secid": "BRM6",
                "instrument_id": "FUT_BR",
                "timeframe": "1d",
                "target_minutes": 1440,
                "source_interval": 1440,
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    report = run_moex_canonicalization_spark_delta_job(
        raw_table_path=raw_table_path,
        changed_windows_path=changed_windows_path,
        selected_source_intervals_path=selected_intervals_path,
        session_intervals_path=session_intervals_path,
        output_dir=tmp_path / "spark" / "canonicalization-daily",
        build_run_id="canonicalization-daily",
        built_at_utc="2026-04-02T22:00:00Z",
        spark_master="local[2]",
    )

    assert report["source_rows"] == 1
    assert report["canonical_rows"] == 1
    assert report["provenance_rows"] == 1
    admission = report["session_admission_report"]
    assert admission["admitted_source_rows"] == 1
    assert admission["rejected_non_1m_source_rows"] == 0
    bars = _read_batched_delta_rows(Path(str(report["output_paths"]["canonical_bars"])))
    assert len(bars) == 1
    assert bars[0]["contract_id"] == "BRM6@MOEX"
    assert bars[0]["timeframe"] == "1d"
    assert bars[0]["ts"] == "2026-04-02T00:00:00Z"


def test_canonicalization_admits_moex_opening_minute_begin_end_label(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows: list[dict[str, object]] = []
    start = datetime(2026, 4, 2, 9, 59, tzinfo=UTC)
    for minute in range(2):
        ts_open = start + timedelta(minutes=minute)
        rows.append(
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6@MOEX",
                "moex_secid": "BRM6",
                "timeframe": "1m",
                "source_interval": 1,
                "ts_open": _iso(ts_open),
                "ts_close": _iso(ts_open + timedelta(seconds=59)),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10 + minute,
                "open_interest": None,
                "ingest_run_id": "raw-ingest-open-minute",
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
                "provenance_json": {
                    "source_provider": "moex_iss",
                    "source_interval": 1,
                    "source_timeframe": "1m",
                    "run_id": "raw-ingest-open-minute",
                    "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json",
                },
            }
        )
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        [
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-02",
                "interval_id": "FUT_BR-2026-04-02-regular-1",
                "interval_seq": 1,
                "expected_open_ts": "2026-04-02T10:00:00Z",
                "expected_close_ts": "2026-04-02T10:05:00Z",
                "session_class": "regular",
                "interval_type": "regular_trading",
                "policy_id": "moex-official-session-v1",
                "source_id": "moex-official-schedule-fixture",
                "source_document_hash": "sha256:fixture",
            }
        ],
    )

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-open-minute",
        run_id="canonicalization-open-minute",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-open-minute"
        ),
        session_intervals_path=session_intervals_path,
    )

    admission = report["spark_execution_report"]["session_admission_report"]
    assert admission["admission_open_tolerance_seconds"] == 60
    assert admission["admitted_source_rows"] == 2
    assert admission["rejected_out_of_session_rows"] == 0
    assert report["publish_decision"] == "publish"


def test_canonicalization_ignores_corroborated_opening_boundary_hour_bucket(
    tmp_path: Path,
) -> None:
    opening_minute_open = datetime(2026, 4, 2, 9, 59, tzinfo=UTC)
    source_rows = [
        {
            "contract_id": "BRM6@MOEX",
            "instrument_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "ts_open": _iso(opening_minute_open),
            "ts_close": _iso(opening_minute_open + timedelta(seconds=59)),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10,
            "open_interest": 0,
            "open_interest_imputed": True,
            "source_provider": "moex_iss",
            "source_run_id": "raw-ingest-opening-boundary",
            "source_ingest_run_id": "raw-ingest-opening-boundary",
        },
        {
            "contract_id": "BRM6@MOEX",
            "instrument_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "ts_open": _iso(opening_minute_open + timedelta(minutes=1)),
            "ts_close": _iso(opening_minute_open + timedelta(minutes=1, seconds=59)),
            "open": 101.0,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume": 11,
            "open_interest": 0,
            "open_interest_imputed": True,
            "source_provider": "moex_iss",
            "source_run_id": "raw-ingest-opening-boundary",
            "source_ingest_run_id": "raw-ingest-opening-boundary",
        },
        {
            "contract_id": "BRM6@MOEX",
            "instrument_id": "FUT_BR",
            "source_timeframe": "1h",
            "source_interval": 60,
            "ts_open": _iso(opening_minute_open.replace(minute=0)),
            "ts_close": _iso(opening_minute_open + timedelta(seconds=59)),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10,
            "open_interest": 0,
            "open_interest_imputed": True,
            "source_provider": "moex_iss",
            "source_run_id": "raw-ingest-opening-boundary",
            "source_ingest_run_id": "raw-ingest-opening-boundary",
        },
    ]
    source_path = tmp_path / "spark" / "normalized-source.jsonl"
    session_intervals_path = tmp_path / "spark" / "session-intervals.jsonl"
    selected_intervals_path = tmp_path / "spark" / "selected-source-intervals.jsonl"
    _write_jsonl(source_path, source_rows)
    _write_jsonl(
        session_intervals_path,
        [
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-02",
                "interval_id": "FUT_BR-2026-04-02-regular-1",
                "interval_seq": 1,
                "expected_open_ts": "2026-04-02T10:00:00Z",
                "expected_close_ts": "2026-04-02T10:15:00Z",
                "session_class": "regular",
                "interval_type": "regular_trading",
                "policy_id": "moex-official-session-v1",
                "source_id": "moex-official-schedule-fixture",
                "source_document_hash": "sha256:fixture",
            }
        ],
    )
    _write_jsonl(
        selected_intervals_path,
        [
            {
                "contract_id": "BRM6@MOEX",
                "moex_secid": "BRM6",
                "instrument_id": "FUT_BR",
                "timeframe": "15m",
                "target_minutes": 15,
                "source_interval": 1,
            },
            {
                "contract_id": "BRM6@MOEX",
                "moex_secid": "BRM6",
                "instrument_id": "FUT_BR",
                "timeframe": "1h",
                "target_minutes": 60,
                "source_interval": 60,
            },
        ],
    )

    report = run_moex_canonicalization_spark_job(
        normalized_source_path=source_path,
        selected_source_intervals_path=selected_intervals_path,
        session_intervals_path=session_intervals_path,
        output_dir=tmp_path / "spark" / "canonicalization-opening-boundary-hour",
        build_run_id="canonicalization-opening-boundary-hour",
        built_at_utc="2026-04-02T10:10:00Z",
        spark_master="local[2]",
    )

    admission = report["session_admission_report"]
    assert admission["admitted_source_rows"] == 2
    assert admission["out_of_schedule_rows"] == 1
    assert admission["ignored_out_of_schedule_rows"] == 1
    assert admission["ignored_out_of_schedule_reason_counts"] == {"finer_covered_coarse_bucket": 1}
    assert admission["ignored_opening_boundary_rows"] == 1
    assert admission["rejected_out_of_session_rows"] == 0
    assert report["canonical_rows"] == 1


def test_canonicalization_classifies_finer_covered_and_uncorroborated_coarse_buckets(
    tmp_path: Path,
) -> None:
    session_start = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)
    source_rows: list[dict[str, object]] = []
    instruments = [
        ("BRM6@MOEX", "FUT_BR", 100.0, 12, "nonfatal"),
        ("NGM6@MOEX", "FUT_NG", 200.0, 13, "fatal"),
    ]
    for contract_id, instrument_id, base_price, coarse_volume, _case in instruments:
        minute_open = session_start + timedelta(minutes=28)
        for offset, volume in enumerate([5, 7]):
            ts_open = minute_open + timedelta(minutes=offset)
            source_rows.append(
                {
                    "contract_id": contract_id,
                    "instrument_id": instrument_id,
                    "source_timeframe": "1m",
                    "source_interval": 1,
                    "ts_open": _iso(ts_open),
                    "ts_close": _iso(ts_open + timedelta(minutes=1)),
                    "open": base_price + (2 * offset),
                    "high": base_price + 3 + offset,
                    "low": base_price - 1 + (2 * offset),
                    "close": base_price + 2 + offset,
                    "volume": volume,
                    "open_interest": 0,
                    "open_interest_imputed": True,
                    "source_provider": "moex_iss",
                    "source_run_id": "raw-ingest-coarse-bucket",
                    "source_ingest_run_id": "raw-ingest-coarse-bucket",
                }
            )
        source_rows.append(
            {
                "contract_id": contract_id,
                "instrument_id": instrument_id,
                "source_timeframe": "1h",
                "source_interval": 60,
                "ts_open": _iso(session_start),
                "ts_close": _iso(session_start + timedelta(minutes=59, seconds=59)),
                "open": base_price,
                "high": base_price + 4,
                "low": base_price - 1,
                "close": base_price + 3,
                "volume": coarse_volume,
                "open_interest": 0,
                "open_interest_imputed": True,
                "source_provider": "moex_iss",
                "source_run_id": "raw-ingest-coarse-bucket",
                "source_ingest_run_id": "raw-ingest-coarse-bucket",
            }
        )

    source_path = tmp_path / "spark" / "normalized-source.jsonl"
    session_intervals_path = tmp_path / "spark" / "session-intervals.jsonl"
    selected_intervals_path = tmp_path / "spark" / "selected-source-intervals.jsonl"
    _write_jsonl(source_path, source_rows)
    _write_jsonl(
        session_intervals_path,
        [
            {
                "instrument_id": instrument_id,
                "session_date": "2026-04-02",
                "interval_id": f"{instrument_id}-2026-04-02-regular-1",
                "interval_seq": 1,
                "expected_open_ts": "2026-04-02T10:00:00Z",
                "expected_close_ts": "2026-04-02T10:30:00Z",
                "session_class": "regular",
                "interval_type": "regular_trading",
                "policy_id": "moex-official-session-v1",
                "source_id": "moex-official-schedule-fixture",
                "source_document_hash": "sha256:fixture",
            }
            for _contract_id, instrument_id, _base_price, _coarse_volume, _case in instruments
        ],
    )
    _write_jsonl(
        selected_intervals_path,
        [
            {
                "contract_id": contract_id,
                "moex_secid": contract_id.split("@", maxsplit=1)[0],
                "instrument_id": instrument_id,
                "timeframe": timeframe,
                "target_minutes": target_minutes,
                "source_interval": source_interval,
            }
            for contract_id, instrument_id, _base_price, _coarse_volume, _case in instruments
            for timeframe, target_minutes, source_interval in [("15m", 15, 1), ("1h", 60, 60)]
        ],
    )

    report = run_moex_canonicalization_spark_job(
        normalized_source_path=source_path,
        selected_source_intervals_path=selected_intervals_path,
        session_intervals_path=session_intervals_path,
        output_dir=tmp_path / "spark" / "canonicalization-coarse-buckets",
        build_run_id="canonicalization-coarse-buckets",
        built_at_utc="2026-04-02T10:35:00Z",
        spark_master="local[2]",
    )

    admission = report["session_admission_report"]
    assert admission["admitted_source_rows"] == 4
    assert admission["out_of_schedule_rows"] == 2
    assert admission["ignored_out_of_schedule_rows"] == 1
    assert admission["ignored_out_of_schedule_reason_counts"] == {"finer_covered_coarse_bucket": 1}
    assert admission["ignored_opening_boundary_rows"] == 0
    assert admission["rejected_out_of_session_rows"] == 1
    assert report["canonical_rows"] == 2


def test_canonicalization_uses_moscow_session_date_for_utc_boundary(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    start = datetime(2026, 4, 21, 21, 15, tzinfo=UTC)
    rows = []
    for minute in range(10):
        ts_open = start + timedelta(minutes=minute)
        rows.append(
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6@MOEX",
                "moex_secid": "BRM6",
                "timeframe": "1m",
                "source_interval": 1,
                "ts_open": _iso(ts_open),
                "ts_close": _iso(ts_open + timedelta(minutes=1)),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10 + minute,
                "open_interest": None,
                "ingest_run_id": "raw-ingest-boundary",
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
                "provenance_json": {
                    "source_provider": "moex_iss",
                    "source_interval": 1,
                    "source_timeframe": "1m",
                    "run_id": "raw-ingest-boundary",
                    "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json",
                },
            }
        )
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        [
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-22",
                "interval_id": "FUT_BR-2026-04-22-overnight-1",
                "interval_seq": 1,
                "expected_open_ts": "2026-04-21T21:15:00Z",
                "expected_close_ts": "2026-04-21T21:25:00Z",
                "session_class": "regular",
                "interval_type": "evening_session",
                "policy_id": "moex-official-session-v1",
                "source_id": "moex-official-schedule-fixture",
                "source_document_hash": "sha256:fixture",
            }
        ],
    )

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-session-boundary",
        run_id="canonicalization-session-boundary",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-boundary"
        ),
        session_intervals_path=session_intervals_path,
    )

    admission = report["spark_execution_report"]["session_admission_report"]
    assert admission["missing_official_coverage_rows"] == 0
    assert admission["rejected_out_of_session_rows"] == 0
    assert report["publish_decision"] == "publish"


def test_canonicalization_normalizes_ohlc_envelope_from_raw_1m(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    rows[0]["high"] = 101.0
    rows[0]["close"] = 102.0
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-ohlc-envelope",
        run_id="canonicalization-ohlc-envelope",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-ohlc-envelope"
        ),
        session_intervals_path=session_intervals_path,
    )

    assert report["qc_report"]["status"] == "PASS"
    bars = _read_batched_delta_rows(
        Path(str(report["spark_execution_report"]["output_paths"]["canonical_bars"]))
    )
    minute_bar = next(
        row for row in bars if row["timeframe"] == "1m" and row["ts"] == "2026-04-02T10:00:00Z"
    )
    assert minute_bar["high"] == 102.0
    assert minute_bar["low"] == 99.0


def test_canonicalization_reports_skips_for_incompatible_daily_only_contract(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows_with_daily_only_contract(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-mixed",
        run_id="canonicalization-int-mixed",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
        session_intervals_path=session_intervals_path,
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["canonicalization_engine"] == "spark"
    assert int(report["resampling_skips"]["count"]) > 0
    assert "5m" in report["resampling_skips"]["by_timeframe"]


def test_canonicalization_pass_noop_does_not_mutate_existing_tables(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    output_dir = tmp_path / "canonicalization-noop"
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    first = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="canonicalization-int-first",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
        session_intervals_path=session_intervals_path,
    )
    bars_before = _read_batched_delta_rows(Path(str(first["output_paths"]["canonical_bars"])))

    second = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="canonicalization-int-noop",
        raw_ingest_run_report=_build_raw_ingest_report_noop(run_id="raw-ingest-pass2"),
        session_intervals_path=session_intervals_path,
    )
    bars_after = _read_batched_delta_rows(Path(str(second["output_paths"]["canonical_bars"])))

    assert second["status"] == "PASS-NOOP"
    assert second["publish_decision"] == "publish"
    assert second["mutation_applied"] is False
    assert bars_before == bars_after


def test_canonicalization_avoids_full_raw_table_read(tmp_path: Path, monkeypatch) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows_with_daily_only_contract(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    raw_table_resolved = raw_table_path.resolve()
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    original_read = canonicalization_module.read_delta_table_rows

    def guarded_read(
        table_path: Path,
        *,
        columns: list[str] | None = None,
        filters: object = None,
    ) -> list[dict[str, object]]:
        if table_path.resolve() == raw_table_resolved and columns is None:
            raise AssertionError("canonicalization attempted a full raw-table materialization")
        return original_read(table_path, columns=columns, filters=filters)

    monkeypatch.setattr(canonicalization_module, "read_delta_table_rows", guarded_read)

    report = canonicalization_module.run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonicalization-guarded",
        run_id="canonicalization-int-guarded",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
        session_intervals_path=session_intervals_path,
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["scoped_source_rows"] > 0


def test_canonicalization_fails_closed_on_missing_spark_output_paths(
    tmp_path: Path, monkeypatch
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows_with_daily_only_contract(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    def missing_spark_outputs(**kwargs):  # noqa: ANN003
        return {
            "output_paths": {
                "canonical_bars": "",
                "canonical_bar_provenance": "",
            }
        }

    monkeypatch.setattr(
        canonicalization_module,
        "_run_spark_canonicalization",
        missing_spark_outputs,
    )

    with pytest.raises(ValueError, match="canonical_bars"):
        run_moex_canonicalization(
            raw_table_path=raw_table_path,
            output_dir=tmp_path / "canonicalization-missing-spark-output",
            run_id="canonicalization-int-missing-spark-output",
            raw_ingest_run_report=_build_raw_ingest_report_for_rows(
                rows=rows, run_id="raw-ingest-pass1"
            ),
            session_intervals_path=session_intervals_path,
        )


def test_canonicalization_fails_closed_on_incomplete_spark_outputs(
    tmp_path: Path, monkeypatch
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows_with_daily_only_contract(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    scoped_bars_path = tmp_path / "spark" / "canonical_bars.delta"
    scoped_provenance_path = tmp_path / "spark" / "canonical_bar_provenance.delta"
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    def incomplete_spark_outputs(**kwargs):  # noqa: ANN003
        return {
            "output_paths": {
                "canonical_bars": str(scoped_bars_path),
                "canonical_bar_provenance": str(scoped_provenance_path),
            }
        }

    def fake_spark_output_path(output_paths, key):  # noqa: ANN001
        return scoped_bars_path if key == "canonical_bars" else scoped_provenance_path

    original_iter_delta_rows = canonicalization_module._iter_delta_rows_for_merge

    def fake_iter_delta_rows(table_path: Path, **kwargs):
        if Path(table_path) in {scoped_bars_path, scoped_provenance_path}:
            return iter(())
        return original_iter_delta_rows(table_path, **kwargs)

    monkeypatch.setattr(
        canonicalization_module,
        "_run_spark_canonicalization",
        incomplete_spark_outputs,
    )
    monkeypatch.setattr(
        canonicalization_module,
        "_require_spark_delta_output_path",
        fake_spark_output_path,
    )
    monkeypatch.setattr(
        canonicalization_module,
        "_iter_delta_rows_for_merge",
        fake_iter_delta_rows,
    )

    with pytest.raises(RuntimeError, match="incomplete canonical bars"):
        run_moex_canonicalization(
            raw_table_path=raw_table_path,
            output_dir=tmp_path / "canonicalization-incomplete-spark-output",
            run_id="canonicalization-int-incomplete-spark-output",
            raw_ingest_run_report=_build_raw_ingest_report_for_rows(
                rows=rows, run_id="raw-ingest-pass1"
            ),
            session_intervals_path=session_intervals_path,
        )


def test_canonicalization_pass_noop_skips_raw_table_read_entirely(
    tmp_path: Path, monkeypatch
) -> None:
    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    output_dir = tmp_path / "canonicalization-noop-skip-raw"
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        _session_intervals_for_rows(rows),
    )

    first = canonicalization_module.run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="canonicalization-int-first-noop-guard",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(
            rows=rows, run_id="raw-ingest-pass1"
        ),
        session_intervals_path=session_intervals_path,
    )
    assert first["publish_decision"] == "publish"

    raw_table_resolved = raw_table_path.resolve()
    original_read = canonicalization_module.read_delta_table_rows

    def guarded_read(
        table_path: Path,
        *,
        columns: list[str] | None = None,
        filters: object = None,
    ) -> list[dict[str, object]]:
        if table_path.resolve() == raw_table_resolved:
            raise AssertionError("canonicalization PASS-NOOP should not read the raw table")
        return original_read(table_path, columns=columns, filters=filters)

    monkeypatch.setattr(canonicalization_module, "read_delta_table_rows", guarded_read)

    second = canonicalization_module.run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="canonicalization-int-noop-skip-raw",
        raw_ingest_run_report=_build_raw_ingest_report_noop(run_id="raw-ingest-pass2"),
        session_intervals_path=session_intervals_path,
    )

    assert second["status"] == "PASS-NOOP"
    assert second["publish_decision"] == "publish"
    assert second["mutation_applied"] is False
