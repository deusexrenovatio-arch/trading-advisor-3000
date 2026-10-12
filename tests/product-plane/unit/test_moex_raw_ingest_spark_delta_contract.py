from __future__ import annotations

import json
from pathlib import Path

import pytest
from support.spark_runtime import require_configured_spark_delta_profile

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
        "open": 100.0,
        "high": 102.0,
        "low": 98.0,
        "close": close,
        "volume": 50,
        "open_interest": None,
        "ingest_run_id": run_id,
        "ingested_at_utc": "2026-04-01T08:00:00Z",
        "provenance_json": provenance_json
        or {"source_provider": "moex_iss", "revision": "initial", "run_id": run_id},
    }


def _scope(
    *,
    window_start_utc: str = "2026-04-01T06:59:59Z",
    window_end_utc: str = "2026-04-01T08:00:00Z",
    watermark_utc: str = "2026-04-01T07:29:59Z",
) -> dict[str, object]:
    return {
        "internal_id": "FUT_BR",
        "timeframe": "1m",
        "source_interval": 1,
        "moex_secid": "BRQ6",
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "watermark_utc": watermark_utc,
    }


def _job_paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "progress_path": tmp_path / "raw-ingest-progress.jsonl",
        "progress_latest_path": tmp_path / "raw-ingest-progress.latest.json",
        "error_path": tmp_path / "raw-ingest-errors.jsonl",
        "error_latest_path": tmp_path / "raw-ingest-error.latest.json",
    }


def _read_rows(raw_table_path: Path) -> list[dict[str, object]]:
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


def test_raw_ingest_job_requires_configured_windows_spark_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(raw_ingest_job.os, "name", "nt", raising=False)
    monkeypatch.delenv("HADOOP_HOME", raising=False)
    raw_table_path = tmp_path / "raw_moex_history.delta"

    with pytest.raises(RuntimeError, match="HADOOP_HOME.*Docker/Linux Spark proof profile"):
        run_moex_raw_ingest_spark_delta_job(
            table_path=raw_table_path,
            source_rows=[],
            window_scopes=[],
            initial_watermarks={},
            run_id="missing-hadoop",
            ingest_till_utc="2026-04-01T08:00:00Z",
            refresh_overlap_minutes=20,
            **_job_paths(tmp_path),
        )

    assert not raw_table_path.exists()


def test_raw_ingest_job_writes_empty_delta_report_for_empty_bootstrap(tmp_path: Path) -> None:
    require_configured_spark_delta_profile()
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-current" / "raw_moex_history.delta"

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=raw_table_path,
        source_rows=[],
        window_scopes=[],
        initial_watermarks={},
        run_id="empty-bootstrap",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_overlap_minutes=20,
        **_job_paths(tmp_path),
    )

    progress_latest = json.loads(
        (tmp_path / "raw-ingest-progress.latest.json").read_text(encoding="utf-8")
    )
    assert report["status"] == "PASS-NOOP"
    assert report["source_rows"] == 0
    assert report["changed_windows"] == []
    assert (raw_table_path / "_delta_log").exists()
    assert progress_latest["runtime_owner"] == "spark_delta"
    assert progress_latest["incremental_rows"] == 0


def test_raw_ingest_job_reconciles_changed_rows_and_deletes_missing_window_rows(
    tmp_path: Path,
) -> None:
    require_configured_spark_delta_profile()
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-current" / "raw_moex_history.delta"
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
                close=99.75,
                run_id="reconcile",
            ),
            _raw_row(
                ts_open="2026-04-01T07:20:00Z",
                ts_close="2026-04-01T07:29:59Z",
                close=101.8,
                run_id="reconcile",
            ),
        ],
        window_scopes=[_scope()],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:29:59Z"},
        run_id="reconcile",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_overlap_minutes=60,
        **_job_paths(tmp_path),
    )

    rows_by_ts_close = {
        str(row["ts_close"]): float(row["close"]) for row in _read_rows(raw_table_path)
    }
    progress_latest = json.loads(
        (tmp_path / "raw-ingest-progress.latest.json").read_text(encoding="utf-8")
    )
    fingerprint_columns = set(progress_latest["fingerprint_columns"])

    assert report["status"] == "PASS"
    assert report["incremental_rows"] == 2
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
    assert rows_by_ts_close == {
        "2026-04-01T07:09:59Z": 99.75,
        "2026-04-01T07:29:59Z": 101.8,
    }
    assert progress_latest["deleted_rows"] == 1
    assert {"moex_board", "open_interest", "provenance_json"} <= fingerprint_columns
    assert "ingest_run_id" not in fingerprint_columns
    assert "ingested_at_utc" not in fingerprint_columns


def test_raw_ingest_job_tail_catchup_appends_new_rows(tmp_path: Path) -> None:
    require_configured_spark_delta_profile()
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(ts_open="2026-04-01T07:00:00Z", ts_close="2026-04-01T07:09:59Z", close=100.5),
        ],
        columns=RAW_COLUMNS,
    )
    version_before = sorted((raw_table_path / "_delta_log").glob("*.json"))[-1].name

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=raw_table_path,
        source_rows=[
            _raw_row(
                ts_open="2026-04-01T07:10:00Z",
                ts_close="2026-04-01T07:19:59Z",
                close=101.2,
                run_id="tail",
            )
        ],
        window_scopes=[
            _scope(
                window_start_utc="2026-04-01T07:10:00Z",
                window_end_utc="2026-04-01T07:19:59Z",
                watermark_utc="2026-04-01T07:09:59Z",
            )
        ],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:09:59Z"},
        run_id="tail",
        ingest_till_utc="2026-04-01T07:19:59Z",
        refresh_overlap_minutes=0,
        **_job_paths(tmp_path),
    )
    version_after = sorted((raw_table_path / "_delta_log").glob("*.json"))[-1].name

    assert report["status"] == "PASS"
    assert report["incremental_rows"] == 1
    assert {row["ts_close"] for row in _read_rows(raw_table_path)} == {
        "2026-04-01T07:09:59Z",
        "2026-04-01T07:19:59Z",
    }
    assert version_after > version_before


def test_raw_ingest_job_ignores_volatile_pipeline_provenance_only_changes(
    tmp_path: Path,
) -> None:
    require_configured_spark_delta_profile()
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-current" / "raw_moex_history.delta"
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
            _scope(
                window_start_utc="2026-04-01T07:10:00Z",
                window_end_utc="2026-04-01T07:19:59Z",
                watermark_utc="2026-04-01T07:09:59Z",
            )
        ],
        initial_watermarks={("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:09:59Z"},
        run_id="provenance-only",
        ingest_till_utc="2026-04-01T07:19:59Z",
        refresh_overlap_minutes=180,
        **_job_paths(tmp_path),
    )
    version_after = sorted((raw_table_path / "_delta_log").glob("*.json"))[-1].name

    assert report["status"] == "PASS-NOOP"
    assert report["incremental_rows"] == 0
    assert report["deduplicated_rows"] == 1
    assert report["changed_windows"] == []
    assert version_after == version_before
