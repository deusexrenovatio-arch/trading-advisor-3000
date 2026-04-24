from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.moex import historical_canonical_route as phase02_module
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import build_raw_ingest_run_report_v2, run_historical_canonical_route


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


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
                "ingest_run_id": "phase01-pass1",
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
                "provenance_json": {
                    "source_provider": provider,
                    "source_interval": 1,
                    "source_timeframe": "1m",
                    "run_id": "phase01-pass1",
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
                "ingest_run_id": "phase01-pass1",
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
                "provenance_json": {
                    "source_provider": provider,
                    "source_interval": 24,
                    "source_timeframe": "1d",
                    "run_id": "phase01-pass1",
                    "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/W4J6/candleborders.json",
                },
            }
        )
    return rows


def _write_raw_table(path: Path, rows: list[dict[str, object]]) -> None:
    write_delta_table_rows(table_path=path, rows=rows, columns=RAW_COLUMNS)


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
        raw_table_path="phase01/raw_moex_history.delta",
        raw_ingest_progress_path="phase01/raw-ingest-progress.jsonl",
        raw_ingest_error_path="phase01/raw-ingest-errors.jsonl",
        raw_ingest_error_latest_path="phase01/raw-ingest-error.latest.json",
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
        raw_table_path="phase01/raw_moex_history.delta",
        raw_ingest_progress_path="phase01/raw-ingest-progress.jsonl",
        raw_ingest_error_path="phase01/raw-ingest-errors.jsonl",
        raw_ingest_error_latest_path="phase01/raw-ingest-error.latest.json",
        changed_windows=[],
    )


def test_historical_canonical_route_generates_resampling_outputs_and_reports(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)

    report = run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "phase02",
        run_id="phase02-int-pass",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(rows=rows, run_id="phase01-pass1"),
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

    bars = read_delta_table_rows(Path(str(report["output_paths"]["canonical_bars"])))
    assert bars
    assert {row["timeframe"] for row in bars} == {"5m", "15m", "1h", "4h", "1d", "1w"}
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


def test_historical_canonical_route_is_fail_closed_when_qc_fails(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=False)
    _write_raw_table(raw_table_path, rows)

    report = run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "phase02-blocked",
        run_id="phase02-int-fail",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(rows=rows, run_id="phase01-pass1"),
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


def test_historical_canonical_route_reports_skips_for_incompatible_daily_only_contract(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows_with_daily_only_contract(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)

    report = run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "phase02-mixed",
        run_id="phase02-int-mixed",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(rows=rows, run_id="phase01-pass1"),
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["canonicalization_engine"] == "spark"
    assert int(report["resampling_skips"]["count"]) > 0
    assert "5m" in report["resampling_skips"]["by_timeframe"]


def test_historical_canonical_route_pass_noop_does_not_mutate_existing_tables(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    output_dir = tmp_path / "phase02-noop"

    first = run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="phase02-int-first",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(rows=rows, run_id="phase01-pass1"),
    )
    bars_before = read_delta_table_rows(Path(str(first["output_paths"]["canonical_bars"])))

    second = run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="phase02-int-noop",
        raw_ingest_run_report=_build_raw_ingest_report_noop(run_id="phase01-pass2"),
    )
    bars_after = read_delta_table_rows(Path(str(second["output_paths"]["canonical_bars"])))

    assert second["status"] == "PASS-NOOP"
    assert second["publish_decision"] == "publish"
    assert second["mutation_applied"] is False
    assert bars_before == bars_after


def test_historical_canonical_route_avoids_full_raw_table_read(tmp_path: Path, monkeypatch) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows_with_daily_only_contract(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    raw_table_resolved = raw_table_path.resolve()

    original_read = phase02_module.read_delta_table_rows

    def guarded_read(
        table_path: Path,
        *,
        columns: list[str] | None = None,
        filters: object = None,
    ) -> list[dict[str, object]]:
        if table_path.resolve() == raw_table_resolved and columns is None:
            raise AssertionError("phase-02 attempted a full raw-table materialization")
        return original_read(table_path, columns=columns, filters=filters)

    monkeypatch.setattr(phase02_module, "read_delta_table_rows", guarded_read)

    report = phase02_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "phase02-guarded",
        run_id="phase02-int-guarded",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(rows=rows, run_id="phase01-pass1"),
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["scoped_source_rows"] > 0


def test_canonical_route_pass_noop_skips_raw_table_read_entirely(tmp_path: Path, monkeypatch) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    rows = _raw_rows(with_source_provider=True)
    _write_raw_table(raw_table_path, rows)
    output_dir = tmp_path / "phase02-noop-skip-raw"

    first = phase02_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="phase02-int-first-noop-guard",
        raw_ingest_run_report=_build_raw_ingest_report_for_rows(rows=rows, run_id="phase01-pass1"),
    )
    assert first["publish_decision"] == "publish"

    raw_table_resolved = raw_table_path.resolve()
    original_read = phase02_module.read_delta_table_rows

    def guarded_read(
        table_path: Path,
        *,
        columns: list[str] | None = None,
        filters: object = None,
    ) -> list[dict[str, object]]:
        if table_path.resolve() == raw_table_resolved:
            raise AssertionError("phase-02 PASS-NOOP should not read the raw table")
        return original_read(table_path, columns=columns, filters=filters)

    monkeypatch.setattr(phase02_module, "read_delta_table_rows", guarded_read)

    second = phase02_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id="phase02-int-noop-skip-raw",
        raw_ingest_run_report=_build_raw_ingest_report_noop(run_id="phase01-pass2"),
    )

    assert second["status"] == "PASS-NOOP"
    assert second["publish_decision"] == "publish"
    assert second["mutation_applied"] is False

