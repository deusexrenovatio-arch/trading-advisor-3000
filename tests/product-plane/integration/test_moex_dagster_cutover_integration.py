from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import build_raw_ingest_run_report_v2, run_moex_dagster_cutover
from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import read_technical_route_run_ledger


RAW_COLUMNS: dict[str, str] = {
    "internal_id": "string",
    "finam_symbol": "string",
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


def _prepare_raw_ingest_inputs(tmp_path: Path, *, run_id: str) -> tuple[Path, Path]:
    start = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for minute in range(24):
        ts_open = start + timedelta(minutes=minute)
        rows.append(
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6@MOEX",
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
                "ingest_run_id": run_id,
                "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
                "provenance_json": {
                    "source_provider": "moex_iss",
                    "source_interval": 1,
                    "source_timeframe": "1m",
                    "run_id": run_id,
                    "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json",
                },
            }
        )

    raw_table_path = tmp_path / "raw_ingest" / "delta" / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=rows, columns=RAW_COLUMNS)

    changed_windows = [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6@MOEX",
            "window_start_utc": rows[0]["ts_open"],
            "window_end_utc": rows[-1]["ts_close"],
            "incremental_rows": len(rows),
        }
    ]
    raw_report_payload = build_raw_ingest_run_report_v2(
        run_id=run_id,
        ingest_till_utc=str(rows[-1]["ts_close"]),
        source_rows=len(rows),
        incremental_rows=len(rows),
        deduplicated_rows=0,
        stale_rows=0,
        watermark_by_key={"FUT_BR|1m|BRM6@MOEX": str(rows[-1]["ts_close"])},
        raw_table_path=raw_table_path.as_posix(),
        raw_ingest_progress_path=(tmp_path / "raw_ingest" / "raw-ingest-progress.jsonl").as_posix(),
        raw_ingest_error_path=(tmp_path / "raw_ingest" / "raw-ingest-errors.jsonl").as_posix(),
        raw_ingest_error_latest_path=(tmp_path / "raw_ingest" / "raw-ingest-error.latest.json").as_posix(),
        changed_windows=changed_windows,
    )

    raw_report_path = tmp_path / "raw_ingest" / "raw-ingest-report.json"
    raw_report_path.parent.mkdir(parents=True, exist_ok=True)
    raw_report_path.write_text(json.dumps(raw_report_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return raw_table_path, raw_report_path


def test_dagster_cutover_dagster_cutover_materializes_route_and_emits_recovery_artifacts(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _prepare_raw_ingest_inputs(tmp_path, run_id="dagster-cutover-int-raw")
    output_dir = tmp_path / "dagster-cutover-cutover"

    report = run_moex_dagster_cutover(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_report_path,
        output_dir=output_dir,
        run_id="dagster-cutover-int",
        nightly_readiness_observed_at_utc=[
            "2026-04-04T02:30:00Z",
            "2026-04-05T02:35:00Z",
        ],
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["single_writer_probe"]["blocked_conflict_proven"] is True
    assert report["recovery_drill"]["status"] == "PASS"
    assert Path(str(report["artifact_paths"]["dagster_graph_definition"])).exists()
    assert Path(str(report["artifact_paths"]["dagster_runtime_binding"])).exists()
    assert Path(str(report["artifact_paths"]["schedule_retry_lock_contract"])).exists()
    assert Path(str(report["artifact_paths"]["recovery_drill"])).exists()

    for cycle in report["cycles"]:
        assert cycle["status"] == "PASS"
        materialization = cycle["materialization"]
        assert materialization["success"] is True
        assert str(materialization["dagster_job_name"]) == "moex_historical_cutover_job"
        assert str(materialization["dagster_run_id"]).strip()
        canonicalization_report_path = Path(str(materialization["output_paths"]["canonicalization_report"]))
        assert canonicalization_report_path.exists()
        canonicalization_payload = json.loads(canonicalization_report_path.read_text(encoding="utf-8"))
        assert canonicalization_payload["publish_decision"] == "publish"
        bars_path = Path(str(materialization["output_paths"]["canonical_bars"]))
        assert bars_path.exists()
        bars = read_delta_table_rows(bars_path)
        assert bars
        indicator_path = Path(str(materialization["output_paths"]["technical_indicator_snapshot"]))
        assert (indicator_path / "_delta_log").exists()
        if cycle["mode"] == "nightly":
            assert materialization["schedule"] is not None
            assert materialization["schedule"]["name"] == "moex_historical_nightly_schedule"
        else:
            assert materialization["schedule"] is None

    ledger_rows = read_technical_route_run_ledger(
        ledger_table_path=Path(str(report["output_paths"]["technical_route_ledger"])),
        route_id=str(report["route_id"]),
    )
    assert ledger_rows
    run_ids = {str(row.get("run_id", "")).strip() for row in ledger_rows}
    assert "dagster-cutover-int-nightly-1" in run_ids
    assert "dagster-cutover-int-nightly-2" in run_ids
    assert "dagster-cutover-int-repair-1" in run_ids
    assert "dagster-cutover-int-backfill-1" in run_ids
