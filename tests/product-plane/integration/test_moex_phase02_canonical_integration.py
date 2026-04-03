from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import run_phase02_canonical


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


def test_phase02_canonical_generates_resampling_outputs_and_reports(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    _write_raw_table(raw_table_path, _raw_rows(with_source_provider=True))

    report = run_phase02_canonical(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "phase02",
        run_id="phase02-int-pass",
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["qc_report"]["status"] == "PASS"
    assert report["contract_compatibility_report"]["status"] == "PASS"
    assert report["runtime_decoupling_proof"]["status"] == "PASS"
    assert report["output_paths"]["canonical_bars"]
    assert report["output_paths"]["canonical_bar_provenance"]
    assert report["real_bindings"]

    bars = read_delta_table_rows(Path(str(report["output_paths"]["canonical_bars"])))
    assert bars
    assert {row["timeframe"] for row in bars} == {"5m", "15m", "1h", "4h", "1d", "1w"}
    for artifact_path in report["artifact_paths"].values():
        assert Path(str(artifact_path)).exists()


def test_phase02_canonical_is_fail_closed_when_qc_fails(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    _write_raw_table(raw_table_path, _raw_rows(with_source_provider=False))

    report = run_phase02_canonical(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "phase02-blocked",
        run_id="phase02-int-fail",
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert report["qc_report"]["status"] == "FAIL"
    assert "provenance_completeness" in report["qc_report"]["failed_gates"]
    assert report["output_paths"] == {}
    qc_report_path = Path(str(report["artifact_paths"]["qc_report"]))
    payload = json.loads(qc_report_path.read_text(encoding="utf-8"))
    assert payload["publish_decision"] == "blocked"


def test_phase02_canonical_reports_skips_for_incompatible_daily_only_contract(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
    _write_raw_table(raw_table_path, _raw_rows_with_daily_only_contract(with_source_provider=True))

    report = run_phase02_canonical(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "phase02-mixed",
        run_id="phase02-int-mixed",
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert int(report["resampling_skips"]["count"]) > 0
    assert "5m" in report["resampling_skips"]["by_timeframe"]

