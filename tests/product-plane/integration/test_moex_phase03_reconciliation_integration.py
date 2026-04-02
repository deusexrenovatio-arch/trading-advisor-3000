from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import run_phase03_reconciliation


CANONICAL_COLUMNS: dict[str, str] = {
    "contract_id": "string",
    "instrument_id": "string",
    "timeframe": "string",
    "ts": "timestamp",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "bigint",
    "open_interest": "bigint",
}

PROVENANCE_COLUMNS: dict[str, str] = {
    "contract_id": "string",
    "instrument_id": "string",
    "timeframe": "string",
    "ts": "timestamp",
    "source_provider": "string",
    "source_timeframe": "string",
    "source_interval": "int",
    "source_run_id": "string",
    "source_ingest_run_id": "string",
    "source_row_count": "int",
    "source_ts_open_first": "timestamp",
    "source_ts_close_last": "timestamp",
    "open_interest_imputed": "int",
    "build_run_id": "string",
    "built_at_utc": "timestamp",
}


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _prepare_phase02_tables(base: Path) -> tuple[Path, Path]:
    bars_path = base / "delta" / "canonical_bars.delta"
    provenance_path = base / "delta" / "canonical_bar_provenance.delta"
    start = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)

    bars: list[dict[str, object]] = []
    provenance: list[dict[str, object]] = []
    for idx in range(4):
        ts = start + timedelta(minutes=5 * idx)
        bars.append(
            {
                "contract_id": "RIM6@MOEX",
                "instrument_id": "FUT_RTS",
                "timeframe": "5m",
                "ts": _iso(ts),
                "open": 210_000.0 + idx,
                "high": 210_100.0 + idx,
                "low": 209_900.0 + idx,
                "close": 210_050.0 + idx,
                "volume": 700 + idx * 10,
                "open_interest": 2500 + idx,
            }
        )
        provenance.append(
            {
                "contract_id": "RIM6@MOEX",
                "instrument_id": "FUT_RTS",
                "timeframe": "5m",
                "ts": _iso(ts),
                "source_provider": "moex_iss",
                "source_timeframe": "1m",
                "source_interval": 1,
                "source_run_id": "phase01-pass",
                "source_ingest_run_id": "phase01-pass",
                "source_row_count": 5,
                "source_ts_open_first": _iso(ts - timedelta(minutes=5)),
                "source_ts_close_last": _iso(ts + timedelta(minutes=4, seconds=59)),
                "open_interest_imputed": 0,
                "build_run_id": "phase02-run",
                "built_at_utc": _iso(ts + timedelta(minutes=7)),
            }
        )

    write_delta_table_rows(table_path=bars_path, rows=bars, columns=CANONICAL_COLUMNS)
    write_delta_table_rows(table_path=provenance_path, rows=provenance, columns=PROVENANCE_COLUMNS)
    return bars_path, provenance_path


def _prepare_finam_archive(path: Path) -> None:
    start = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for idx in range(4):
        ts = start + timedelta(minutes=5 * idx)
        rows.append(
            {
                "contract_id": "RIM6@MOEX",
                "instrument_id": "FUT_RTS",
                "timeframe": "5m",
                "ts": _iso(ts),
                "close": 210_050.0 + idx + 0.15,
                "volume": 700 + idx * 10 + 5,
                "source_ts_utc": _iso(ts + timedelta(minutes=5)),
                "received_at_utc": _iso(ts + timedelta(minutes=8)),
                "archive_batch_id": "archive-20260402-a",
                "source_provider": "finam_archive",
                "source_binding": "finam://archive/20260402-a",
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_phase03_reconciliation_generates_metrics_and_artifacts(tmp_path: Path) -> None:
    bars_path, provenance_path = _prepare_phase02_tables(tmp_path / "phase02")
    finam_source = tmp_path / "finam" / "archive.json"
    _prepare_finam_archive(finam_source)

    report = run_phase03_reconciliation(
        canonical_bars_path=bars_path,
        canonical_provenance_path=provenance_path,
        finam_archive_source_path=finam_source,
        threshold_policy_path=Path("configs/moex_phase03/reconciliation_thresholds.v1.yaml"),
        mapping_registry_path=Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml"),
        output_dir=tmp_path / "phase03",
        run_id="phase03-int-pass",
        allow_degraded_publish=False,
    )

    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["counts"]["matched_rows"] == 4
    assert report["counts"]["missing_in_finam"] == 0
    assert report["counts"]["unexpected_in_finam"] == 0
    assert report["artifacts_complete"] is True
    assert report["real_bindings"]

    for artifact in report["artifact_paths"].values():
        assert Path(str(artifact)).exists()

    provenance_path = Path(str(report["artifact_paths"]["finam_archive_provenance"]))
    provenance_payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    assert provenance_payload["source_capture"]["fingerprint_sha256"]
    assert provenance_payload["source_bindings"] == ["finam://archive/20260402-a"]
    assert provenance_payload["source_providers"] == ["finam_archive"]

    metrics_path = Path(str(report["output_paths"]["reconciliation_metrics"]))
    metrics_rows = read_delta_table_rows(metrics_path)
    assert len(metrics_rows) == 4
    assert all(row["missing_in_finam"] == 0 for row in metrics_rows)
    assert all(row["unexpected_in_finam"] == 0 for row in metrics_rows)
    assert any(row["close_drift_bps"] is not None for row in metrics_rows)
