from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex.phase03_reconciliation import (
    load_phase03_threshold_policy,
    run_phase03_reconciliation,
)


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


def _write_phase02_inputs(base: Path) -> tuple[Path, Path]:
    bars_path = base / "delta" / "canonical_bars.delta"
    provenance_path = base / "delta" / "canonical_bar_provenance.delta"
    start = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)
    bars: list[dict[str, object]] = []
    provenance: list[dict[str, object]] = []
    for idx in range(2):
        ts = start + timedelta(minutes=5 * idx)
        bars.append(
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "ts": _iso(ts),
                "open": 100.0 + idx,
                "high": 101.0 + idx,
                "low": 99.0 + idx,
                "close": 100.5 + idx,
                "volume": 100 + idx,
                "open_interest": 10 + idx,
            }
        )
        provenance.append(
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
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
                "built_at_utc": _iso(ts + timedelta(minutes=8)),
            }
        )
    write_delta_table_rows(table_path=bars_path, rows=bars, columns=CANONICAL_COLUMNS)
    write_delta_table_rows(table_path=provenance_path, rows=provenance, columns=PROVENANCE_COLUMNS)
    return bars_path, provenance_path


def _write_finam_source(
    path: Path,
    *,
    close_offset: float = 0.0,
    omit_source_binding: bool = False,
    omit_archive_batch_id: bool = False,
    omit_source_provider: bool = False,
    use_alias_timestamps: bool = False,
) -> None:
    start = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for idx in range(2):
        ts = start + timedelta(minutes=5 * idx)
        rows.append(
            {
                "contract_id": "BRM6@MOEX",
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "ts": _iso(ts),
                "close": 100.5 + idx + close_offset,
                "volume": 100 + idx,
                "source_ts_utc": _iso(ts + timedelta(minutes=5)),
                "received_at_utc": _iso(ts + timedelta(minutes=7)),
                "archive_batch_id": "finam-batch-1",
                "source_provider": "finam_archive",
                "source_binding": "finam://archive/test-batch-1",
            }
        )
        if use_alias_timestamps:
            rows[-1]["source_timestamp_utc"] = rows[-1].pop("source_ts_utc")
            rows[-1]["archived_at_utc"] = rows[-1].pop("received_at_utc")
        if omit_archive_batch_id:
            rows[-1].pop("archive_batch_id", None)
        if omit_source_provider:
            rows[-1].pop("source_provider", None)
        if omit_source_binding:
            rows[-1].pop("source_binding", None)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _policy_path() -> Path:
    return Path("configs/moex_phase03/reconciliation_thresholds.v1.yaml")


def _mapping_registry_path() -> Path:
    return Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml")


def test_phase03_threshold_policy_rejects_missing_lag_class_dimension(tmp_path: Path) -> None:
    payload = {
        "version": 1,
        "policy_id": "broken.v1",
        "required_dimensions": [
            "close_drift_bps",
            "volume_drift_ratio",
            "missing_bars_ratio",
        ],
        "lag_class_seconds": {"low": 300, "medium": 1200},
        "close_drift_bps": {"hard_by_asset_group": {"default": 40.0}},
        "volume_drift_ratio": {"hard_by_timeframe": {"5m": 0.2}},
        "missing_bars_ratio": {"hard_max": 0.05},
        "lag_class_mismatch_ratio": {"hard_max": 0.2},
        "escalation": {"require_alert_simulation": True, "hard_violation_requires_incident": True, "channels": ["ops"]},
    }
    broken = tmp_path / "policy.yaml"
    broken.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="missing mandatory dimensions"):
        load_phase03_threshold_policy(broken)


def test_phase03_blocks_hard_drift_without_degraded_publish(tmp_path: Path) -> None:
    bars_path, provenance_path = _write_phase02_inputs(tmp_path / "phase02")
    finam_source = tmp_path / "finam" / "archive.json"
    _write_finam_source(finam_source, close_offset=12.0)

    report = run_phase03_reconciliation(
        canonical_bars_path=bars_path,
        canonical_provenance_path=provenance_path,
        finam_archive_source_path=finam_source,
        threshold_policy_path=_policy_path(),
        mapping_registry_path=_mapping_registry_path(),
        output_dir=tmp_path / "phase03",
        run_id="phase03-hard-block",
        allow_degraded_publish=False,
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert "close_drift_bps" in report["hard_failed_gates"]
    assert report["alert_simulation"]["incident_count"] >= 1
    assert Path(str(report["artifact_paths"]["alert_simulation"])).exists()


def test_phase03_blocks_when_alert_simulation_not_executed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bars_path, provenance_path = _write_phase02_inputs(tmp_path / "phase02")
    finam_source = tmp_path / "finam" / "archive.json"
    _write_finam_source(finam_source, close_offset=10.0)

    def _disabled_alerts(*, run_id: str, hard_failed_gates: list[str], policy):  # noqa: ANN001
        return (
            {"run_id": run_id, "executed": False, "incidents": [], "incident_count": 0},
            {"run_id": run_id, "executed": False, "events": []},
        )

    monkeypatch.setattr(
        "trading_advisor_3000.product_plane.data_plane.moex.phase03_reconciliation._build_alert_simulation",
        _disabled_alerts,
    )

    report = run_phase03_reconciliation(
        canonical_bars_path=bars_path,
        canonical_provenance_path=provenance_path,
        finam_archive_source_path=finam_source,
        threshold_policy_path=_policy_path(),
        mapping_registry_path=_mapping_registry_path(),
        output_dir=tmp_path / "phase03-no-alerts",
        run_id="phase03-alert-missing",
        allow_degraded_publish=True,
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert "alert_simulation_missing" in report["hard_failed_gates"]


def test_phase03_rejects_missing_proof_critical_metadata_fields(tmp_path: Path) -> None:
    bars_path, provenance_path = _write_phase02_inputs(tmp_path / "phase02")
    finam_source = tmp_path / "finam" / "archive-missing-fields.json"
    _write_finam_source(
        finam_source,
        omit_source_binding=True,
        omit_archive_batch_id=True,
        omit_source_provider=True,
    )

    with pytest.raises(ValueError, match="archive_batch_id"):
        run_phase03_reconciliation(
            canonical_bars_path=bars_path,
            canonical_provenance_path=provenance_path,
            finam_archive_source_path=finam_source,
            threshold_policy_path=_policy_path(),
            mapping_registry_path=_mapping_registry_path(),
            output_dir=tmp_path / "phase03-metadata-missing",
            run_id="phase03-metadata-missing",
            allow_degraded_publish=False,
        )


def test_phase03_rejects_aliased_timestamp_metadata(tmp_path: Path) -> None:
    bars_path, provenance_path = _write_phase02_inputs(tmp_path / "phase02")
    finam_source = tmp_path / "finam" / "archive-aliased-fields.json"
    _write_finam_source(finam_source, use_alias_timestamps=True)

    with pytest.raises(ValueError, match="source_ts_utc"):
        run_phase03_reconciliation(
            canonical_bars_path=bars_path,
            canonical_provenance_path=provenance_path,
            finam_archive_source_path=finam_source,
            threshold_policy_path=_policy_path(),
            mapping_registry_path=_mapping_registry_path(),
            output_dir=tmp_path / "phase03-metadata-aliased",
            run_id="phase03-metadata-aliased",
            allow_degraded_publish=False,
        )
