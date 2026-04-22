from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import baseline_update as baseline_module
from trading_advisor_3000.product_plane.data_plane.moex.phase02_canonical import (
    CANONICAL_BAR_COLUMNS,
    CANONICAL_MERGE_SCOPED_DELETE_INSERT,
    PROVENANCE_COLUMNS,
)
from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS


def _write_empty_baseline(tmp_path: Path) -> tuple[Path, Path, Path]:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    canonical_bars_path = tmp_path / "canonical" / "moex" / "baseline-4y-current" / "canonical_bars.delta"
    canonical_provenance_path = (
        tmp_path / "canonical" / "moex" / "baseline-4y-current" / "canonical_bar_provenance.delta"
    )
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    write_delta_table_rows(table_path=canonical_bars_path, rows=[], columns=CANONICAL_BAR_COLUMNS)
    write_delta_table_rows(table_path=canonical_provenance_path, rows=[], columns=PROVENANCE_COLUMNS)
    return raw_table_path, canonical_bars_path, canonical_provenance_path


def _patch_common_inputs(monkeypatch: pytest.MonkeyPatch, changed_windows: list[dict[str, object]]) -> None:
    monkeypatch.setattr(baseline_module, "load_universe", lambda _path: [])
    monkeypatch.setattr(baseline_module, "load_mapping_registry", lambda _path: [])
    monkeypatch.setattr(baseline_module, "validate_mapping_registry", lambda _mappings: None)
    monkeypatch.setattr(baseline_module, "validate_universe_mapping_alignment", lambda _universe, _mappings: None)
    monkeypatch.setattr(baseline_module, "discover_coverage", lambda **_kwargs: [])
    monkeypatch.setattr(
        baseline_module,
        "ingest_moex_bootstrap_window",
        lambda **kwargs: {
            "run_id": kwargs["run_id"],
            "status": "PASS" if changed_windows else "PASS-NOOP",
            "ingest_till_utc": kwargs["ingest_till_utc"],
            "source_rows": 12,
            "incremental_rows": sum(int(item["incremental_rows"]) for item in changed_windows),
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "watermark_by_key": {},
            "raw_table_path": kwargs["table_path"].as_posix(),
            "raw_ingest_progress_path": kwargs["progress_path"].as_posix(),
            "raw_ingest_error_path": kwargs["error_path"].as_posix(),
            "raw_ingest_error_latest_path": kwargs["error_latest_path"].as_posix(),
            "changed_windows": changed_windows,
        },
    )


def test_baseline_update_writes_to_stable_paths_and_scoped_canonical_refresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    changed_windows = [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6@MOEX",
            "window_start_utc": "2026-04-21T00:00:00Z",
            "window_end_utc": "2026-04-22T00:00:00Z",
            "incremental_rows": 12,
        }
    ]
    _patch_common_inputs(monkeypatch, changed_windows)
    captured: dict[str, object] = {}

    def _fake_canonical(**kwargs):
        captured.update(kwargs)
        return {
            "status": "PASS",
            "publish_decision": "publish",
            "scoped_source_rows": 12,
            "scoped_canonical_rows": 4,
            "canonical_rows": 4,
            "mutation_applied": True,
        }

    monkeypatch.setattr(baseline_module, "run_phase02_canonical", _fake_canonical)

    report = baseline_module.run_moex_baseline_update(
        mapping_registry_path=tmp_path / "mapping.yaml",
        universe_path=tmp_path / "universe.yaml",
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        evidence_dir=tmp_path / "evidence",
        run_id="baseline-daily",
        timeframes={"5m", "15m"},
        ingest_till_utc="2026-04-22T00:00:00Z",
        refresh_window_days=7,
        contract_discovery_lookback_days=45,
        max_changed_window_days=10,
    )

    assert report["status"] == "PASS"
    assert captured["raw_table_path"] == raw_table_path
    assert captured["canonical_bars_path"] == canonical_bars_path
    assert captured["canonical_provenance_path"] == canonical_provenance_path
    assert captured["canonical_merge_strategy"] == CANONICAL_MERGE_SCOPED_DELETE_INSERT
    assert captured["max_changed_window_days"] == 10
    assert report["effective_changed_windows"] == 1
    assert not (tmp_path / "evidence" / "pending-changed-windows.json").exists()


def test_baseline_update_persists_pending_windows_when_canonical_refresh_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    changed_windows = [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6@MOEX",
            "window_start_utc": "2026-04-21T00:00:00Z",
            "window_end_utc": "2026-04-22T00:00:00Z",
            "incremental_rows": 12,
        }
    ]
    _patch_common_inputs(monkeypatch, changed_windows)

    def _failing_canonical(**_kwargs):
        raise RuntimeError("spark failed")

    monkeypatch.setattr(baseline_module, "run_phase02_canonical", _failing_canonical)

    with pytest.raises(RuntimeError, match="spark failed"):
        baseline_module.run_moex_baseline_update(
            mapping_registry_path=tmp_path / "mapping.yaml",
            universe_path=tmp_path / "universe.yaml",
            raw_table_path=raw_table_path,
            canonical_bars_path=canonical_bars_path,
            canonical_provenance_path=canonical_provenance_path,
            evidence_dir=tmp_path / "evidence",
            run_id="baseline-daily-failed",
            timeframes={"5m"},
            ingest_till_utc="2026-04-22T00:00:00Z",
            refresh_window_days=7,
            contract_discovery_lookback_days=45,
            max_changed_window_days=10,
        )

    pending_path = tmp_path / "evidence" / "pending-changed-windows.json"
    pending = json.loads(pending_path.read_text(encoding="utf-8"))
    assert pending["status"] == "PENDING"
    assert len(pending["changed_windows"]) == 1
