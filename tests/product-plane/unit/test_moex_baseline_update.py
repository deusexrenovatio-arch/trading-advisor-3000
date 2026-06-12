from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import baseline_update as baseline_module
from trading_advisor_3000.product_plane.data_plane.moex.foundation import (
    RAW_COLUMNS,
    DiscoveryRecord,
    ingest_moex_baseline_window,
    ingest_moex_bootstrap_window,
)
from trading_advisor_3000.product_plane.data_plane.moex.historical_canonical_route import (
    CANONICAL_BAR_COLUMNS,
    CANONICAL_MERGE_SCOPED_DELETE_INSERT,
    PROVENANCE_COLUMNS,
)
from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import (
    changed_windows_hash_sha256,
)
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexCandle


def _write_empty_baseline(tmp_path: Path) -> tuple[Path, Path, Path]:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    canonical_bars_path = (
        tmp_path / "canonical" / "moex" / "baseline-4y-current" / "canonical_bars.delta"
    )
    canonical_provenance_path = (
        tmp_path / "canonical" / "moex" / "baseline-4y-current" / "canonical_bar_provenance.delta"
    )
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    write_delta_table_rows(table_path=canonical_bars_path, rows=[], columns=CANONICAL_BAR_COLUMNS)
    write_delta_table_rows(
        table_path=canonical_provenance_path, rows=[], columns=PROVENANCE_COLUMNS
    )
    return raw_table_path, canonical_bars_path, canonical_provenance_path


def _patch_common_inputs(
    monkeypatch: pytest.MonkeyPatch, changed_windows: list[dict[str, object]]
) -> None:
    monkeypatch.setattr(baseline_module, "load_universe", lambda _path: [])
    monkeypatch.setattr(baseline_module, "load_mapping_registry", lambda _path: [])
    monkeypatch.setattr(baseline_module, "validate_mapping_registry", lambda _mappings: None)
    monkeypatch.setattr(
        baseline_module, "validate_universe_mapping_alignment", lambda _universe, _mappings: None
    )
    monkeypatch.setattr(baseline_module, "discover_coverage", lambda **_kwargs: [])
    monkeypatch.setattr(
        baseline_module,
        "ingest_moex_baseline_window",
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
    monkeypatch.setattr(
        baseline_module,
        "materialize_reconstructed_session_schedule_for_changed_windows",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("baseline update must not materialize session schedule")
        ),
        raising=False,
    )


def test_baseline_raw_report_for_canonical_rehashes_merged_changed_windows() -> None:
    pending_windows = [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1d",
            "source_interval": 24,
            "moex_secid": "BRN6",
            "window_start_utc": "2026-06-10T21:00:00Z",
            "window_end_utc": "2026-06-11T21:00:00Z",
            "incremental_rows": 1,
        }
    ]
    current_windows = [
        {
            "internal_id": "FUT_WHEAT",
            "source_timeframe": "1d",
            "source_interval": 24,
            "moex_secid": "W4U6",
            "window_start_utc": "2026-06-11T21:00:00Z",
            "window_end_utc": "2026-06-12T21:00:00Z",
            "incremental_rows": 1,
        }
    ]
    raw_report = {
        "status": "PASS-NOOP",
        "changed_windows": [],
        "changed_windows_hash_sha256": changed_windows_hash_sha256([]),
    }

    merged = baseline_module._merge_changed_windows(
        pending=pending_windows, current=current_windows
    )
    canonical_report = baseline_module._baseline_raw_report_for_canonical(
        raw_report=raw_report,
        merged_changed_windows=merged,
    )

    assert canonical_report["status"] == "PASS"
    assert canonical_report["changed_windows"] == merged
    assert canonical_report["changed_windows_hash_sha256"] == changed_windows_hash_sha256(merged)
    assert (
        canonical_report["changed_windows_hash_sha256"] != raw_report["changed_windows_hash_sha256"]
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

    monkeypatch.setattr(baseline_module, "run_historical_canonical_route", _fake_canonical)

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
    assert (
        captured["canonical_session_calendar_path"]
        == canonical_bars_path.parent / "canonical_session_calendar.delta"
    )
    assert captured.get("canonical_session_intervals_path") is None
    assert (
        captured["canonical_roll_map_path"]
        == canonical_bars_path.parent / "canonical_roll_map.delta"
    )
    assert captured["canonical_merge_strategy"] == CANONICAL_MERGE_SCOPED_DELETE_INSERT
    assert captured["max_changed_window_days"] == 10
    assert captured["target_timeframes"] == {"5m", "15m"}
    assert (
        report["canonical_session_calendar_path"]
        == (canonical_bars_path.parent / "canonical_session_calendar.delta").as_posix()
    )
    assert report["session_schedule_mode"] == "manual_backfill_required"
    assert report["session_schedule_required"] is True
    assert report["canonical_session_intervals_path"] == ""
    assert report["raw_session_schedule_path"] == ""
    assert report["artifact_paths"]["session_schedule_report"] == ""
    assert report["artifact_paths"]["official_session_schedule_report"] == ""
    assert (
        report["canonical_roll_map_path"]
        == (canonical_bars_path.parent / "canonical_roll_map.delta").as_posix()
    )
    assert report["effective_changed_windows"] == 1
    assert report["runtime_boundary"] == {
        "orchestrator": "dagster",
        "hot_table_runtime": "spark_delta",
        "python_role": "source_adapter_config_and_evidence",
    }
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

    monkeypatch.setattr(baseline_module, "run_historical_canonical_route", _failing_canonical)

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


class _BaselineWindowClient:
    def iter_candles(
        self,
        *,
        engine: str,
        market: str,
        board: str,
        secid: str,
        interval: int,
        date_from,
        date_till,
    ):
        del engine, market, board, secid, interval, date_from, date_till
        yield MoexCandle(
            open=100.0,
            high=102.0,
            low=98.0,
            close=99.75,
            volume=50,
            begin="2026-04-01 10:00:00",
            end="2026-04-01 10:09:59",
        )
        yield MoexCandle(
            open=100.5,
            high=101.5,
            low=100.4,
            close=101.2,
            volume=75,
            begin="2026-04-01 10:10:00",
            end="2026-04-01 10:19:59",
        )
        yield MoexCandle(
            open=101.2,
            high=102.0,
            low=101.0,
            close=101.8,
            volume=80,
            begin="2026-04-01 10:20:00",
            end="2026-04-01 10:29:59",
        )


def _raw_row(
    *, ts_open: str, ts_close: str, close: float, run_id: str = "seed"
) -> dict[str, object]:
    return {
        "internal_id": "FUT_BR",
        "finam_symbol": "BRQ6",
        "moex_engine": "futures",
        "moex_market": "forts",
        "moex_board": "RFUD",
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
        "provenance_json": {"source_provider": "moex_iss", "run_id": run_id},
    }


def test_baseline_raw_window_updates_only_scoped_rows_without_full_table_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(ts_open="2026-04-01T07:00:00Z", ts_close="2026-04-01T07:09:59Z", close=100.5),
            _raw_row(ts_open="2026-04-01T07:10:00Z", ts_close="2026-04-01T07:19:59Z", close=101.2),
        ],
        columns=RAW_COLUMNS,
    )
    coverage = [
        DiscoveryRecord(
            internal_id="FUT_BR",
            finam_symbol="BRQ6",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRQ6",
            asset_group="commodity",
            requested_target_timeframes="5m,15m",
            source_interval=1,
            source_timeframe="1m",
            coverage_begin_utc="2026-04-01T00:00:00Z",
            coverage_end_utc="2026-04-01T08:00:00Z",
            discovered_at_utc="2026-04-01T08:00:00Z",
            discovery_url="https://iss.moex.com/example",
        )
    ]

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    captured: dict[str, object] = {}

    def _spark_watermarks(**kwargs):
        captured["watermark_kwargs"] = kwargs
        return {("FUT_BR", "1m", 1, "BRQ6"): "2026-04-01T07:19:59Z"}

    def _spark_raw_ingest(**kwargs):
        captured["spark_ingest_kwargs"] = kwargs
        assert "source_rows_path" in kwargs
        assert kwargs.get("source_rows", []) == []
        staged_rows = [
            line
            for line in kwargs["source_rows_path"].read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return {
            "run_id": kwargs["run_id"],
            "status": "PASS",
            "ingest_till_utc": kwargs["ingest_till_utc"],
            "source_rows": len(staged_rows),
            "incremental_rows": 2,
            "deduplicated_rows": 1,
            "stale_rows": 0,
            "watermark_by_key": {"FUT_BR|1m|BRQ6": "2026-04-01T07:29:59Z"},
            "raw_table_path": kwargs["table_path"].as_posix(),
            "raw_ingest_progress_path": kwargs["progress_path"].as_posix(),
            "raw_ingest_error_path": kwargs["error_path"].as_posix(),
            "raw_ingest_error_latest_path": kwargs["error_latest_path"].as_posix(),
            "changed_windows": [
                {
                    "internal_id": "FUT_BR",
                    "source_timeframe": "1m",
                    "source_interval": 1,
                    "moex_secid": "BRQ6",
                    "window_start_utc": "2026-04-01T06:59:59Z",
                    "window_end_utc": "2026-04-01T08:00:00Z",
                    "incremental_rows": 2,
                }
            ],
        }

    def _forbidden_hot_python_delta_operation(*_args, **_kwargs):
        pytest.fail("raw refresh hot-table read/diff/mutation must be Spark/Delta-owned")

    monkeypatch.setattr(foundation_module, "compute_raw_watermarks_spark_delta", _spark_watermarks)
    monkeypatch.setattr(foundation_module, "run_moex_raw_ingest_spark_delta_job", _spark_raw_ingest)
    monkeypatch.setattr(
        foundation_module,
        "read_delta_table_rows",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )
    monkeypatch.setattr(
        foundation_module,
        "append_delta_table_rows",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )
    monkeypatch.setattr(
        foundation_module,
        "delete_delta_table_rows",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )
    monkeypatch.setattr(
        foundation_module,
        "iter_delta_table_row_batches",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )

    report = ingest_moex_baseline_window(
        client=_BaselineWindowClient(),
        coverage=coverage,
        table_path=raw_table_path,
        run_id="baseline-scoped",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_window_days=1,
        stability_lag_minutes=0,
        refresh_overlap_minutes=20,
    )

    assert report["incremental_rows"] == 2
    assert report["deduplicated_rows"] == 1
    assert captured["watermark_kwargs"]["keys"] == {("FUT_BR", "1m", 1, "BRQ6")}
    spark_kwargs = captured["spark_ingest_kwargs"]
    assert spark_kwargs["table_path"] == raw_table_path
    assert spark_kwargs["source_rows_path"].name == "baseline-scoped-source-rows.jsonl"
    assert len(spark_kwargs["source_rows_path"].read_text(encoding="utf-8").splitlines()) == 3
    assert spark_kwargs["window_scopes"] == [
        {
            "internal_id": "FUT_BR",
            "timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRQ6",
            "window_start_utc": "2026-04-01T06:59:59Z",
            "window_end_utc": "2026-04-01T08:00:00Z",
            "watermark_utc": "2026-04-01T07:19:59Z",
        }
    ]


def test_bootstrap_raw_window_uses_spark_delta_hot_table_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "bootstrap" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(ts_open="2026-04-01T07:00:00Z", ts_close="2026-04-01T07:09:59Z", close=100.5),
            _raw_row(ts_open="2026-04-01T07:10:00Z", ts_close="2026-04-01T07:19:59Z", close=101.2),
        ],
        columns=RAW_COLUMNS,
    )
    coverage = [
        DiscoveryRecord(
            internal_id="FUT_BR",
            finam_symbol="BRQ6",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRQ6",
            asset_group="commodity",
            requested_target_timeframes="5m,15m",
            source_interval=1,
            source_timeframe="1m",
            coverage_begin_utc="2026-04-01T00:00:00Z",
            coverage_end_utc="2026-04-01T08:00:00Z",
            discovered_at_utc="2026-04-01T08:00:00Z",
            discovery_url="https://iss.moex.com/example",
        )
    ]

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    captured: dict[str, object] = {}

    def _spark_watermarks(**kwargs):
        captured["watermark_kwargs"] = kwargs
        return {("FUT_BR", "1m", 1, "BRQ6"): "2026-04-01T07:19:59Z"}

    def _spark_raw_ingest(**kwargs):
        captured["spark_ingest_kwargs"] = kwargs
        assert "source_rows_path" in kwargs
        assert kwargs.get("source_rows", []) == []
        staged_rows = [
            line
            for line in kwargs["source_rows_path"].read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return {
            "run_id": kwargs["run_id"],
            "status": "PASS",
            "ingest_till_utc": kwargs["ingest_till_utc"],
            "source_rows": len(staged_rows),
            "incremental_rows": 2,
            "deduplicated_rows": 1,
            "stale_rows": 0,
            "watermark_by_key": {"FUT_BR|1m|BRQ6": "2026-04-01T07:29:59Z"},
            "raw_table_path": kwargs["table_path"].as_posix(),
            "raw_ingest_progress_path": kwargs["progress_path"].as_posix(),
            "raw_ingest_error_path": kwargs["error_path"].as_posix(),
            "raw_ingest_error_latest_path": kwargs["error_latest_path"].as_posix(),
            "changed_windows": [
                {
                    "internal_id": "FUT_BR",
                    "source_timeframe": "1m",
                    "source_interval": 1,
                    "moex_secid": "BRQ6",
                    "window_start_utc": "2026-04-01T06:59:59Z",
                    "window_end_utc": "2026-04-01T08:00:00Z",
                    "incremental_rows": 2,
                }
            ],
        }

    def _forbidden_hot_python_delta_operation(*_args, **_kwargs):
        pytest.fail("bootstrap raw hot-table read/diff/mutation must be Spark/Delta-owned")

    monkeypatch.setattr(foundation_module, "compute_raw_watermarks_spark_delta", _spark_watermarks)
    monkeypatch.setattr(foundation_module, "run_moex_raw_ingest_spark_delta_job", _spark_raw_ingest)
    monkeypatch.setattr(
        foundation_module,
        "read_delta_table_rows",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )
    monkeypatch.setattr(
        foundation_module,
        "write_delta_table_rows",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )
    monkeypatch.setattr(
        foundation_module,
        "append_delta_table_rows",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )
    monkeypatch.setattr(
        foundation_module,
        "delete_delta_table_rows",
        _forbidden_hot_python_delta_operation,
        raising=False,
    )

    report = ingest_moex_bootstrap_window(
        client=_BaselineWindowClient(),
        coverage=coverage,
        table_path=raw_table_path,
        run_id="bootstrap-scoped",
        ingest_till_utc="2026-04-01T08:00:00Z",
        bootstrap_window_days=1,
        stability_lag_minutes=0,
        refresh_overlap_minutes=20,
    )

    assert report["incremental_rows"] == 2
    assert report["deduplicated_rows"] == 1
    assert captured["watermark_kwargs"]["keys"] == {("FUT_BR", "1m", 1, "BRQ6")}
    spark_kwargs = captured["spark_ingest_kwargs"]
    assert spark_kwargs["table_path"] == raw_table_path
    assert spark_kwargs["source_rows_path"].name == "bootstrap-scoped-source-rows.jsonl"
    assert len(spark_kwargs["source_rows_path"].read_text(encoding="utf-8").splitlines()) == 3
    assert spark_kwargs["window_scopes"] == [
        {
            "internal_id": "FUT_BR",
            "timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRQ6",
            "window_start_utc": "2026-04-01T06:59:59Z",
            "window_end_utc": "2026-04-01T08:00:00Z",
            "watermark_utc": "2026-04-01T07:19:59Z",
        }
    ]
