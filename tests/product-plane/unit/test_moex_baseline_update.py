from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    read_delta_table_rows,
    read_filtered_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex import baseline_update as baseline_module
from trading_advisor_3000.product_plane.data_plane.moex.foundation import (
    RAW_COLUMNS,
    DiscoveryRecord,
    MappingRecord,
    UniverseSymbol,
    ingest_moex_baseline_window,
    ingest_moex_bootstrap_window,
)
from trading_advisor_3000.product_plane.data_plane.moex.historical_canonical_route import (
    CANONICAL_BAR_COLUMNS,
    CANONICAL_MERGE_SCOPED_DELETE_INSERT,
    PROVENANCE_COLUMNS,
)
from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import (
    build_parity_manifest_v1,
    changed_windows_hash_sha256,
)
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexCandle


def test_baseline_update_timeframes_accepts_comma_delimited_string() -> None:
    assert baseline_module._baseline_update_timeframes("5m,15m,1d") == {"5m", "15m", "1d"}
    with pytest.raises(ValueError, match="unsupported baseline update timeframes: 1"):
        baseline_module._baseline_update_timeframes("1d,1")


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
    universe = [
        UniverseSymbol(
            internal_id="FUT_BR",
            asset_class="futures",
            asset_group="energy",
            status="active",
            finam_symbol="BR",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRM6",
            moex_asset_codes=("BR",),
        )
    ]
    mappings = [
        MappingRecord(
            internal_id="FUT_BR",
            finam_symbol="BR",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRM6",
            asset_class="futures",
            asset_group="energy",
            mapping_version=1,
            is_active=True,
            activated_at_utc="2026-01-01T00:00:00Z",
            deactivated_at_utc=None,
            change_reason="test",
        )
    ]
    monkeypatch.setattr(baseline_module, "load_universe", lambda _path: universe)
    monkeypatch.setattr(baseline_module, "load_mapping_registry", lambda _path: mappings)
    monkeypatch.setattr(baseline_module, "validate_mapping_registry", lambda _mappings: None)
    monkeypatch.setattr(
        baseline_module, "validate_universe_mapping_alignment", lambda _universe, _mappings: None
    )
    monkeypatch.setattr(baseline_module, "discover_coverage", lambda **_kwargs: [])
    monkeypatch.setattr(
        baseline_module,
        "ingest_moex_baseline_window",
        lambda **kwargs: {
            "contract_version": "raw_ingest_run_report.v2",
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
            "changed_windows_hash_sha256": changed_windows_hash_sha256(changed_windows),
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
    cf_catch_up = report["cf_catch_up"]
    assert cf_catch_up["status"] == "READY"
    assert cf_catch_up["overlap_minutes"] == 180
    assert cf_catch_up["target_timeframes"] == ["15m", "1h", "4h", "1d"]
    assert len(cf_catch_up["windows"]) == 4
    cf_15m_window = next(
        window for window in cf_catch_up["windows"] if window["timeframe"] == "15m"
    )
    assert cf_15m_window["instrument_id"] == "FUT_BR"
    assert cf_15m_window["start_ts"] == "2026-04-20T21:00:00Z"
    assert cf_15m_window["end_ts"] == "2026-04-22T00:00:00Z"
    assert cf_15m_window["source_window_start_utc"] == "2026-04-21T00:00:00Z"
    assert cf_15m_window["source_window_end_utc"] == "2026-04-22T00:00:00Z"
    assert cf_15m_window["source_changed_window_count"] == 1
    assert cf_15m_window["window_hash_sha256"]
    assert report["runtime_boundary"] == {
        "orchestrator": "dagster",
        "hot_table_runtime": "spark_delta_raw_tail+spark_delta_canonical",
        "python_role": "source_adapter_config_and_evidence",
    }
    assert not (tmp_path / "evidence" / "pending-changed-windows.json").exists()


def test_baseline_update_can_refresh_money_math_side_tables_before_canonical_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    changed_windows = [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1d",
            "source_interval": 24,
            "moex_secid": "BRN6",
            "window_start_utc": "2026-06-11T00:00:00Z",
            "window_end_utc": "2026-06-12T00:00:00Z",
            "incremental_rows": 2,
        }
    ]
    _patch_common_inputs(monkeypatch, changed_windows)
    stage_order: list[str] = []

    def _fake_economics(**kwargs):
        stage_order.append("economics")
        assert kwargs["raw_economics_root"] == tmp_path / "raw" / "economics"
        assert kwargs["canonical_economics_root"] == tmp_path / "canonical" / "economics"
        assert kwargs["changed_windows"] == changed_windows
        return {
            "status": "PASS",
            "mode": "baseline_update_economics_refresh",
            "row_counts": {
                "canonical_fx_rates": 2,
                "canonical_asset_risk_parameters": 1,
                "canonical_contract_economics": 1,
            },
            "missing_economics_rows": 0,
            "defaulted_radius_rows": 1,
            "official_margin_dominates_rows": 1,
            "formula_margin_dominates_rows": 0,
            "affected_downstream_partitions": [{"instrument_id": "FUT_BR", "timeframe": "1d"}],
        }

    def _fake_canonical(**_kwargs):
        stage_order.append("canonical")
        return {
            "status": "PASS",
            "publish_decision": "publish",
            "scoped_source_rows": 2,
            "scoped_canonical_rows": 2,
            "canonical_rows": 2,
            "mutation_applied": True,
        }

    monkeypatch.setattr(baseline_module, "refresh_moex_contract_economics", _fake_economics)
    monkeypatch.setattr(baseline_module, "run_historical_canonical_route", _fake_canonical)

    report = baseline_module.run_moex_baseline_update(
        mapping_registry_path=tmp_path / "mapping.yaml",
        universe_path=tmp_path / "universe.yaml",
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        evidence_dir=tmp_path / "evidence",
        run_id="baseline-money-math",
        timeframes={"1d"},
        ingest_till_utc="2026-06-12T00:00:00Z",
        refresh_window_days=7,
        contract_discovery_lookback_days=45,
        max_changed_window_days=10,
        economics_mode="refresh",
        raw_economics_root=tmp_path / "raw" / "economics",
        canonical_economics_root=tmp_path / "canonical" / "economics",
    )

    assert stage_order == ["economics", "canonical"]
    assert report["economics_refresh"]["status"] == "PASS"
    assert report["economics_refresh"]["row_counts"]["canonical_contract_economics"] == 1
    assert report["economics_refresh"]["defaulted_radius_rows"] == 1
    assert (
        report["artifact_paths"]["moex_request_log"]
        == (tmp_path / "evidence" / "baseline-money-math" / "moex-request-log.jsonl").as_posix()
    )
    assert (
        report["artifact_paths"]["moex_request_latest"]
        == (tmp_path / "evidence" / "baseline-money-math" / "moex-request.latest.json").as_posix()
    )


def test_contract_economics_refresh_writes_iss_raw_side_tables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        def fetch_futures_contract_securities(self) -> list[dict[str, object]]:
            return [
                {
                    "SECID": "BRN6",
                    "ASSETCODE": "BR",
                    "MINSTEP": 0.01,
                    "LOTVOLUME": 10,
                    "STEPPRICE": 7.19077,
                    "INITIALMARGIN": 17_721.61,
                    "LASTSETTLEPRICE": 93.99,
                    "MATDATE": "2026-07-15",
                }
            ]

        def fetch_futures_indicative_rates(self, **_kwargs) -> list[dict[str, object]]:
            return [
                {
                    "tradedate": "2026-06-11",
                    "tradetime": "19:00:00",
                    "secid": "USD/RUB",
                    "rate": 71.9077,
                    "clearing": "mc",
                }
            ]

        def fetch_futures_rms_limits(self, **_kwargs) -> list[dict[str, object]]:
            return [
                {
                    "tradedate": "2026-06-11",
                    "assetcode": "BR",
                    "mr1": 0.12,
                    "mr2": 0.0,
                    "mr3": 0.0,
                    "lk1": 0,
                    "lk2": 0,
                    "title": "Brent",
                    "group_title": "Oil",
                    "updatetime": "2026-06-11 19:00:00",
                }
            ]

        def fetch_futures_rms_staticparams(self, **_kwargs) -> list[dict[str, object]]:
            return [
                {
                    "tradedate": "2026-06-11",
                    "assetcode": "BR",
                    "radius": 15.0,
                    "updatetime": "2026-06-11 19:00:00",
                }
            ]

    def _fake_job(**kwargs):
        contract_rows = read_delta_table_rows(kwargs["raw_contract_specs_path"], limit=10)
        fx_rows = read_delta_table_rows(kwargs["raw_fx_rates_path"], limit=10)
        limits_rows = read_delta_table_rows(kwargs["raw_rms_limits_path"], limit=10)
        static_rows = read_delta_table_rows(kwargs["raw_rms_staticparams_path"], limit=10)
        contract_dates = {str(row["trade_date"]) for row in contract_rows}
        assert contract_dates == {"2026-06-10", "2026-06-11"}
        current_contract = next(row for row in contract_rows if row["trade_date"] == "2026-06-11")
        assert current_contract["moex_secid"] == "BRN6"
        assert json.loads(str(current_contract["raw_payload_json"]))["SECID"] == "BRN6"
        assert fx_rows[0]["fx_pair"] == "USD/RUB"
        assert json.loads(str(fx_rows[0]["raw_payload_json"]))["secid"] == "USD/RUB"
        assert limits_rows[0]["assetcode"] == "BR"
        assert limits_rows[0]["mr1"] == pytest.approx(0.12)
        assert json.loads(str(limits_rows[0]["raw_payload_json"]))["assetcode"] == "BR"
        assert static_rows[0]["assetcode"] == "BR"
        assert static_rows[0]["radius_pct"] == pytest.approx(15.0)
        assert json.loads(str(static_rows[0]["raw_payload_json"]))["assetcode"] == "BR"
        return {
            "status": "PASS",
            "row_counts": {
                "canonical_fx_rates": 2,
                "canonical_asset_risk_parameters": 1,
                "canonical_contract_economics": 1,
            },
        }

    monkeypatch.setattr(
        "trading_advisor_3000.spark_jobs.moex_contract_economics_job.run_moex_contract_economics_spark_job",
        _fake_job,
    )

    raw_root = tmp_path / "raw" / "economics"
    write_delta_table_rows(
        table_path=raw_root / "raw_moex_contract_securities.delta",
        rows=[
            {
                "source_id": "moex_iss_forts_securities",
                "source_url": "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json",
                "source_document_id": "BRM6-2026-06-10",
                "source_document_hash": "old-contract-hash",
                "fetched_at_utc": "2026-06-10T19:30:00Z",
                "engine": "futures",
                "market": "forts",
                "board": "RFUD",
                "moex_secid": "BRM6",
                "trade_date": "2026-06-10",
                "assetcode": "BR",
                "raw_payload_json": json.dumps({"SECID": "BRM6"}),
            }
        ],
        columns=baseline_module._economics_columns("raw_moex_contract_securities"),
    )

    report = baseline_module.refresh_moex_contract_economics(
        client=_Client(),
        universe=[],
        mappings=[],
        raw_economics_root=raw_root,
        canonical_economics_root=tmp_path / "canonical" / "economics",
        evidence_dir=tmp_path / "evidence",
        run_id="money-math-refresh",
        ingest_till_utc="2026-06-11T19:30:00Z",
        changed_windows=[],
        refresh_window_days=7,
    )

    assert report["status"] == "PASS"
    assert report["raw_refresh"]["raw_written_rows"] == {
        "raw_moex_contract_securities": 1,
        "raw_moex_indicative_fx_rates": 1,
        "raw_moex_rms_limits": 1,
        "raw_moex_rms_staticparams": 1,
    }
    assert report["raw_refresh"]["raw_write_mode"] == "scoped_replace_by_trade_date"


def test_baseline_update_heartbeats_during_canonical_refresh(
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
    heartbeat_path = (
        tmp_path / "evidence" / "baseline-heartbeat" / "canonical-refresh-heartbeat.json"
    )

    def _fake_canonical(**_kwargs):
        payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        assert payload["status"] == "RUNNING"
        assert payload["step"] == "canonical_refresh"
        assert payload["run_id"] == "baseline-heartbeat"
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
        run_id="baseline-heartbeat",
        timeframes={"5m", "15m"},
        ingest_till_utc="2026-04-22T00:00:00Z",
        refresh_window_days=7,
        contract_discovery_lookback_days=45,
        max_changed_window_days=10,
    )

    assert report["status"] == "PASS"
    heartbeat_payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    assert heartbeat_payload["status"] == "COMPLETED"
    assert heartbeat_payload["publish_decision"] == "publish"
    assert heartbeat_payload["completed_at_utc"]


def test_baseline_update_skips_canonical_when_raw_and_pending_are_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    _patch_common_inputs(monkeypatch, [])

    def _unexpected_canonical(**_kwargs):
        raise AssertionError("canonical route must not run when raw and pending are noop")

    monkeypatch.setattr(baseline_module, "run_historical_canonical_route", _unexpected_canonical)

    report = baseline_module.run_moex_baseline_update(
        mapping_registry_path=tmp_path / "mapping.yaml",
        universe_path=tmp_path / "universe.yaml",
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        evidence_dir=tmp_path / "evidence",
        run_id="baseline-daily-noop",
        timeframes={"5m"},
        ingest_till_utc="2026-04-22T00:00:00Z",
        refresh_window_days=7,
        contract_discovery_lookback_days=45,
        max_changed_window_days=10,
    )

    assert report["status"] == "PASS"
    assert report["source_rows"] == 12
    assert report["incremental_rows"] == 0
    assert report["current_changed_windows"] == 0
    assert report["effective_changed_windows"] == 0
    assert report["cf_catch_up"]["status"] == "NOOP"
    assert report["cf_catch_up"]["windows"] == []
    assert report["canonical_report"] == {
        "status": "PASS-NOOP",
        "publish_decision": "publish",
        "scoped_source_rows": 0,
        "scoped_canonical_rows": 0,
        "canonical_rows": 0,
        "sidecar_refresh": "skipped",
        "mutation_applied": False,
        "skipped_reason": "no_raw_or_pending_changed_windows",
    }
    assert report["artifact_paths"]["canonical_refresh_report"] == ""
    assert not (tmp_path / "evidence" / "pending-changed-windows.json").exists()
    assert not (tmp_path / "evidence" / "baseline-daily-noop" / "canonical-refresh").exists()


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
    heartbeat_path = (
        tmp_path / "evidence" / "baseline-daily-failed" / "canonical-refresh-heartbeat.json"
    )
    heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    assert heartbeat["status"] == "FAILED"
    assert heartbeat["error_type"] == "RuntimeError"
    assert heartbeat["completed_at_utc"]


def test_baseline_update_rehashes_pending_windows_for_canonical_after_raw_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    pending_windows = [
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
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "pending-changed-windows.json").write_text(
        json.dumps(
            {
                "status": "PENDING",
                "run_id": "previous-run",
                "reason": "canonical_refresh_failed",
                "changed_windows": pending_windows,
            }
        ),
        encoding="utf-8",
    )
    _patch_common_inputs(monkeypatch, [])
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
        evidence_dir=evidence_dir,
        run_id="baseline-daily-retry",
        timeframes={"5m"},
        ingest_till_utc="2026-04-22T00:00:00Z",
        refresh_window_days=7,
        contract_discovery_lookback_days=45,
        max_changed_window_days=10,
    )

    canonical_raw_report = captured["raw_ingest_run_report"]
    assert report["pending_changed_windows_in"] == 1
    assert report["current_changed_windows"] == 0
    assert report["effective_changed_windows"] == 1
    assert canonical_raw_report["status"] == "PASS"
    assert canonical_raw_report["changed_windows"] == pending_windows
    assert canonical_raw_report["changed_windows_hash_sha256"] == (
        changed_windows_hash_sha256(pending_windows)
    )
    parity_manifest = build_parity_manifest_v1(
        run_id="baseline-daily-retry",
        raw_ingest_run_report=canonical_raw_report,
    )
    assert parity_manifest["window_count"] == 1
    assert not (evidence_dir / "pending-changed-windows.json").exists()


def test_baseline_update_projects_pending_and_current_windows_into_merged_cf_catch_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    pending_windows = [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6@MOEX",
            "window_start_utc": "2026-04-21T00:00:00Z",
            "window_end_utc": "2026-04-21T00:30:00Z",
            "incremental_rows": 12,
        }
    ]
    current_windows = [
        {
            "internal_id": "FUT_BR",
            "source_timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRM6@MOEX",
            "window_start_utc": "2026-04-21T02:00:00Z",
            "window_end_utc": "2026-04-22T00:00:00Z",
            "incremental_rows": 18,
        }
    ]
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "pending-changed-windows.json").write_text(
        json.dumps(
            {
                "status": "PENDING",
                "run_id": "previous-run",
                "reason": "canonical_refresh_failed",
                "changed_windows": pending_windows,
            }
        ),
        encoding="utf-8",
    )
    _patch_common_inputs(monkeypatch, current_windows)

    monkeypatch.setattr(
        baseline_module,
        "run_historical_canonical_route",
        lambda **_kwargs: {
            "status": "PASS",
            "publish_decision": "publish",
            "scoped_source_rows": 30,
            "scoped_canonical_rows": 8,
            "canonical_rows": 8,
            "mutation_applied": True,
        },
    )

    report = baseline_module.run_moex_baseline_update(
        mapping_registry_path=tmp_path / "mapping.yaml",
        universe_path=tmp_path / "universe.yaml",
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        evidence_dir=evidence_dir,
        run_id="baseline-daily-pending-current",
        timeframes={"5m"},
        ingest_till_utc="2026-04-22T00:00:00Z",
        refresh_window_days=7,
        contract_discovery_lookback_days=45,
        max_changed_window_days=10,
        cf_catch_up_timeframes=("15m",),
    )

    assert report["pending_changed_windows_in"] == 1
    assert report["current_changed_windows"] == 1
    assert report["effective_changed_windows"] == 2
    cf_catch_up = report["cf_catch_up"]
    assert cf_catch_up["status"] == "READY"
    assert len(cf_catch_up["windows"]) == 1
    window = cf_catch_up["windows"][0]
    assert window["instrument_id"] == "FUT_BR"
    assert window["timeframe"] == "15m"
    assert window["start_ts"] == "2026-04-20T21:00:00Z"
    assert window["end_ts"] == "2026-04-22T00:00:00Z"
    assert window["source_window_start_utc"] == "2026-04-21T00:00:00Z"
    assert window["source_window_end_utc"] == "2026-04-22T00:00:00Z"
    assert window["source_changed_window_count"] == 2


def test_baseline_update_blocks_duplicate_completed_run_id_before_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    run_dir = tmp_path / "evidence" / "baseline-daily"
    run_dir.mkdir(parents=True)
    (run_dir / baseline_module.BASELINE_UPDATE_REPORT_FILENAME).write_text(
        json.dumps({"run_id": "baseline-daily", "status": "PASS"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        baseline_module,
        "discover_coverage",
        lambda **_kwargs: pytest.fail("duplicate run id must stop before discovery"),
    )

    with pytest.raises(RuntimeError, match="already has a completed baseline update report"):
        baseline_module.run_moex_baseline_update(
            mapping_registry_path=tmp_path / "mapping.yaml",
            universe_path=tmp_path / "universe.yaml",
            raw_table_path=raw_table_path,
            canonical_bars_path=canonical_bars_path,
            canonical_provenance_path=canonical_provenance_path,
            evidence_dir=tmp_path / "evidence",
            run_id="baseline-daily",
            timeframes={"5m"},
            ingest_till_utc="2026-04-22T00:00:00Z",
            refresh_window_days=7,
            contract_discovery_lookback_days=45,
            max_changed_window_days=10,
        )


def test_baseline_update_uses_local_tail_coverage_without_live_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    universe = [
        UniverseSymbol(
            internal_id="FUT_BR",
            asset_class="futures",
            asset_group="energy",
            status="active",
            finam_symbol="BR",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRM6",
            moex_asset_codes=("BR",),
        )
    ]
    mappings = [
        MappingRecord(
            internal_id="FUT_BR",
            finam_symbol="BR",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRM6",
            asset_class="futures",
            asset_group="energy",
            mapping_version=1,
            is_active=True,
            activated_at_utc="2026-01-01T00:00:00Z",
            deactivated_at_utc=None,
            change_reason="test",
        )
    ]
    captured: dict[str, object] = {}

    monkeypatch.setattr(baseline_module, "load_universe", lambda _path: universe)
    monkeypatch.setattr(baseline_module, "load_mapping_registry", lambda _path: mappings)
    monkeypatch.setattr(baseline_module, "validate_mapping_registry", lambda _mappings: None)
    monkeypatch.setattr(
        baseline_module, "validate_universe_mapping_alignment", lambda _universe, _mappings: None
    )
    monkeypatch.setattr(
        baseline_module,
        "discover_coverage",
        lambda **_kwargs: pytest.fail("tail update must not call live MOEX discovery"),
    )

    class _CapturedMoexClient:
        def __init__(self, **kwargs):
            captured["client_kwargs"] = kwargs

    monkeypatch.setattr(baseline_module, "MoexISSClient", _CapturedMoexClient)

    def _fake_ingest(**kwargs):
        captured["coverage"] = kwargs["coverage"]
        return {
            "run_id": kwargs["run_id"],
            "status": "PASS-NOOP",
            "ingest_till_utc": kwargs["ingest_till_utc"],
            "source_rows": 0,
            "incremental_rows": 0,
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "watermark_by_key": {},
            "raw_table_path": kwargs["table_path"].as_posix(),
            "raw_ingest_progress_path": kwargs["progress_path"].as_posix(),
            "raw_ingest_error_path": kwargs["error_path"].as_posix(),
            "raw_ingest_error_latest_path": kwargs["error_latest_path"].as_posix(),
            "changed_windows": [],
        }

    monkeypatch.setattr(baseline_module, "ingest_moex_baseline_window", _fake_ingest)
    monkeypatch.setattr(
        baseline_module,
        "run_historical_canonical_route",
        lambda **_kwargs: {
            "status": "PASS",
            "publish_decision": "publish",
            "scoped_source_rows": 0,
            "scoped_canonical_rows": 0,
            "canonical_rows": 0,
            "mutation_applied": False,
        },
    )

    report = baseline_module.run_moex_baseline_update(
        mapping_registry_path=tmp_path / "mapping.yaml",
        universe_path=tmp_path / "universe.yaml",
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        evidence_dir=tmp_path / "evidence",
        run_id="baseline-tail",
        timeframes={"5m", "15m"},
        ingest_till_utc="2026-04-22T00:00:00Z",
        refresh_window_days=1,
        contract_discovery_lookback_days=45,
        max_changed_window_days=2,
    )

    coverage = captured["coverage"]
    assert report["coverage_mode"] == "local_tail"
    assert len(coverage) == 1
    item = coverage[0]
    assert item.internal_id == "FUT_BR"
    assert item.moex_secid == "BRM6"
    assert item.source_interval == 1
    assert item.requested_target_timeframes == "5m,15m"
    assert item.coverage_end_utc == "2026-04-22T00:00:00Z"
    assert item.discovery_url == "local-tail://mapping-registry/FUT_BR/1/BRM6"
    assert captured["client_kwargs"]["timeout_seconds"] == 6.0
    assert captured["client_kwargs"]["max_retries"] == 1
    assert captured["client_kwargs"]["retry_backoff_seconds"] == 0.5
    assert captured["client_kwargs"]["retry_jitter_ratio"] == 0.0


def test_baseline_update_live_discovery_keeps_default_request_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    _patch_common_inputs(monkeypatch, [])
    captured: dict[str, object] = {}

    class _CapturedMoexClient:
        def __init__(self, **kwargs):
            captured["client_kwargs"] = kwargs

    monkeypatch.setattr(baseline_module, "MoexISSClient", _CapturedMoexClient)
    monkeypatch.setattr(
        baseline_module,
        "discover_coverage",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        baseline_module,
        "run_historical_canonical_route",
        lambda **_kwargs: pytest.fail("empty raw/pending should skip canonical"),
    )

    report = baseline_module.run_moex_baseline_update(
        mapping_registry_path=tmp_path / "mapping.yaml",
        universe_path=tmp_path / "universe.yaml",
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        evidence_dir=tmp_path / "evidence",
        run_id="baseline-live-discovery",
        timeframes={"5m"},
        ingest_till_utc="2026-04-22T00:00:00Z",
        refresh_window_days=1,
        contract_discovery_lookback_days=45,
        max_changed_window_days=2,
        coverage_mode=baseline_module.BASELINE_COVERAGE_MODE_LIVE_DISCOVERY,
    )

    assert report["coverage_mode"] == baseline_module.BASELINE_COVERAGE_MODE_LIVE_DISCOVERY
    assert "timeout_seconds" not in captured["client_kwargs"]
    assert "max_retries" not in captured["client_kwargs"]
    assert "retry_backoff_seconds" not in captured["client_kwargs"]
    assert "retry_jitter_ratio" not in captured["client_kwargs"]


def test_baseline_update_local_tail_uses_latest_roll_map_contract_over_stale_mapping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path, canonical_bars_path, canonical_provenance_path = _write_empty_baseline(tmp_path)
    canonical_roll_map_path = canonical_bars_path.parent / "canonical_roll_map.delta"
    write_delta_table_rows(
        table_path=canonical_roll_map_path,
        rows=[
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-06-08",
                "active_contract_id": "BRQ6@MOEX",
                "reason": "test-latest-roll",
            }
        ],
        columns={
            "instrument_id": "string",
            "session_date": "date",
            "active_contract_id": "string",
            "reason": "string",
        },
    )
    universe = [
        UniverseSymbol(
            internal_id="FUT_BR",
            asset_class="futures",
            asset_group="energy",
            status="active",
            finam_symbol="BR",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRK6",
            moex_asset_codes=("BR",),
        )
    ]
    mappings = [
        MappingRecord(
            internal_id="FUT_BR",
            finam_symbol="BRK6@MOEX",
            moex_engine="futures",
            moex_market="forts",
            moex_board="RFUD",
            moex_secid="BRK6",
            asset_class="futures",
            asset_group="energy",
            mapping_version=1,
            is_active=True,
            activated_at_utc="2026-04-01T00:00:00Z",
            deactivated_at_utc=None,
            change_reason="stale-test",
        )
    ]
    captured: dict[str, object] = {}

    monkeypatch.setattr(baseline_module, "load_universe", lambda _path: universe)
    monkeypatch.setattr(baseline_module, "load_mapping_registry", lambda _path: mappings)
    monkeypatch.setattr(baseline_module, "validate_mapping_registry", lambda _mappings: None)
    monkeypatch.setattr(
        baseline_module, "validate_universe_mapping_alignment", lambda _universe, _mappings: None
    )
    monkeypatch.setattr(
        baseline_module,
        "discover_coverage",
        lambda **_kwargs: pytest.fail("roll-map local tail must not call live discovery"),
    )

    def _fake_ingest(**kwargs):
        captured["coverage"] = kwargs["coverage"]
        return {
            "run_id": kwargs["run_id"],
            "status": "PASS-NOOP",
            "ingest_till_utc": kwargs["ingest_till_utc"],
            "source_rows": 0,
            "incremental_rows": 0,
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "watermark_by_key": {},
            "raw_table_path": kwargs["table_path"].as_posix(),
            "raw_ingest_progress_path": kwargs["progress_path"].as_posix(),
            "raw_ingest_error_path": kwargs["error_path"].as_posix(),
            "raw_ingest_error_latest_path": kwargs["error_latest_path"].as_posix(),
            "changed_windows": [],
        }

    monkeypatch.setattr(baseline_module, "ingest_moex_baseline_window", _fake_ingest)
    monkeypatch.setattr(
        baseline_module,
        "run_historical_canonical_route",
        lambda **_kwargs: {
            "status": "PASS",
            "publish_decision": "publish",
            "scoped_source_rows": 0,
            "scoped_canonical_rows": 0,
            "canonical_rows": 0,
            "mutation_applied": False,
        },
    )

    baseline_module.run_moex_baseline_update(
        mapping_registry_path=tmp_path / "mapping.yaml",
        universe_path=tmp_path / "universe.yaml",
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        canonical_roll_map_path=canonical_roll_map_path,
        evidence_dir=tmp_path / "evidence",
        run_id="baseline-roll-map-tail",
        timeframes={"5m"},
        ingest_till_utc="2026-06-10T00:00:00Z",
        refresh_window_days=1,
        contract_discovery_lookback_days=1,
        max_changed_window_days=2,
    )

    coverage = captured["coverage"]
    assert len(coverage) == 1
    item = coverage[0]
    assert item.moex_secid == "BRQ6"
    assert item.finam_symbol == "BRQ6@MOEX"
    assert item.discovery_url == "local-tail://canonical-roll-map/FUT_BR/1/BRQ6/2026-06-08"


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


def test_raw_watermark_delegates_to_spark_job_with_three_part_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module
    import trading_advisor_3000.spark_jobs.moex_raw_ingest_job as spark_raw_module

    captured: dict[str, object] = {}

    def _fake_compute_raw_watermarks_spark_delta(**kwargs):
        captured.update(kwargs)
        return {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"}

    monkeypatch.setattr(
        spark_raw_module,
        "compute_raw_watermarks_spark_delta",
        _fake_compute_raw_watermarks_spark_delta,
    )

    watermarks = foundation_module.compute_raw_watermarks_spark_delta(
        table_path=raw_table_path,
        keys={("FUT_BR", "1m", "BRQ6")},
        min_ts_close_utc="2026-04-01T00:00:00Z",
    )

    assert captured == {
        "table_path": raw_table_path,
        "keys": {("FUT_BR", "1m", "BRQ6")},
    }
    assert watermarks == {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"}


def test_raw_watermark_wrapper_does_not_use_legacy_delta_rs_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module
    import trading_advisor_3000.spark_jobs.moex_raw_ingest_job as spark_raw_module

    monkeypatch.setattr(
        foundation_module,
        "_compute_raw_watermarks_delta_rs",
        lambda **_kwargs: pytest.fail("raw watermark wrapper must delegate to Spark"),
    )
    monkeypatch.setattr(
        spark_raw_module,
        "compute_raw_watermarks_spark_delta",
        lambda **_kwargs: {},
    )

    assert (
        foundation_module.compute_raw_watermarks_spark_delta(
            table_path=tmp_path / "raw_moex_history.delta",
            keys={("FUT_BR", "1m", "BRQ6")},
        )
        == {}
    )


def test_watermark_key_for_discovery_excludes_source_interval() -> None:
    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    record = DiscoveryRecord(
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
        coverage_end_utc="2026-04-01T07:29:59Z",
        discovered_at_utc="2026-04-01T07:29:59Z",
        discovery_url="local-tail://mapping-registry/FUT_BR/1/BRQ6",
    )

    assert foundation_module._watermark_key_for_discovery(record) == ("FUT_BR", "1m", "BRQ6")


def test_baseline_raw_tail_ingest_delegates_staged_rows_to_spark_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
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
            coverage_end_utc="2026-04-01T07:29:59Z",
            discovered_at_utc="2026-04-01T07:29:59Z",
            discovery_url="local-tail://mapping-registry/FUT_BR/1/BRQ6",
        )
    ]

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    captured: dict[str, object] = {}

    def _fake_compute_raw_watermarks_spark_delta(**kwargs):
        assert kwargs["keys"] == {("FUT_BR", "1m", "BRQ6")}
        return {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:09:59Z"}

    def _fake_run_moex_raw_ingest_spark_delta_job(**kwargs):
        captured.update(kwargs)
        source_rows_path = kwargs["source_rows_path"]
        source_rows = [
            line
            for line in source_rows_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return {
            "status": "PASS",
            "source_rows": len(source_rows),
            "incremental_rows": len(source_rows),
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "watermark_by_key": {"FUT_BR|1m|BRQ6": "2026-04-01T07:29:59Z"},
            "changed_windows": [],
        }

    monkeypatch.setattr(
        foundation_module,
        "compute_raw_watermarks_spark_delta",
        _fake_compute_raw_watermarks_spark_delta,
    )
    monkeypatch.setattr(
        foundation_module,
        "run_moex_raw_ingest_spark_delta_job",
        _fake_run_moex_raw_ingest_spark_delta_job,
    )

    report = ingest_moex_baseline_window(
        client=_BaselineWindowClient(),
        coverage=coverage,
        table_path=raw_table_path,
        run_id="baseline-spark-tail",
        ingest_till_utc="2026-04-01T07:29:59Z",
        refresh_window_days=1,
        stability_lag_minutes=0,
        refresh_overlap_minutes=20,
    )

    assert report["status"] == "PASS"
    assert report["source_rows"] == 3
    assert captured["table_path"] == raw_table_path
    assert captured["initial_watermarks"] == {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:09:59Z"}
    assert captured["refresh_overlap_minutes"] == 20
    assert captured["window_scopes"] == [
        {
            "internal_id": "FUT_BR",
            "timeframe": "1m",
            "source_interval": 1,
            "moex_secid": "BRQ6",
            "window_start_utc": "2026-04-01T06:49:59Z",
            "window_end_utc": "2026-04-01T07:29:59Z",
            "watermark_utc": "2026-04-01T07:09:59Z",
        }
    ]


def test_baseline_raw_tail_noop_skips_fetch_and_reconcile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(
                ts_open="2026-04-01T07:10:00Z",
                ts_close="2026-04-01T07:19:59Z",
                close=101.2,
            ),
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
            coverage_end_utc="2026-04-01T07:19:59Z",
            discovered_at_utc="2026-04-01T07:19:59Z",
            discovery_url="local-tail://mapping-registry/FUT_BR/1/BRQ6",
        )
    ]

    class _UnexpectedClient:
        def iter_candles(self, **_kwargs):
            pytest.fail("noop tail must not fetch MOEX candles")

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    monkeypatch.setattr(
        foundation_module,
        "compute_raw_watermarks_spark_delta",
        lambda **_kwargs: {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"},
    )
    monkeypatch.setattr(
        foundation_module,
        "run_moex_raw_ingest_spark_delta_job",
        lambda **_kwargs: pytest.fail("noop tail must not enter raw reconcile"),
    )

    progress_path = tmp_path / "raw-ingest-progress.jsonl"
    report = ingest_moex_baseline_window(
        client=_UnexpectedClient(),
        coverage=coverage,
        table_path=raw_table_path,
        run_id="baseline-delta-rs-noop-fast",
        ingest_till_utc="2026-04-01T07:19:59Z",
        refresh_window_days=1,
        stability_lag_minutes=0,
        refresh_overlap_minutes=20,
        progress_path=progress_path,
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    assert report["status"] == "PASS-NOOP"
    assert report["source_rows"] == 0
    assert report["incremental_rows"] == 0
    assert report["changed_windows"] == []
    assert report["watermark_by_key"] == {"FUT_BR|1m|BRQ6": "2026-04-01T07:19:59Z"}
    assert "noop_reason" in progress_path.read_text(encoding="utf-8")


def test_baseline_raw_tail_skips_single_missing_source_interval_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(
                ts_open="2026-04-01T07:19:00Z",
                ts_close="2026-04-01T07:19:59Z",
                close=101.2,
            ),
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
            coverage_end_utc="2026-04-01T07:20:59Z",
            discovered_at_utc="2026-04-01T07:20:59Z",
            discovery_url="local-tail://mapping-registry/FUT_BR/1/BRQ6",
        )
    ]

    class _UnexpectedClient:
        def iter_candles(self, **_kwargs):
            pytest.fail("single missing source interval gap must not fetch MOEX candles")

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    monkeypatch.setattr(
        foundation_module,
        "compute_raw_watermarks_spark_delta",
        lambda **_kwargs: {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"},
    )
    monkeypatch.setattr(
        foundation_module,
        "run_moex_raw_ingest_spark_delta_job",
        lambda **_kwargs: pytest.fail("single interval gap must not enter raw reconcile"),
    )

    report = ingest_moex_baseline_window(
        client=_UnexpectedClient(),
        coverage=coverage,
        table_path=raw_table_path,
        run_id="baseline-single-interval-gap",
        ingest_till_utc="2026-04-01T07:20:59Z",
        refresh_window_days=1,
        stability_lag_minutes=0,
        refresh_overlap_minutes=20,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    assert report["status"] == "PASS-NOOP"
    assert report["source_rows"] == 0
    assert report["incremental_rows"] == 0
    assert report["changed_windows"] == []


def test_baseline_raw_tail_skips_short_sparse_trade_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(
                ts_open="2026-04-01T07:19:00Z",
                ts_close="2026-04-01T07:19:59Z",
                close=101.2,
            ),
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
            coverage_end_utc="2026-04-01T07:23:59Z",
            discovered_at_utc="2026-04-01T07:23:59Z",
            discovery_url="local-tail://mapping-registry/FUT_BR/1/BRQ6",
        )
    ]

    class _UnexpectedClient:
        def iter_candles(self, **_kwargs):
            pytest.fail("short sparse tail gap must not fetch MOEX candles")

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    monkeypatch.setattr(
        foundation_module,
        "compute_raw_watermarks_spark_delta",
        lambda **_kwargs: {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"},
    )
    monkeypatch.setattr(
        foundation_module,
        "run_moex_raw_ingest_spark_delta_job",
        lambda **_kwargs: pytest.fail("short sparse tail gap must not enter raw reconcile"),
    )

    report = ingest_moex_baseline_window(
        client=_UnexpectedClient(),
        coverage=coverage,
        table_path=raw_table_path,
        run_id="baseline-short-sparse-gap",
        ingest_till_utc="2026-04-01T07:23:59Z",
        refresh_window_days=1,
        stability_lag_minutes=0,
        refresh_overlap_minutes=20,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    assert report["status"] == "PASS-NOOP"
    assert report["source_rows"] == 0
    assert report["incremental_rows"] == 0
    assert report["changed_windows"] == []


def test_raw_reconcile_ignores_pipeline_provenance_only_changes(
    tmp_path: Path,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    existing = _raw_row(
        ts_open="2026-04-01T07:10:00Z",
        ts_close="2026-04-01T07:19:59Z",
        close=101.2,
    )
    existing["ingest_run_id"] = "old-run"
    existing["ingested_at_utc"] = "2026-04-01T07:20:00Z"
    existing["provenance_json"] = {
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
    }
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

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    report = foundation_module.run_moex_raw_ingest_delta_rs_job(
        table_path=raw_table_path,
        source_rows=[source],
        window_scopes=[
            {
                "internal_id": "FUT_BR",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRQ6",
                "window_start_utc": "2026-04-01T07:10:00Z",
                "window_end_utc": "2026-04-01T07:19:59Z",
                "watermark_utc": "2026-04-01T07:09:59Z",
            }
        ],
        initial_watermarks={("FUT_BR", "1m", 1, "BRQ6"): "2026-04-01T07:09:59Z"},
        run_id="raw-provenance-only",
        ingest_till_utc="2026-04-01T07:19:59Z",
        refresh_overlap_minutes=180,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )
    version_after = sorted((raw_table_path / "_delta_log").glob("*.json"))[-1].name

    assert report["status"] == "PASS-NOOP"
    assert report["source_rows"] == 1
    assert report["incremental_rows"] == 0
    assert report["deduplicated_rows"] == 1
    assert report["changed_windows"] == []
    assert version_after == version_before


def test_raw_reconcile_delta_rs_uses_action_merge_for_update_and_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    import trading_advisor_3000.product_plane.data_plane.moex.foundation as foundation_module

    def _forbidden_window_rewrite(*_args, **_kwargs):
        pytest.fail(
            "raw reconcile must use Delta merge action rows, not delete/append window rewrites"
        )

    monkeypatch.setattr(
        foundation_module,
        "delete_delta_table_rows",
        _forbidden_window_rewrite,
        raising=False,
    )
    monkeypatch.setattr(
        foundation_module,
        "append_delta_table_rows",
        _forbidden_window_rewrite,
        raising=False,
    )

    report = foundation_module.run_moex_raw_ingest_delta_rs_job(
        table_path=raw_table_path,
        source_rows=[
            _raw_row(
                ts_open="2026-04-01T07:00:00Z",
                ts_close="2026-04-01T07:09:59Z",
                close=99.75,
                run_id="raw-action-merge",
            ),
            _raw_row(
                ts_open="2026-04-01T07:20:00Z",
                ts_close="2026-04-01T07:29:59Z",
                close=101.8,
                run_id="raw-action-merge",
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
        run_id="raw-action-merge",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_overlap_minutes=60,
        progress_path=tmp_path / "raw-ingest-progress.jsonl",
        progress_latest_path=tmp_path / "raw-ingest-progress.latest.json",
        error_path=tmp_path / "raw-ingest-errors.jsonl",
        error_latest_path=tmp_path / "raw-ingest-error.latest.json",
    )

    rows = read_filtered_delta_table_rows(
        raw_table_path,
        filters=[("internal_id", "=", "FUT_BR")],
        columns=["ts_close", "close"],
    )
    rows_by_ts_close = {str(row["ts_close"]): float(row["close"]) for row in rows}

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
        return {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"}

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
    assert captured["watermark_kwargs"]["keys"] == {("FUT_BR", "1m", "BRQ6")}
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


def test_baseline_raw_window_skips_completed_watermark_even_with_overlap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            _raw_row(ts_open="2026-04-01T07:50:00Z", ts_close="2026-04-01T07:59:59Z", close=101.2),
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

    class _ForbiddenClient:
        def iter_candles(self, **_kwargs):
            pytest.fail("completed raw watermark must not refetch overlap")

    monkeypatch.setattr(
        foundation_module,
        "compute_raw_watermarks_spark_delta",
        lambda **_kwargs: {("FUT_BR", "1m", "BRQ6"): "2026-04-01T08:00:00Z"},
    )

    monkeypatch.setattr(
        foundation_module,
        "run_moex_raw_ingest_spark_delta_job",
        lambda **_kwargs: pytest.fail("completed raw watermark must not enter raw reconcile"),
    )

    report = ingest_moex_baseline_window(
        client=_ForbiddenClient(),
        coverage=coverage,
        table_path=raw_table_path,
        run_id="baseline-completed",
        ingest_till_utc="2026-04-01T08:00:00Z",
        refresh_window_days=1,
        stability_lag_minutes=0,
        refresh_overlap_minutes=180,
    )

    assert report["incremental_rows"] == 0
    assert report["source_rows"] == 0
    assert report["changed_windows"] == []


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
        return {("FUT_BR", "1m", "BRQ6"): "2026-04-01T07:19:59Z"}

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
    assert captured["watermark_kwargs"]["keys"] == {("FUT_BR", "1m", "BRQ6")}
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
