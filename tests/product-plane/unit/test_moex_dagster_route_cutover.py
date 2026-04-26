from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from dagster import DagsterInstance, build_schedule_context

from trading_advisor_3000.dagster_defs import (
    assert_moex_historical_definitions_executable,
    build_moex_historical_dagster_binding_artifact,
    build_moex_historical_definitions,
    materialize_moex_historical_assets,
    moex_historical_asset_specs,
)
from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import build_raw_ingest_run_report_v2, run_historical_dagster_cutover
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    MOEX_HISTORICAL_DATA_ROOT_ENV,
)


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


@pytest.fixture(autouse=True)
def _external_data_root_env(monkeypatch: pytest.MonkeyPatch) -> None:
    external_root = Path.cwd().resolve().parent / ".ta3000-moex-historical-tests-unit"
    external_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv(MOEX_HISTORICAL_DATA_ROOT_ENV, external_root.as_posix())


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _raw_rows() -> list[dict[str, object]]:
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
                    "source_provider": "moex_iss",
                    "source_interval": 1,
                    "source_timeframe": "1m",
                    "run_id": "phase01-pass1",
                    "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json",
                },
            }
        )
    return rows


def _write_raw_table_and_report(tmp_path: Path, *, run_id: str) -> tuple[Path, Path]:
    rows = _raw_rows()
    raw_table_path = tmp_path / "phase01" / "delta" / "raw_moex_history.delta"
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
    watermarks = {
        "FUT_BR|1m|BRM6@MOEX": str(rows[-1]["ts_close"]),
    }
    raw_report_payload = build_raw_ingest_run_report_v2(
        run_id=run_id,
        ingest_till_utc=str(rows[-1]["ts_close"]),
        source_rows=len(rows),
        incremental_rows=len(rows),
        deduplicated_rows=0,
        stale_rows=0,
        watermark_by_key=watermarks,
        raw_table_path=raw_table_path.as_posix(),
        raw_ingest_progress_path=(tmp_path / "phase01" / "raw-ingest-progress.jsonl").as_posix(),
        raw_ingest_error_path=(tmp_path / "phase01" / "raw-ingest-errors.jsonl").as_posix(),
        raw_ingest_error_latest_path=(tmp_path / "phase01" / "raw-ingest-error.latest.json").as_posix(),
        changed_windows=changed_windows,
    )
    raw_report_path = tmp_path / "phase01" / "raw-ingest-report.json"
    raw_report_path.parent.mkdir(parents=True, exist_ok=True)
    raw_report_path.write_text(json.dumps(raw_report_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return raw_table_path, raw_report_path


def _write_staging_binding_report(tmp_path: Path) -> Path:
    payload = {
        "proof_class": "staging-real",
        "environment": "staging-real",
        "orchestrator": "dagster-daemon",
        "dagster_url": "https://dagster-staging.example.internal",
        "run_ids": {
            "nightly_1": "phase03-staging-nightly-1",
            "nightly_2": "phase03-staging-nightly-2",
            "repair": "phase03-staging-repair-1",
            "backfill": "phase03-staging-backfill-1",
            "recovery": "phase03-staging-recovery-1",
        },
        "artifact_paths": [
            "artifacts/codex/moex-staging-binding/nightly-1.json",
            "artifacts/codex/moex-staging-binding/nightly-2.json",
            "artifacts/codex/moex-staging-binding/recovery.json",
        ],
        "real_bindings": [
            "dagster://staging/moex-historical-cutover",
            "delta-ledger-cas://technical-route-run-ledger",
        ],
    }
    report_path = tmp_path / "phase03" / "staging-binding-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report_path


def test_historical_dagster_cutover_definitions_are_executable(
    tmp_path: Path,
) -> None:
    assert_moex_historical_definitions_executable()
    specs = {item.key: item for item in moex_historical_asset_specs()}
    assert set(specs) == {"moex_baseline_update", "moex_raw_ingest", "moex_canonical_refresh"}
    assert specs["moex_canonical_refresh"].inputs == ("raw_ingest_owner_payload",)
    definitions = build_moex_historical_definitions()
    repository = definitions.get_repository_def()
    schedule_names = {schedule_def.name for schedule_def in repository.schedule_defs}
    assert "moex_baseline_daily_update_schedule" in schedule_names
    assert "moex_historical_nightly_schedule" not in schedule_names
    binding = build_moex_historical_dagster_binding_artifact()
    assert binding["schedule"]["cron"] == "0 2 * * *"
    assert binding["job"]["name"] == "moex_baseline_update_job"
    assert binding["retry_policy"]["max_retries"] == 3

    schedule_def = repository.get_schedule_def("moex_baseline_daily_update_schedule")
    schedule_context = build_schedule_context(
        instance=DagsterInstance.ephemeral(),
        repository_def=repository,
        scheduled_execution_time=datetime(2026, 4, 15, 2, 0, tzinfo=UTC),
    )
    execution_data = schedule_def.evaluate_tick(schedule_context)
    run_requests = list(getattr(execution_data, "run_requests", []) or [])
    assert len(run_requests) == 1
    run_request = run_requests[0]
    assert isinstance(run_request.run_config, dict)
    assert run_request.run_config.get("ops")
    op_config = run_request.run_config["ops"]["moex_baseline_update"]["config"]
    assert op_config["canonical_session_calendar_path"].endswith(
        "canonical/moex/baseline-4y-current/canonical_session_calendar.delta"
    )
    assert op_config["canonical_roll_map_path"].endswith(
        "canonical/moex/baseline-4y-current/canonical_roll_map.delta"
    )


def test_dagster_route_schedule_fails_closed_without_external_data_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(MOEX_HISTORICAL_DATA_ROOT_ENV, raising=False)
    definitions = build_moex_historical_definitions()
    repository = definitions.get_repository_def()
    schedule_def = repository.get_schedule_def("moex_baseline_daily_update_schedule")
    schedule_context = build_schedule_context(
        instance=DagsterInstance.ephemeral(),
        repository_def=repository,
        scheduled_execution_time=datetime(2026, 4, 15, 2, 0, tzinfo=UTC),
    )

    with pytest.raises(Exception) as exc_info:
        schedule_def.evaluate_tick(schedule_context)
    detail = str(exc_info.value)
    cause = getattr(exc_info.value, "__cause__", None)
    if cause is not None:
        detail += f" {cause}"
    assert MOEX_HISTORICAL_DATA_ROOT_ENV in detail


def test_dagster_route_canonical_refresh_is_blocked_when_raw_status_failed(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-raw-failed")
    payload = json.loads(raw_report_path.read_text(encoding="utf-8"))
    payload["status"] = "FAILED"
    raw_report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = materialize_moex_historical_assets(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_report_path,
        canonical_output_dir=tmp_path / "canonicalization",
        canonical_run_id="phase03-raw-failed",
        raise_on_error=False,
    )
    assert report["success"] is False


def test_dagster_route_cutover_blocks_when_second_nightly_misses_morning_target(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-nightly-target-blocked")
    report = run_historical_dagster_cutover(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_report_path,
        output_dir=tmp_path / "phase03-cutover",
        run_id="phase03-nightly-target-blocked",
        nightly_readiness_observed_at_utc=[
            "2026-04-04T02:40:00Z",
            "2026-04-05T03:10:00Z",
        ],
    )
    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert any("morning readiness target missed" in reason for reason in report["reasons"])


def test_dagster_route_cutover_rejects_non_increasing_nightly_sequence(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-nightly-order")

    with pytest.raises(ValueError, match="strictly increasing order"):
        run_historical_dagster_cutover(
            raw_table_path=raw_table_path,
            raw_ingest_report_path=raw_report_path,
            output_dir=tmp_path / "phase03-cutover",
            run_id="phase03-nightly-order",
            nightly_readiness_observed_at_utc=[
                "2026-04-05T02:40:00Z",
                "2026-04-04T02:45:00Z",
            ],
        )


def test_dagster_route_cutover_rejects_non_consecutive_local_nightly_cycles(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-nightly-gap")

    with pytest.raises(ValueError, match="two consecutive nightly cycles"):
        run_historical_dagster_cutover(
            raw_table_path=raw_table_path,
            raw_ingest_report_path=raw_report_path,
            output_dir=tmp_path / "phase03-cutover",
            run_id="phase03-nightly-gap",
            nightly_readiness_observed_at_utc=[
                "2026-04-04T02:40:00Z",
                "2026-04-06T02:45:00Z",
            ],
        )


def test_dagster_route_cutover_rejects_non_canonical_schedule_cron(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-schedule-drift")
    with pytest.raises(ValueError, match="Dagster-route proof requires baseline daily cron"):
        run_historical_dagster_cutover(
            raw_table_path=raw_table_path,
            raw_ingest_report_path=raw_report_path,
            output_dir=tmp_path / "phase03-cutover",
            run_id="phase03-schedule-drift",
            nightly_readiness_observed_at_utc=[
                "2026-04-04T02:40:00Z",
                "2026-04-05T02:45:00Z",
            ],
            schedule_cron="0 3 * * *",
        )


def test_dagster_route_cutover_rejects_retry_max_attempts_drift(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-retry-attempts-drift")
    with pytest.raises(ValueError, match="Dagster-route proof requires `retry_max_attempts=3`"):
        run_historical_dagster_cutover(
            raw_table_path=raw_table_path,
            raw_ingest_report_path=raw_report_path,
            output_dir=tmp_path / "phase03-cutover",
            run_id="phase03-retry-attempts-drift",
            nightly_readiness_observed_at_utc=[
                "2026-04-04T02:40:00Z",
                "2026-04-05T02:45:00Z",
            ],
            retry_max_attempts=2,
        )


def test_dagster_route_cutover_rejects_retry_backoff_drift(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-retry-backoff-drift")
    with pytest.raises(
        ValueError,
        match=r"Dagster-route proof requires `retry_backoff_seconds=\[60, 300, 900\]`",
    ):
        run_historical_dagster_cutover(
            raw_table_path=raw_table_path,
            raw_ingest_report_path=raw_report_path,
            output_dir=tmp_path / "phase03-cutover",
            run_id="phase03-retry-backoff-drift",
            nightly_readiness_observed_at_utc=[
                "2026-04-04T02:40:00Z",
                "2026-04-05T02:45:00Z",
            ],
            retry_backoff_seconds=[30, 60, 120],
        )


def test_dagster_route_cutover_passes_with_two_nightly_cycles_and_recovery(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-cutover-pass")
    report = run_historical_dagster_cutover(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_report_path,
        output_dir=tmp_path / "phase03-cutover",
        run_id="phase03-cutover-pass",
        nightly_readiness_observed_at_utc=[
            "2026-04-04T02:40:00Z",
            "2026-04-05T02:45:00Z",
        ],
    )
    assert report["status"] == "PASS"
    assert report["publish_decision"] == "publish"
    assert report["proof_class"] == "integration"
    assert report["single_writer_probe"]["status"] == "PASS"
    assert report["recovery_drill"]["status"] == "PASS"
    assert Path(str(report["artifact_paths"]["dagster_runtime_binding"])).exists()
    assert len(report["cycles"]) == 4
    assert {item["mode"] for item in report["cycles"]} == {"nightly", "repair", "backfill"}
    assert all(item["route_id"] == report["route_id"] for item in report["cycles"])
    for path_text in report["artifact_paths"]["nightly_cycle_reports"]:
        assert Path(path_text).exists()


def test_dagster_route_cutover_blocks_when_staging_real_is_required_without_external_binding(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-staging-required")
    report = run_historical_dagster_cutover(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_report_path,
        output_dir=tmp_path / "phase03-cutover",
        run_id="phase03-staging-required",
        nightly_readiness_observed_at_utc=[
            "2026-04-04T02:40:00Z",
            "2026-04-05T02:45:00Z",
        ],
        require_staging_real=True,
    )
    assert report["status"] == "BLOCKED"
    assert report["proof_class"] == "integration"
    assert any(
        "staging-real proof requires --staging-binding-report-path" in reason
        for reason in report["reasons"]
    )


def test_dagster_route_cutover_promotes_to_staging_real_when_external_binding_report_is_provided(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-staging-binding")
    staging_binding_report_path = _write_staging_binding_report(tmp_path)
    report = run_historical_dagster_cutover(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_report_path,
        output_dir=tmp_path / "phase03-cutover",
        run_id="phase03-staging-binding",
        nightly_readiness_observed_at_utc=[
            "2026-04-04T02:40:00Z",
            "2026-04-05T02:45:00Z",
        ],
        staging_binding_report_path=staging_binding_report_path,
        require_staging_real=True,
    )
    assert report["status"] == "PASS"
    assert report["proof_class"] == "staging-real"
    assert report["staging_binding"]["orchestrator"] == "dagster-daemon"
    assert "dagster://staging/moex-historical-cutover" in report["real_bindings"]


def test_dagster_route_cutover_rejects_localhost_staging_binding_report(tmp_path: Path) -> None:
    raw_table_path, raw_report_path = _write_raw_table_and_report(tmp_path, run_id="phase03-localhost-binding")
    staging_binding_report_path = _write_staging_binding_report(tmp_path)
    payload = json.loads(staging_binding_report_path.read_text(encoding="utf-8"))
    payload["dagster_url"] = "http://127.0.0.1:3011"
    staging_binding_report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="loopback or unspecified host|external staging Dagster host"):
        run_historical_dagster_cutover(
            raw_table_path=raw_table_path,
            raw_ingest_report_path=raw_report_path,
            output_dir=tmp_path / "phase03-cutover",
            run_id="phase03-localhost-binding",
            nightly_readiness_observed_at_utc=[
                "2026-04-04T02:40:00Z",
                "2026-04-05T02:45:00Z",
            ],
            staging_binding_report_path=staging_binding_report_path,
            require_staging_real=True,
        )
