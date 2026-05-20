from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    count_delta_table_rows,
    has_delta_log,
    iter_delta_table_row_batches,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.schemas import (
    historical_data_delta_schema_manifest,
)
from trading_advisor_3000.spark_jobs.moex_canonical_publish_job import (
    CANONICAL_PROVENANCE_COLUMNS,
    SIDECAR_OVERLAP_POLICY,
    run_moex_canonical_publish_spark_delta_job,
)

SESSION_INTERVAL_COLUMNS: dict[str, str] = {
    "instrument_id": "string",
    "session_date": "date",
    "interval_id": "string",
    "interval_seq": "int",
    "expected_open_ts": "timestamp",
    "expected_close_ts": "timestamp",
    "session_class": "string",
    "interval_type": "string",
    "policy_id": "string",
    "source_id": "string",
    "source_document_hash": "string",
}


def _read_rows(path: Path) -> list[dict[str, object]]:
    return [
        row
        for batch in iter_delta_table_row_batches(
            path,
            filters=[("instrument_id", "=", "FUT_BR")],
        )
        for row in batch
    ]


def _bar(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "contract_id": "BRM6@MOEX",
        "instrument_id": "FUT_BR",
        "timeframe": "5m",
        "ts": "2026-04-02T10:00:00Z",
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 10,
        "open_interest": 100,
    }
    payload.update(overrides)
    return payload


def _provenance(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "contract_id": "BRM6@MOEX",
        "instrument_id": "FUT_BR",
        "timeframe": "5m",
        "ts": "2026-04-02T10:00:00Z",
        "source_provider": "moex_iss",
        "source_timeframe": "1m",
        "source_interval": 1,
        "source_run_id": "raw-pass",
        "source_ingest_run_id": "raw-pass",
        "source_row_count": 5,
        "source_ts_open_first": "2026-04-02T10:00:00Z",
        "source_ts_close_last": "2026-04-02T10:05:00Z",
        "open_interest_imputed": 0,
        "build_run_id": "canonical-pass",
        "built_at_utc": "2026-04-02T10:06:00Z",
    }
    payload.update(overrides)
    if "bar_start_ts" not in overrides:
        payload["bar_start_ts"] = payload["source_ts_open_first"]
    if "bar_end_ts" not in overrides:
        payload["bar_end_ts"] = payload["source_ts_close_last"]
    if "session_interval_id" not in overrides:
        payload["session_interval_id"] = f"FUT_BR-{str(payload['bar_start_ts'])[:10]}-regular-1"
    return payload


def _write_session_intervals(path: Path, session_dates: list[str]) -> Path:
    write_delta_table_rows(
        table_path=path,
        rows=[
            {
                "instrument_id": "FUT_BR",
                "session_date": session_date,
                "interval_id": f"FUT_BR-{session_date}-regular-1",
                "interval_seq": 1,
                "expected_open_ts": f"{session_date}T10:00:00Z",
                "expected_close_ts": f"{session_date}T18:45:00Z",
                "session_class": "regular",
                "interval_type": "regular_trading",
                "policy_id": "moex-official-session-v1",
                "source_id": "moex-official-schedule-fixture",
                "source_document_hash": "sha256:fixture",
            }
            for session_date in session_dates
        ],
        columns=SESSION_INTERVAL_COLUMNS,
    )
    return path


def test_spark_publish_mutates_delta_tables_and_refreshes_sidecars_with_overlap(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; "
            "Docker/Linux proof profile runs this path"
        )

    manifest = historical_data_delta_schema_manifest()
    canonical_columns = manifest["canonical_bars"]["columns"]
    session_columns = manifest["canonical_session_calendar"]["columns"]
    roll_columns = manifest["canonical_roll_map"]["columns"]

    staged_bars_path = tmp_path / "staged" / "canonical_bars.delta"
    staged_provenance_path = tmp_path / "staged" / "canonical_bar_provenance.delta"
    publish_scope_path = tmp_path / "staged" / "publish-scope.jsonl"
    target_bars_path = tmp_path / "target" / "canonical_bars.delta"
    target_provenance_path = tmp_path / "target" / "canonical_bar_provenance.delta"
    session_calendar_path = tmp_path / "target" / "canonical_session_calendar.delta"
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        ["2026-04-01", "2026-04-02", "2026-04-05"],
    )
    roll_map_path = tmp_path / "target" / "canonical_roll_map.delta"

    write_delta_table_rows(
        table_path=staged_bars_path,
        rows=[_bar(high=112.0, close=111.0, volume=55, open_interest=150)],
        columns=canonical_columns,
    )
    write_delta_table_rows(
        table_path=staged_provenance_path,
        rows=[_provenance(source_row_count=5, source_ts_close_last="2026-04-02T10:10:00Z")],
        columns=CANONICAL_PROVENANCE_COLUMNS,
    )
    publish_scope_path.write_text(
        json.dumps(
            {
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "target_minutes": 5,
                "window_start_utc": "2026-04-02T10:00:00Z",
                "window_end_utc": "2026-04-02T10:10:00Z",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    with publish_scope_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "instrument_id": "FUT_BR",
                    "timeframe": "5m",
                    "target_minutes": 5,
                    "window_start_utc": "2026-04-05T10:00:00Z",
                    "window_end_utc": "2026-04-05T10:05:00Z",
                },
                sort_keys=True,
            )
            + "\n"
        )
    write_delta_table_rows(
        table_path=target_bars_path,
        rows=[
            _bar(open=90.0, high=91.0, low=89.0, close=90.5, volume=1, open_interest=50),
            _bar(ts="2026-04-02T10:05:00Z", close=88.0, volume=99),
            _bar(
                ts="2026-04-01T10:00:00Z",
                close=99.0,
                source_ts_open_first="unused",
            ),
            _bar(ts="2026-04-05T10:00:00Z", close=87.0, volume=199),
        ],
        columns=canonical_columns,
    )
    write_delta_table_rows(
        table_path=target_provenance_path,
        rows=[
            _provenance(source_run_id="old"),
            _provenance(
                ts="2026-04-02T10:05:00Z",
                source_run_id="stale",
                source_ts_open_first="2026-04-02T10:05:00Z",
                source_ts_close_last="2026-04-02T10:10:00Z",
            ),
            _provenance(
                ts="2026-04-01T10:00:00Z",
                source_ts_open_first="2026-04-01T10:00:00Z",
                source_ts_close_last="2026-04-01T10:05:00Z",
            ),
            _provenance(
                ts="2026-04-05T10:00:00Z",
                source_run_id="delete-only-stale",
                source_ts_open_first="2026-04-05T10:00:00Z",
                source_ts_close_last="2026-04-05T10:05:00Z",
            ),
        ],
        columns=CANONICAL_PROVENANCE_COLUMNS,
    )
    write_delta_table_rows(
        table_path=session_calendar_path,
        rows=[
            {
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "session_date": "2026-04-01",
                "session_open_ts": "2026-04-01T00:00:00Z",
                "session_close_ts": "2026-04-01T00:00:00Z",
                "session_class": "regular",
            },
            {
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "session_date": "2026-04-02",
                "session_open_ts": "2026-04-02T00:00:00Z",
                "session_close_ts": "2026-04-02T00:00:00Z",
                "session_class": "regular",
            },
            {
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "session_date": "2026-04-05",
                "session_open_ts": "2026-04-05T00:00:00Z",
                "session_close_ts": "2026-04-05T00:00:00Z",
                "session_class": "regular",
            },
        ],
        columns=session_columns,
    )
    write_delta_table_rows(
        table_path=roll_map_path,
        rows=[
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-01",
                "active_contract_id": "STALE",
                "reason": "stale",
            },
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-02",
                "active_contract_id": "STALE",
                "reason": "stale",
            },
            {
                "instrument_id": "FUT_BR",
                "session_date": "2026-04-05",
                "active_contract_id": "STALE",
                "reason": "delete-only-stale",
            },
        ],
        columns=roll_columns,
    )

    report = run_moex_canonical_publish_spark_delta_job(
        staged_bars_path=staged_bars_path,
        staged_provenance_path=staged_provenance_path,
        publish_scope_path=publish_scope_path,
        target_bars_path=target_bars_path,
        target_provenance_path=target_provenance_path,
        session_calendar_path=session_calendar_path,
        session_intervals_path=session_intervals_path,
        roll_map_path=roll_map_path,
        output_dir=tmp_path / "publish-proof",
        run_id="spark-publish-proof",
    )

    assert report["status"] == "PASS", report
    assert report["runtime_owner"] == "spark_delta"
    assert report["mutation_applied"] is True
    assert report["publish_protocol"]["scoped_replacement"]["stale_bar_rows"] == 2
    assert report["publish_protocol"]["scoped_replacement"]["stale_provenance_rows"] == 2
    assert report["canonical_rows"] == 2
    assert report["provenance_rows"] == 2, report
    assert report["qc_report"]["status"] == "PASS"
    assert report["contract_compatibility_report"]["status"] == "PASS"
    assert report["contract_compatibility_report"]["checked_rows"] == 2
    assert report["publish_protocol"]["operation"] == "delta_merge_replace"
    assert report["publish_protocol"]["recoverable"] is True
    assert Path(str(report["publish_protocol"]["recovery_manifest_path"])).exists()
    assert report["delta_log"]["canonical_bars"]["delta_log"] is True
    assert has_delta_log(target_bars_path)
    assert count_delta_table_rows(target_bars_path) == 2
    assert count_delta_table_rows(target_provenance_path) == 2

    final_bars = _read_rows(target_bars_path)
    assert not any(str(row["ts"]).startswith("2026-04-02T10:05") for row in final_bars)
    affected_bar = next(row for row in final_bars if row["ts"] == "2026-04-02T10:00:00Z")
    assert affected_bar["close"] == 111.0
    assert affected_bar["volume"] == 55

    sidecar_report = report["sidecar_refresh"]
    assert sidecar_report["mode"] == "scoped"
    assert sidecar_report["overlap_policy"] == SIDECAR_OVERLAP_POLICY
    assert sidecar_report["affected_session_rows"] == 2
    assert sidecar_report["overlap_session_rows"] == 6
    assert count_delta_table_rows(session_calendar_path) == 2
    assert count_delta_table_rows(roll_map_path) == 2
    assert not any(
        str(row["session_date"]).startswith("2026-04-05")
        for row in _read_rows(session_calendar_path)
    )
    assert not any(
        str(row["session_date"]).startswith("2026-04-05") for row in _read_rows(roll_map_path)
    )
    assert {row["active_contract_id"] for row in _read_rows(roll_map_path)} == {"BRM6@MOEX"}


def test_spark_publish_blocks_non_monotonic_provenance_without_mutating_target(
    tmp_path: Path,
) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; "
            "Docker/Linux proof profile runs this path"
        )

    manifest = historical_data_delta_schema_manifest()
    canonical_columns = manifest["canonical_bars"]["columns"]

    staged_bars_path = tmp_path / "staged" / "canonical_bars.delta"
    staged_provenance_path = tmp_path / "staged" / "canonical_bar_provenance.delta"
    target_bars_path = tmp_path / "target" / "canonical_bars.delta"
    target_provenance_path = tmp_path / "target" / "canonical_bar_provenance.delta"
    session_calendar_path = tmp_path / "target" / "canonical_session_calendar.delta"
    session_intervals_path = _write_session_intervals(
        tmp_path / "official" / "canonical_session_intervals.delta",
        ["2026-04-02"],
    )
    roll_map_path = tmp_path / "target" / "canonical_roll_map.delta"

    original_bar = _bar(open=90.0, high=91.0, low=89.0, close=90.5, volume=1)
    write_delta_table_rows(
        table_path=staged_bars_path,
        rows=[_bar(high=112.0, close=111.0, volume=55, open_interest=150)],
        columns=canonical_columns,
    )
    write_delta_table_rows(
        table_path=staged_provenance_path,
        rows=[
            _provenance(
                source_ts_open_first="2026-04-02T10:05:00Z",
                source_ts_close_last="2026-04-02T10:00:00Z",
            )
        ],
        columns=CANONICAL_PROVENANCE_COLUMNS,
    )
    write_delta_table_rows(
        table_path=target_bars_path,
        rows=[original_bar],
        columns=canonical_columns,
    )
    write_delta_table_rows(
        table_path=target_provenance_path,
        rows=[_provenance(source_run_id="old")],
        columns=CANONICAL_PROVENANCE_COLUMNS,
    )

    report = run_moex_canonical_publish_spark_delta_job(
        staged_bars_path=staged_bars_path,
        staged_provenance_path=staged_provenance_path,
        target_bars_path=target_bars_path,
        target_provenance_path=target_provenance_path,
        session_calendar_path=session_calendar_path,
        session_intervals_path=session_intervals_path,
        roll_map_path=roll_map_path,
        output_dir=tmp_path / "publish-proof",
        run_id="spark-publish-monotonicity-blocked",
    )

    assert report["status"] == "BLOCKED", report
    assert report["mutation_applied"] is False
    assert "source_window_monotonicity" in report["qc_report"]["failed_gates"]
    assert count_delta_table_rows(target_bars_path) == 1
    assert _read_rows(target_bars_path)[0]["close"] == original_bar["close"]


def test_spark_publish_blocks_zero_checked_contract_rows(tmp_path: Path) -> None:
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Windows Spark execution requires HADOOP_HOME; "
            "Docker/Linux proof profile runs this path"
        )

    report = run_moex_canonical_publish_spark_delta_job(
        staged_bars_path=tmp_path / "missing" / "canonical_bars.delta",
        staged_provenance_path=tmp_path / "missing" / "canonical_bar_provenance.delta",
        target_bars_path=tmp_path / "target" / "canonical_bars.delta",
        target_provenance_path=tmp_path / "target" / "canonical_bar_provenance.delta",
        session_calendar_path=tmp_path / "target" / "canonical_session_calendar.delta",
        session_intervals_path=_write_session_intervals(
            tmp_path / "official" / "canonical_session_intervals.delta",
            ["2026-04-02"],
        ),
        roll_map_path=tmp_path / "target" / "canonical_roll_map.delta",
        output_dir=tmp_path / "publish-proof",
        run_id="spark-publish-zero-contract-rows",
    )

    contract_report = report["contract_compatibility_report"]
    assert report["status"] == "BLOCKED", report
    assert report["mutation_applied"] is False
    assert contract_report["status"] == "FAIL"
    assert contract_report["checked_rows"] == 0
    assert "No canonical bars checked: 0 rows" in contract_report["errors"]
    assert not has_delta_log(tmp_path / "target" / "canonical_bars.delta")
