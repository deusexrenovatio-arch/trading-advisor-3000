from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS
from trading_advisor_3000.product_plane.data_plane.moex.historical_canonical_route import (
    CanonicalProvenance,
    ChangedWindowScope,
    _build_scoped_raw_read_filters,
    run_historical_canonical_route,
    run_contract_compatibility_check,
    run_qc_gates,
    run_runtime_decoupling_check,
)


def _provenance(**overrides: object) -> CanonicalProvenance:
    payload: dict[str, object] = {
        "contract_id": "BRM6@MOEX",
        "instrument_id": "FUT_BR",
        "timeframe": "15m",
        "ts": "2026-04-02T10:00:00Z",
        "source_provider": "moex_iss",
        "source_timeframe": "1m",
        "source_interval": 1,
        "source_run_id": "phase01-pass1",
        "source_ingest_run_id": "phase01-pass1",
        "source_row_count": 15,
        "source_ts_open_first": "2026-04-02T10:00:00Z",
        "source_ts_close_last": "2026-04-02T10:15:00Z",
        "open_interest_imputed": 1,
        "build_run_id": "phase02-qc",
        "built_at_utc": "2026-04-02T11:00:00Z",
    }
    payload.update(overrides)
    return CanonicalProvenance(**payload)


def test_canonical_route_qc_fails_when_provenance_is_incomplete() -> None:
    bar = CanonicalBar(
        contract_id="BRM6@MOEX",
        instrument_id="FUT_BR",
        timeframe=Timeframe.M15,
        ts="2026-04-02T10:00:00Z",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=100,
        open_interest=0,
    )
    qc_report = run_qc_gates(
        bars=[bar],
        provenance_rows=[_provenance(source_provider="")],
        run_id="phase02-qc",
    )
    assert qc_report["status"] == "FAIL"
    assert qc_report["publish_decision"] == "blocked"
    assert "provenance_completeness" in qc_report["failed_gates"]


def test_canonical_route_qc_fails_when_duplicate_bar_key_is_present() -> None:
    bar = CanonicalBar(
        contract_id="BRM6@MOEX",
        instrument_id="FUT_BR",
        timeframe=Timeframe.M15,
        ts="2026-04-02T10:00:00Z",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=100,
        open_interest=0,
    )
    qc_report = run_qc_gates(
        bars=[bar, bar],
        provenance_rows=[_provenance(), _provenance()],
        run_id="phase02-qc-duplicate",
    )
    assert qc_report["status"] == "FAIL"
    assert "unique_bar_key" in qc_report["failed_gates"]


def test_canonical_route_qc_monotonicity_is_independent_of_physical_row_order() -> None:
    later = CanonicalBar(
        contract_id="BRM6@MOEX",
        instrument_id="FUT_BR",
        timeframe=Timeframe.M5,
        ts="2026-04-02T10:05:00Z",
        open=101.0,
        high=102.0,
        low=100.0,
        close=101.5,
        volume=100,
        open_interest=0,
    )
    earlier = CanonicalBar(
        contract_id="BRM6@MOEX",
        instrument_id="FUT_BR",
        timeframe=Timeframe.M5,
        ts="2026-04-02T10:00:00Z",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=90,
        open_interest=0,
    )

    qc_report = run_qc_gates(
        bars=[later, earlier],
        provenance_rows=[
            _provenance(timeframe="5m", ts="2026-04-02T10:05:00Z"),
            _provenance(timeframe="5m", ts="2026-04-02T10:00:00Z"),
        ],
        run_id="phase02-qc-row-order",
    )

    assert qc_report["status"] == "PASS"


def test_canonical_route_contract_compatibility_detects_schema_drift(tmp_path: Path) -> None:
    schema_path = (
        tmp_path
        / "src"
        / "trading_advisor_3000"
        / "product_plane"
        / "contracts"
        / "schemas"
        / "canonical_bar.v1.json"
    )
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "contracts/canonical_bar.v1.json",
                "type": "object",
                "required": [
                    "contract_id",
                    "instrument_id",
                    "timeframe",
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "open_interest",
                    "provider",
                ],
                "properties": {
                    "contract_id": {"type": "string"},
                    "instrument_id": {"type": "string"},
                    "timeframe": {"type": "string", "enum": ["5m", "15m", "1h"]},
                    "ts": {"type": "string"},
                    "open": {"type": "number"},
                    "high": {"type": "number"},
                    "low": {"type": "number"},
                    "close": {"type": "number"},
                    "volume": {"type": "integer"},
                    "open_interest": {"type": "integer"},
                    "provider": {"type": "string"},
                },
                "additionalProperties": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    report = run_contract_compatibility_check(
        bars=[
            CanonicalBar(
                contract_id="BRM6@MOEX",
                instrument_id="FUT_BR",
                timeframe=Timeframe.M5,
                ts="2026-04-02T10:00:00Z",
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=10,
                open_interest=0,
            )
        ],
        repo_root=tmp_path,
    )
    assert report["status"] == "FAIL"
    assert any("required fields mismatch" in item for item in report["errors"])


def test_canonical_route_runtime_decoupling_check_fails_when_runtime_imports_spark(tmp_path: Path) -> None:
    runtime_file = tmp_path / "src" / "trading_advisor_3000" / "app" / "runtime" / "spark_bridge.py"
    runtime_file.parent.mkdir(parents=True, exist_ok=True)
    runtime_file.write_text(
        "from pyspark.sql import SparkSession\n",
        encoding="utf-8",
    )
    report = run_runtime_decoupling_check(repo_root=tmp_path)
    assert report["status"] == "FAIL"
    assert report["violations"]


def test_canonical_route_runtime_decoupling_prefers_product_plane_runtime(tmp_path: Path) -> None:
    runtime_file = (
        tmp_path
        / "src"
        / "trading_advisor_3000"
        / "product_plane"
        / "runtime"
        / "spark_bridge.py"
    )
    runtime_file.parent.mkdir(parents=True, exist_ok=True)
    runtime_file.write_text(
        "from pyspark.sql import SparkSession\n",
        encoding="utf-8",
    )
    report = run_runtime_decoupling_check(repo_root=tmp_path)
    assert report["status"] == "FAIL"
    assert report["runtime_root"].endswith("/src/trading_advisor_3000/product_plane/runtime")
    assert report["violations"]


def test_canonical_route_rejects_changed_window_wider_than_baseline_update_guard(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)

    with pytest.raises(ValueError, match="wider than allowed for baseline update"):
        run_historical_canonical_route(
            raw_table_path=raw_table_path,
            output_dir=tmp_path / "canonical",
            run_id="wide-window",
            raw_ingest_run_report={
                "run_id": "wide-window",
                "status": "PASS",
                "source_rows": 1,
                "changed_windows": [
                    {
                        "internal_id": "FUT_BR",
                        "source_timeframe": "1m",
                        "source_interval": 1,
                        "moex_secid": "BRM6@MOEX",
                        "window_start_utc": "2026-04-01T00:00:00Z",
                        "window_end_utc": "2026-04-15T00:00:00Z",
                        "incremental_rows": 1,
                    }
                ],
            },
            max_changed_window_days=10,
        )


def test_canonical_route_scoped_raw_filters_match_string_timestamp_schema(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(
        table_path=raw_table_path,
        rows=[
            {
                "internal_id": "FUT_BR",
                "finam_symbol": "BRM6",
                "moex_engine": "futures",
                "moex_market": "forts",
                "moex_board": "RFUD",
                "moex_secid": "BRM6",
                "asset_group": "commodity",
                "timeframe": "1m",
                "source_interval": 1,
                "ts_open": "2026-05-04T07:00:00Z",
                "ts_close": "2026-05-04T07:00:59Z",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10,
                "open_interest": None,
                "ingest_run_id": "raw-pass",
                "ingested_at_utc": "2026-05-04T07:01:00Z",
                "provenance_json": {"source_provider": "moex_iss", "run_id": "raw-pass"},
            }
        ],
        columns=RAW_COLUMNS,
    )

    filters = _build_scoped_raw_read_filters(
        [
            ChangedWindowScope(
                internal_id="FUT_BR",
                source_timeframe="1m",
                source_interval=1,
                moex_secid="BRM6",
                window_start_utc="2026-05-04T07:00:00Z",
                window_end_utc="2026-05-04T07:01:00Z",
                incremental_rows=1,
            )
        ]
    )

    rows = read_delta_table_rows(raw_table_path, columns=["internal_id", "ts_close"], filters=filters)

    assert rows == [{"internal_id": "FUT_BR", "ts_close": "2026-05-04T07:00:59Z"}]
    assert filters[0][2][2] == "2026-05-04T07:00:00Z"
    assert filters[0][3][2] == "2026-05-04T07:01:00Z"
