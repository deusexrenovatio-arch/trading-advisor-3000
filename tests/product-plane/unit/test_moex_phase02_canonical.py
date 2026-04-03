from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.data_plane.moex.phase02_canonical import (
    build_phase02_canonical_outputs,
    run_contract_compatibility_check,
    run_qc_gates,
    run_runtime_decoupling_check,
)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _raw_row(
    *,
    ts_open: datetime,
    open_value: float,
    high_value: float,
    low_value: float,
    close_value: float,
    volume: int,
    open_interest: int | None = None,
    instrument_id: str = "FUT_BR",
    contract_id: str = "BRM6@MOEX",
    source_timeframe: str = "1m",
    source_interval: int = 1,
    candle_minutes: int = 1,
) -> dict[str, object]:
    ts_close = ts_open + timedelta(minutes=candle_minutes)
    return {
        "internal_id": instrument_id,
        "finam_symbol": contract_id,
        "timeframe": source_timeframe,
        "source_interval": source_interval,
        "ts_open": _iso(ts_open),
        "ts_close": _iso(ts_close),
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "volume": volume,
        "open_interest": open_interest,
        "ingest_run_id": "phase01-pass1",
        "ingested_at_utc": _iso(ts_open + timedelta(minutes=2)),
        "provenance_json": {
            "source_provider": "moex_iss",
            "source_interval": source_interval,
            "source_timeframe": source_timeframe,
            "run_id": "phase01-pass1",
            "discovery_url": "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/BRM6/candleborders.json",
        },
    }


def test_phase02_resampling_is_deterministic_and_uses_contract_safe_timeframes() -> None:
    start = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)
    rows = [
        _raw_row(
            ts_open=start + timedelta(minutes=index),
            open_value=100.0 + index,
            high_value=101.0 + index,
            low_value=99.0 + index,
            close_value=100.5 + index,
            volume=10 + index,
        )
        for index in range(20)
    ]

    bars_a, provenance_a = build_phase02_canonical_outputs(
        raw_rows=rows,
        build_run_id="phase02-a",
        built_at_utc="2026-04-02T10:30:00Z",
    )
    bars_b, provenance_b = build_phase02_canonical_outputs(
        raw_rows=list(reversed(rows)),
        build_run_id="phase02-b",
        built_at_utc="2026-04-02T10:31:00Z",
    )

    assert [item.to_dict() for item in bars_a] == [item.to_dict() for item in bars_b]
    assert len(provenance_a) == len(bars_a)
    assert len(provenance_b) == len(bars_b)

    timeframes = {item.timeframe.value for item in bars_a}
    assert timeframes == {"5m", "15m", "1h", "4h", "1d", "1w"}

    first_m5 = next(item for item in bars_a if item.timeframe == Timeframe.M5 and item.ts == "2026-04-02T10:00:00Z")
    assert first_m5.open == 100.0
    assert first_m5.close == 104.5
    assert first_m5.high == 105.0
    assert first_m5.low == 99.0
    assert first_m5.volume == sum(10 + i for i in range(5))
    assert first_m5.open_interest == 0


def test_phase02_qc_fails_when_provenance_is_incomplete() -> None:
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
    bars = [bar]
    _, provenance_rows = build_phase02_canonical_outputs(
        raw_rows=[
            _raw_row(
                ts_open=datetime(2026, 4, 2, 10, 0, tzinfo=UTC),
                open_value=100.0,
                high_value=101.0,
                low_value=99.0,
                close_value=100.5,
                volume=100,
            )
        ],
        build_run_id="phase02-qc",
        built_at_utc="2026-04-02T11:00:00Z",
    )
    broken = provenance_rows[0].to_dict()
    broken["source_provider"] = ""
    qc_report = run_qc_gates(
        bars=bars,
        provenance_rows=[type(provenance_rows[0])(**broken)],
        run_id="phase02-qc",
    )
    assert qc_report["status"] == "FAIL"
    assert qc_report["publish_decision"] == "blocked"
    assert "provenance_completeness" in qc_report["failed_gates"]


def test_phase02_mixed_coverage_skips_incompatible_frames_without_failing() -> None:
    start = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)
    minute_rows = [
        _raw_row(
            ts_open=start + timedelta(minutes=index),
            open_value=100.0 + index,
            high_value=101.0 + index,
            low_value=99.0 + index,
            close_value=100.5 + index,
            volume=10 + index,
            instrument_id="FUT_BR",
            contract_id="BRM6@MOEX",
            source_timeframe="1m",
            source_interval=1,
            candle_minutes=1,
        )
        for index in range(12)
    ]
    daily_rows = [
        _raw_row(
            ts_open=datetime(2026, 3, 28, 21, 0, tzinfo=UTC) + timedelta(days=index),
            open_value=200.0 + index,
            high_value=205.0 + index,
            low_value=195.0 + index,
            close_value=202.0 + index,
            volume=200 + index,
            instrument_id="FUT_WHEAT",
            contract_id="W4J6@MOEX",
            source_timeframe="1d",
            source_interval=24,
            candle_minutes=24 * 60,
        )
        for index in range(3)
    ]

    bars, provenance = build_phase02_canonical_outputs(
        raw_rows=[*minute_rows, *daily_rows],
        build_run_id="phase02-mixed",
        built_at_utc="2026-04-03T00:00:00Z",
    )

    assert bars
    assert provenance
    wheat_timeframes = {
        item.timeframe.value
        for item in bars
        if item.instrument_id == "FUT_WHEAT" and item.contract_id == "W4J6@MOEX"
    }
    assert wheat_timeframes == {"1d", "1w"}


def test_phase02_contract_compatibility_detects_schema_drift(tmp_path: Path) -> None:
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


def test_phase02_runtime_decoupling_check_fails_when_runtime_imports_spark(tmp_path: Path) -> None:
    runtime_file = tmp_path / "src" / "trading_advisor_3000" / "app" / "runtime" / "spark_bridge.py"
    runtime_file.parent.mkdir(parents=True, exist_ok=True)
    runtime_file.write_text(
        "from pyspark.sql import SparkSession\n",
        encoding="utf-8",
    )
    report = run_runtime_decoupling_check(repo_root=tmp_path)
    assert report["status"] == "FAIL"
    assert report["violations"]

