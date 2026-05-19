from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_moex_session_schedule_backfill.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("run_moex_session_schedule_backfill", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load script module: {SCRIPT_PATH.as_posix()}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_current_bars(path: Path) -> None:
    write_delta_table_rows(
        table_path=path,
        columns={
            "instrument_id": "string",
            "timeframe": "string",
            "ts": "string",
            "open": "double",
            "high": "double",
            "low": "double",
            "close": "double",
            "volume": "bigint",
        },
        rows=[
            {
                "instrument_id": "FUT_RTS",
                "timeframe": "1w",
                "ts": "2020-06-18T00:00:00Z",
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
            },
            {
                "instrument_id": "FUT_BR",
                "timeframe": "5m",
                "ts": "2026-05-17T15:55:00Z",
                "open": 2.0,
                "high": 2.0,
                "low": 2.0,
                "close": 2.0,
                "volume": 2,
            },
        ],
    )


def test_backfill_rejects_range_that_does_not_cover_current_data(tmp_path: Path) -> None:
    backfill = _load_script_module()
    current_path = tmp_path / "canonical_bars.delta"
    _write_current_bars(current_path)

    try:
        backfill._validate_backfill_covers_current_data(
            date_from=date(2022, 1, 1),
            date_till=date(2026, 5, 17),
            current_data_path=current_path,
            timestamp_column=None,
        )
    except ValueError as exc:
        message = str(exc)
        assert "does not cover current data range" in message
        assert "2020-06-18" in message
    else:
        raise AssertionError("undercovered current-data range must be rejected")


def test_backfill_accepts_range_that_covers_current_data(tmp_path: Path) -> None:
    backfill = _load_script_module()
    current_path = tmp_path / "canonical_bars.delta"
    _write_current_bars(current_path)

    coverage = backfill._validate_backfill_covers_current_data(
        date_from=date(2020, 6, 18),
        date_till=date(2026, 5, 17),
        current_data_path=current_path,
        timestamp_column=None,
    )

    assert coverage["current_data_date_from"] == "2020-06-18"
    assert coverage["current_data_date_till"] == "2026-05-17"
    assert coverage["timestamp_column"] == "ts"
    assert coverage["current_data_rows_scanned"] == 2
