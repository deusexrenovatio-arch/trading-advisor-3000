from __future__ import annotations

from trading_advisor_3000.product_plane.data_plane.canonical import (
    build_canonical_bars,
    build_canonical_dataset,
)


def _session_interval(
    *,
    instrument_id: str,
    session_date: str = "2026-03-16",
    expected_open_ts: str = "2026-03-16T09:45:00Z",
    expected_close_ts: str = "2026-03-16T18:45:00Z",
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "session_date": session_date,
        "interval_id": f"{instrument_id}-{session_date}-regular-1",
        "interval_seq": 1,
        "expected_open_ts": expected_open_ts,
        "expected_close_ts": expected_close_ts,
        "session_class": "regular",
        "interval_type": "regular_trading",
        "policy_id": "moex-official-session-v1",
        "source_id": "moex-official-schedule-fixture",
        "source_document_hash": "sha256:fixture",
    }


def test_builder_prefers_latest_ts_close_for_duplicate_key() -> None:
    rows = [
        {
            "contract_id": "BR-6.26",
            "instrument_id": "BR",
            "timeframe": "15m",
            "ts_open": "2026-03-16T10:00:00Z",
            "ts_close": "2026-03-16T10:16:00Z",
            "open": 82.1,
            "high": 82.9,
            "low": 81.9,
            "close": 82.6,
            "volume": 1600,
            "open_interest": 21000,
        },
        {
            "contract_id": "BR-6.26",
            "instrument_id": "BR",
            "timeframe": "15m",
            "ts_open": "2026-03-16T10:00:00Z",
            "ts_close": "2026-03-16T10:15:00Z",
            "open": 82.1,
            "high": 82.8,
            "low": 81.9,
            "close": 82.4,
            "volume": 1500,
            "open_interest": 20000,
        },
    ]

    bars = build_canonical_bars(rows)
    assert len(bars) == 1
    assert bars[0].ts == "2026-03-16T10:00:00Z"
    assert bars[0].close == 82.6
    assert bars[0].open_interest == 21000


def test_builder_emits_full_canonical_surfaces() -> None:
    rows = [
        {
            "contract_id": "BR-6.26",
            "instrument_id": "BR",
            "timeframe": "15m",
            "ts_open": "2026-03-16T10:00:00Z",
            "ts_close": "2026-03-16T10:15:00Z",
            "open": 82.1,
            "high": 82.8,
            "low": 81.9,
            "close": 82.4,
            "volume": 1500,
            "open_interest": 20000,
        },
        {
            "contract_id": "Si-6.26",
            "instrument_id": "Si",
            "timeframe": "15m",
            "ts_open": "2026-03-16T10:00:00Z",
            "ts_close": "2026-03-16T10:15:00Z",
            "open": 104210.0,
            "high": 104350.0,
            "low": 104100.0,
            "close": 104330.0,
            "volume": 900,
            "open_interest": 17800,
        },
    ]
    dataset = build_canonical_dataset(
        rows,
        session_intervals=[
            _session_interval(instrument_id="BR"),
            _session_interval(instrument_id="Si"),
        ],
    )
    assert len(dataset.bars) == 2
    assert {row.instrument_id for row in dataset.instruments} == {"BR", "Si"}
    assert {row.contract_id for row in dataset.contracts} == {"BR-6.26", "Si-6.26"}
    assert len(dataset.session_calendar) == 2
    assert {row.session_open_ts for row in dataset.session_calendar} == {"2026-03-16T09:45:00Z"}
    assert len(dataset.roll_map) == 2


def test_builder_fails_closed_without_official_session_intervals() -> None:
    rows = [
        {
            "contract_id": "BR-6.26",
            "instrument_id": "BR",
            "timeframe": "15m",
            "ts_open": "2026-03-16T10:00:00Z",
            "ts_close": "2026-03-16T10:15:00Z",
            "open": 82.1,
            "high": 82.8,
            "low": 81.9,
            "close": 82.4,
            "volume": 1500,
            "open_interest": 20000,
        },
    ]

    try:
        build_canonical_dataset(rows)
    except ValueError as exc:
        assert "official session intervals are required" in str(exc)
    else:
        raise AssertionError("expected official session interval requirement")
