from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.data_plane.canonical import run_data_quality_checks


def _bar(*, contract_id: str, instrument_id: str, ts: str) -> CanonicalBar:
    return CanonicalBar(
        contract_id=contract_id,
        instrument_id=instrument_id,
        timeframe=Timeframe.M15,
        ts=ts,
        open=10.0,
        high=12.0,
        low=9.5,
        close=11.0,
        volume=100,
        open_interest=5000,
    )


def test_quality_detects_duplicates() -> None:
    rows = [
        _bar(contract_id="BR-6.26", instrument_id="BR", ts="2026-03-16T10:00:00Z"),
        _bar(contract_id="BR-6.26", instrument_id="BR", ts="2026-03-16T10:00:00Z"),
    ]
    errors = run_data_quality_checks(rows, whitelist_contracts={"BR-6.26"})
    assert any("duplicate key" in err for err in errors)


def test_quality_detects_whitelist_violations() -> None:
    rows = [_bar(contract_id="NQ-6.26", instrument_id="NQ", ts="2026-03-16T10:00:00Z")]
    errors = run_data_quality_checks(rows, whitelist_contracts={"BR-6.26"})
    assert any("outside whitelist" in err for err in errors)
