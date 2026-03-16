from __future__ import annotations

from trading_advisor_3000.app.contracts import Mode, PositionSnapshot
from trading_advisor_3000.app.execution.reconciliation import reconcile_position_snapshots


def _position(*, account_id: str, contract_id: str, qty: int, avg_price: float) -> PositionSnapshot:
    return PositionSnapshot(
        account_id=account_id,
        contract_id=contract_id,
        mode=Mode.PAPER,
        quantity=qty,
        avg_price=avg_price,
        broker_ts="2026-03-16T10:20:00Z",
    )


def test_reconciliation_detects_drift_and_missing_positions() -> None:
    expected = [
        _position(account_id="PAPER-01", contract_id="BR-6.26", qty=1, avg_price=82.5),
        _position(account_id="PAPER-01", contract_id="Si-6.26", qty=2, avg_price=92010.0),
    ]
    observed = [
        _position(account_id="PAPER-01", contract_id="BR-6.26", qty=2, avg_price=82.5),
    ]
    report = reconcile_position_snapshots(expected, observed)

    assert report.matched == 0
    assert len(report.missing) == 1
    assert len(report.mismatched) == 1
    assert report.is_clean is False
