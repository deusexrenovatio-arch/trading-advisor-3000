from __future__ import annotations

from trading_advisor_3000.app.contracts import BrokerFill, BrokerOrder, Mode, OrderIntent, PositionSnapshot
from trading_advisor_3000.app.execution.reconciliation import reconcile_live_execution


def _intent(*, intent_id: str, qty: int = 1) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id=f"SIG-{intent_id}",
        mode=Mode.LIVE,
        broker_adapter="stocksharp-sidecar-stub",
        action="buy",
        contract_id="BR-6.26",
        qty=qty,
        price=82.5,
        stop_price=81.8,
        created_at="2026-03-17T12:00:00Z",
    )


def _position(*, qty: int, avg_price: float) -> PositionSnapshot:
    return PositionSnapshot(
        position_key="LIVE-01:BR-6.26:live",
        account_id="LIVE-01",
        contract_id="BR-6.26",
        mode=Mode.LIVE,
        qty=qty,
        avg_price=avg_price,
        as_of_ts="2026-03-17T12:00:10Z",
    )


def test_live_reconciliation_is_clean_when_orders_fills_and_positions_match() -> None:
    intent = _intent(intent_id="INT-REC-1", qty=1)
    report = reconcile_live_execution(
        expected_intents=[intent],
        observed_orders=[
            BrokerOrder(
                broker_order_id="BORD-1",
                intent_id=intent.intent_id,
                external_order_id="EXT-1",
                broker="stocksharp-sidecar-stub",
                state="filled",
                submitted_at="2026-03-17T12:00:00Z",
                updated_at="2026-03-17T12:00:05Z",
            )
        ],
        observed_fills=[
            BrokerFill(
                fill_id="FILL-1",
                broker_order_id="BORD-1",
                fill_ts="2026-03-17T12:00:05Z",
                qty=1,
                price=82.5,
                fee=0.0,
                external_trade_id="TRD-1",
            )
        ],
        expected_positions=[_position(qty=1, avg_price=82.5)],
        observed_positions=[_position(qty=1, avg_price=82.5)],
    )

    assert report.is_clean is True
    assert report.orders_matched == 1
    assert report.fills_matched == 1
    assert report.incidents == []


def test_live_reconciliation_surfaces_order_fill_and_position_incidents() -> None:
    expected = [_intent(intent_id="INT-REC-2", qty=2)]
    report = reconcile_live_execution(
        expected_intents=expected,
        observed_orders=[
            BrokerOrder(
                broker_order_id="BORD-2",
                intent_id="INT-REC-2",
                external_order_id="EXT-2",
                broker="stocksharp-sidecar-stub",
                state="filled",
                submitted_at="2026-03-17T12:10:00Z",
                updated_at="2026-03-17T12:10:05Z",
            )
        ],
        observed_fills=[
            BrokerFill(
                fill_id="FILL-ORPHAN",
                broker_order_id="BORD-ORPHAN",
                fill_ts="2026-03-17T12:10:06Z",
                qty=1,
                price=83.0,
                fee=0.0,
                external_trade_id="TRD-ORPHAN",
            )
        ],
        expected_positions=[_position(qty=2, avg_price=82.5)],
        observed_positions=[_position(qty=1, avg_price=82.5)],
    )

    reasons = {item.reason for item in report.incidents}
    assert "filled_state_without_enough_fills" in reasons
    assert "orphan_fill_without_order" in reasons
    assert "quantity_mismatch" in reasons
    assert report.is_clean is False
