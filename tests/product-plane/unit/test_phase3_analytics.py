from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import Mode, PositionSnapshot
from trading_advisor_3000.product_plane.contracts import (
    BrokerFill,
)
from trading_advisor_3000.product_plane.runtime.analytics import build_signal_outcomes, phase3_outcome_store_contract


def test_phase3_outcomes_contract_has_required_tables_and_columns() -> None:
    manifest = phase3_outcome_store_contract()
    assert {"research_forward_observations", "analytics_signal_outcomes"} <= set(manifest)
    forward_columns = set(manifest["research_forward_observations"]["columns"])
    analytics_columns = set(manifest["analytics_signal_outcomes"]["columns"])
    assert {"forward_obs_id", "candidate_id", "result_state", "pnl_r", "mfe_r", "mae_r"} <= forward_columns
    assert {"signal_id", "strategy_version_id", "contract_id", "mode", "pnl_r", "close_reason"} <= analytics_columns
    assert manifest["analytics_signal_outcomes"]["inputs"] == [
        "signal.signal_events",
        "execution.broker_fills",
        "execution.positions",
    ]


def test_build_signal_outcomes_uses_events_fills_positions() -> None:
    signal_events = [
        {
            "event_id": "SEVT-1",
            "signal_id": "SIG-20260317-0001",
            "event_ts": "2026-03-17T09:30:00Z",
            "event_type": "signal_opened",
            "reason_code": "candidate_created",
            "payload_json": {
                "strategy_version_id": "trend-follow-v1",
                "contract_id": "BR-6.26",
                "mode": "shadow",
                "side": "long",
                "entry_ref": 100.0,
                "stop_ref": 99.0,
            },
        },
        {
            "event_id": "SEVT-2",
            "signal_id": "SIG-20260317-0001",
            "event_ts": "2026-03-17T09:30:00Z",
            "event_type": "execution_fill",
            "reason_code": "open",
            "payload_json": {"fill_id": "FILL-OPEN", "role": "open"},
        },
        {
            "event_id": "SEVT-3",
            "signal_id": "SIG-20260317-0001",
            "event_ts": "2026-03-17T10:15:00Z",
            "event_type": "execution_fill",
            "reason_code": "close",
            "payload_json": {"fill_id": "FILL-CLOSE", "role": "close"},
        },
        {
            "event_id": "SEVT-4",
            "signal_id": "SIG-20260317-0001",
            "event_ts": "2026-03-17T10:15:00Z",
            "event_type": "signal_closed",
            "reason_code": "closed_profit",
            "payload_json": {"state": "closed"},
        },
    ]
    broker_fills = [
        BrokerFill(
            fill_id="FILL-OPEN",
            broker_order_id="BORD-1",
            fill_ts="2026-03-17T09:30:00Z",
            qty=1,
            price=100.0,
            fee=0.0,
            external_trade_id="TRD-OPEN",
        ),
        BrokerFill(
            fill_id="FILL-CLOSE",
            broker_order_id="BORD-2",
            fill_ts="2026-03-17T10:15:00Z",
            qty=1,
            price=102.0,
            fee=0.0,
            external_trade_id="TRD-CLOSE",
        ),
    ]
    positions = [
        PositionSnapshot(
            position_key="PAPER-REPLAY:BR-6.26:shadow",
            account_id="PAPER-REPLAY",
            contract_id="BR-6.26",
            mode=Mode.SHADOW,
            qty=0,
            avg_price=0.0,
            as_of_ts="2026-03-17T10:15:00Z",
        )
    ]
    outcomes = build_signal_outcomes(
        signal_events=signal_events,
        broker_fills=[item.to_dict() for item in broker_fills],
        positions=[item.to_dict() for item in positions],
    )

    assert len(outcomes) == 1
    payload = outcomes[0].to_dict()
    assert payload["signal_id"] == "SIG-20260317-0001"
    assert payload["close_reason"] == "closed_profit"
    assert payload["mode"] == "shadow"
    assert payload["pnl_r"] == 2.0
