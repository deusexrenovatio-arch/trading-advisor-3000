from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.contracts import Mode, OrderIntent
from trading_advisor_3000.product_plane.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionDisabledError,
    LiveExecutionFeatureFlags,
    StockSharpSidecarStub,
    UnsupportedBrokerAdapterError,
)


def _intent(*, intent_id: str, mode: Mode) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id="SIG-20260317-1001",
        mode=mode,
        broker_adapter="stocksharp-sidecar-stub",
        action="buy",
        contract_id="BR-6.26",
        qty=1,
        price=82.5,
        stop_price=81.8,
        created_at="2026-03-17T10:00:00Z",
    )


def test_live_bridge_blocks_live_intents_when_feature_flags_are_disabled() -> None:
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=LiveExecutionFeatureFlags(),
    )
    with pytest.raises(LiveExecutionDisabledError):
        bridge.submit_order_intent(
            _intent(intent_id="INT-LIVE-1", mode=Mode.LIVE),
            accepted_at="2026-03-17T10:00:01Z",
        )

    health = bridge.health()
    assert health["status"] == "degraded"
    assert "live_execution_disabled" in health["preflight_errors"]
    assert health["live_path"] == "disabled"


def test_live_bridge_accepts_live_intents_when_full_path_is_enabled() -> None:
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=LiveExecutionFeatureFlags(
            enable_live_execution=True,
            enable_stocksharp_bridge=True,
            enable_quik_connector=True,
            enable_finam_transport=True,
            environment="staging-live-sim",
        ),
    )
    ack = bridge.submit_order_intent(
        _intent(intent_id="INT-LIVE-2", mode=Mode.LIVE),
        accepted_at="2026-03-17T10:01:00Z",
    )

    assert ack["accepted"] is True
    assert ack["state"] == "submitted"
    assert ack["accepted_at"] == "2026-03-17T10:01:00Z"
    assert bridge.health()["live_path"] == "stocksharp->quik->finam"


def test_live_bridge_allows_paper_mode_even_if_live_is_disabled() -> None:
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=LiveExecutionFeatureFlags(),
    )
    ack = bridge.submit_order_intent(
        _intent(intent_id="INT-PAPER-1", mode=Mode.PAPER),
        accepted_at="2026-03-17T10:02:00Z",
    )
    assert ack["accepted"] is True
    assert ack["intent_id"] == "INT-PAPER-1"


def test_live_bridge_drain_streams_returns_only_incremental_rows() -> None:
    sidecar = StockSharpSidecarStub()
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=LiveExecutionFeatureFlags(
            enable_live_execution=True,
            enable_stocksharp_bridge=True,
            enable_quik_connector=True,
            enable_finam_transport=True,
        ),
    )
    intent = _intent(intent_id="INT-LIVE-3", mode=Mode.LIVE)
    ack = bridge.submit_order_intent(intent, accepted_at="2026-03-17T10:03:00Z")
    sidecar.push_broker_update(
        external_order_id=str(ack["external_order_id"]),
        state="submitted",
        event_ts="2026-03-17T10:03:01Z",
    )
    sidecar.push_broker_fill(
        external_order_id=str(ack["external_order_id"]),
        fill_id="FILL-LIVE-3",
        qty=1,
        price=82.55,
        fill_ts="2026-03-17T10:03:02Z",
    )

    first = bridge.drain_broker_streams()
    second = bridge.drain_broker_streams()

    assert len(first["updates"]) == 1
    assert len(first["fills"]) == 1
    assert second["updates"] == []
    assert second["fills"] == []


def test_live_bridge_rejects_unsupported_broker_adapter() -> None:
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=LiveExecutionFeatureFlags(
            enable_live_execution=True,
            enable_stocksharp_bridge=True,
            enable_quik_connector=True,
            enable_finam_transport=True,
        ),
    )
    with pytest.raises(UnsupportedBrokerAdapterError):
        bridge.submit_order_intent(
            OrderIntent(
                intent_id="INT-LIVE-BAD-ADAPTER",
                signal_id="SIG-20260317-BAD",
                mode=Mode.LIVE,
                broker_adapter="unsupported-adapter",
                action="buy",
                contract_id="BR-6.26",
                qty=1,
                price=82.5,
                stop_price=81.8,
                created_at="2026-03-17T10:05:00Z",
            ),
            accepted_at="2026-03-17T10:05:01Z",
        )
