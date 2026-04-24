from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.contracts import Mode, OrderIntent
from trading_advisor_3000.product_plane.execution.adapters import (
    AdapterTransportNotConfiguredError,
    ExecutionAdapterCatalog,
    ExecutionAdapterSpec,
    LiveExecutionBridge,
    LiveExecutionFeatureFlags,
    StockSharpSidecarStub,
    UnsupportedBrokerAdapterError,
    default_execution_adapter_catalog,
)


def _intent(*, intent_id: str, broker_adapter: str, mode: Mode) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id=f"SIG-{intent_id}",
        mode=mode,
        broker_adapter=broker_adapter,
        action="buy",
        contract_id="BR-6.26",
        qty=1,
        price=82.5,
        stop_price=81.8,
        created_at="2026-03-18T19:00:00Z",
    )


def _full_live_flags() -> LiveExecutionFeatureFlags:
    return LiveExecutionFeatureFlags(
        enable_live_execution=True,
        enable_stocksharp_bridge=True,
        enable_quik_connector=True,
        enable_finam_transport=True,
        enforce_live_secrets=True,
        environment="staging-live-sim",
    )


def _secrets_env() -> dict[str, str]:
    return {
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-token-001",
    }


class _StaticTransport:
    def __init__(self, adapter_id: str) -> None:
        self.adapter_id = adapter_id
        self._acked: dict[str, dict[str, object]] = {}
        self._broker_updates: list[dict[str, object]] = []
        self._broker_fills: list[dict[str, object]] = []

    def health(self) -> dict[str, object]:
        return {"adapter": self.adapter_id, "status": "ok", "acked": len(self._acked)}

    def submit_order_intent(self, intent: OrderIntent) -> dict[str, object]:
        ack = {
            "intent_id": intent.intent_id,
            "external_order_id": f"{self.adapter_id}-{intent.intent_id}",
            "accepted": True,
            "broker_adapter": self.adapter_id,
            "state": "submitted",
        }
        self._acked[intent.intent_id] = ack
        self._broker_updates.append(
            {
                "external_order_id": str(ack["external_order_id"]),
                "state": "submitted",
                "event_ts": intent.created_at,
                "payload": {"intent_id": intent.intent_id},
            }
        )
        return ack

    def cancel_order_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        if intent_id not in self._acked:
            raise ValueError(f"unknown intent_id: {intent_id}")
        ack = self._acked[intent_id]
        self._broker_updates.append(
            {
                "external_order_id": str(ack["external_order_id"]),
                "state": "canceled",
                "event_ts": canceled_at,
                "payload": {"intent_id": intent_id},
            }
        )
        return {"intent_id": intent_id, "state": "canceled", "canceled_at": canceled_at}

    def replace_order_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        if intent_id not in self._acked:
            raise ValueError(f"unknown intent_id: {intent_id}")
        if new_qty <= 0 or new_price <= 0:
            raise ValueError("new_qty/new_price must be positive")
        ack = self._acked[intent_id]
        self._broker_updates.append(
            {
                "external_order_id": str(ack["external_order_id"]),
                "state": "replaced",
                "event_ts": replaced_at,
                "payload": {"intent_id": intent_id, "new_qty": new_qty, "new_price": new_price},
            }
        )
        return {
            "intent_id": intent_id,
            "state": "replaced",
            "new_qty": new_qty,
            "new_price": new_price,
            "replaced_at": replaced_at,
        }

    def list_broker_updates(self) -> list[dict[str, object]]:
        return list(self._broker_updates)

    def list_broker_fills(self) -> list[dict[str, object]]:
        return list(self._broker_fills)


def test_execution_adapter_catalog_default_execution_adapter_catalog_has_scale_up_seams() -> None:
    catalog = default_execution_adapter_catalog()
    adapter_ids = [item.adapter_id for item in catalog.list_specs()]

    assert "stocksharp-sidecar-stub" in adapter_ids
    assert "stocksharp-sidecar" in adapter_ids
    assert catalog.get("stocksharp-sidecar-stub") is not None


def test_execution_adapter_catalog_live_bridge_accepts_custom_registered_adapter() -> None:
    catalog = default_execution_adapter_catalog()
    catalog.register(
        ExecutionAdapterSpec(
            adapter_id="finam-direct-sim",
            route="finam-direct",
            supports_live=True,
            supports_paper=False,
            description="scale-up extension seam adapter",
            capabilities=("submit", "cancel"),
        )
    )
    transport = _StaticTransport(adapter_id="finam-direct-sim")
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=_full_live_flags(),
        adapter_catalog=catalog,
        adapter_transports={"finam-direct-sim": transport},
        env=_secrets_env(),
    )

    ack = bridge.submit_order_intent(
        _intent(
            intent_id="INT-SCALEUP-ADAPTER-1",
            broker_adapter="finam-direct-sim",
            mode=Mode.LIVE,
        ),
        accepted_at="2026-03-18T19:00:01Z",
    )

    assert ack["accepted"] is True
    assert ack["broker_adapter"] == "finam-direct-sim"
    assert ack["external_order_id"] == "finam-direct-sim-INT-SCALEUP-ADAPTER-1"


def test_execution_adapter_catalog_live_bridge_rejects_adapter_without_transport_binding() -> None:
    catalog = default_execution_adapter_catalog()
    catalog.register(
        ExecutionAdapterSpec(
            adapter_id="finam-direct-unbound",
            route="finam-direct",
            supports_live=True,
            supports_paper=False,
        )
    )
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=_full_live_flags(),
        adapter_catalog=catalog,
        env=_secrets_env(),
    )

    with pytest.raises(AdapterTransportNotConfiguredError):
        bridge.submit_order_intent(
            _intent(
                intent_id="INT-SCALEUP-ADAPTER-UNBOUND",
                broker_adapter="finam-direct-unbound",
                mode=Mode.LIVE,
            ),
            accepted_at="2026-03-18T19:00:01Z",
        )


def test_execution_adapter_catalog_live_bridge_rejects_adapter_without_live_mode_support() -> None:
    catalog = ExecutionAdapterCatalog(
        specs=[
            ExecutionAdapterSpec(
                adapter_id="paper-only-adapter",
                route="paper-only",
                supports_live=False,
                supports_paper=True,
                description="paper mode only seam",
            )
        ]
    )
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=_full_live_flags(),
        adapter_catalog=catalog,
        env=_secrets_env(),
    )

    with pytest.raises(UnsupportedBrokerAdapterError):
        bridge.submit_order_intent(
            _intent(
                intent_id="INT-SCALEUP-ADAPTER-2",
                broker_adapter="paper-only-adapter",
                mode=Mode.LIVE,
            ),
            accepted_at="2026-03-18T19:00:02Z",
        )


def test_execution_adapter_catalog_live_bridge_routes_operations_and_streams_per_adapter_transport() -> None:
    catalog = default_execution_adapter_catalog()
    catalog.register(
        ExecutionAdapterSpec(
            adapter_id="finam-direct-sim",
            route="finam-direct",
            supports_live=True,
            supports_paper=False,
        )
    )
    sidecar = StockSharpSidecarStub()
    finam_transport = _StaticTransport(adapter_id="finam-direct-sim")
    bridge = LiveExecutionBridge(
        sidecar=sidecar,
        flags=_full_live_flags(),
        adapter_catalog=catalog,
        adapter_transports={"finam-direct-sim": finam_transport},
        env=_secrets_env(),
    )

    sidecar_ack = bridge.submit_order_intent(
        _intent(
            intent_id="INT-SCALEUP-SIDECAR-1",
            broker_adapter="stocksharp-sidecar-stub",
            mode=Mode.LIVE,
        ),
        accepted_at="2026-03-18T19:10:01Z",
    )
    finam_ack = bridge.submit_order_intent(
        _intent(
            intent_id="INT-SCALEUP-FINAM-1",
            broker_adapter="finam-direct-sim",
            mode=Mode.LIVE,
        ),
        accepted_at="2026-03-18T19:10:02Z",
    )
    replace_ack = bridge.replace_order_intent(
        intent_id="INT-SCALEUP-FINAM-1",
        new_qty=2,
        new_price=82.7,
        replaced_at="2026-03-18T19:10:03Z",
    )
    cancel_ack = bridge.cancel_order_intent(
        intent_id="INT-SCALEUP-SIDECAR-1",
        canceled_at="2026-03-18T19:10:04Z",
    )
    first_drain = bridge.drain_broker_streams()
    second_drain = bridge.drain_broker_streams()

    assert sidecar_ack["broker_adapter"] == "stocksharp-sidecar-stub"
    assert finam_ack["broker_adapter"] == "finam-direct-sim"
    assert sidecar_ack["transport_key"] != finam_ack["transport_key"]
    assert replace_ack["transport_key"] == finam_ack["transport_key"]
    assert cancel_ack["transport_key"] == sidecar_ack["transport_key"]
    assert len(first_drain["updates"]) >= 2
    assert {item["transport_key"] for item in first_drain["updates"]} == {
        sidecar_ack["transport_key"],
        finam_ack["transport_key"],
    }
    assert second_drain == {"updates": [], "fills": []}
