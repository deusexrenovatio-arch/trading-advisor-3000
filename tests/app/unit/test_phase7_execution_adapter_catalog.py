from __future__ import annotations

import pytest

from trading_advisor_3000.app.contracts import Mode, OrderIntent
from trading_advisor_3000.app.execution.adapters import (
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


def test_phase7_default_execution_adapter_catalog_has_scale_up_seams() -> None:
    catalog = default_execution_adapter_catalog()
    adapter_ids = [item.adapter_id for item in catalog.list_specs()]

    assert "stocksharp-sidecar-stub" in adapter_ids
    assert "stocksharp-sidecar" in adapter_ids
    assert catalog.get("stocksharp-sidecar-stub") is not None


def test_phase7_live_bridge_accepts_custom_registered_adapter() -> None:
    catalog = default_execution_adapter_catalog()
    catalog.register(
        ExecutionAdapterSpec(
            adapter_id="finam-direct-sim",
            route="finam-direct",
            supports_live=True,
            supports_paper=False,
            description="phase7 extension seam adapter",
            capabilities=("submit", "cancel"),
        )
    )
    bridge = LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=_full_live_flags(),
        adapter_catalog=catalog,
        env=_secrets_env(),
    )

    ack = bridge.submit_order_intent(
        _intent(
            intent_id="INT-PHASE7-ADAPTER-1",
            broker_adapter="finam-direct-sim",
            mode=Mode.LIVE,
        ),
        accepted_at="2026-03-18T19:00:01Z",
    )

    assert ack["accepted"] is True
    assert ack["broker_adapter"] == "finam-direct-sim"


def test_phase7_live_bridge_rejects_adapter_without_live_mode_support() -> None:
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
                intent_id="INT-PHASE7-ADAPTER-2",
                broker_adapter="paper-only-adapter",
                mode=Mode.LIVE,
            ),
            accepted_at="2026-03-18T19:00:02Z",
        )
