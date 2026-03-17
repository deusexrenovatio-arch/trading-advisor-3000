from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.app.contracts import Mode, OrderIntent

from .stocksharp_sidecar_stub import StockSharpSidecarStub


class LiveExecutionBridgeError(RuntimeError):
    pass


class LiveExecutionDisabledError(LiveExecutionBridgeError):
    pass


class UnsupportedExecutionModeError(LiveExecutionBridgeError):
    pass


class UnsupportedBrokerAdapterError(LiveExecutionBridgeError):
    pass


@dataclass(frozen=True)
class LiveExecutionFeatureFlags:
    enable_live_execution: bool = False
    enable_stocksharp_bridge: bool = False
    enable_quik_connector: bool = False
    enable_finam_transport: bool = False
    environment: str = "dev"

    def preflight_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.enable_live_execution:
            errors.append("live_execution_disabled")
        if not self.enable_stocksharp_bridge:
            errors.append("stocksharp_bridge_disabled")
        if not self.enable_quik_connector:
            errors.append("quik_connector_disabled")
        if not self.enable_finam_transport:
            errors.append("finam_transport_disabled")
        return errors

    @property
    def live_path(self) -> str:
        if self.preflight_errors():
            return "disabled"
        return "stocksharp->quik->finam"

    def to_dict(self) -> dict[str, object]:
        return {
            "enable_live_execution": self.enable_live_execution,
            "enable_stocksharp_bridge": self.enable_stocksharp_bridge,
            "enable_quik_connector": self.enable_quik_connector,
            "enable_finam_transport": self.enable_finam_transport,
            "environment": self.environment,
            "live_path": self.live_path,
        }


class LiveExecutionBridge:
    def __init__(
        self,
        *,
        sidecar: StockSharpSidecarStub,
        flags: LiveExecutionFeatureFlags,
    ) -> None:
        self._sidecar = sidecar
        self._flags = flags
        self._seen_update_index = 0
        self._seen_fill_index = 0
        self._allowed_adapters = {"stocksharp-sidecar-stub", "stocksharp-sidecar"}

    def _ensure_supported_mode(self, intent: OrderIntent) -> None:
        if intent.mode not in {Mode.PAPER, Mode.LIVE}:
            raise UnsupportedExecutionModeError(
                f"execution bridge supports only paper/live intents, got mode={intent.mode.value}"
            )

    def _ensure_supported_adapter(self, intent: OrderIntent) -> None:
        if intent.broker_adapter not in self._allowed_adapters:
            raise UnsupportedBrokerAdapterError(
                f"unsupported broker_adapter for controlled live bridge: {intent.broker_adapter}"
            )

    def _ensure_live_allowed(self, intent: OrderIntent) -> None:
        if intent.mode != Mode.LIVE:
            return
        errors = self._flags.preflight_errors()
        if errors:
            raise LiveExecutionDisabledError(
                "live execution blocked by feature flags: " + ", ".join(errors)
            )

    def health(self) -> dict[str, object]:
        errors = self._flags.preflight_errors()
        return {
            "bridge": "controlled-live-execution-bridge",
            "status": "ok" if not errors else "degraded",
            "environment": self._flags.environment,
            "live_path": self._flags.live_path,
            "preflight_errors": errors,
            "sidecar": self._sidecar.health(),
        }

    def submit_order_intent(self, intent: OrderIntent, *, accepted_at: str) -> dict[str, object]:
        self._ensure_supported_mode(intent)
        self._ensure_supported_adapter(intent)
        self._ensure_live_allowed(intent)
        ack = self._sidecar.submit_order_intent(intent)
        return {**ack, "accepted_at": accepted_at}

    def cancel_order_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        return self._sidecar.cancel_order_intent(intent_id=intent_id, canceled_at=canceled_at)

    def replace_order_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        return self._sidecar.replace_order_intent(
            intent_id=intent_id,
            new_qty=new_qty,
            new_price=new_price,
            replaced_at=replaced_at,
        )

    def drain_broker_streams(self) -> dict[str, list[dict[str, object]]]:
        all_updates = self._sidecar.list_broker_updates()
        all_fills = self._sidecar.list_broker_fills()

        new_updates = all_updates[self._seen_update_index :]
        new_fills = all_fills[self._seen_fill_index :]

        self._seen_update_index = len(all_updates)
        self._seen_fill_index = len(all_fills)

        return {
            "updates": new_updates,
            "fills": new_fills,
        }
