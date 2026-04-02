from __future__ import annotations

from typing import Protocol

from trading_advisor_3000.product_plane.contracts import OrderIntent


class ExecutionAdapterTransport(Protocol):
    def submit_order_intent(self, intent: OrderIntent) -> dict[str, object]:
        ...

    def cancel_order_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        ...

    def replace_order_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        ...

    def list_broker_updates(self) -> list[dict[str, object]]:
        ...

    def list_broker_fills(self) -> list[dict[str, object]]:
        ...
