from __future__ import annotations

import hashlib

from trading_advisor_3000.app.contracts import OrderIntent


class StockSharpSidecarStub:
    def __init__(self) -> None:
        self._queued_intents: list[dict[str, object]] = []

    def health(self) -> dict[str, object]:
        return {
            "adapter": "stocksharp-sidecar-stub",
            "status": "ok",
            "queued_intents": len(self._queued_intents),
        }

    def submit_order_intent(self, intent: OrderIntent) -> dict[str, object]:
        external_order_id = "stub-" + hashlib.sha256(intent.intent_id.encode("utf-8")).hexdigest()[:12]
        envelope = {
            "intent_id": intent.intent_id,
            "external_order_id": external_order_id,
            "accepted": True,
            "broker_adapter": intent.broker_adapter,
        }
        self._queued_intents.append({"intent": intent.to_dict(), "ack": envelope})
        return envelope

    def list_submitted(self) -> list[dict[str, object]]:
        return list(self._queued_intents)
