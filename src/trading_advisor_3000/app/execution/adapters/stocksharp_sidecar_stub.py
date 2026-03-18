from __future__ import annotations

import hashlib

from trading_advisor_3000.app.contracts import OrderIntent


class TransientSidecarError(RuntimeError):
    pass


class StockSharpSidecarStub:
    def __init__(self) -> None:
        self._queued_intents: list[dict[str, object]] = []
        self._acks_by_intent_id: dict[str, dict[str, object]] = {}
        self._broker_updates: list[dict[str, object]] = []
        self._broker_fills: list[dict[str, object]] = []
        self._transport_path = "stocksharp->quik->finam"
        self._transient_failures: dict[str, int] = {}

    def inject_transient_failures(self, *, operation: str, failures: int) -> None:
        if failures < 0:
            raise ValueError("failures must be non-negative")
        self._transient_failures[operation] = failures

    def _consume_transient_failure(self, operation: str) -> None:
        remaining = self._transient_failures.get(operation, 0)
        if remaining <= 0:
            return
        self._transient_failures[operation] = remaining - 1
        raise TransientSidecarError(f"transient sidecar failure: operation={operation}, remaining={remaining - 1}")

    def health(self) -> dict[str, object]:
        return {
            "adapter": "stocksharp-sidecar-stub",
            "status": "ok",
            "transport_path": self._transport_path,
            "queued_intents": len(self._queued_intents),
            "acked_intents": len(self._acks_by_intent_id),
            "transient_failures": dict(self._transient_failures),
        }

    def readiness(self) -> dict[str, object]:
        return {
            "adapter": "stocksharp-sidecar-stub",
            "ready": True,
            "reason": "stub_ready",
            "transport_path": self._transport_path,
        }

    def submit_order_intent(self, intent: OrderIntent) -> dict[str, object]:
        self._consume_transient_failure("submit_order_intent")
        external_order_id = "stub-" + hashlib.sha256(intent.intent_id.encode("utf-8")).hexdigest()[:12]
        envelope = {
            "intent_id": intent.intent_id,
            "external_order_id": external_order_id,
            "accepted": True,
            "broker_adapter": intent.broker_adapter,
            "state": "submitted",
        }
        self._queued_intents.append({"intent": intent.to_dict(), "ack": envelope})
        self._acks_by_intent_id[intent.intent_id] = envelope
        return envelope

    def list_submitted(self) -> list[dict[str, object]]:
        return list(self._queued_intents)

    def cancel_order_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        self._consume_transient_failure("cancel_order_intent")
        existing = self._acks_by_intent_id.get(intent_id)
        if existing is None:
            raise ValueError(f"unknown intent_id: {intent_id}")
        envelope = {
            **existing,
            "state": "canceled",
            "canceled_at": canceled_at,
        }
        self._acks_by_intent_id[intent_id] = envelope
        self.push_broker_update(
            external_order_id=str(existing["external_order_id"]),
            state="canceled",
            event_ts=canceled_at,
            payload={"intent_id": intent_id},
        )
        return envelope

    def replace_order_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        self._consume_transient_failure("replace_order_intent")
        if new_qty <= 0:
            raise ValueError("new_qty must be positive")
        if new_price <= 0:
            raise ValueError("new_price must be positive")
        existing = self._acks_by_intent_id.get(intent_id)
        if existing is None:
            raise ValueError(f"unknown intent_id: {intent_id}")
        envelope = {
            **existing,
            "state": "replaced",
            "replaced_at": replaced_at,
            "new_qty": new_qty,
            "new_price": new_price,
        }
        self._acks_by_intent_id[intent_id] = envelope
        self.push_broker_update(
            external_order_id=str(existing["external_order_id"]),
            state="replaced",
            event_ts=replaced_at,
            payload={"intent_id": intent_id, "new_qty": new_qty, "new_price": new_price},
        )
        return envelope

    def push_broker_update(
        self,
        *,
        external_order_id: str,
        state: str,
        event_ts: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        update = {
            "external_order_id": external_order_id,
            "state": state,
            "event_ts": event_ts,
            "payload": payload or {},
        }
        self._broker_updates.append(update)
        return update

    def push_broker_fill(
        self,
        *,
        external_order_id: str,
        fill_id: str,
        qty: int,
        price: float,
        fill_ts: str,
        fee: float = 0.0,
    ) -> dict[str, object]:
        if qty <= 0:
            raise ValueError("qty must be positive")
        if price <= 0:
            raise ValueError("price must be positive")
        if fee < 0:
            raise ValueError("fee must be non-negative")
        fill = {
            "external_order_id": external_order_id,
            "fill_id": fill_id,
            "qty": qty,
            "price": price,
            "fee": fee,
            "fill_ts": fill_ts,
        }
        self._broker_fills.append(fill)
        return fill

    def list_broker_updates(self) -> list[dict[str, object]]:
        return list(self._broker_updates)

    def list_broker_fills(self) -> list[dict[str, object]]:
        return list(self._broker_fills)
