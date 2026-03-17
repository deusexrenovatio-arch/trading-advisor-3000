from __future__ import annotations

import hashlib
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import (
    BrokerEvent,
    BrokerFill,
    BrokerOrder,
    Mode,
    OrderIntent,
    PositionSnapshot,
)


def _stable_id(prefix: str, seed: str) -> str:
    return f"{prefix}-{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:12].upper()}"


@dataclass(frozen=True)
class PaperExecutionResult:
    broker_order: BrokerOrder
    broker_fill: BrokerFill
    position_snapshot: PositionSnapshot

    def to_dict(self) -> dict[str, object]:
        return {
            "broker_order": self.broker_order.to_dict(),
            "broker_fill": self.broker_fill.to_dict(),
            "position_snapshot": self.position_snapshot.to_dict(),
        }


class PaperBrokerEngine:
    def __init__(self, *, account_id: str, broker_adapter: str = "stocksharp-sidecar-stub") -> None:
        self._account_id = account_id
        self._broker_adapter = broker_adapter
        self._positions: dict[str, PositionSnapshot] = {}
        self._results_by_intent: dict[str, PaperExecutionResult] = {}
        self._broker_events: list[BrokerEvent] = []

    def _append_event(
        self,
        *,
        external_object_id: str,
        event_type: str,
        event_ts: str,
        payload_json: dict[str, object],
    ) -> None:
        event = BrokerEvent(
            event_id=_stable_id(
                "BEVT",
                f"{external_object_id}|{event_type}|{event_ts}|{len(self._broker_events)}",
            ),
            broker_adapter=self._broker_adapter,
            external_object_id=external_object_id,
            event_type=event_type,
            event_ts=event_ts,
            payload_json=payload_json,
        )
        self._broker_events.append(event)

    @staticmethod
    def _position_key(*, account_id: str, contract_id: str, mode: Mode) -> str:
        return f"{account_id}:{contract_id}:{mode.value}"

    @staticmethod
    def _signed_quantity(*, action: str, quantity: int) -> int:
        if action == "buy":
            return quantity
        if action == "sell":
            return -quantity
        raise ValueError(f"unsupported order action: {action}")

    @staticmethod
    def _merge_position(
        *,
        existing_qty: int,
        existing_avg: float,
        fill_qty_signed: int,
        fill_price: float,
    ) -> tuple[int, float]:
        if fill_qty_signed == 0:
            return existing_qty, existing_avg
        if existing_qty == 0:
            return fill_qty_signed, fill_price

        same_direction = (existing_qty > 0 and fill_qty_signed > 0) or (existing_qty < 0 and fill_qty_signed < 0)
        new_qty = existing_qty + fill_qty_signed
        if new_qty == 0:
            return 0, 0.0

        if same_direction:
            weighted_notional = abs(existing_qty) * existing_avg + abs(fill_qty_signed) * fill_price
            return new_qty, weighted_notional / abs(new_qty)

        if abs(fill_qty_signed) < abs(existing_qty):
            return new_qty, existing_avg

        if abs(fill_qty_signed) > abs(existing_qty):
            return new_qty, fill_price

        return 0, 0.0

    def execute_intent(
        self,
        intent: OrderIntent,
        *,
        fill_price: float | None = None,
        fill_ts: str | None = None,
        fee: float = 0.0,
    ) -> PaperExecutionResult:
        if intent.mode != Mode.PAPER:
            raise ValueError("paper broker supports only paper mode intents")
        if intent.broker_adapter != self._broker_adapter:
            raise ValueError("intent broker_adapter does not match paper broker adapter")
        existing = self._results_by_intent.get(intent.intent_id)
        if existing is not None:
            return existing

        resolved_fill_ts = fill_ts or intent.created_at
        resolved_price = float(fill_price if fill_price is not None else intent.price)
        if resolved_price <= 0:
            raise ValueError("fill_price must be positive")

        broker_order_id = _stable_id("BORD", intent.intent_id)
        broker_order = BrokerOrder(
            broker_order_id=broker_order_id,
            intent_id=intent.intent_id,
            external_order_id=_stable_id("EXT", intent.intent_id),
            broker=intent.broker_adapter,
            state="filled",
            submitted_at=intent.created_at,
            updated_at=resolved_fill_ts,
        )
        broker_fill = BrokerFill(
            fill_id=_stable_id("BFILL", f"{intent.intent_id}|{resolved_fill_ts}"),
            broker_order_id=broker_order_id,
            fill_ts=resolved_fill_ts,
            qty=intent.qty,
            price=resolved_price,
            fee=fee,
            external_trade_id=_stable_id("TRD", f"{intent.intent_id}|{resolved_fill_ts}"),
        )

        key = self._position_key(
            account_id=self._account_id,
            contract_id=intent.contract_id,
            mode=intent.mode,
        )
        previous = self._positions.get(key)
        previous_qty = previous.qty if previous is not None else 0
        previous_avg = previous.avg_price if previous is not None else 0.0
        signed_fill_qty = self._signed_quantity(action=intent.action, quantity=intent.qty)
        merged_qty, merged_avg = self._merge_position(
            existing_qty=previous_qty,
            existing_avg=previous_avg,
            fill_qty_signed=signed_fill_qty,
            fill_price=resolved_price,
        )
        position = PositionSnapshot(
            position_key=key,
            account_id=self._account_id,
            contract_id=intent.contract_id,
            mode=intent.mode,
            qty=merged_qty,
            avg_price=merged_avg,
            as_of_ts=resolved_fill_ts,
        )
        self._positions[key] = position

        self._append_event(
            external_object_id=intent.intent_id,
            event_type="intent_received",
            event_ts=intent.created_at,
            payload_json=intent.to_dict(),
        )
        self._append_event(
            external_object_id=broker_order_id,
            event_type="order_submitted",
            event_ts=intent.created_at,
            payload_json=broker_order.to_dict(),
        )
        self._append_event(
            external_object_id=broker_fill.fill_id,
            event_type="order_filled",
            event_ts=resolved_fill_ts,
            payload_json=broker_fill.to_dict(),
        )
        self._append_event(
            external_object_id=position.position_key,
            event_type="position_updated",
            event_ts=resolved_fill_ts,
            payload_json=position.to_dict(),
        )

        result = PaperExecutionResult(
            broker_order=broker_order,
            broker_fill=broker_fill,
            position_snapshot=position,
        )
        self._results_by_intent[intent.intent_id] = result
        return result

    def get_position_snapshot(self, *, contract_id: str, mode: Mode = Mode.PAPER) -> PositionSnapshot | None:
        key = self._position_key(account_id=self._account_id, contract_id=contract_id, mode=mode)
        return self._positions.get(key)

    def list_broker_events(self) -> list[BrokerEvent]:
        return list(self._broker_events)
