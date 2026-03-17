from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import (
    BrokerEvent,
    BrokerFill,
    BrokerOrder,
    Mode,
    OrderIntent,
    PositionSnapshot,
)
from trading_advisor_3000.app.contracts.execution import BROKER_ORDER_STATES


def _stable_id(prefix: str, seed: str) -> str:
    return f"{prefix}-{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:12].upper()}"


@dataclass(frozen=True)
class SyncIncident:
    incident_id: str
    severity: str
    reason: str
    intent_id: str | None
    external_order_id: str | None
    details: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "incident_id": self.incident_id,
            "severity": self.severity,
            "reason": self.reason,
            "intent_id": self.intent_id,
            "external_order_id": self.external_order_id,
            "details": self.details,
        }


class BrokerSyncEngine:
    def __init__(self, *, account_id: str, broker_adapter: str = "stocksharp-sidecar-stub") -> None:
        self._account_id = account_id
        self._broker_adapter = broker_adapter

        self._intents_by_id: dict[str, OrderIntent] = {}
        self._intent_by_external_id: dict[str, str] = {}
        self._orders_by_external_id: dict[str, BrokerOrder] = {}
        self._fills_by_fill_id: dict[str, BrokerFill] = {}
        self._fills_by_external_id: dict[str, list[BrokerFill]] = {}
        self._positions_by_key: dict[str, PositionSnapshot] = {}

        self._broker_events: list[BrokerEvent] = []
        self._event_dedup_keys: set[str] = set()

        self._incidents: list[SyncIncident] = []
        self._incident_dedup_keys: set[str] = set()

    def _append_event(
        self,
        *,
        external_object_id: str,
        event_type: str,
        event_ts: str,
        payload_json: dict[str, object],
        dedup_key: str,
    ) -> None:
        if dedup_key in self._event_dedup_keys:
            return
        self._event_dedup_keys.add(dedup_key)
        self._broker_events.append(
            BrokerEvent(
                event_id=_stable_id("BEVT", dedup_key),
                broker_adapter=self._broker_adapter,
                external_object_id=external_object_id,
                event_type=event_type,
                event_ts=event_ts,
                payload_json=payload_json,
            )
        )

    def _append_incident(
        self,
        *,
        severity: str,
        reason: str,
        intent_id: str | None,
        external_order_id: str | None,
        details: dict[str, object] | None = None,
    ) -> None:
        payload = details or {}
        dedup_seed = json.dumps(
            {
                "severity": severity,
                "reason": reason,
                "intent_id": intent_id,
                "external_order_id": external_order_id,
                "details": payload,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if dedup_seed in self._incident_dedup_keys:
            return
        self._incident_dedup_keys.add(dedup_seed)
        self._incidents.append(
            SyncIncident(
                incident_id=_stable_id("SYNCINC", dedup_seed),
                severity=severity,
                reason=reason,
                intent_id=intent_id,
                external_order_id=external_order_id,
                details=payload,
            )
        )

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

    @staticmethod
    def _position_key(*, account_id: str, contract_id: str, mode: Mode) -> str:
        return f"{account_id}:{contract_id}:{mode.value}"

    def _apply_replace_payload(
        self,
        *,
        intent_id: str,
        external_order_id: str,
        payload_json: dict[str, object],
    ) -> None:
        intent = self._intents_by_id.get(intent_id)
        if intent is None:
            self._append_incident(
                severity="high",
                reason="replace_for_unknown_intent",
                intent_id=intent_id,
                external_order_id=external_order_id,
                details={"payload": payload_json},
            )
            return

        new_qty_raw = payload_json.get("new_qty")
        new_price_raw = payload_json.get("new_price")
        try:
            if isinstance(new_qty_raw, bool) or isinstance(new_price_raw, bool):
                raise ValueError("new_qty/new_price must not be boolean")
            if isinstance(new_qty_raw, int):
                new_qty = new_qty_raw
            else:
                new_qty = int(float(new_qty_raw))
            new_price = float(new_price_raw)
            if new_qty <= 0:
                raise ValueError("new_qty must be positive")
            if new_price <= 0:
                raise ValueError("new_price must be positive")
        except (TypeError, ValueError) as exc:
            self._append_incident(
                severity="high",
                reason="invalid_replace_payload",
                intent_id=intent_id,
                external_order_id=external_order_id,
                details={"payload": payload_json, "error": str(exc)},
            )
            return

        self._intents_by_id[intent_id] = OrderIntent(
            intent_id=intent.intent_id,
            signal_id=intent.signal_id,
            mode=intent.mode,
            broker_adapter=intent.broker_adapter,
            action=intent.action,
            contract_id=intent.contract_id,
            qty=new_qty,
            price=new_price,
            stop_price=intent.stop_price,
            created_at=intent.created_at,
        )

    def register_intent(self, intent: OrderIntent) -> None:
        existing = self._intents_by_id.get(intent.intent_id)
        if existing is not None and existing.to_dict() != intent.to_dict():
            self._append_incident(
                severity="high",
                reason="intent_id_payload_conflict",
                intent_id=intent.intent_id,
                external_order_id=None,
                details={"existing": existing.to_dict(), "incoming": intent.to_dict()},
            )
            return
        self._intents_by_id[intent.intent_id] = intent

    def record_submission_ack(self, *, intent_id: str, ack: dict[str, object], acknowledged_at: str) -> BrokerOrder | None:
        intent = self._intents_by_id.get(intent_id)
        if intent is None:
            self._append_incident(
                severity="high",
                reason="ack_for_unknown_intent",
                intent_id=intent_id,
                external_order_id=str(ack.get("external_order_id")),
                details={"ack": ack},
            )
            return None

        external_order_id_raw = ack.get("external_order_id")
        external_order_id = str(external_order_id_raw) if external_order_id_raw is not None else None
        state = str(ack.get("state", "submitted"))
        accepted = bool(ack.get("accepted", False))

        if not accepted:
            state = "rejected"
        if state not in BROKER_ORDER_STATES:
            state = "submitted"
            self._append_incident(
                severity="medium",
                reason="unsupported_ack_state",
                intent_id=intent_id,
                external_order_id=external_order_id,
                details={"ack_state": ack.get("state")},
            )

        broker_order = BrokerOrder(
            broker_order_id=_stable_id("BORD", f"{intent_id}|{external_order_id}|{intent.created_at}"),
            intent_id=intent_id,
            external_order_id=external_order_id,
            broker=intent.broker_adapter,
            state=state,
            submitted_at=intent.created_at,
            updated_at=acknowledged_at,
        )
        if external_order_id is None:
            self._append_incident(
                severity="high",
                reason="missing_external_order_id_in_ack",
                intent_id=intent_id,
                external_order_id=None,
                details={"ack": ack},
            )
            return broker_order

        self._intent_by_external_id[external_order_id] = intent_id
        self._orders_by_external_id[external_order_id] = broker_order
        self._append_event(
            external_object_id=external_order_id,
            event_type="order_acknowledged",
            event_ts=acknowledged_at,
            payload_json={"intent_id": intent_id, "state": state, "accepted": accepted},
            dedup_key=f"ack|{intent_id}|{external_order_id}|{acknowledged_at}|{state}",
        )
        return broker_order

    def _update_order_state_from_fill(self, *, external_order_id: str, fill_ts: str) -> None:
        order = self._orders_by_external_id.get(external_order_id)
        if order is None:
            return
        intent = self._intents_by_id.get(order.intent_id)
        if intent is None:
            return
        fills = self._fills_by_external_id.get(external_order_id, [])
        filled_qty = sum(item.qty for item in fills)
        expected_qty = intent.qty

        if filled_qty == 0:
            new_state = order.state
        elif filled_qty < expected_qty:
            new_state = "partially_filled"
        else:
            new_state = "filled"
            if filled_qty > expected_qty:
                self._append_incident(
                    severity="high",
                    reason="filled_qty_exceeds_intent_qty",
                    intent_id=order.intent_id,
                    external_order_id=external_order_id,
                    details={"expected_qty": expected_qty, "filled_qty": filled_qty},
                )

        if new_state != order.state or order.updated_at != fill_ts:
            self._orders_by_external_id[external_order_id] = BrokerOrder(
                broker_order_id=order.broker_order_id,
                intent_id=order.intent_id,
                external_order_id=order.external_order_id,
                broker=order.broker,
                state=new_state,
                submitted_at=order.submitted_at,
                updated_at=fill_ts,
            )

    def ingest_broker_updates(self, updates: list[dict[str, object]]) -> None:
        for update in updates:
            external_order_id = str(update.get("external_order_id", "")).strip()
            state = str(update.get("state", "")).strip()
            event_ts = str(update.get("event_ts", "")).strip()
            payload = update.get("payload", {})
            payload_json = payload if isinstance(payload, dict) else {"raw_payload": payload}

            if not external_order_id:
                self._append_incident(
                    severity="high",
                    reason="broker_update_missing_external_order_id",
                    intent_id=None,
                    external_order_id=None,
                    details={"update": update},
                )
                continue

            if state not in BROKER_ORDER_STATES:
                self._append_incident(
                    severity="medium",
                    reason="broker_update_unsupported_state",
                    intent_id=self._intent_by_external_id.get(external_order_id),
                    external_order_id=external_order_id,
                    details={"state": state},
                )
                self._append_event(
                    external_object_id=external_order_id,
                    event_type="order_update_rejected",
                    event_ts=event_ts or "1970-01-01T00:00:00Z",
                    payload_json={"state": state, "payload": payload_json},
                    dedup_key=f"update-rejected|{external_order_id}|{event_ts}|{state}",
                )
                continue

            order = self._orders_by_external_id.get(external_order_id)
            if order is None:
                self._append_incident(
                    severity="high",
                    reason="broker_update_for_unknown_order",
                    intent_id=None,
                    external_order_id=external_order_id,
                    details={"state": state},
                )
                self._append_event(
                    external_object_id=external_order_id,
                    event_type="order_update_unmapped",
                    event_ts=event_ts or "1970-01-01T00:00:00Z",
                    payload_json={"state": state, "payload": payload_json},
                    dedup_key=f"update-unmapped|{external_order_id}|{event_ts}|{state}",
                )
                continue

            updated = BrokerOrder(
                broker_order_id=order.broker_order_id,
                intent_id=order.intent_id,
                external_order_id=order.external_order_id,
                broker=order.broker,
                state=state,
                submitted_at=order.submitted_at,
                updated_at=event_ts or order.updated_at,
            )
            self._orders_by_external_id[external_order_id] = updated
            if state == "replaced":
                self._apply_replace_payload(
                    intent_id=order.intent_id,
                    external_order_id=external_order_id,
                    payload_json=payload_json,
                )
            self._append_event(
                external_object_id=external_order_id,
                event_type="order_state_updated",
                event_ts=updated.updated_at,
                payload_json={"state": state, "payload": payload_json},
                dedup_key=f"update|{external_order_id}|{updated.updated_at}|{state}",
            )

    def ingest_broker_fills(self, fills: list[dict[str, object]]) -> None:
        for row in fills:
            external_order_id = str(row.get("external_order_id", "")).strip()
            fill_id = str(row.get("fill_id", "")).strip()
            fill_ts = str(row.get("fill_ts", "")).strip()
            qty = row.get("qty")
            price = row.get("price")
            fee = row.get("fee", 0.0)

            if not fill_id:
                self._append_incident(
                    severity="high",
                    reason="broker_fill_missing_fill_id",
                    intent_id=None,
                    external_order_id=external_order_id or None,
                    details={"fill": row},
                )
                continue
            if fill_id in self._fills_by_fill_id:
                continue
            if not external_order_id:
                self._append_incident(
                    severity="high",
                    reason="broker_fill_missing_external_order_id",
                    intent_id=None,
                    external_order_id=None,
                    details={"fill_id": fill_id},
                )
                continue
            if not fill_ts:
                self._append_incident(
                    severity="high",
                    reason="broker_fill_missing_fill_ts",
                    intent_id=None,
                    external_order_id=external_order_id,
                    details={"fill_id": fill_id},
                )
                continue

            order = self._orders_by_external_id.get(external_order_id)
            if order is None:
                self._append_incident(
                    severity="high",
                    reason="broker_fill_for_unknown_order",
                    intent_id=None,
                    external_order_id=external_order_id,
                    details={"fill_id": fill_id},
                )
                self._append_event(
                    external_object_id=external_order_id,
                    event_type="fill_unmapped",
                    event_ts=fill_ts or "1970-01-01T00:00:00Z",
                    payload_json={"fill_id": fill_id},
                    dedup_key=f"fill-unmapped|{external_order_id}|{fill_id}|{fill_ts}",
                )
                continue

            try:
                if isinstance(qty, bool) or isinstance(price, bool) or isinstance(fee, bool):
                    raise ValueError("qty/price/fee must not be boolean")
                parsed_qty = int(qty) if isinstance(qty, int) else int(float(qty))
                parsed_price = float(price)
                parsed_fee = float(fee) if isinstance(fee, (int, float)) else float(fee or 0.0)
                fill = BrokerFill(
                    fill_id=fill_id,
                    broker_order_id=order.broker_order_id,
                    fill_ts=fill_ts,
                    qty=parsed_qty,
                    price=parsed_price,
                    fee=parsed_fee,
                    external_trade_id=_stable_id("TRD", f"{external_order_id}|{fill_id}"),
                )
            except (TypeError, ValueError) as exc:
                self._append_incident(
                    severity="high",
                    reason="invalid_broker_fill_payload",
                    intent_id=order.intent_id,
                    external_order_id=external_order_id,
                    details={"fill_id": fill_id, "error": str(exc)},
                )
                continue
            self._fills_by_fill_id[fill_id] = fill
            bucket = self._fills_by_external_id.setdefault(external_order_id, [])
            bucket.append(fill)
            self._update_order_state_from_fill(external_order_id=external_order_id, fill_ts=fill.fill_ts)
            self._append_event(
                external_object_id=external_order_id,
                event_type="fill_synced",
                event_ts=fill.fill_ts,
                payload_json=fill.to_dict(),
                dedup_key=f"fill|{external_order_id}|{fill_id}",
            )

            intent_id = self._intent_by_external_id.get(external_order_id)
            intent = self._intents_by_id.get(intent_id) if intent_id is not None else None
            if intent is None:
                self._append_incident(
                    severity="high",
                    reason="fill_without_registered_intent",
                    intent_id=None,
                    external_order_id=external_order_id,
                    details={"fill_id": fill_id},
                )
                continue

            key = self._position_key(
                account_id=self._account_id,
                contract_id=intent.contract_id,
                mode=intent.mode,
            )
            previous = self._positions_by_key.get(key)
            existing_qty = previous.qty if previous is not None else 0
            existing_avg = previous.avg_price if previous is not None else 0.0
            signed_fill_qty = self._signed_quantity(action=intent.action, quantity=fill.qty)
            merged_qty, merged_avg = self._merge_position(
                existing_qty=existing_qty,
                existing_avg=existing_avg,
                fill_qty_signed=signed_fill_qty,
                fill_price=fill.price,
            )
            position = PositionSnapshot(
                position_key=key,
                account_id=self._account_id,
                contract_id=intent.contract_id,
                mode=intent.mode,
                qty=merged_qty,
                avg_price=merged_avg,
                as_of_ts=fill.fill_ts,
            )
            self._positions_by_key[key] = position
            self._append_event(
                external_object_id=key,
                event_type="position_synced",
                event_ts=fill.fill_ts,
                payload_json=position.to_dict(),
                dedup_key=f"position|{key}|{fill.fill_id}|{fill.fill_ts}",
            )

    def list_broker_orders(self) -> list[BrokerOrder]:
        return sorted(
            self._orders_by_external_id.values(),
            key=lambda item: (item.intent_id, item.updated_at, item.broker_order_id),
        )

    def list_registered_intents(self) -> list[OrderIntent]:
        return sorted(
            self._intents_by_id.values(),
            key=lambda item: (item.created_at, item.intent_id),
        )

    def list_broker_fills(self) -> list[BrokerFill]:
        return sorted(
            self._fills_by_fill_id.values(),
            key=lambda item: (item.fill_ts, item.fill_id),
        )

    def list_positions(self) -> list[PositionSnapshot]:
        return sorted(
            self._positions_by_key.values(),
            key=lambda item: (item.contract_id, item.as_of_ts, item.position_key),
        )

    def list_broker_events(self) -> list[BrokerEvent]:
        return list(self._broker_events)

    def list_incidents(self) -> list[SyncIncident]:
        return list(self._incidents)

    def to_dict(self) -> dict[str, object]:
        return {
            "orders": [item.to_dict() for item in self.list_broker_orders()],
            "fills": [item.to_dict() for item in self.list_broker_fills()],
            "positions": [item.to_dict() for item in self.list_positions()],
            "events": [item.to_dict() for item in self.list_broker_events()],
            "incidents": [item.to_dict() for item in self.list_incidents()],
            "stats": {
                "registered_intents": len(self._intents_by_id),
                "orders_synced": len(self._orders_by_external_id),
                "fills_synced": len(self._fills_by_fill_id),
                "positions_synced": len(self._positions_by_key),
                "incidents": len(self._incidents),
            },
        }
