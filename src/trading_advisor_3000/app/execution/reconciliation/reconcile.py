from __future__ import annotations

import hashlib
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import BrokerFill, BrokerOrder, OrderIntent, PositionSnapshot


@dataclass(frozen=True)
class PositionDrift:
    position_key: str
    expected_qty: int | None
    observed_qty: int | None
    expected_avg_price: float | None
    observed_avg_price: float | None
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "position_key": self.position_key,
            "expected_qty": self.expected_qty,
            "observed_qty": self.observed_qty,
            "expected_avg_price": self.expected_avg_price,
            "observed_avg_price": self.observed_avg_price,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class OrderDrift:
    intent_id: str
    broker_order_id: str | None
    external_order_id: str | None
    expected_qty: int
    observed_filled_qty: int
    observed_state: str | None
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "intent_id": self.intent_id,
            "broker_order_id": self.broker_order_id,
            "external_order_id": self.external_order_id,
            "expected_qty": self.expected_qty,
            "observed_filled_qty": self.observed_filled_qty,
            "observed_state": self.observed_state,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class FillDrift:
    broker_order_id: str
    observed_fill_qty: int
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "broker_order_id": self.broker_order_id,
            "observed_fill_qty": self.observed_fill_qty,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ReconciliationIncident:
    incident_id: str
    position_key: str
    severity: str
    reason: str
    recovery_action: str

    def to_dict(self) -> dict[str, str]:
        return {
            "incident_id": self.incident_id,
            "position_key": self.position_key,
            "severity": self.severity,
            "reason": self.reason,
            "recovery_action": self.recovery_action,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    matched: int
    missing: list[PositionDrift]
    unexpected: list[PositionDrift]
    mismatched: list[PositionDrift]
    incidents: list[ReconciliationIncident]

    @property
    def is_clean(self) -> bool:
        return not self.missing and not self.unexpected and not self.mismatched and not self.incidents

    def to_dict(self) -> dict[str, object]:
        return {
            "matched": self.matched,
            "missing": [item.to_dict() for item in self.missing],
            "unexpected": [item.to_dict() for item in self.unexpected],
            "mismatched": [item.to_dict() for item in self.mismatched],
            "incidents": [item.to_dict() for item in self.incidents],
            "is_clean": self.is_clean,
        }


@dataclass(frozen=True)
class LiveExecutionReconciliationReport:
    orders_matched: int
    fills_matched: int
    order_drifts: list[OrderDrift]
    fill_drifts: list[FillDrift]
    position_report: ReconciliationReport
    incidents: list[ReconciliationIncident]

    @property
    def is_clean(self) -> bool:
        return (
            not self.order_drifts
            and not self.fill_drifts
            and self.position_report.is_clean
            and not self.incidents
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "orders_matched": self.orders_matched,
            "fills_matched": self.fills_matched,
            "order_drifts": [item.to_dict() for item in self.order_drifts],
            "fill_drifts": [item.to_dict() for item in self.fill_drifts],
            "position_report": self.position_report.to_dict(),
            "incidents": [item.to_dict() for item in self.incidents],
            "is_clean": self.is_clean,
        }


def _incident_id(*, position_key: str, reason: str) -> str:
    return "INC-" + hashlib.sha256(f"{position_key}|{reason}".encode("utf-8")).hexdigest()[:12].upper()


def _incident_for_position_drift(drift: PositionDrift) -> ReconciliationIncident:
    if drift.reason == "missing_position":
        severity = "high"
        recovery_action = "rebuild_expected_from_broker_and_resync_position"
    elif drift.reason == "unexpected_position":
        severity = "high"
        recovery_action = "freeze_auto_trading_and_open_manual_investigation"
    elif drift.reason == "quantity_mismatch":
        severity = "high"
        recovery_action = "replay_fills_and_recompute_quantity"
    else:
        severity = "medium"
        recovery_action = "reprice_position_from_fill_log"
    return ReconciliationIncident(
        incident_id=_incident_id(position_key=drift.position_key, reason=drift.reason),
        position_key=drift.position_key,
        severity=severity,
        reason=drift.reason,
        recovery_action=recovery_action,
    )


def _incident_for_order_drift(drift: OrderDrift) -> ReconciliationIncident:
    if drift.reason in {"missing_broker_order_ack", "unexpected_broker_order"}:
        severity = "high"
        recovery_action = "resync_order_index_from_broker_event_log"
    elif drift.reason in {"missing_external_order_id", "stale_order_state_vs_fill"}:
        severity = "high"
        recovery_action = "freeze_new_submissions_and_reconcile_order_state"
    else:
        severity = "medium"
        recovery_action = "review_order_state_transition_and_retry_sync"
    key = drift.external_order_id or drift.broker_order_id or drift.intent_id
    return ReconciliationIncident(
        incident_id=_incident_id(position_key=f"order:{key}", reason=drift.reason),
        position_key=f"order:{key}",
        severity=severity,
        reason=drift.reason,
        recovery_action=recovery_action,
    )


def _incident_for_fill_drift(drift: FillDrift) -> ReconciliationIncident:
    if drift.reason in {"orphan_fill_without_order", "fill_qty_exceeds_expected"}:
        severity = "high"
        recovery_action = "block_position_updates_and_rebuild_fills_from_broker"
    else:
        severity = "medium"
        recovery_action = "replay_fill_ingestion_for_order"
    return ReconciliationIncident(
        incident_id=_incident_id(position_key=f"fill:{drift.broker_order_id}", reason=drift.reason),
        position_key=f"fill:{drift.broker_order_id}",
        severity=severity,
        reason=drift.reason,
        recovery_action=recovery_action,
    )


def _position_key(position: PositionSnapshot) -> str:
    return position.position_key


def _latest_orders_by_intent(observed_orders: list[BrokerOrder]) -> dict[str, BrokerOrder]:
    latest: dict[str, BrokerOrder] = {}
    for order in sorted(observed_orders, key=lambda item: (item.intent_id, item.updated_at, item.broker_order_id)):
        latest[order.intent_id] = order
    return latest


def reconcile_position_snapshots(
    expected: list[PositionSnapshot],
    observed: list[PositionSnapshot],
    *,
    price_tolerance: float = 1e-9,
) -> ReconciliationReport:
    expected_map = {_position_key(item): item for item in expected}
    observed_map = {_position_key(item): item for item in observed}
    all_keys = sorted(set(expected_map) | set(observed_map))

    missing: list[PositionDrift] = []
    unexpected: list[PositionDrift] = []
    mismatched: list[PositionDrift] = []
    matched = 0

    for key in all_keys:
        expected_row = expected_map.get(key)
        observed_row = observed_map.get(key)
        if expected_row is None and observed_row is not None:
            unexpected.append(
                PositionDrift(
                    position_key=key,
                    expected_qty=None,
                    observed_qty=observed_row.qty,
                    expected_avg_price=None,
                    observed_avg_price=observed_row.avg_price,
                    reason="unexpected_position",
                )
            )
            continue
        if expected_row is not None and observed_row is None:
            missing.append(
                PositionDrift(
                    position_key=key,
                    expected_qty=expected_row.qty,
                    observed_qty=None,
                    expected_avg_price=expected_row.avg_price,
                    observed_avg_price=None,
                    reason="missing_position",
                )
            )
            continue
        assert expected_row is not None
        assert observed_row is not None
        if expected_row.qty != observed_row.qty:
            mismatched.append(
                PositionDrift(
                    position_key=key,
                    expected_qty=expected_row.qty,
                    observed_qty=observed_row.qty,
                    expected_avg_price=expected_row.avg_price,
                    observed_avg_price=observed_row.avg_price,
                    reason="quantity_mismatch",
                )
            )
            continue
        if abs(expected_row.avg_price - observed_row.avg_price) > price_tolerance:
            mismatched.append(
                PositionDrift(
                    position_key=key,
                    expected_qty=expected_row.qty,
                    observed_qty=observed_row.qty,
                    expected_avg_price=expected_row.avg_price,
                    observed_avg_price=observed_row.avg_price,
                    reason="avg_price_mismatch",
                )
            )
            continue
        matched += 1

    incidents = [_incident_for_position_drift(item) for item in [*missing, *unexpected, *mismatched]]
    return ReconciliationReport(
        matched=matched,
        missing=missing,
        unexpected=unexpected,
        mismatched=mismatched,
        incidents=incidents,
    )


def reconcile_live_execution(
    *,
    expected_intents: list[OrderIntent],
    observed_orders: list[BrokerOrder],
    observed_fills: list[BrokerFill],
    expected_positions: list[PositionSnapshot],
    observed_positions: list[PositionSnapshot],
    price_tolerance: float = 1e-9,
) -> LiveExecutionReconciliationReport:
    expected_by_intent = {item.intent_id: item for item in expected_intents}
    latest_orders_by_intent = _latest_orders_by_intent(observed_orders)
    orders_by_broker_id = {item.broker_order_id: item for item in observed_orders}

    fills_by_order_id: dict[str, list[BrokerFill]] = {}
    for fill in observed_fills:
        fills_by_order_id.setdefault(fill.broker_order_id, []).append(fill)
    fill_qty_by_order_id = {key: sum(item.qty for item in value) for key, value in fills_by_order_id.items()}

    order_drifts: list[OrderDrift] = []
    fill_drifts: list[FillDrift] = []
    orders_matched = 0
    fills_matched = 0

    for intent in sorted(expected_intents, key=lambda item: (item.created_at, item.intent_id)):
        order = latest_orders_by_intent.get(intent.intent_id)
        if order is None:
            order_drifts.append(
                OrderDrift(
                    intent_id=intent.intent_id,
                    broker_order_id=None,
                    external_order_id=None,
                    expected_qty=intent.qty,
                    observed_filled_qty=0,
                    observed_state=None,
                    reason="missing_broker_order_ack",
                )
            )
            continue

        observed_fill_qty = fill_qty_by_order_id.get(order.broker_order_id, 0)
        had_order_drift = False
        had_fill_drift = False

        if not order.external_order_id:
            had_order_drift = True
            order_drifts.append(
                OrderDrift(
                    intent_id=intent.intent_id,
                    broker_order_id=order.broker_order_id,
                    external_order_id=order.external_order_id,
                    expected_qty=intent.qty,
                    observed_filled_qty=observed_fill_qty,
                    observed_state=order.state,
                    reason="missing_external_order_id",
                )
            )

        if observed_fill_qty > 0 and order.state in {"new", "submitted", "replaced"}:
            had_order_drift = True
            order_drifts.append(
                OrderDrift(
                    intent_id=intent.intent_id,
                    broker_order_id=order.broker_order_id,
                    external_order_id=order.external_order_id,
                    expected_qty=intent.qty,
                    observed_filled_qty=observed_fill_qty,
                    observed_state=order.state,
                    reason="stale_order_state_vs_fill",
                )
            )

        if order.state == "filled" and observed_fill_qty < intent.qty:
            had_fill_drift = True
            fill_drifts.append(
                FillDrift(
                    broker_order_id=order.broker_order_id,
                    observed_fill_qty=observed_fill_qty,
                    reason="filled_state_without_enough_fills",
                )
            )

        if observed_fill_qty > intent.qty:
            had_fill_drift = True
            fill_drifts.append(
                FillDrift(
                    broker_order_id=order.broker_order_id,
                    observed_fill_qty=observed_fill_qty,
                    reason="fill_qty_exceeds_expected",
                )
            )

        if observed_fill_qty > 0 and order.state in {"partially_filled", "filled"} and observed_fill_qty <= intent.qty:
            fills_matched += 1
        if not had_order_drift and not had_fill_drift:
            orders_matched += 1

    for order in observed_orders:
        if order.intent_id not in expected_by_intent:
            order_drifts.append(
                OrderDrift(
                    intent_id=order.intent_id,
                    broker_order_id=order.broker_order_id,
                    external_order_id=order.external_order_id,
                    expected_qty=0,
                    observed_filled_qty=fill_qty_by_order_id.get(order.broker_order_id, 0),
                    observed_state=order.state,
                    reason="unexpected_broker_order",
                )
            )

    for fill in observed_fills:
        if fill.broker_order_id not in orders_by_broker_id:
            fill_drifts.append(
                FillDrift(
                    broker_order_id=fill.broker_order_id,
                    observed_fill_qty=fill.qty,
                    reason="orphan_fill_without_order",
                )
            )

    position_report = reconcile_position_snapshots(
        expected_positions,
        observed_positions,
        price_tolerance=price_tolerance,
    )
    incidents = [
        *position_report.incidents,
        *[_incident_for_order_drift(item) for item in order_drifts],
        *[_incident_for_fill_drift(item) for item in fill_drifts],
    ]
    return LiveExecutionReconciliationReport(
        orders_matched=orders_matched,
        fills_matched=fills_matched,
        order_drifts=order_drifts,
        fill_drifts=fill_drifts,
        position_report=position_report,
        incidents=incidents,
    )
