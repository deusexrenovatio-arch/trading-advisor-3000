from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from trading_advisor_3000.app.execution.reconciliation import LiveExecutionReconciliationReport


@dataclass(frozen=True)
class RecoveryAction:
    step: int
    source: str
    trigger_reason: str
    severity: str
    title: str
    operation: str
    blocking: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "step": self.step,
            "source": self.source,
            "trigger_reason": self.trigger_reason,
            "severity": self.severity,
            "title": self.title,
            "operation": self.operation,
            "blocking": self.blocking,
        }


@dataclass(frozen=True)
class LiveRecoveryPlan:
    generated_at: str
    incidents_total: int
    high_severity_incidents: int
    escalation_required: bool
    uncovered_reasons: list[str]
    actions: list[RecoveryAction]

    def to_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "incidents_total": self.incidents_total,
            "high_severity_incidents": self.high_severity_incidents,
            "escalation_required": self.escalation_required,
            "uncovered_reasons": list(self.uncovered_reasons),
            "actions": [item.to_dict() for item in self.actions],
        }


_REASON_ACTIONS: dict[str, tuple[str, str, bool]] = {
    "missing_broker_order_ack": (
        "Replay broker order index",
        "Pause new submissions and rebuild order ack map from broker event log.",
        True,
    ),
    "stale_order_state_vs_fill": (
        "Resync order state from broker snapshots",
        "Refresh order states and re-apply fills before reconciliation.",
        True,
    ),
    "filled_state_without_enough_fills": (
        "Rebuild fill ledger",
        "Re-ingest fills for affected orders and recompute filled quantities.",
        True,
    ),
    "orphan_fill_without_order": (
        "Quarantine unmapped fills",
        "Quarantine orphan fills, rebuild order mapping, and replay fill ingestion.",
        True,
    ),
    "fill_qty_exceeds_expected": (
        "Halt position updates and rebuild fill quantities",
        "Stop applying incremental fills, replay broker fills, and validate order quantity caps.",
        True,
    ),
    "quantity_mismatch": (
        "Recompute positions from fills",
        "Replay fills and recompute positions for mismatched contracts.",
        True,
    ),
    "avg_price_mismatch": (
        "Reprice position from fill tape",
        "Recalculate average price from ordered fills for affected position keys.",
        False,
    ),
    "missing_position": (
        "Restore missing positions",
        "Rebuild local positions from broker snapshots and replayed fills.",
        True,
    ),
    "unexpected_position": (
        "Manual verification for unexpected positions",
        "Freeze execution on affected contract and confirm broker truth source.",
        True,
    ),
    "broker_fill_for_unknown_order": (
        "Re-map fill to known order",
        "Validate broker order IDs and replay fill ingestion with corrected mapping.",
        True,
    ),
    "invalid_broker_fill_payload": (
        "Reject malformed fill and request broker replay",
        "Reject malformed payload, then request canonical replay from sidecar.",
        False,
    ),
    "missing_external_order_id": (
        "Rebuild external order mapping",
        "Rehydrate external_order_id mapping from broker snapshots before processing new fills.",
        True,
    ),
    "unexpected_broker_order": (
        "Quarantine unexpected broker order",
        "Freeze impacted intent and investigate whether order came from out-of-band workflow.",
        True,
    ),
    "fill_without_registered_intent": (
        "Backfill missing intent registration",
        "Restore missing intent registration from broker/order history and replay fill ingestion.",
        True,
    ),
    "broker_update_for_unknown_order": (
        "Re-index broker updates",
        "Rebuild order index and re-apply deferred broker updates for unknown orders.",
        True,
    ),
    "broker_update_unsupported_state": (
        "Normalize unsupported broker state",
        "Map broker-specific state to canonical state and replay update ingestion.",
        False,
    ),
    "missing_external_order_id_in_ack": (
        "Repair submission ack payload",
        "Replay submission acknowledgment with external_order_id and verify order map consistency.",
        True,
    ),
    "replace_for_unknown_intent": (
        "Reconcile replace against canonical intent",
        "Resolve replace event to a valid intent and replay replace processing.",
        True,
    ),
    "invalid_replace_payload": (
        "Reject malformed replace payload",
        "Reject invalid replace payload and request sidecar replay with validated schema.",
        False,
    ),
    "unsupported_ack_state": (
        "Map unsupported ack state",
        "Normalize unsupported ack state to canonical state and verify transition consistency.",
        False,
    ),
    "ack_for_unknown_intent": (
        "Recover unknown intent before ack apply",
        "Restore missing intent in sync-state and replay submission ack processing.",
        True,
    ),
    "intent_id_payload_conflict": (
        "Resolve intent payload conflict",
        "Block conflicting intent ID until payload conflict is manually resolved.",
        True,
    ),
    "broker_fill_missing_fill_id": (
        "Reject fill row without identifier",
        "Drop malformed fill row and request broker replay with valid fill_id.",
        False,
    ),
    "broker_fill_missing_external_order_id": (
        "Repair fill order linkage",
        "Re-map fill rows to canonical external_order_id before ingesting positions.",
        True,
    ),
    "broker_fill_missing_fill_ts": (
        "Recover missing fill timestamp",
        "Request replay for fills missing fill_ts and quarantine incomplete rows.",
        False,
    ),
    "broker_update_missing_external_order_id": (
        "Reject update without external order id",
        "Reject malformed broker update and request corrected replay payload.",
        False,
    ),
    "filled_qty_exceeds_intent_qty": (
        "Freeze contract and validate overfill",
        "Freeze affected contract, reconcile overfill at broker, and recover intent/order ledger.",
        True,
    ),
}

_CRITICAL_REASONS = {
    "missing_broker_order_ack",
    "orphan_fill_without_order",
    "fill_qty_exceeds_expected",
    "filled_qty_exceeds_intent_qty",
    "missing_position",
    "quantity_mismatch",
    "unexpected_position",
    "unexpected_broker_order",
    "intent_id_payload_conflict",
}


def _incident_reason(item: object) -> str:
    return str(getattr(item, "reason", "")).strip()


def _incident_severity(item: object) -> str:
    severity = str(getattr(item, "severity", "")).strip().lower()
    if severity in {"high", "medium", "low"}:
        return severity
    return "medium"


def _severity_rank(value: str) -> int:
    if value == "high":
        return 0
    if value == "medium":
        return 1
    return 2


def _add_action(
    actions: list[RecoveryAction],
    *,
    source: str,
    trigger_reason: str,
    severity: str,
    title: str,
    operation: str,
    blocking: bool,
) -> None:
    actions.append(
        RecoveryAction(
            step=len(actions) + 1,
            source=source,
            trigger_reason=trigger_reason,
            severity=severity,
            title=title,
            operation=operation,
            blocking=blocking,
        )
    )


def build_live_recovery_plan(
    *,
    reconciliation_report: LiveExecutionReconciliationReport,
    sync_incidents: list[object],
) -> LiveRecoveryPlan:
    labeled_incidents = [
        *[("reconciliation", item) for item in reconciliation_report.incidents],
        *[("sync", item) for item in sync_incidents],
    ]
    if not labeled_incidents:
        return LiveRecoveryPlan(
            generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            incidents_total=0,
            high_severity_incidents=0,
            escalation_required=False,
            uncovered_reasons=[],
            actions=[],
        )

    reason_counts: dict[str, int] = {}
    reason_severity: dict[str, str] = {}
    source_by_reason: dict[str, str] = {}
    high_incidents = 0
    for source, incident in labeled_incidents:
        reason = _incident_reason(incident) or "unknown_incident"
        severity = _incident_severity(incident)
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        previous = reason_severity.get(reason)
        if previous is None or _severity_rank(severity) < _severity_rank(previous):
            reason_severity[reason] = severity
            source_by_reason[reason] = source
        if severity == "high":
            high_incidents += 1

    actions: list[RecoveryAction] = []
    if high_incidents:
        _add_action(
            actions,
            source="global",
            trigger_reason="high_severity_incident",
            severity="high",
            title="Freeze new live submissions",
            operation="Stop new intents until recovery plan is executed and reconciliation is clean.",
            blocking=True,
        )

    uncovered: list[str] = []
    ordered_reasons = sorted(
        reason_counts.keys(),
        key=lambda item: (_severity_rank(reason_severity[item]), -reason_counts[item], item),
    )
    for reason in ordered_reasons:
        mapped = _REASON_ACTIONS.get(reason)
        if mapped is None:
            uncovered.append(reason)
            _add_action(
                actions,
                source=source_by_reason[reason],
                trigger_reason=reason,
                severity=reason_severity[reason],
                title="Manual incident triage",
                operation="Capture evidence, classify incident, and define deterministic replay procedure.",
                blocking=reason_severity[reason] == "high",
            )
            continue
        title, operation, blocking = mapped
        _add_action(
            actions,
            source=source_by_reason[reason],
            trigger_reason=reason,
            severity=reason_severity[reason],
            title=title,
            operation=operation,
            blocking=blocking,
        )

    _add_action(
        actions,
        source="global",
        trigger_reason="post_recovery_validation",
        severity="medium",
        title="Re-run sync and reconciliation",
        operation="Poll broker streams, reconcile orders/fills/positions, and confirm clean report.",
        blocking=True,
    )

    escalation_required = bool(uncovered) or any(
        reason in _CRITICAL_REASONS and reason_counts.get(reason, 0) > 0 for reason in reason_counts
    )
    return LiveRecoveryPlan(
        generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        incidents_total=len(labeled_incidents),
        high_severity_incidents=high_incidents,
        escalation_required=escalation_required,
        uncovered_reasons=uncovered,
        actions=actions,
    )
