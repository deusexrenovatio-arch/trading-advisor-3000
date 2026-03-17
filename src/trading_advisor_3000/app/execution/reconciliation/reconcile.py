from __future__ import annotations

import hashlib
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import PositionSnapshot


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
class ReconciliationReport:
    matched: int
    missing: list[PositionDrift]
    unexpected: list[PositionDrift]
    mismatched: list[PositionDrift]
    incidents: list["ReconciliationIncident"]

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


def _incident_id(*, position_key: str, reason: str) -> str:
    return "INC-" + hashlib.sha256(f"{position_key}|{reason}".encode("utf-8")).hexdigest()[:12].upper()


def _incident_from_drift(drift: PositionDrift) -> ReconciliationIncident:
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


def _position_key(position: PositionSnapshot) -> str:
    return position.position_key


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

    incidents = [
        _incident_from_drift(item)
        for item in [*missing, *unexpected, *mismatched]
    ]
    return ReconciliationReport(
        matched=matched,
        missing=missing,
        unexpected=unexpected,
        mismatched=mismatched,
        incidents=incidents,
    )
