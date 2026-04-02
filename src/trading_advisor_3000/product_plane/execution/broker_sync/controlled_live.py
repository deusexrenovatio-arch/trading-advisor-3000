from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.product_plane.contracts import OrderIntent, PositionSnapshot
from trading_advisor_3000.product_plane.execution.adapters import LiveExecutionBridge
from trading_advisor_3000.product_plane.execution.recovery import LiveRecoveryPlan, build_live_recovery_plan
from trading_advisor_3000.product_plane.execution.reconciliation import (
    LiveExecutionReconciliationReport,
    reconcile_live_execution,
)

from .live_sync import BrokerSyncEngine


@dataclass(frozen=True)
class ControlledLiveCycleReport:
    submitted_intents: int
    synced_updates: int
    synced_fills: int
    reconciliation: LiveExecutionReconciliationReport

    @property
    def incidents(self) -> int:
        return len(self.reconciliation.incidents)

    def to_dict(self) -> dict[str, object]:
        return {
            "submitted_intents": self.submitted_intents,
            "synced_updates": self.synced_updates,
            "synced_fills": self.synced_fills,
            "incidents": self.incidents,
            "reconciliation": self.reconciliation.to_dict(),
        }


@dataclass(frozen=True)
class ControlledLiveHardeningReport:
    cycle: ControlledLiveCycleReport
    recovery_plan: LiveRecoveryPlan

    def to_dict(self) -> dict[str, object]:
        return {
            "cycle": self.cycle.to_dict(),
            "recovery_plan": self.recovery_plan.to_dict(),
        }


class ControlledLiveExecutionEngine:
    def __init__(self, *, bridge: LiveExecutionBridge, sync_engine: BrokerSyncEngine) -> None:
        self._bridge = bridge
        self._sync_engine = sync_engine
        self._reconciliation_cycles_total = 0
        self._reconciliation_incidents_total = 0

    def _record_reconciliation(self, *, incidents: int) -> None:
        self._reconciliation_cycles_total += 1
        self._reconciliation_incidents_total += max(0, incidents)

    def observability_snapshot(self) -> dict[str, object]:
        cycles = self._reconciliation_cycles_total
        incident_rate = (
            round(self._reconciliation_incidents_total / cycles, 6)
            if cycles > 0
            else 0.0
        )
        return {
            "reconciliation_cycles_total": cycles,
            "reconciliation_incidents_total": self._reconciliation_incidents_total,
            "reconciliation_incident_rate": incident_rate,
            "bridge": self._bridge.health().get("execution_telemetry", {}),
        }

    def submit_intents(self, intents: list[OrderIntent], *, submitted_at: str) -> list[dict[str, object]]:
        acks: list[dict[str, object]] = []
        for intent in intents:
            existing_ack = self._sync_engine.build_submission_ack_snapshot(intent.intent_id)
            if existing_ack is not None:
                self._sync_engine.register_intent(intent)
                acks.append(existing_ack)
                continue

            ack = self._bridge.submit_order_intent(intent, accepted_at=submitted_at)
            self._sync_engine.register_intent(intent)
            self._sync_engine.record_submission_ack(
                intent_id=intent.intent_id,
                ack=ack,
                acknowledged_at=submitted_at,
            )
            acks.append({**ack, "idempotent_reuse": False})
        return acks

    def replace_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        return self._bridge.replace_order_intent(
            intent_id=intent_id,
            new_qty=new_qty,
            new_price=new_price,
            replaced_at=replaced_at,
        )

    def cancel_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        return self._bridge.cancel_order_intent(intent_id=intent_id, canceled_at=canceled_at)

    def poll_broker_sync(self) -> dict[str, int]:
        streams = self._bridge.drain_broker_streams()
        updates = streams["updates"]
        fills = streams["fills"]
        self._sync_engine.ingest_broker_updates(updates)
        self._sync_engine.ingest_broker_fills(fills)
        return {"updates": len(updates), "fills": len(fills)}

    def reconcile(self, *, expected_positions: list[PositionSnapshot]) -> LiveExecutionReconciliationReport:
        report = reconcile_live_execution(
            expected_intents=self._sync_engine.list_registered_intents(),
            observed_orders=self._sync_engine.list_broker_orders(),
            observed_fills=self._sync_engine.list_broker_fills(),
            expected_positions=expected_positions,
            observed_positions=self._sync_engine.list_positions(),
        )
        self._record_reconciliation(incidents=len(report.incidents))
        return report

    def run_controlled_cycle(
        self,
        *,
        intents: list[OrderIntent],
        submitted_at: str,
        expected_positions: list[PositionSnapshot],
    ) -> ControlledLiveCycleReport:
        self.submit_intents(intents, submitted_at=submitted_at)
        sync_stats = self.poll_broker_sync()
        reconciliation = self.reconcile(expected_positions=expected_positions)
        return ControlledLiveCycleReport(
            submitted_intents=len(intents),
            synced_updates=sync_stats["updates"],
            synced_fills=sync_stats["fills"],
            reconciliation=reconciliation,
        )

    def build_recovery_plan(self, *, expected_positions: list[PositionSnapshot]) -> LiveRecoveryPlan:
        reconciliation = self.reconcile(expected_positions=expected_positions)
        return build_live_recovery_plan(
            reconciliation_report=reconciliation,
            sync_incidents=self._sync_engine.list_incidents(),
        )

    def run_hardened_cycle(
        self,
        *,
        intents: list[OrderIntent],
        submitted_at: str,
        expected_positions: list[PositionSnapshot],
    ) -> ControlledLiveHardeningReport:
        cycle = self.run_controlled_cycle(
            intents=intents,
            submitted_at=submitted_at,
            expected_positions=expected_positions,
        )
        recovery_plan = build_live_recovery_plan(
            reconciliation_report=cycle.reconciliation,
            sync_incidents=self._sync_engine.list_incidents(),
        )
        return ControlledLiveHardeningReport(
            cycle=cycle,
            recovery_plan=recovery_plan,
        )
