from __future__ import annotations

from trading_advisor_3000.app.contracts import (
    DecisionCandidate,
    FeatureSnapshotRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.app.interfaces.api import RuntimeAPI
from trading_advisor_3000.app.runtime import build_runtime_stack
from trading_advisor_3000.app.runtime.config import StrategyVersion


def _candidate(*, ts_decision: str = "2026-03-16T10:16:00Z") -> DecisionCandidate:
    return DecisionCandidate(
        signal_id="SIG-20260316-0001",
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="trend-follow-v1",
        mode=Mode.SHADOW,
        side=TradeSide.LONG,
        entry_ref=82.45,
        stop_ref=81.70,
        target_ref=83.95,
        confidence=0.77,
        ts_decision=ts_decision,
        feature_snapshot=FeatureSnapshotRef(
            dataset_version="bars-whitelist-v1",
            snapshot_id="FS-20260316-0001",
        ),
    )


def test_phase2c_runtime_replay_lifecycle_and_idempotent_publication() -> None:
    stack = build_runtime_stack(telegram_channel="@ta3000_signals")
    stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id="trend-follow-v1",
            status="active",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-16T09:00:00Z",
        )
    )
    api = RuntimeAPI(runtime_stack=stack)

    first = api.replay_candidates([_candidate()])
    assert first["replay_report"]["accepted"] == 1
    assert first["replay_report"]["published"] == 1
    assert len(first["active_signals"]) == 1
    assert len(first["publications"]) == 1
    active = first["active_signals"][0]
    assert active["entry_price"] == 82.45
    assert active["stop_price"] == 81.70
    assert active["target_price"] == 83.95
    assert active["expires_at"] is not None
    assert active["state"] == "active"

    second = api.replay_candidates([_candidate()])
    assert second["replay_report"]["accepted"] == 1
    assert second["replay_report"]["published"] == 0
    assert len(second["publications"]) == 1

    close_result = api.close_signal(
        signal_id="SIG-20260316-0001",
        closed_at="2026-03-16T10:30:00Z",
        reason_code="manual_close",
    )
    assert close_result["close_result"]["publication"]["status"] == "closed"
    assert close_result["active_signals"] == []

    event_types = {row["event_type"] for row in api.list_signal_events()}
    assert {"signal_opened", "signal_activated", "signal_closed"} <= event_types


def test_phase2c_runtime_supports_cancel_and_removes_active_signal() -> None:
    stack = build_runtime_stack(telegram_channel="@ta3000_signals")
    stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id="trend-follow-v1",
            status="active",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-16T09:00:00Z",
        )
    )
    api = RuntimeAPI(runtime_stack=stack)
    api.replay_candidates([_candidate()])
    canceled = api.cancel_signal(
        signal_id="SIG-20260316-0001",
        canceled_at="2026-03-16T10:20:00Z",
        reason_code="manual_cancel",
    )
    assert canceled["cancel_result"]["publication"]["status"] == "canceled"
    assert canceled["active_signals"] == []
