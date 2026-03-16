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
    assert {"signal_opened", "signal_published", "signal_closed"} <= event_types
