from __future__ import annotations

from trading_advisor_3000.app.contracts import (
    DecisionCandidate,
    FeatureSnapshotRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.app.runtime.config import StrategyRegistry, StrategyVersion
from trading_advisor_3000.app.runtime.decision import SignalRuntimeEngine
from trading_advisor_3000.app.runtime.publishing import TelegramPublicationEngine
from trading_advisor_3000.app.runtime.signal_store import InMemorySignalStore


def _candidate(*, signal_id: str = "SIG-20260316-0001") -> DecisionCandidate:
    return DecisionCandidate(
        signal_id=signal_id,
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="trend-follow-v1",
        mode=Mode.SHADOW,
        side=TradeSide.LONG,
        entry_ref=82.45,
        stop_ref=81.70,
        target_ref=83.95,
        confidence=0.72,
        ts_decision="2026-03-16T10:16:00Z",
        feature_snapshot=FeatureSnapshotRef(
            dataset_version="bars-whitelist-v1",
            snapshot_id="FS-20260316-0001",
        ),
    )


def test_strategy_registry_requires_active_state() -> None:
    registry = StrategyRegistry()
    registry.register(
        StrategyVersion(
            strategy_version_id="trend-follow-v1",
            status="draft",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-16T09:00:00Z",
        )
    )
    is_allowed, reason = registry.allows(_candidate())
    assert not is_allowed
    assert reason == "strategy_not_active"

    registry.activate("trend-follow-v1", activated_from="2026-03-16T09:05:00Z")
    is_allowed, reason = registry.allows(_candidate())
    assert is_allowed
    assert reason == "ok"


def test_telegram_publisher_is_idempotent_on_create() -> None:
    publisher = TelegramPublicationEngine(channel="@ta3000_signals")
    first, created_first = publisher.publish(
        signal_id="SIG-20260316-0001",
        rendered_message="long BR",
        published_at="2026-03-16T10:16:00Z",
    )
    second, created_second = publisher.publish(
        signal_id="SIG-20260316-0001",
        rendered_message="long BR",
        published_at="2026-03-16T10:17:00Z",
    )

    assert created_first is True
    assert created_second is False
    assert first.message_id == second.message_id


def test_telegram_publisher_supports_cancel_lifecycle() -> None:
    publisher = TelegramPublicationEngine(channel="@ta3000_signals")
    created, _ = publisher.publish(
        signal_id="SIG-20260316-0002",
        rendered_message="long BR",
        published_at="2026-03-16T10:16:00Z",
    )
    canceled, is_new = publisher.cancel(
        signal_id="SIG-20260316-0002",
        canceled_at="2026-03-16T10:18:00Z",
    )
    assert is_new is True
    assert canceled.status.value == "canceled"
    assert canceled.publication_type.value == "cancel"
    assert canceled.message_id == created.message_id


def test_runtime_engine_enforces_cooldown_and_blackout() -> None:
    registry = StrategyRegistry()
    registry.register(
        StrategyVersion(
            strategy_version_id="trend-follow-v1",
            status="active",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-16T09:00:00Z",
        )
    )
    store = InMemorySignalStore()
    publisher = TelegramPublicationEngine(channel="@ta3000_signals")
    engine = SignalRuntimeEngine(
        strategy_registry=registry,
        signal_store=store,
        publisher=publisher,
        cooldown_seconds=120,
        blackout_windows_by_contract={"BR-6.26": [("2026-03-16T10:20:00Z", "2026-03-16T10:30:00Z")]},
    )

    first = _candidate(signal_id="SIG-20260316-0003")
    second = DecisionCandidate.from_dict(
        {
            **first.to_dict(),
            "signal_id": "SIG-20260316-0004",
            "ts_decision": "2026-03-16T10:17:00Z",
        }
    )
    blackout = DecisionCandidate.from_dict(
        {
            **first.to_dict(),
            "signal_id": "SIG-20260316-0005",
            "ts_decision": "2026-03-16T10:25:00Z",
        }
    )
    report = engine.replay_candidates([first, second, blackout])
    assert report["accepted"] == 1
    assert report["rejected"] == 2
    assert report["rejection_reasons"]["cooldown_active"] == 1
    assert report["rejection_reasons"]["blackout_window"] == 1
