from __future__ import annotations

from trading_advisor_3000.app.contracts import (
    DecisionCandidate,
    FeatureSnapshotRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.app.runtime.config import StrategyRegistry, StrategyVersion
from trading_advisor_3000.app.runtime.publishing import TelegramPublicationEngine


def _candidate(*, signal_id: str = "SIG-20260316-0001") -> DecisionCandidate:
    return DecisionCandidate(
        signal_id=signal_id,
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="trend-follow-v1",
        mode=Mode.SHADOW,
        side=TradeSide.LONG,
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
