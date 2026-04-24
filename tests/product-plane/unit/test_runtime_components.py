from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import (
    DecisionCandidate,
    FeatureSnapshotRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.product_plane.runtime.config import StrategyRegistry, StrategyVersion
from trading_advisor_3000.product_plane.runtime.decision import SignalRuntimeEngine
from trading_advisor_3000.product_plane.runtime.publishing import TelegramPublicationEngine
from trading_advisor_3000.product_plane.runtime.publishing.telegram import TelegramApiError
from trading_advisor_3000.product_plane.runtime.signal_store import InMemorySignalStore


def _candidate(*, signal_id: str = "SIG-20260316-0001") -> DecisionCandidate:
    return DecisionCandidate(
        signal_id=signal_id,
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="ma-cross-v1",
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
            strategy_version_id="ma-cross-v1",
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

    registry.activate("ma-cross-v1", activated_from="2026-03-16T09:05:00Z")
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
            strategy_version_id="ma-cross-v1",
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


def test_telegram_publisher_uses_bot_api_transport_when_token_present() -> None:
    def _fake_api_call(method: str, params: dict[str, str] | None) -> dict[str, object]:
        if method == "sendMessage":
            signal_tag = str((params or {}).get("text", ""))
            message_id = 101 if "#1" in signal_tag else 202
            return {
                "http_status": 200,
                "ok": True,
                "error_code": None,
                "description": "",
                "result": {
                    "message_id": message_id,
                    "chat": {"id": -1003000},
                },
            }
        if method == "editMessageText":
            return {
                "http_status": 200,
                "ok": True,
                "error_code": None,
                "description": "",
                "result": {
                    "message_id": int((params or {}).get("message_id", "0")),
                    "chat": {"id": -1003000},
                },
            }
        if method == "deleteMessage":
            return {
                "http_status": 200,
                "ok": True,
                "error_code": None,
                "description": "",
                "result": True,
            }
        raise AssertionError(f"unexpected Telegram method: {method}")

    publisher = TelegramPublicationEngine(
        channel="@publication_real_channel",
        bot_token="token",
        api_call=_fake_api_call,
    )
    assert publisher.transport == "telegram-bot-api"

    first, first_created = publisher.publish(
        signal_id="SIG-PUBLICATION-001",
        rendered_message="F1-B publication contour probe #1",
        published_at="2026-03-31T12:00:00Z",
    )
    second, second_created = publisher.publish(
        signal_id="SIG-PUBLICATION-001",
        rendered_message="F1-B publication contour probe #1 duplicate",
        published_at="2026-03-31T12:00:01Z",
    )
    edited, edited_changed = publisher.edit(
        signal_id="SIG-PUBLICATION-001",
        rendered_message="F1-B publication contour probe #1 edited",
        edited_at="2026-03-31T12:00:02Z",
    )
    closed, closed_changed = publisher.close(
        signal_id="SIG-PUBLICATION-001",
        closed_at="2026-03-31T12:00:03Z",
    )
    _, second_signal_created = publisher.publish(
        signal_id="SIG-PUBLICATION-002",
        rendered_message="F1-B publication contour probe #2",
        published_at="2026-03-31T12:00:04Z",
    )
    canceled, canceled_changed = publisher.cancel(
        signal_id="SIG-PUBLICATION-002",
        canceled_at="2026-03-31T12:00:05Z",
    )

    assert first_created is True
    assert second_created is False
    assert edited_changed is True
    assert closed_changed is True
    assert second_signal_created is True
    assert canceled_changed is True
    assert first.message_id == second.message_id == edited.message_id == closed.message_id
    assert canceled.message_id == "202"

    receipts = [item.to_dict() for item in publisher.list_api_receipts()]
    methods = [item["method"] for item in receipts]
    assert methods == [
        "sendMessage",
        "editMessageText",
        "editMessageText",
        "sendMessage",
        "deleteMessage",
    ]
    assert all(item["ok"] is True for item in receipts)


def test_telegram_publisher_raises_when_bot_api_call_fails() -> None:
    def _failing_api_call(method: str, params: dict[str, str] | None) -> dict[str, object]:
        return {
            "http_status": 403,
            "ok": False,
            "error_code": 403,
            "description": "Forbidden: bot is not a member of the channel chat",
            "result": None,
        }

    publisher = TelegramPublicationEngine(
        channel="@publication_unreachable_channel",
        bot_token="token",
        api_call=_failing_api_call,
    )
    try:
        publisher.publish(
            signal_id="SIG-PUBLICATION-FAIL",
            rendered_message="F1-B publication contour probe failure",
            published_at="2026-03-31T12:01:00Z",
        )
        raise AssertionError("expected TelegramApiError")
    except TelegramApiError as exc:
        assert "sendMessage" in str(exc)
        assert exc.receipt.error_code == 403

