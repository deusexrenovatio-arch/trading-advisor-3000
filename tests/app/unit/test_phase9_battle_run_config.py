from __future__ import annotations

import pytest

from trading_advisor_3000.app.contracts import DecisionPublication, PublicationState, PublicationType, RuntimeSignal, SignalEvent
from trading_advisor_3000.app.runtime import build_phase9_battle_run_stack
from trading_advisor_3000.app.runtime.analytics import (
    build_phase9_battle_run_audit,
    export_phase9_battle_run_prometheus,
)
from trading_advisor_3000.app.runtime.config import evaluate_phase9_battle_run_preflight
from trading_advisor_3000.app.runtime.signal_store import PostgresSignalStore


def _base_env() -> dict[str, str]:
    return {
        "TA3000_RUNTIME_PROFILE": "phase9-battle-run",
        "TA3000_SIGNAL_STORE_BACKEND": "postgres",
        "TA3000_SIGNAL_STORE_SCHEMA": "signal",
        "TA3000_APP_DSN": "postgresql://postgres:postgres@127.0.0.1:5432/ta3000",
        "TA3000_TELEGRAM_TRANSPORT": "bot-api",
        "TA3000_TELEGRAM_BOT_TOKEN": "telegram-bot-token-001",
        "TA3000_TELEGRAM_SHADOW_CHANNEL": "@ta3000_shadow",
        "TA3000_TELEGRAM_ADVISORY_CHANNEL": "@ta3000_advisory",
        "TA3000_PROMETHEUS_BASE_URL": "http://127.0.0.1:9090",
        "TA3000_LOKI_BASE_URL": "http://127.0.0.1:3100",
        "TA3000_GRAFANA_DASHBOARD_URL": "http://127.0.0.1:3000/d/phase9",
    }


def _publication(*, signal_id: str, publication_type: PublicationType, status: PublicationState, published_at: str) -> DecisionPublication:
    return DecisionPublication(
        publication_id=f"PUB-{signal_id}-{publication_type.value}-{published_at}",
        signal_id=signal_id,
        channel="@ta3000_shadow",
        message_id=f"tg-{signal_id}",
        publication_type=publication_type,
        status=status,
        published_at=published_at,
    )


def _event(*, signal_id: str, event_type: str, event_ts: str) -> SignalEvent:
    return SignalEvent(
        event_id=f"SEVT-{signal_id}-{event_type}-{event_ts}",
        signal_id=signal_id,
        event_ts=event_ts,
        event_type=event_type,
        reason_code=event_type,
        payload_json={"signal_id": signal_id},
    )


def test_phase9_battle_run_preflight_is_ready_with_explicit_external_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PostgresSignalStore, "list_publication_events", lambda self: [])
    report = evaluate_phase9_battle_run_preflight(_base_env())
    stack = build_phase9_battle_run_stack(env=_base_env())

    assert report.is_ready is True
    assert report.config.signal_store_backend == "postgres"
    assert report.config.telegram_shadow_channel == "@ta3000_shadow"
    assert type(stack.signal_store).__name__ == "PostgresSignalStore"


def test_phase9_battle_run_preflight_is_fail_closed_for_wrong_backend_and_missing_secret() -> None:
    env = _base_env()
    env["TA3000_SIGNAL_STORE_BACKEND"] = "memory"
    del env["TA3000_TELEGRAM_BOT_TOKEN"]

    report = evaluate_phase9_battle_run_preflight(env)

    assert report.is_ready is False
    assert "TA3000_SIGNAL_STORE_BACKEND must stay postgres for Phase 9 battle-run mode" in report.invalid_config
    assert report.secrets_policy["missing_secret_names"] == ["TA3000_TELEGRAM_BOT_TOKEN"]


def test_phase9_battle_run_preflight_is_fail_closed_for_non_real_telegram_transport() -> None:
    env = _base_env()
    env["TA3000_TELEGRAM_TRANSPORT"] = "stub"

    report = evaluate_phase9_battle_run_preflight(env)

    assert report.is_ready is False
    assert (
        "TA3000_TELEGRAM_TRANSPORT must be bot-api for real Telegram Phase 9 battle-run closure"
        in report.invalid_config
    )


def test_phase9_battle_run_audit_exports_expected_observability_counts() -> None:
    audit = build_phase9_battle_run_audit(
        publication_events=[
            _publication(
                signal_id="SIG-1",
                publication_type=PublicationType.CREATE,
                status=PublicationState.PUBLISHED,
                published_at="2026-03-22T07:00:00Z",
            ),
            _publication(
                signal_id="SIG-1",
                publication_type=PublicationType.EDIT,
                status=PublicationState.PUBLISHED,
                published_at="2026-03-22T07:01:00Z",
            ),
            _publication(
                signal_id="SIG-1",
                publication_type=PublicationType.CLOSE,
                status=PublicationState.CLOSED,
                published_at="2026-03-22T07:02:00Z",
            ),
            _publication(
                signal_id="SIG-2",
                publication_type=PublicationType.CANCEL,
                status=PublicationState.CANCELED,
                published_at="2026-03-22T07:03:00Z",
            ),
        ],
        signal_events=[
            _event(signal_id="SIG-1", event_type="signal_opened", event_ts="2026-03-22T07:00:00Z"),
            _event(signal_id="SIG-1", event_type="signal_closed", event_ts="2026-03-22T07:02:00Z"),
        ],
        active_signals=[],
        restart_published_delta=0,
        preflight_ready=True,
        warnings=[],
        observability_targets={"prometheus_base_url": "http://127.0.0.1:9090"},
    )
    metrics = export_phase9_battle_run_prometheus(audit)

    assert audit["status"] == "ok"
    assert audit["publication_type_counts"] == {"cancel": 1, "close": 1, "create": 1, "edit": 1}
    assert "ta3000_phase9_battle_run_ready 1" in metrics
    assert 'ta3000_phase9_telegram_publications_total{publication_type="edit"} 1' in metrics
    assert "ta3000_phase9_restart_published_delta 0" in metrics
