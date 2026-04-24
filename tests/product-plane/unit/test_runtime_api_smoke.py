from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from trading_advisor_3000.product_plane.interfaces.asgi import create_app
from trading_advisor_3000.product_plane.runtime import RuntimeConfigurationError
from trading_advisor_3000.product_plane.runtime.signal_store import PostgresSignalStore


def _staging_env_with_postgres() -> dict[str, str]:
    return {
        "TA3000_RUNTIME_PROFILE": "staging",
        "TA3000_SIGNAL_STORE_BACKEND": "postgres",
        "TA3000_APP_DSN": "postgresql://postgres:postgres@127.0.0.1:5432/ta3000",
        "TA3000_TELEGRAM_CHANNEL": "@ta3000_runtime",
    }


def test_runtime_api_smoke_fastapi_smoke_boots_with_durable_runtime_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PostgresSignalStore, "list_publication_events", lambda self: [])

    with TestClient(create_app(env=_staging_env_with_postgres())) as client:
        health_response = client.get("/health")
        assert health_response.status_code == 200
        health_payload = health_response.json()
        assert health_payload["status"] == "ok"
        assert health_payload["profile"] == "staging"
        assert health_payload["signal_store_backend"] == "postgres"
        assert health_payload["durable_runtime_required"] is True
        assert health_payload["postgres_dsn_configured"] is True

        ready_response = client.get("/ready")
        assert ready_response.status_code == 200
        ready_payload = ready_response.json()
        assert ready_payload["ready"] is True


def test_runtime_api_smoke_fastapi_boot_fails_closed_without_postgres_dsn_on_staging() -> None:
    app = create_app(
        env={
            "TA3000_RUNTIME_PROFILE": "staging",
            "TA3000_SIGNAL_STORE_BACKEND": "postgres",
        }
    )

    with pytest.raises(RuntimeConfigurationError):
        with TestClient(app):
            pass
