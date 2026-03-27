from __future__ import annotations

import pytest

from trading_advisor_3000.app.runtime import (
    RuntimeConfigurationError,
    build_runtime_stack_from_env,
)
from trading_advisor_3000.app.runtime.signal_store import InMemorySignalStore, PostgresSignalStore


def test_phase6_staging_profile_requires_postgres_and_has_no_inmemory_fallback() -> None:
    with pytest.raises(RuntimeConfigurationError):
        build_runtime_stack_from_env(
            {
                "TA3000_RUNTIME_PROFILE": "staging",
            }
        )

    with pytest.raises(RuntimeConfigurationError):
        build_runtime_stack_from_env(
            {
                "TA3000_RUNTIME_PROFILE": "staging",
                "TA3000_SIGNAL_STORE_BACKEND": "inmemory",
            }
        )


def test_phase6_default_profile_is_durable_and_fails_closed_without_postgres_dsn() -> None:
    with pytest.raises(RuntimeConfigurationError):
        build_runtime_stack_from_env(
            {
                "TA3000_TELEGRAM_CHANNEL": "@ta3000_runtime",
            }
        )


def test_phase6_production_profile_requires_postgres_and_has_no_inmemory_fallback() -> None:
    with pytest.raises(RuntimeConfigurationError):
        build_runtime_stack_from_env(
            {
                "TA3000_RUNTIME_PROFILE": "production",
            }
        )

    with pytest.raises(RuntimeConfigurationError):
        build_runtime_stack_from_env(
            {
                "TA3000_RUNTIME_PROFILE": "production",
                "TA3000_SIGNAL_STORE_BACKEND": "inmemory",
            }
        )


def test_phase6_staging_profile_bootstraps_postgres_store_when_dsn_is_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PostgresSignalStore, "list_publication_events", lambda self: [])

    result = build_runtime_stack_from_env(
        {
            "TA3000_RUNTIME_PROFILE": "staging",
            "TA3000_SIGNAL_STORE_BACKEND": "postgres",
            "TA3000_APP_DSN": "postgresql://postgres:postgres@127.0.0.1:5432/ta3000",
            "TA3000_TELEGRAM_CHANNEL": "@ta3000_runtime",
        }
    )

    assert result.config.durable_runtime_required is True
    assert result.config.signal_store_backend == "postgres"
    assert isinstance(result.runtime_stack.signal_store, PostgresSignalStore)


def test_phase6_production_profile_bootstraps_postgres_store_when_dsn_is_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(PostgresSignalStore, "list_publication_events", lambda self: [])

    result = build_runtime_stack_from_env(
        {
            "TA3000_RUNTIME_PROFILE": "production",
            "TA3000_SIGNAL_STORE_BACKEND": "postgres",
            "TA3000_APP_DSN": "postgresql://postgres:postgres@127.0.0.1:5432/ta3000",
            "TA3000_TELEGRAM_CHANNEL": "@ta3000_runtime",
        }
    )

    assert result.config.profile == "production"
    assert result.config.durable_runtime_required is True
    assert result.config.signal_store_backend == "postgres"
    assert isinstance(result.runtime_stack.signal_store, PostgresSignalStore)


def test_phase6_test_profile_can_use_inmemory_store_without_affecting_staging_policy() -> None:
    result = build_runtime_stack_from_env(
        {
            "TA3000_RUNTIME_PROFILE": "test",
            "TA3000_TELEGRAM_CHANNEL": "@ta3000_runtime",
        }
    )

    assert result.config.durable_runtime_required is False
    assert result.config.signal_store_backend == "inmemory"
    assert isinstance(result.runtime_stack.signal_store, InMemorySignalStore)
