from __future__ import annotations

import pytest

from trading_advisor_3000.app.data_plane import (
    DataProviderRegistry,
    DataProviderSpec,
    default_data_provider_registry,
)
from trading_advisor_3000.app.runtime.context import ContextProviderRegistry


class _StaticContextProvider:
    def __init__(self, provider_id: str, payload: dict[str, object] | None) -> None:
        self.provider_id = provider_id
        self._payload = payload

    def fetch(self, *, contract_id: str, as_of_ts: str) -> dict[str, object] | None:
        if self._payload is None:
            return None
        return {
            "contract_id": contract_id,
            "as_of_ts": as_of_ts,
            **self._payload,
        }


def test_phase7_default_data_provider_registry_includes_market_fundamentals_news_paths() -> None:
    registry = default_data_provider_registry()

    market = registry.list_providers(provider_kind="market")
    fundamentals = registry.list_providers(provider_kind="fundamentals")
    news = registry.list_providers(provider_kind="news")

    assert len(market) == 1
    assert len(fundamentals) == 1
    assert len(news) == 1
    assert market[0].provider_id == "market-bars-default"
    assert fundamentals[0].provider_id == "fundamentals-default"
    assert news[0].provider_id == "news-default"


def test_phase7_data_provider_registry_rejects_duplicate_provider_id() -> None:
    registry = DataProviderRegistry()
    provider = DataProviderSpec(
        provider_id="provider-a",
        provider_kind="news",
        asset_classes=("futures",),
        supports_incremental=True,
        supports_replay=True,
        latency_profile="event-driven",
    )
    registry.register(provider)

    with pytest.raises(ValueError):
        registry.register(provider)


def test_phase7_context_provider_registry_aggregates_fundamentals_and_news_slices() -> None:
    registry = ContextProviderRegistry()
    registry.register(
        context_kind="fundamentals",
        provider=_StaticContextProvider(
            "fund-provider",
            {"pe_ratio": 12.4},
        ),
    )
    registry.register(
        context_kind="news",
        provider=_StaticContextProvider(
            "news-provider",
            {"headline": "inventory update"},
        ),
    )
    registry.register(
        context_kind="news",
        provider=_StaticContextProvider("news-empty", None),
    )

    fundamentals = registry.fetch_slices(
        context_kind="fundamentals",
        contract_id="BR-6.26",
        as_of_ts="2026-03-18T19:10:00Z",
    )
    news = registry.fetch_slices(
        context_kind="news",
        contract_id="BR-6.26",
        as_of_ts="2026-03-18T19:10:00Z",
    )

    assert len(fundamentals) == 1
    assert fundamentals[0].payload["pe_ratio"] == 12.4
    assert len(news) == 1
    assert news[0].payload["headline"] == "inventory update"
