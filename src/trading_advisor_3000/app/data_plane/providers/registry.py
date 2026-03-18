from __future__ import annotations

from dataclasses import dataclass


PROVIDER_KINDS = {"market", "fundamentals", "news"}


@dataclass(frozen=True)
class DataProviderSpec:
    provider_id: str
    provider_kind: str
    asset_classes: tuple[str, ...]
    supports_incremental: bool
    supports_replay: bool
    latency_profile: str
    description: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.provider_id, str) or not self.provider_id.strip():
            raise ValueError("provider_id must be non-empty string")
        if self.provider_kind not in PROVIDER_KINDS:
            raise ValueError(f"unsupported provider_kind: {self.provider_kind}")
        if not self.asset_classes:
            raise ValueError("asset_classes must be non-empty")

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "provider_kind": self.provider_kind,
            "asset_classes": list(self.asset_classes),
            "supports_incremental": self.supports_incremental,
            "supports_replay": self.supports_replay,
            "latency_profile": self.latency_profile,
            "description": self.description,
        }


class DataProviderRegistry:
    def __init__(self, *, providers: list[DataProviderSpec] | None = None) -> None:
        self._providers_by_id: dict[str, DataProviderSpec] = {}
        for provider in providers or []:
            self.register(provider)

    def register(self, provider: DataProviderSpec) -> None:
        if provider.provider_id in self._providers_by_id:
            raise ValueError(f"provider already registered: {provider.provider_id}")
        self._providers_by_id[provider.provider_id] = provider

    def get(self, provider_id: str) -> DataProviderSpec | None:
        return self._providers_by_id.get(provider_id)

    def list_providers(self, *, provider_kind: str | None = None) -> list[DataProviderSpec]:
        providers = list(self._providers_by_id.values())
        if provider_kind is not None:
            if provider_kind not in PROVIDER_KINDS:
                raise ValueError(f"unsupported provider_kind: {provider_kind}")
            providers = [item for item in providers if item.provider_kind == provider_kind]
        return sorted(providers, key=lambda item: item.provider_id)

    def to_dict(self) -> list[dict[str, object]]:
        return [item.to_dict() for item in self.list_providers()]


def default_data_provider_registry() -> DataProviderRegistry:
    return DataProviderRegistry(
        providers=[
            DataProviderSpec(
                provider_id="market-bars-default",
                provider_kind="market",
                asset_classes=("futures",),
                supports_incremental=True,
                supports_replay=True,
                latency_profile="intraday",
                description="default market bars provider seam",
            ),
            DataProviderSpec(
                provider_id="fundamentals-default",
                provider_kind="fundamentals",
                asset_classes=("equity", "futures"),
                supports_incremental=False,
                supports_replay=True,
                latency_profile="daily",
                description="placeholder fundamentals provider seam for scale-up wave",
            ),
            DataProviderSpec(
                provider_id="news-default",
                provider_kind="news",
                asset_classes=("equity", "futures", "fx"),
                supports_incremental=True,
                supports_replay=True,
                latency_profile="event-driven",
                description="placeholder news provider seam for scale-up wave",
            ),
        ]
    )
