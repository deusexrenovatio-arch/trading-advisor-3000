from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


CONTEXT_KINDS = {"fundamentals", "news"}


@dataclass(frozen=True)
class ContextSlice:
    provider_id: str
    context_kind: str
    contract_id: str
    as_of_ts: str
    payload: dict[str, object]

    def __post_init__(self) -> None:
        if self.context_kind not in CONTEXT_KINDS:
            raise ValueError(f"unsupported context_kind: {self.context_kind}")
        if not isinstance(self.provider_id, str) or not self.provider_id.strip():
            raise ValueError("provider_id must be non-empty string")
        if not isinstance(self.contract_id, str) or not self.contract_id.strip():
            raise ValueError("contract_id must be non-empty string")
        if not isinstance(self.as_of_ts, str) or not self.as_of_ts.strip():
            raise ValueError("as_of_ts must be non-empty string")

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "context_kind": self.context_kind,
            "contract_id": self.contract_id,
            "as_of_ts": self.as_of_ts,
            "payload": self.payload,
        }


class ContextProvider(Protocol):
    provider_id: str

    def fetch(self, *, contract_id: str, as_of_ts: str) -> dict[str, object] | None:
        ...


class ContextProviderRegistry:
    def __init__(self) -> None:
        self._providers_by_kind: dict[str, dict[str, ContextProvider]] = {kind: {} for kind in CONTEXT_KINDS}

    def register(self, *, context_kind: str, provider: ContextProvider) -> None:
        if context_kind not in CONTEXT_KINDS:
            raise ValueError(f"unsupported context_kind: {context_kind}")
        provider_id = str(getattr(provider, "provider_id", "")).strip()
        if not provider_id:
            raise ValueError("provider must expose non-empty provider_id")
        kind_map = self._providers_by_kind[context_kind]
        if provider_id in kind_map:
            raise ValueError(f"provider already registered: context_kind={context_kind}, provider_id={provider_id}")
        kind_map[provider_id] = provider

    def list_provider_ids(self, *, context_kind: str) -> list[str]:
        if context_kind not in CONTEXT_KINDS:
            raise ValueError(f"unsupported context_kind: {context_kind}")
        return sorted(self._providers_by_kind[context_kind].keys())

    def fetch_slices(self, *, context_kind: str, contract_id: str, as_of_ts: str) -> list[ContextSlice]:
        if context_kind not in CONTEXT_KINDS:
            raise ValueError(f"unsupported context_kind: {context_kind}")
        slices: list[ContextSlice] = []
        for provider_id in self.list_provider_ids(context_kind=context_kind):
            provider = self._providers_by_kind[context_kind][provider_id]
            payload = provider.fetch(contract_id=contract_id, as_of_ts=as_of_ts)
            if payload is None:
                continue
            slices.append(
                ContextSlice(
                    provider_id=provider_id,
                    context_kind=context_kind,
                    contract_id=contract_id,
                    as_of_ts=as_of_ts,
                    payload=payload,
                )
            )
        return slices
