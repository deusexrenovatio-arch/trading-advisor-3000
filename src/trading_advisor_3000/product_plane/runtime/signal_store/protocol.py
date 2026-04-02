from __future__ import annotations

from typing import Protocol

from trading_advisor_3000.product_plane.contracts import DecisionCandidate, DecisionPublication, RuntimeSignal, SignalEvent


class SignalStore(Protocol):
    def upsert_candidate(self, candidate: DecisionCandidate, *, expires_at: str | None) -> tuple[RuntimeSignal, bool]:
        ...

    def mark_published(self, *, signal_id: str, publication: DecisionPublication) -> RuntimeSignal:
        ...

    def close_signal(
        self,
        *,
        signal_id: str,
        closed_at: str,
        reason_code: str,
        publication: DecisionPublication | None = None,
    ) -> RuntimeSignal:
        ...

    def cancel_signal(
        self,
        *,
        signal_id: str,
        canceled_at: str,
        reason_code: str,
        publication: DecisionPublication | None = None,
    ) -> RuntimeSignal:
        ...

    def expire_signal(self, *, signal_id: str, expired_at: str) -> RuntimeSignal:
        ...

    def record_execution_fill(
        self,
        *,
        signal_id: str,
        event_ts: str,
        fill_id: str,
        role: str,
        contract_id: str,
        qty: int,
        price: float,
    ) -> None:
        ...

    def record_context_slice(
        self,
        *,
        signal_id: str,
        event_ts: str,
        context_kind: str,
        provider_id: str,
        payload_json: dict[str, object],
    ) -> None:
        ...

    def list_active_signals(self) -> list[RuntimeSignal]:
        ...

    def get_signal(self, signal_id: str) -> RuntimeSignal | None:
        ...

    def list_signal_events(self) -> list[SignalEvent]:
        ...

    def list_publications(self) -> list[DecisionPublication]:
        ...

    def list_publication_events(self) -> list[DecisionPublication]:
        ...
