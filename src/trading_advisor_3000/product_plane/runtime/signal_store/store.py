from __future__ import annotations

import hashlib

from trading_advisor_3000.product_plane.contracts import DecisionCandidate, DecisionPublication, RuntimeSignal, SignalEvent
from trading_advisor_3000.product_plane.research.ids import candidate_id_from_candidate


def _event_id(seed: str) -> str:
    return "SEVT-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()


def _publication_dedup_key(publication: DecisionPublication) -> str:
    return (
        f"{publication.signal_id}|{publication.publication_type.value}|{publication.message_id}|"
        f"{publication.status.value}|{publication.published_at}"
    )


def _publication_event_type(publication: DecisionPublication) -> str:
    return {
        "create": "signal_activated",
        "edit": "signal_published_edit",
        "close": "signal_publication_closed",
        "cancel": "signal_publication_canceled",
    }.get(publication.publication_type.value, "signal_publication_updated")


class InMemorySignalStore:
    def __init__(self) -> None:
        self._signals: dict[str, RuntimeSignal] = {}
        self._events: list[SignalEvent] = []
        self._event_dedup_keys: set[str] = set()
        self._publications: dict[str, DecisionPublication] = {}
        self._publication_history: list[DecisionPublication] = []
        self._publication_dedup_keys: set[str] = set()

    def _append_event(
        self,
        *,
        signal_id: str,
        event_ts: str,
        event_type: str,
        reason_code: str,
        payload_json: dict[str, object],
        dedup_key: str,
    ) -> None:
        if dedup_key in self._event_dedup_keys:
            return
        self._event_dedup_keys.add(dedup_key)
        self._events.append(
            SignalEvent(
                event_id=_event_id(dedup_key),
                signal_id=signal_id,
                event_ts=event_ts,
                event_type=event_type,
                reason_code=reason_code,
                payload_json=payload_json,
            )
        )

    def _record_publication(self, publication: DecisionPublication) -> None:
        dedup_key = _publication_dedup_key(publication)
        if dedup_key in self._publication_dedup_keys:
            return
        self._publication_dedup_keys.add(dedup_key)
        self._publications[publication.signal_id] = publication
        self._publication_history.append(publication)

    def upsert_candidate(self, candidate: DecisionCandidate, *, expires_at: str | None) -> tuple[RuntimeSignal, bool]:
        existing = self._signals.get(candidate.signal_id)
        candidate_payload = {
            **candidate.to_dict(),
            "candidate_id": candidate_id_from_candidate(candidate),
        }
        if existing is None:
            signal = RuntimeSignal(
                signal_id=candidate.signal_id,
                strategy_version_id=candidate.strategy_version_id,
                contract_id=candidate.contract_id,
                mode=candidate.mode,
                side=candidate.side,
                entry_price=candidate.entry_ref,
                stop_price=candidate.stop_ref,
                target_price=candidate.target_ref,
                confidence=candidate.confidence,
                state="candidate",
                opened_at=candidate.ts_decision,
                updated_at=candidate.ts_decision,
                expires_at=expires_at,
                publication_message_id=None,
            )
            self._signals[candidate.signal_id] = signal
            self._append_event(
                signal_id=candidate.signal_id,
                event_ts=candidate.ts_decision,
                event_type="signal_opened",
                reason_code="candidate_created",
                payload_json=candidate_payload,
                dedup_key=f"{candidate.signal_id}|opened|{candidate.ts_decision}",
            )
            return signal, True

        if (
            existing.side == candidate.side
            and abs(existing.entry_price - candidate.entry_ref) < 1e-9
            and abs(existing.stop_price - candidate.stop_ref) < 1e-9
            and abs(existing.target_price - candidate.target_ref) < 1e-9
            and abs(existing.confidence - candidate.confidence) < 1e-9
            and existing.updated_at == candidate.ts_decision
        ):
            return existing, False

        updated = RuntimeSignal(
            signal_id=existing.signal_id,
            strategy_version_id=existing.strategy_version_id,
            contract_id=existing.contract_id,
            mode=existing.mode,
            side=candidate.side,
            entry_price=candidate.entry_ref,
            stop_price=candidate.stop_ref,
            target_price=candidate.target_ref,
            confidence=candidate.confidence,
            state=existing.state,
            opened_at=existing.opened_at,
            updated_at=candidate.ts_decision,
            expires_at=expires_at if expires_at is not None else existing.expires_at,
            publication_message_id=existing.publication_message_id,
        )
        self._signals[candidate.signal_id] = updated
        self._append_event(
            signal_id=candidate.signal_id,
            event_ts=candidate.ts_decision,
            event_type="signal_updated",
            reason_code="candidate_upsert",
            payload_json=candidate_payload,
            dedup_key=f"{candidate.signal_id}|updated|{candidate.ts_decision}|{candidate.side.value}|{candidate.confidence:.8f}",
        )
        return updated, True

    def mark_published(self, *, signal_id: str, publication: DecisionPublication) -> RuntimeSignal:
        existing = self._signals.get(signal_id)
        if existing is None:
            raise ValueError(f"signal not found: {signal_id}")
        updated = RuntimeSignal(
            signal_id=existing.signal_id,
            strategy_version_id=existing.strategy_version_id,
            contract_id=existing.contract_id,
            mode=existing.mode,
            side=existing.side,
            entry_price=existing.entry_price,
            stop_price=existing.stop_price,
            target_price=existing.target_price,
            confidence=existing.confidence,
            state="active" if existing.state in {"candidate", "active"} else existing.state,
            opened_at=existing.opened_at,
            updated_at=publication.published_at,
            expires_at=existing.expires_at,
            publication_message_id=publication.message_id,
        )
        self._signals[signal_id] = updated
        self._append_event(
            signal_id=signal_id,
            event_ts=publication.published_at,
            event_type=_publication_event_type(publication),
            reason_code=publication.status.value,
            payload_json=publication.to_dict(),
            dedup_key=f"{signal_id}|publication|{_publication_dedup_key(publication)}",
        )
        self._record_publication(publication)
        return updated

    def close_signal(
        self,
        *,
        signal_id: str,
        closed_at: str,
        reason_code: str,
        publication: DecisionPublication | None = None,
    ) -> RuntimeSignal:
        existing = self._signals.get(signal_id)
        if existing is None:
            raise ValueError(f"signal not found: {signal_id}")
        if existing.state == "closed":
            if publication is not None:
                self._record_publication(publication)
            return existing
        closed = RuntimeSignal(
            signal_id=existing.signal_id,
            strategy_version_id=existing.strategy_version_id,
            contract_id=existing.contract_id,
            mode=existing.mode,
            side=existing.side,
            entry_price=existing.entry_price,
            stop_price=existing.stop_price,
            target_price=existing.target_price,
            confidence=existing.confidence,
            state="closed",
            opened_at=existing.opened_at,
            updated_at=closed_at,
            expires_at=closed_at,
            publication_message_id=existing.publication_message_id,
        )
        self._signals[signal_id] = closed
        self._append_event(
            signal_id=signal_id,
            event_ts=closed_at,
            event_type="signal_closed",
            reason_code=reason_code,
            payload_json={"state": "closed"},
            dedup_key=f"{signal_id}|closed|{closed_at}|{reason_code}",
        )
        if publication is not None:
            self._record_publication(publication)
        return closed

    def cancel_signal(
        self,
        *,
        signal_id: str,
        canceled_at: str,
        reason_code: str,
        publication: DecisionPublication | None = None,
    ) -> RuntimeSignal:
        existing = self._signals.get(signal_id)
        if existing is None:
            raise ValueError(f"signal not found: {signal_id}")
        if existing.state == "canceled":
            if publication is not None:
                self._record_publication(publication)
            return existing
        canceled = RuntimeSignal(
            signal_id=existing.signal_id,
            strategy_version_id=existing.strategy_version_id,
            contract_id=existing.contract_id,
            mode=existing.mode,
            side=existing.side,
            entry_price=existing.entry_price,
            stop_price=existing.stop_price,
            target_price=existing.target_price,
            confidence=existing.confidence,
            state="canceled",
            opened_at=existing.opened_at,
            updated_at=canceled_at,
            expires_at=canceled_at,
            publication_message_id=existing.publication_message_id,
        )
        self._signals[signal_id] = canceled
        self._append_event(
            signal_id=signal_id,
            event_ts=canceled_at,
            event_type="signal_canceled",
            reason_code=reason_code,
            payload_json={"state": "canceled"},
            dedup_key=f"{signal_id}|canceled|{canceled_at}|{reason_code}",
        )
        if publication is not None:
            self._record_publication(publication)
        return canceled

    def expire_signal(self, *, signal_id: str, expired_at: str) -> RuntimeSignal:
        existing = self._signals.get(signal_id)
        if existing is None:
            raise ValueError(f"signal not found: {signal_id}")
        if existing.state == "expired":
            return existing
        expired = RuntimeSignal(
            signal_id=existing.signal_id,
            strategy_version_id=existing.strategy_version_id,
            contract_id=existing.contract_id,
            mode=existing.mode,
            side=existing.side,
            entry_price=existing.entry_price,
            stop_price=existing.stop_price,
            target_price=existing.target_price,
            confidence=existing.confidence,
            state="expired",
            opened_at=existing.opened_at,
            updated_at=expired_at,
            expires_at=expired_at,
            publication_message_id=existing.publication_message_id,
        )
        self._signals[signal_id] = expired
        self._append_event(
            signal_id=signal_id,
            event_ts=expired_at,
            event_type="signal_expired",
            reason_code="validity_window_elapsed",
            payload_json={"state": "expired"},
            dedup_key=f"{signal_id}|expired|{expired_at}",
        )
        return expired

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
        self._append_event(
            signal_id=signal_id,
            event_ts=event_ts,
            event_type="execution_fill",
            reason_code=role,
            payload_json={
                "fill_id": fill_id,
                "role": role,
                "contract_id": contract_id,
                "qty": qty,
                "price": price,
            },
            dedup_key=f"{signal_id}|execution_fill|{fill_id}|{role}",
        )

    def record_context_slice(
        self,
        *,
        signal_id: str,
        event_ts: str,
        context_kind: str,
        provider_id: str,
        payload_json: dict[str, object],
    ) -> None:
        self._append_event(
            signal_id=signal_id,
            event_ts=event_ts,
            event_type="signal_context",
            reason_code=context_kind,
            payload_json={
                "context_kind": context_kind,
                "provider_id": provider_id,
                "payload": payload_json,
            },
            dedup_key=f"{signal_id}|signal_context|{context_kind}|{provider_id}|{event_ts}",
        )

    def list_active_signals(self) -> list[RuntimeSignal]:
        active_states = {"candidate", "active"}
        items = [signal for signal in self._signals.values() if signal.state in active_states]
        return sorted(items, key=lambda signal: (signal.contract_id, signal.updated_at, signal.signal_id))

    def get_signal(self, signal_id: str) -> RuntimeSignal | None:
        return self._signals.get(signal_id)

    def list_signal_events(self) -> list[SignalEvent]:
        return list(self._events)

    def list_publications(self) -> list[DecisionPublication]:
        return sorted(
            self._publications.values(),
            key=lambda publication: (publication.signal_id, publication.published_at, publication.publication_id),
        )

    def list_publication_events(self) -> list[DecisionPublication]:
        return list(self._publication_history)
