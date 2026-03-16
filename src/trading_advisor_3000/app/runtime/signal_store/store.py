from __future__ import annotations

import hashlib
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import DecisionCandidate, Mode, TradeSide


def _event_id(seed: str) -> str:
    return "SEVT-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()


@dataclass(frozen=True)
class RuntimeSignal:
    signal_id: str
    strategy_version_id: str
    contract_id: str
    mode: Mode
    side: TradeSide
    confidence: float
    state: str
    opened_at: str
    updated_at: str
    expires_at: str | None
    publication_message_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "strategy_version_id": self.strategy_version_id,
            "contract_id": self.contract_id,
            "mode": self.mode.value,
            "side": self.side.value,
            "confidence": self.confidence,
            "state": self.state,
            "opened_at": self.opened_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "publication_message_id": self.publication_message_id,
        }


@dataclass(frozen=True)
class SignalEvent:
    event_id: str
    signal_id: str
    event_ts: str
    event_type: str
    reason_code: str
    payload_json: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "signal_id": self.signal_id,
            "event_ts": self.event_ts,
            "event_type": self.event_type,
            "reason_code": self.reason_code,
            "payload_json": self.payload_json,
        }


class InMemorySignalStore:
    def __init__(self) -> None:
        self._signals: dict[str, RuntimeSignal] = {}
        self._events: list[SignalEvent] = []
        self._event_dedup_keys: set[str] = set()

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

    def upsert_candidate(self, candidate: DecisionCandidate) -> tuple[RuntimeSignal, bool]:
        existing = self._signals.get(candidate.signal_id)
        if existing is None:
            signal = RuntimeSignal(
                signal_id=candidate.signal_id,
                strategy_version_id=candidate.strategy_version_id,
                contract_id=candidate.contract_id,
                mode=candidate.mode,
                side=candidate.side,
                confidence=candidate.confidence,
                state="candidate",
                opened_at=candidate.ts_decision,
                updated_at=candidate.ts_decision,
                expires_at=None,
                publication_message_id=None,
            )
            self._signals[candidate.signal_id] = signal
            self._append_event(
                signal_id=candidate.signal_id,
                event_ts=candidate.ts_decision,
                event_type="signal_opened",
                reason_code="candidate_created",
                payload_json=candidate.to_dict(),
                dedup_key=f"{candidate.signal_id}|opened|{candidate.ts_decision}",
            )
            return signal, True

        if (
            existing.side == candidate.side
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
            confidence=candidate.confidence,
            state=existing.state,
            opened_at=existing.opened_at,
            updated_at=candidate.ts_decision,
            expires_at=existing.expires_at,
            publication_message_id=existing.publication_message_id,
        )
        self._signals[candidate.signal_id] = updated
        self._append_event(
            signal_id=candidate.signal_id,
            event_ts=candidate.ts_decision,
            event_type="signal_updated",
            reason_code="candidate_upsert",
            payload_json=candidate.to_dict(),
            dedup_key=f"{candidate.signal_id}|updated|{candidate.ts_decision}|{candidate.side.value}|{candidate.confidence:.8f}",
        )
        return updated, True

    def mark_published(self, *, signal_id: str, message_id: str, published_at: str, status: str) -> RuntimeSignal:
        existing = self._signals.get(signal_id)
        if existing is None:
            raise ValueError(f"signal not found: {signal_id}")
        updated = RuntimeSignal(
            signal_id=existing.signal_id,
            strategy_version_id=existing.strategy_version_id,
            contract_id=existing.contract_id,
            mode=existing.mode,
            side=existing.side,
            confidence=existing.confidence,
            state=status,
            opened_at=existing.opened_at,
            updated_at=published_at,
            expires_at=existing.expires_at,
            publication_message_id=message_id,
        )
        self._signals[signal_id] = updated
        self._append_event(
            signal_id=signal_id,
            event_ts=published_at,
            event_type="signal_published",
            reason_code=status,
            payload_json={"message_id": message_id, "status": status},
            dedup_key=f"{signal_id}|published|{message_id}|{status}",
        )
        return updated

    def close_signal(self, *, signal_id: str, closed_at: str, reason_code: str) -> RuntimeSignal:
        existing = self._signals.get(signal_id)
        if existing is None:
            raise ValueError(f"signal not found: {signal_id}")
        if existing.state == "closed":
            return existing
        closed = RuntimeSignal(
            signal_id=existing.signal_id,
            strategy_version_id=existing.strategy_version_id,
            contract_id=existing.contract_id,
            mode=existing.mode,
            side=existing.side,
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
        return closed

    def list_active_signals(self) -> list[RuntimeSignal]:
        active_states = {"candidate", "published"}
        items = [signal for signal in self._signals.values() if signal.state in active_states]
        return sorted(items, key=lambda signal: (signal.contract_id, signal.updated_at, signal.signal_id))

    def get_signal(self, signal_id: str) -> RuntimeSignal | None:
        return self._signals.get(signal_id)

    def list_signal_events(self) -> list[SignalEvent]:
        return list(self._events)
