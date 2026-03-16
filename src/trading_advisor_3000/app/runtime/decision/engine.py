from __future__ import annotations

from trading_advisor_3000.app.contracts import DecisionCandidate

from trading_advisor_3000.app.runtime.config import StrategyRegistry
from trading_advisor_3000.app.runtime.publishing import TelegramPublicationEngine
from trading_advisor_3000.app.runtime.signal_store import InMemorySignalStore


def _render_message(candidate: DecisionCandidate) -> str:
    return (
        f"[{candidate.mode.value.upper()}] {candidate.strategy_version_id} "
        f"{candidate.contract_id} {candidate.timeframe.value} {candidate.side.value} "
        f"conf={candidate.confidence:.2f} at {candidate.ts_decision}"
    )


class SignalRuntimeEngine:
    def __init__(
        self,
        *,
        strategy_registry: StrategyRegistry,
        signal_store: InMemorySignalStore,
        publisher: TelegramPublicationEngine,
    ) -> None:
        self._strategy_registry = strategy_registry
        self._signal_store = signal_store
        self._publisher = publisher

    def replay_candidates(self, candidates: list[DecisionCandidate]) -> dict[str, int]:
        accepted = 0
        rejected = 0
        published = 0
        edited = 0

        ordered = sorted(candidates, key=lambda item: (item.ts_decision, item.signal_id))
        for candidate in ordered:
            is_allowed, _ = self._strategy_registry.allows(candidate)
            if not is_allowed:
                rejected += 1
                continue

            signal, changed = self._signal_store.upsert_candidate(candidate)
            message = _render_message(candidate)
            if signal.publication_message_id is None:
                publication, created = self._publisher.publish(
                    signal_id=signal.signal_id,
                    rendered_message=message,
                    published_at=candidate.ts_decision,
                )
                self._signal_store.mark_published(
                    signal_id=signal.signal_id,
                    message_id=publication.message_id,
                    published_at=candidate.ts_decision,
                    status=publication.status.value,
                )
                if created:
                    published += 1
            elif changed:
                _, was_edited = self._publisher.edit(
                    signal_id=signal.signal_id,
                    rendered_message=message,
                    edited_at=candidate.ts_decision,
                )
                if was_edited:
                    edited += 1

            accepted += 1

        return {
            "accepted": accepted,
            "rejected": rejected,
            "published": published,
            "edited": edited,
            "active_signals": len(self._signal_store.list_active_signals()),
        }

    def close_signal(self, *, signal_id: str, closed_at: str, reason_code: str) -> dict[str, object]:
        signal = self._signal_store.close_signal(signal_id=signal_id, closed_at=closed_at, reason_code=reason_code)
        publication, _ = self._publisher.close(signal_id=signal_id, closed_at=closed_at)
        return {
            "signal": signal.to_dict(),
            "publication": publication.to_dict(),
        }
