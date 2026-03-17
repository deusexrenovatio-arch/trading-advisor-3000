from __future__ import annotations

from datetime import datetime, timedelta, timezone

from trading_advisor_3000.app.contracts import DecisionCandidate
from trading_advisor_3000.app.research.ids import candidate_id_from_candidate

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
        validity_minutes_by_timeframe: dict[str, int] | None = None,
        cooldown_seconds: int = 0,
        blackout_windows_by_contract: dict[str, list[tuple[str, str]]] | None = None,
    ) -> None:
        self._strategy_registry = strategy_registry
        self._signal_store = signal_store
        self._publisher = publisher
        self._validity_minutes_by_timeframe = validity_minutes_by_timeframe or {
            "5m": 20,
            "15m": 60,
            "1h": 240,
        }
        self._cooldown_seconds = max(0, cooldown_seconds)
        self._blackout_windows_by_contract = blackout_windows_by_contract or {}
        self._last_accepted_by_key: dict[tuple[str, str, str, str], str] = {}

    @staticmethod
    def _parse_utc(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)

    @staticmethod
    def _format_utc(ts: datetime) -> str:
        return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _compute_expires_at(self, candidate: DecisionCandidate) -> str:
        minutes = self._validity_minutes_by_timeframe.get(candidate.timeframe.value, 60)
        return self._format_utc(self._parse_utc(candidate.ts_decision) + timedelta(minutes=minutes))

    def _is_blackout(self, candidate: DecisionCandidate) -> bool:
        windows = self._blackout_windows_by_contract.get(candidate.contract_id, [])
        for start_ts, end_ts in windows:
            if start_ts <= candidate.ts_decision < end_ts:
                return True
        return False

    def _in_cooldown(self, candidate: DecisionCandidate) -> bool:
        if self._cooldown_seconds <= 0:
            return False
        key = (
            candidate.strategy_version_id,
            candidate.contract_id,
            candidate.timeframe.value,
            candidate.side.value,
        )
        last_ts = self._last_accepted_by_key.get(key)
        if last_ts is None:
            return False
        delta_seconds = (self._parse_utc(candidate.ts_decision) - self._parse_utc(last_ts)).total_seconds()
        return delta_seconds < self._cooldown_seconds

    def _remember_accept(self, candidate: DecisionCandidate) -> None:
        key = (
            candidate.strategy_version_id,
            candidate.contract_id,
            candidate.timeframe.value,
            candidate.side.value,
        )
        self._last_accepted_by_key[key] = candidate.ts_decision

    def _expire_elapsed_signals(self, *, as_of_ts: str) -> None:
        for signal in self._signal_store.list_active_signals():
            if signal.expires_at is None:
                continue
            if signal.expires_at <= as_of_ts:
                self._signal_store.expire_signal(signal_id=signal.signal_id, expired_at=as_of_ts)

    def replay_candidates(self, candidates: list[DecisionCandidate]) -> dict[str, object]:
        accepted = 0
        rejected = 0
        published = 0
        edited = 0
        rejection_reasons: dict[str, int] = {}
        accepted_signal_ids: list[str] = []
        accepted_candidates: list[dict[str, str]] = []
        rejected_signal_ids: list[str] = []

        ordered = sorted(candidates, key=lambda item: (item.ts_decision, item.signal_id))
        for candidate in ordered:
            self._expire_elapsed_signals(as_of_ts=candidate.ts_decision)

            if self._is_blackout(candidate):
                rejected += 1
                rejected_signal_ids.append(candidate.signal_id)
                rejection_reasons["blackout_window"] = rejection_reasons.get("blackout_window", 0) + 1
                continue

            if self._in_cooldown(candidate):
                rejected += 1
                rejected_signal_ids.append(candidate.signal_id)
                rejection_reasons["cooldown_active"] = rejection_reasons.get("cooldown_active", 0) + 1
                continue

            is_allowed, _ = self._strategy_registry.allows(candidate)
            if not is_allowed:
                rejected += 1
                rejected_signal_ids.append(candidate.signal_id)
                rejection_reasons["strategy_policy_reject"] = rejection_reasons.get("strategy_policy_reject", 0) + 1
                continue

            signal, changed = self._signal_store.upsert_candidate(
                candidate,
                expires_at=self._compute_expires_at(candidate),
            )
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
                publication, was_edited = self._publisher.edit(
                    signal_id=signal.signal_id,
                    rendered_message=message,
                    edited_at=candidate.ts_decision,
                )
                if was_edited:
                    edited += 1
                    self._signal_store.mark_published(
                        signal_id=signal.signal_id,
                        message_id=publication.message_id,
                        published_at=candidate.ts_decision,
                        status=publication.status.value,
                    )

            accepted += 1
            accepted_signal_ids.append(candidate.signal_id)
            accepted_candidates.append(
                {
                    "signal_id": candidate.signal_id,
                    "candidate_id": candidate_id_from_candidate(candidate),
                }
            )
            self._remember_accept(candidate)

        return {
            "accepted": accepted,
            "rejected": rejected,
            "published": published,
            "edited": edited,
            "active_signals": len(self._signal_store.list_active_signals()),
            "accepted_signal_ids": accepted_signal_ids,
            "accepted_unique_signals": len(set(accepted_signal_ids)),
            "accepted_candidates": accepted_candidates,
            "rejected_signal_ids": rejected_signal_ids,
            "rejection_reasons": rejection_reasons,
        }

    def close_signal(self, *, signal_id: str, closed_at: str, reason_code: str) -> dict[str, object]:
        signal = self._signal_store.close_signal(signal_id=signal_id, closed_at=closed_at, reason_code=reason_code)
        publication, _ = self._publisher.close(signal_id=signal_id, closed_at=closed_at)
        return {
            "signal": signal.to_dict(),
            "publication": publication.to_dict(),
        }

    def cancel_signal(self, *, signal_id: str, canceled_at: str, reason_code: str) -> dict[str, object]:
        signal = self._signal_store.cancel_signal(signal_id=signal_id, canceled_at=canceled_at, reason_code=reason_code)
        publication, _ = self._publisher.cancel(signal_id=signal_id, canceled_at=canceled_at)
        return {
            "signal": signal.to_dict(),
            "publication": publication.to_dict(),
        }
