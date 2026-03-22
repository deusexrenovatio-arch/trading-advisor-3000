from __future__ import annotations

from trading_advisor_3000.app.contracts import DecisionCandidate
from trading_advisor_3000.app.runtime import RuntimeStack


class RuntimeAPI:
    def __init__(self, *, runtime_stack: RuntimeStack) -> None:
        self._runtime_stack = runtime_stack

    def replay_candidates(self, candidates: list[DecisionCandidate]) -> dict[str, object]:
        replay_report = self._runtime_stack.runtime_engine.replay_candidates(candidates)
        return {
            "replay_report": replay_report,
            "active_signals": [signal.to_dict() for signal in self._runtime_stack.signal_store.list_active_signals()],
            "publications": [publication.to_dict() for publication in self._runtime_stack.signal_store.list_publications()],
        }

    def close_signal(self, *, signal_id: str, closed_at: str, reason_code: str) -> dict[str, object]:
        result = self._runtime_stack.runtime_engine.close_signal(
            signal_id=signal_id,
            closed_at=closed_at,
            reason_code=reason_code,
        )
        return {
            "close_result": result,
            "active_signals": [signal.to_dict() for signal in self._runtime_stack.signal_store.list_active_signals()],
            "publications": [publication.to_dict() for publication in self._runtime_stack.signal_store.list_publications()],
        }

    def cancel_signal(self, *, signal_id: str, canceled_at: str, reason_code: str) -> dict[str, object]:
        result = self._runtime_stack.runtime_engine.cancel_signal(
            signal_id=signal_id,
            canceled_at=canceled_at,
            reason_code=reason_code,
        )
        return {
            "cancel_result": result,
            "active_signals": [signal.to_dict() for signal in self._runtime_stack.signal_store.list_active_signals()],
            "publications": [publication.to_dict() for publication in self._runtime_stack.signal_store.list_publications()],
        }

    def list_signal_events(self) -> list[dict[str, object]]:
        return [event.to_dict() for event in self._runtime_stack.signal_store.list_signal_events()]

    def list_active_signals(self) -> list[dict[str, object]]:
        return [signal.to_dict() for signal in self._runtime_stack.signal_store.list_active_signals()]

    def list_publication_events(self) -> list[dict[str, object]]:
        return [publication.to_dict() for publication in self._runtime_stack.signal_store.list_publication_events()]

    def list_strategy_registry(self) -> list[dict[str, object]]:
        return self._runtime_stack.strategy_registry.snapshot()
