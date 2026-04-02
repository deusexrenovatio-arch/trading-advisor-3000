from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import (
    DecisionCandidate,
    FeatureSnapshotRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.product_plane.runtime import build_runtime_stack
from trading_advisor_3000.product_plane.runtime.config import StrategyVersion


class _StaticContextProvider:
    def __init__(self, provider_id: str, payload: dict[str, object]) -> None:
        self.provider_id = provider_id
        self._payload = payload

    def fetch(self, *, contract_id: str, as_of_ts: str) -> dict[str, object] | None:
        return {
            "contract_id": contract_id,
            "as_of_ts": as_of_ts,
            **self._payload,
        }


def _candidate(*, signal_id: str) -> DecisionCandidate:
    return DecisionCandidate(
        signal_id=signal_id,
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="trend-follow-v1",
        mode=Mode.SHADOW,
        side=TradeSide.LONG,
        entry_ref=82.45,
        stop_ref=81.70,
        target_ref=83.95,
        confidence=0.72,
        ts_decision="2026-03-18T20:00:00Z",
        feature_snapshot=FeatureSnapshotRef(
            dataset_version="bars-whitelist-v1",
            snapshot_id="FS-20260318-0001",
        ),
    )


def test_phase7_runtime_pipeline_uses_context_provider_registry_in_replay_path() -> None:
    stack = build_runtime_stack(telegram_channel="@ta3000_signals")
    stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id="trend-follow-v1",
            status="active",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-18T19:00:00Z",
        )
    )
    stack.context_provider_registry.register(
        context_kind="fundamentals",
        provider=_StaticContextProvider("fund-provider", {"pe_ratio": 12.4}),
    )
    stack.context_provider_registry.register(
        context_kind="news",
        provider=_StaticContextProvider("news-provider", {"headline": "inventory update"}),
    )

    report = stack.runtime_engine.replay_candidates([_candidate(signal_id="SIG-PHASE7-CONTEXT-1")])
    context_events = [item for item in stack.signal_store.list_signal_events() if item.event_type == "signal_context"]

    assert report["accepted"] == 1
    assert report["context_slices_total"] == 2
    assert report["signals_with_context"] == 1
    assert report["context_slices_by_kind"] == {"fundamentals": 1, "news": 1}
    assert len(context_events) == 2
    assert {item.reason_code for item in context_events} == {"fundamentals", "news"}


def test_phase7_runtime_pipeline_handles_missing_context_providers_without_rejection() -> None:
    stack = build_runtime_stack(telegram_channel="@ta3000_signals")
    stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id="trend-follow-v1",
            status="active",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-18T19:00:00Z",
        )
    )

    report = stack.runtime_engine.replay_candidates([_candidate(signal_id="SIG-PHASE7-CONTEXT-EMPTY-1")])
    context_events = [item for item in stack.signal_store.list_signal_events() if item.event_type == "signal_context"]

    assert report["accepted"] == 1
    assert report["rejected"] == 0
    assert report["context_slices_total"] == 0
    assert report["signals_with_context"] == 0
    assert report["context_slices_by_kind"] == {}
    assert context_events == []
