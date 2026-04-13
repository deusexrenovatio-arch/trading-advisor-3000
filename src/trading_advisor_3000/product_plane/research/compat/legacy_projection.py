from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LegacyProjectionBridge:
    source_artifact: str = "research_backtest_runs"
    target_artifact: str = "research_signal_candidates"
    runtime_contract: str = "DecisionCandidate"
    compatibility_mode: str = "downstream-projection-only"

