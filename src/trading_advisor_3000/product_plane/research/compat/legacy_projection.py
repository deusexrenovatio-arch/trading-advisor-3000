from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LegacyProjectionBridge:
    """Compatibility bridge for the pre-Stage-6 candidate path.

    The primary ranking/projection path now lives under
    ``trading_advisor_3000.product_plane.research.backtests``.
    """

    source_artifact: str = "research_backtest_runs"
    target_artifact: str = "research_signal_candidates"
    runtime_contract: str = "DecisionCandidate"
    compatibility_mode: str = "legacy-candidate-bridge-only"
