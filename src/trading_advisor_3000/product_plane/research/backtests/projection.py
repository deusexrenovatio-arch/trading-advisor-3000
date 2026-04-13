from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CandidateProjectionRequest:
    backtest_run_id: str
    selection_policy: str
    runtime_contract: str = "DecisionCandidate"

