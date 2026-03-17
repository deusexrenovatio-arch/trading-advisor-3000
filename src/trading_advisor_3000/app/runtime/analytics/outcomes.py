from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.app.contracts import DecisionCandidate
from trading_advisor_3000.app.research.forward import ForwardObservation, candidate_id_from_signal


_CLOSE_REASON_BY_RESULT = {
    "closed_profit": "forward_window_profit",
    "closed_loss": "forward_window_loss",
    "closed_flat": "forward_window_flat",
    "no_market_data": "no_market_data",
    "no_forward_window": "no_forward_window",
}


@dataclass(frozen=True)
class SignalOutcome:
    signal_id: str
    strategy_version_id: str
    contract_id: str
    mode: str
    opened_at: str
    closed_at: str
    pnl_r: float
    mfe_r: float
    mae_r: float
    close_reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "strategy_version_id": self.strategy_version_id,
            "contract_id": self.contract_id,
            "mode": self.mode,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "pnl_r": self.pnl_r,
            "mfe_r": self.mfe_r,
            "mae_r": self.mae_r,
            "close_reason": self.close_reason,
        }


def phase3_outcome_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_forward_observations": {
            "format": "delta",
            "partition_by": ["mode", "result_state"],
            "constraints": ["unique(candidate_id, mode, opened_at)"],
            "columns": {
                "forward_obs_id": "string",
                "candidate_id": "string",
                "mode": "string",
                "opened_at": "timestamp",
                "closed_at": "timestamp",
                "result_state": "string",
                "pnl_r": "double",
                "mfe_r": "double",
                "mae_r": "double",
            },
        },
        "analytics_signal_outcomes": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "contract_id", "mode"],
            "constraints": ["unique(signal_id, mode)"],
            "columns": {
                "signal_id": "string",
                "strategy_version_id": "string",
                "contract_id": "string",
                "mode": "string",
                "opened_at": "timestamp",
                "closed_at": "timestamp",
                "pnl_r": "double",
                "mfe_r": "double",
                "mae_r": "double",
                "close_reason": "string",
            },
        },
    }


def build_signal_outcomes(
    *,
    candidates: list[DecisionCandidate],
    forward_observations: list[ForwardObservation],
) -> list[SignalOutcome]:
    by_candidate_id = {candidate_id_from_signal(item): item for item in candidates}
    outcomes: list[SignalOutcome] = []
    for observation in forward_observations:
        candidate = by_candidate_id.get(observation.candidate_id)
        if candidate is None:
            raise ValueError(f"unknown candidate_id in forward observation: {observation.candidate_id}")
        outcomes.append(
            SignalOutcome(
                signal_id=candidate.signal_id,
                strategy_version_id=candidate.strategy_version_id,
                contract_id=candidate.contract_id,
                mode=observation.mode,
                opened_at=observation.opened_at,
                closed_at=observation.closed_at,
                pnl_r=observation.pnl_r,
                mfe_r=observation.mfe_r,
                mae_r=observation.mae_r,
                close_reason=_CLOSE_REASON_BY_RESULT.get(observation.result_state, "forward_window_unknown"),
            )
        )
    return sorted(outcomes, key=lambda row: (row.opened_at, row.signal_id))
