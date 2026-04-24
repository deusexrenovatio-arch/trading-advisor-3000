from __future__ import annotations

import hashlib

from trading_advisor_3000.product_plane.contracts import DecisionCandidate


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def candidate_id(
    *,
    contract_id: str,
    timeframe: str,
    ts_signal: str,
    strategy_instance_id: str | None = None,
    strategy_version_id: str | None = None,
) -> str:
    if strategy_instance_id is None and strategy_version_id is None:
        raise ValueError("candidate_id requires strategy_instance_id or strategy_version_id")
    if strategy_instance_id is not None and strategy_version_id is not None and strategy_instance_id != strategy_version_id:
        raise ValueError("candidate_id strategy identifiers must match when both are provided")
    strategy_id = strategy_instance_id or strategy_version_id
    return "CAND-" + _stable_hash(f"{strategy_id}|{contract_id}|{timeframe}|{ts_signal}")


def candidate_id_from_candidate(candidate: DecisionCandidate) -> str:
    return candidate_id(
        strategy_instance_id=candidate.strategy_version_id,
        contract_id=candidate.contract_id,
        timeframe=candidate.timeframe.value,
        ts_signal=candidate.ts_decision,
    )
