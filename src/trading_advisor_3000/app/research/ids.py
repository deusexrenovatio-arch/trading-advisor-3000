from __future__ import annotations

import hashlib

from trading_advisor_3000.app.contracts import DecisionCandidate


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def candidate_id(
    *,
    strategy_version_id: str,
    contract_id: str,
    timeframe: str,
    ts_signal: str,
) -> str:
    return "CAND-" + _stable_hash(f"{strategy_version_id}|{contract_id}|{timeframe}|{ts_signal}")


def candidate_id_from_candidate(candidate: DecisionCandidate) -> str:
    return candidate_id(
        strategy_version_id=candidate.strategy_version_id,
        contract_id=candidate.contract_id,
        timeframe=candidate.timeframe.value,
        ts_signal=candidate.ts_decision,
    )
