from __future__ import annotations

import hashlib
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import CanonicalBar, DecisionCandidate
from trading_advisor_3000.app.research.ids import candidate_id_from_candidate


def _stable_id(prefix: str, seed: str) -> str:
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()
    return f"{prefix}-{digest}"


def candidate_id_from_signal(candidate: DecisionCandidate) -> str:
    return candidate_id_from_candidate(candidate)


@dataclass(frozen=True)
class ForwardObservation:
    forward_obs_id: str
    candidate_id: str
    mode: str
    opened_at: str
    closed_at: str
    result_state: str
    pnl_r: float
    mfe_r: float
    mae_r: float

    def to_dict(self) -> dict[str, object]:
        return {
            "forward_obs_id": self.forward_obs_id,
            "candidate_id": self.candidate_id,
            "mode": self.mode,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "result_state": self.result_state,
            "pnl_r": self.pnl_r,
            "mfe_r": self.mfe_r,
            "mae_r": self.mae_r,
        }


def _risk_unit(price: float, *, fraction: float) -> float:
    return max(abs(price) * fraction, 1e-9)


def _resolve_result_state(pnl_r: float) -> str:
    if pnl_r > 0:
        return "closed_profit"
    if pnl_r < 0:
        return "closed_loss"
    return "closed_flat"


def build_forward_observations(
    *,
    candidates: list[DecisionCandidate],
    bars: list[CanonicalBar],
    horizon_bars: int = 3,
    risk_unit_fraction: float = 0.01,
) -> list[ForwardObservation]:
    if horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive")
    if risk_unit_fraction <= 0:
        raise ValueError("risk_unit_fraction must be positive")

    grouped: dict[tuple[str, str], list[CanonicalBar]] = {}
    for bar in sorted(bars, key=lambda row: (row.contract_id, row.timeframe.value, row.ts)):
        grouped.setdefault((bar.contract_id, bar.timeframe.value), []).append(bar)

    observations: list[ForwardObservation] = []
    ordered_candidates = sorted(candidates, key=lambda row: (row.ts_decision, row.signal_id))
    for candidate in ordered_candidates:
        candidate_id = candidate_id_from_signal(candidate)
        series = grouped.get((candidate.contract_id, candidate.timeframe.value), [])
        entry_index = next((idx for idx, bar in enumerate(series) if bar.ts >= candidate.ts_decision), None)

        if entry_index is None:
            observations.append(
                ForwardObservation(
                    forward_obs_id=_stable_id("FWD", f"{candidate_id}|missing_market_data"),
                    candidate_id=candidate_id,
                    mode=candidate.mode.value,
                    opened_at=candidate.ts_decision,
                    closed_at=candidate.ts_decision,
                    result_state="no_market_data",
                    pnl_r=0.0,
                    mfe_r=0.0,
                    mae_r=0.0,
                )
            )
            continue

        entry_bar = series[entry_index]
        future_window = series[entry_index + 1 : entry_index + 1 + horizon_bars]
        if not future_window:
            observations.append(
                ForwardObservation(
                    forward_obs_id=_stable_id("FWD", f"{candidate_id}|no_forward_window"),
                    candidate_id=candidate_id,
                    mode=candidate.mode.value,
                    opened_at=candidate.ts_decision,
                    closed_at=entry_bar.ts,
                    result_state="no_forward_window",
                    pnl_r=0.0,
                    mfe_r=0.0,
                    mae_r=0.0,
                )
            )
            continue

        entry_price = entry_bar.close
        risk_unit = _risk_unit(entry_price, fraction=risk_unit_fraction)

        if candidate.side.value == "long":
            pnl_raw = future_window[-1].close - entry_price
            mfe_raw = max(bar.high - entry_price for bar in future_window)
            mae_raw = min(bar.low - entry_price for bar in future_window)
        elif candidate.side.value == "short":
            pnl_raw = entry_price - future_window[-1].close
            mfe_raw = max(entry_price - bar.low for bar in future_window)
            mae_raw = min(entry_price - bar.high for bar in future_window)
        else:
            pnl_raw = 0.0
            mfe_raw = 0.0
            mae_raw = 0.0

        pnl_r = pnl_raw / risk_unit
        mfe_r = mfe_raw / risk_unit
        mae_r = mae_raw / risk_unit
        result_state = _resolve_result_state(pnl_r)
        closed_at = future_window[-1].ts

        observations.append(
            ForwardObservation(
                forward_obs_id=_stable_id("FWD", f"{candidate_id}|{closed_at}|{result_state}"),
                candidate_id=candidate_id,
                mode=candidate.mode.value,
                opened_at=candidate.ts_decision,
                closed_at=closed_at,
                result_state=result_state,
                pnl_r=pnl_r,
                mfe_r=mfe_r,
                mae_r=mae_r,
            )
        )

    return sorted(observations, key=lambda row: (row.opened_at, row.candidate_id))
