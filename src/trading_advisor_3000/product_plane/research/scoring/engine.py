from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from datetime import datetime, timezone

from .config import StrategyScoringProfile


def _stable_hash(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()


def _avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _risk_reward(row: dict[str, object]) -> float:
    entry = float(row.get("entry_ref", 0.0) or 0.0)
    stop = float(row.get("stop_ref", 0.0) or 0.0)
    target = float(row.get("target_ref", 0.0) or 0.0)
    denominator = abs(entry - stop)
    numerator = abs(target - entry)
    return numerator / denominator if denominator > 0 else 0.0


def _normalize_strategy_family(strategy_version_id: str, fallback: str | None) -> str:
    if isinstance(fallback, str) and fallback.strip():
        return fallback.strip()
    normalized = strategy_version_id.lower()
    if "trend" in normalized:
        return "trend-following"
    if "mean" in normalized:
        return "mean-reversion"
    return "breakout-volatility"


def _generated_at(rows: list[dict[str, object]]) -> str:
    signals = [str(row.get("ts_signal", "")).strip() for row in rows if str(row.get("ts_signal", "")).strip()]
    if signals:
        return max(signals)
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_strategy_scorecards(
    *,
    projection_rows: list[dict[str, object]],
    profile: StrategyScoringProfile,
    repeatable: bool = True,
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in projection_rows:
        strategy_version_id = str(row.get("strategy_version_id", "")).strip()
        if not strategy_version_id:
            continue
        grouped[strategy_version_id].append(row)

    scorecards: list[dict[str, object]] = []
    for strategy_version_id, rows in sorted(grouped.items()):
        candidate_count = len(rows)
        unique_assets = {
            str(item.get("instrument_id", "")).strip() or str(item.get("contract_id", "")).split("-")[0]
            for item in rows
        }
        active_assets_count = len({asset for asset in unique_assets if asset})
        signals_per_week = candidate_count / 208.0
        avg_score = _avg([float(item.get("score", 0.0) or 0.0) for item in rows])
        avg_rr = _avg([_risk_reward(item) for item in rows])

        annual_return_pct = round(max(0.0, (avg_score * 110.0) + (avg_rr * 14.0)), 4)
        negative_months_4y = max(0, min(48, int(math.ceil(max(0.0, 0.58 - avg_score) * 60.0))))
        sharpe_ratio = round((avg_score * 2.5) + (avg_rr * 0.4), 4)
        max_drawdown_pct = round(max(2.0, 38.0 - (annual_return_pct * 0.55)), 4)

        criteria = {
            "stability_pass": negative_months_4y <= profile.max_negative_months_4y,
            "return_pass": annual_return_pct > profile.min_annual_return_pct,
            "diversification_pass": active_assets_count >= profile.min_active_assets_count,
            "frequency_pass": signals_per_week >= profile.min_signals_per_week,
            "repeatability_pass": (repeatable is True) if profile.require_repeatable else True,
            "sharpe_pass": sharpe_ratio >= profile.min_sharpe_ratio,
            "risk_pass": max_drawdown_pct <= profile.max_drawdown_pct,
        }
        blocked_reasons = [key for key, state in criteria.items() if state is False]
        verdict = "PASS" if not blocked_reasons else "BLOCKED"

        strategy_family = _normalize_strategy_family(
            strategy_version_id,
            fallback=str(rows[0].get("strategy_family", "")).strip() if rows else None,
        )
        generated_at = _generated_at(rows)

        scorecards.append(
            {
                "scorecard_id": "SC-" + _stable_hash(f"{strategy_version_id}|{profile.profile_version}|{generated_at}"),
                "strategy_version_id": strategy_version_id,
                "strategy_family": strategy_family,
                "profile_version": profile.profile_version,
                "capital_rub": profile.capital_rub,
                "annual_return_pct": annual_return_pct,
                "sharpe_ratio": sharpe_ratio,
                "max_drawdown_pct": max_drawdown_pct,
                "negative_months_4y": negative_months_4y,
                "signals_per_week": round(signals_per_week, 6),
                "active_assets_count": active_assets_count,
                "repeatable": repeatable,
                "criteria": criteria,
                "verdict": verdict,
                "blocked_reasons": blocked_reasons,
                "generated_at": generated_at,
            }
        )

    return scorecards


def build_strategy_promotion_decisions(scorecards: list[dict[str, object]]) -> list[dict[str, object]]:
    decisions: list[dict[str, object]] = []
    for row in scorecards:
        strategy_version_id = str(row.get("strategy_version_id", "")).strip()
        profile_version = str(row.get("profile_version", "")).strip()
        verdict = str(row.get("verdict", "BLOCKED")).strip() or "BLOCKED"
        generated_at = str(row.get("generated_at", "")).strip() or datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        scorecard_id = str(row.get("scorecard_id", "")).strip()
        fingerprint = "RFP-" + _stable_hash(f"{strategy_version_id}|{profile_version}|{scorecard_id}|{verdict}")
        blocked = row.get("blocked_reasons", [])
        if not isinstance(blocked, list):
            blocked = []
        promotion_state = "PROMOTED" if verdict == "PASS" else "REJECTED"

        decisions.append(
            {
                "decision_id": "PROMO-" + _stable_hash(f"{scorecard_id}|{verdict}|{generated_at}"),
                "scorecard_id": scorecard_id,
                "strategy_version_id": strategy_version_id,
                "profile_version": profile_version,
                "verdict": verdict,
                "promotion_state": promotion_state,
                "blocked_reasons": [str(item) for item in blocked if str(item).strip()],
                "effective_from": generated_at,
                "capital_rub": int(row.get("capital_rub", 0) or 0),
                "reproducibility_fingerprint": fingerprint,
            }
        )

    return decisions
