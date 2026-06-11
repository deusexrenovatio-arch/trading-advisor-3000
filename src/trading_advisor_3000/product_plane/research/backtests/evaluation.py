from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

STRATEGY_EVALUATION_PROFILE_VERSION = "strategy-evaluation-profile.v1"
APPROVED_UNIVERSE_PROFILE = "approved-universe-v1"


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _created_at() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_json(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        payload = json.loads(value)
        if isinstance(payload, Mapping):
            return {str(key): item for key, item in payload.items()}
    return {}


def _coerce_list(value: object) -> list[object]:
    if isinstance(value, list | tuple | set):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        if isinstance(payload, list):
            return payload
        if isinstance(payload, tuple | set):
            return list(payload)
        return [payload]
    return []


def _present(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _float_from(value: object) -> float | None:
    if not _present(value):
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _first_float(
    ranking_row: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
    keys: Sequence[str],
) -> float | None:
    for key in keys:
        value = _float_from(ranking_row.get(key))
        if value is not None:
            return value
    for candidate in candidate_rows:
        for key in keys:
            value = _float_from(candidate.get(key))
            if value is not None:
                return value
    return None


def _first_bool(
    ranking_row: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
    keys: Sequence[str],
) -> bool | None:
    for key in keys:
        if key in ranking_row and _present(ranking_row.get(key)):
            return _as_bool(ranking_row.get(key))
    for candidate in candidate_rows:
        for key in keys:
            if key in candidate and _present(candidate.get(key)):
                return _as_bool(candidate.get(key))
    return None


def _has_payload(
    ranking_row: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
    keys: Sequence[str],
) -> bool:
    for key in keys:
        if _present(ranking_row.get(key)):
            return True
    return any(_present(candidate.get(key)) for candidate in candidate_rows for key in keys)


def _has_list_payload(
    ranking_row: Mapping[str, object],
    candidate_rows: Sequence[Mapping[str, object]],
    keys: Sequence[str],
) -> bool:
    for key in keys:
        if _coerce_list(ranking_row.get(key)):
            return True
    return any(_coerce_list(candidate.get(key)) for candidate in candidate_rows for key in keys)


def _candidate_index(
    candidate_rows: Sequence[Mapping[str, object]],
) -> tuple[
    dict[str, list[Mapping[str, object]]],
    dict[tuple[str, str, str, str], list[Mapping[str, object]]],
]:
    by_ranking: dict[str, list[Mapping[str, object]]] = {}
    by_series: dict[tuple[str, str, str, str], list[Mapping[str, object]]] = {}
    for row in candidate_rows:
        ranking_id = str(row.get("ranking_id", "")).strip()
        if ranking_id:
            by_ranking.setdefault(ranking_id, []).append(row)
        key = (
            str(row.get("strategy_instance_id", "")),
            str(row.get("contract_id", "")),
            str(row.get("instrument_id", "")),
            str(row.get("timeframe", "")),
        )
        by_series.setdefault(key, []).append(row)
    return by_ranking, by_series


def _matching_candidates(
    ranking_row: Mapping[str, object],
    by_ranking: Mapping[str, Sequence[Mapping[str, object]]],
    by_series: Mapping[tuple[str, str, str, str], Sequence[Mapping[str, object]]],
) -> list[Mapping[str, object]]:
    ranking_id = str(ranking_row.get("ranking_id", "")).strip()
    if ranking_id and ranking_id in by_ranking:
        return list(by_ranking[ranking_id])
    key = (
        str(ranking_row.get("strategy_instance_id", "")),
        str(ranking_row.get("contract_id", "")),
        str(ranking_row.get("instrument_id", "")),
        str(ranking_row.get("timeframe", "")),
    )
    return list(by_series.get(key, ()))


def _profile_group_key(row: Mapping[str, object]) -> tuple[str, str, str, str]:
    return (
        str(row.get("campaign_run_id", "")),
        str(row.get("family_key", "")),
        str(row.get("strategy_version_label", "")),
        str(row.get("params_hash", row.get("strategy_instance_id", ""))),
    )


def _active_instrument_counts(
    ranking_rows: Sequence[Mapping[str, object]],
) -> dict[tuple[str, str, str, str], int]:
    grouped: dict[tuple[str, str, str, str], set[str]] = {}
    for row in ranking_rows:
        if not _as_bool(row.get("policy_pass", False)):
            continue
        key = _profile_group_key(row)
        instrument = str(row.get("instrument_id", "")).strip()
        if instrument:
            grouped.setdefault(key, set()).add(instrument)
    return {key: len(instruments) for key, instruments in grouped.items()}


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def build_strategy_evaluation_profiles(
    *,
    ranking_rows: Sequence[Mapping[str, object]],
    candidate_rows: Sequence[Mapping[str, object]] | None = None,
    profile_version: str = STRATEGY_EVALUATION_PROFILE_VERSION,
    approved_universe_profile: str = APPROVED_UNIVERSE_PROFILE,
) -> list[dict[str, object]]:
    """Normalize post-backtest evidence into one strategy evaluation verdict.

    Ranking rows provide the research-quality view. Candidate rows enrich the
    same evaluation after projection; they do not create a second decision layer.
    """

    candidates = list(candidate_rows or ())
    by_ranking, by_series = _candidate_index(candidates)
    active_instruments = _active_instrument_counts(ranking_rows)
    generated_at = _created_at()

    profiles: list[dict[str, object]] = []
    for row in ranking_rows:
        matched_candidates = _matching_candidates(row, by_ranking, by_series)
        rank_reason = _coerce_json(row.get("rank_reason_json", {}))
        policy_failures = [
            str(item) for item in _coerce_list(row.get("policy_failure_reasons_json", []))
        ]
        validation_scheme = str(rank_reason.get("validation_scheme", "legacy_validation"))
        strict_validation = validation_scheme == "nested_walk_forward_v1"
        walk_forward_reoptimization_pass = bool(
            rank_reason.get("walk_forward_reoptimization_pass", False)
        )
        latest_frozen_param_confirmation_pass = bool(
            rank_reason.get("latest_frozen_param_confirmation_pass", False)
        )
        validation_allows_promotion = not strict_validation or (
            walk_forward_reoptimization_pass and latest_frozen_param_confirmation_pass
        )
        policy_pass = _as_bool(row.get("policy_pass", False))
        projection_eligible = _as_bool(row.get("qualifies_for_projection", False))
        has_candidate = bool(matched_candidates)
        has_levels = any(
            _present(candidate.get("entry_ref"))
            and _present(candidate.get("stop_ref"))
            and _present(candidate.get("target_ref"))
            for candidate in matched_candidates
        )
        has_cost_estimates = any(
            _present(candidate.get("estimated_commission"))
            and _present(candidate.get("estimated_slippage"))
            for candidate in matched_candidates
        )

        blockers: list[str] = []
        missing_data: list[str] = []
        if not policy_pass:
            _append_unique(blockers, "research_ranking_policy_failed")
            for reason in policy_failures:
                _append_unique(blockers, f"ranking_policy:{reason}")
        if policy_pass and not projection_eligible:
            _append_unique(blockers, "not_projection_eligible")
        if strict_validation and not walk_forward_reoptimization_pass:
            _append_unique(blockers, "walk_forward_reoptimization_failed")
        if strict_validation and not latest_frozen_param_confirmation_pass:
            _append_unique(blockers, "blind_confirmation_failed")
        if policy_pass and projection_eligible and not has_candidate:
            _append_unique(blockers, "missing_projected_candidate")
        if has_candidate and not has_levels:
            _append_unique(blockers, "missing_entry_stop_target")
        if has_candidate and not has_cost_estimates:
            _append_unique(blockers, "missing_cost_estimates")

        paper_signal_ready = (
            policy_pass
            and projection_eligible
            and validation_allows_promotion
            and has_candidate
            and has_levels
            and has_cost_estimates
        )

        risk_per_position = _first_float(
            row, matched_candidates, ("risk_per_position_pct", "risk_per_position")
        )
        gross_leverage = _first_float(row, matched_candidates, ("gross_leverage",))
        capital_model_present = _has_payload(
            row, matched_candidates, ("capital_model_json", "capital_model")
        )
        exposure_model_present = _has_payload(
            row, matched_candidates, ("exposure_model_json", "exposure_model")
        )
        no_trade_states_present = _has_list_payload(
            row, matched_candidates, ("no_trade_states_json", "no_trade_states")
        )

        if paper_signal_ready:
            if risk_per_position is None:
                _append_unique(blockers, "missing_risk_per_position")
                _append_unique(missing_data, "risk_per_position")
            if gross_leverage is None:
                _append_unique(blockers, "missing_gross_leverage")
                _append_unique(missing_data, "leverage")
            if not capital_model_present:
                _append_unique(blockers, "missing_capital_model")
                _append_unique(missing_data, "capital_model")
            if not exposure_model_present:
                _append_unique(blockers, "missing_exposure_model")
                _append_unique(missing_data, "exposure_model")
            if not no_trade_states_present:
                _append_unique(blockers, "missing_no_trade_states")
                _append_unique(missing_data, "no_trade_states")

        paper_trade_ready = (
            paper_signal_ready
            and risk_per_position is not None
            and gross_leverage is not None
            and capital_model_present
            and exposure_model_present
            and no_trade_states_present
        )

        active_instrument_count = active_instruments.get(_profile_group_key(row), 0)
        evaluation_window_years = _first_float(
            row, matched_candidates, ("evaluation_window_years", "window_years")
        )
        annual_return_pct = _first_float(row, matched_candidates, ("annual_return_pct",))
        sharpe_ratio = _first_float(row, matched_candidates, ("sharpe_ratio", "sharpe"))
        negative_months_4y = _first_float(
            row, matched_candidates, ("negative_months_4y", "losing_months_4y")
        )
        repeatable = _first_bool(row, matched_candidates, ("repeatable", "repeatability_pass"))
        max_drawdown = _first_float(row, matched_candidates, ("worst_max_drawdown", "max_drawdown"))

        if paper_trade_ready:
            if evaluation_window_years is None:
                _append_unique(blockers, "missing_4y_evaluation_window")
                _append_unique(missing_data, "evaluation_window_years")
            elif evaluation_window_years < 4.0:
                _append_unique(blockers, "live_candidate_requires_4y_window")
            if annual_return_pct is None:
                _append_unique(blockers, "missing_annual_return_pct")
                _append_unique(missing_data, "annual_return_pct")
            elif annual_return_pct <= 30.0:
                _append_unique(blockers, "live_candidate_annual_return_lte_30pct")
            if sharpe_ratio is None:
                _append_unique(blockers, "missing_sharpe_ratio")
                _append_unique(missing_data, "sharpe_ratio")
            elif sharpe_ratio < 1.0:
                _append_unique(blockers, "live_candidate_sharpe_lt_1")
            if negative_months_4y is None:
                _append_unique(blockers, "missing_monthly_return_series")
                _append_unique(missing_data, "monthly_return_series")
            elif negative_months_4y > 0:
                _append_unique(blockers, "live_candidate_losing_months")
            if repeatable is None:
                _append_unique(blockers, "missing_repeatability_fingerprint")
                _append_unique(missing_data, "repeatability_fingerprint")
            elif not repeatable:
                _append_unique(blockers, "live_candidate_not_repeatable")

        if paper_signal_ready:
            if not paper_trade_ready:
                _append_unique(blockers, "live_candidate_requires_paper_trade")
            if active_instrument_count < 3:
                _append_unique(blockers, "live_candidate_min_instruments")
                _append_unique(missing_data, "multi_instrument_breadth")
            if max_drawdown is None:
                _append_unique(blockers, "missing_max_drawdown")
                _append_unique(missing_data, "max_drawdown")
            elif max_drawdown > 0.20:
                _append_unique(blockers, "live_candidate_max_drawdown_gt_20pct")
            if gross_leverage is not None and gross_leverage > 1.2:
                _append_unique(blockers, "live_candidate_gross_leverage_gt_1_2")
            if risk_per_position is not None and risk_per_position > 0.015:
                _append_unique(blockers, "live_candidate_risk_per_position_gt_1_5pct")

        live_candidate_ready = (
            paper_trade_ready
            and active_instrument_count >= 3
            and evaluation_window_years is not None
            and evaluation_window_years >= 4.0
            and annual_return_pct is not None
            and annual_return_pct > 30.0
            and sharpe_ratio is not None
            and sharpe_ratio >= 1.0
            and negative_months_4y == 0
            and max_drawdown is not None
            and max_drawdown <= 0.20
            and gross_leverage is not None
            and gross_leverage <= 1.2
            and risk_per_position is not None
            and risk_per_position <= 0.015
            and repeatable is True
        )

        if live_candidate_ready:
            verdict = "live-candidate"
            promotion_state = "live_candidate"
        elif paper_trade_ready:
            verdict = "paper-trade"
            promotion_state = "paper_trade_ready"
        elif paper_signal_ready:
            verdict = "paper-signal"
            promotion_state = "paper_signal_ready"
        elif policy_pass:
            verdict = "research-only"
            promotion_state = "not_promoted"
        else:
            verdict = "reject"
            promotion_state = "rejected"

        profile_id = "SEP-" + _stable_hash(
            "|".join(
                (
                    profile_version,
                    str(row.get("campaign_run_id", "")),
                    str(row.get("ranking_id", "")),
                    str(row.get("strategy_instance_id", "")),
                    verdict,
                )
            )
        )
        profiles.append(
            {
                "evaluation_profile_id": profile_id,
                "profile_version": profile_version,
                "approved_universe_profile": approved_universe_profile,
                "campaign_run_id": str(row.get("campaign_run_id", "")),
                "ranking_id": str(row.get("ranking_id", "")),
                "backtest_run_id": str(row.get("backtest_run_id", "")),
                "strategy_instance_id": str(row.get("strategy_instance_id", "")),
                "strategy_template_id": str(row.get("strategy_template_id", "")),
                "family_id": str(row.get("family_id", "")),
                "family_key": str(row.get("family_key", "")),
                "strategy_version_label": str(row.get("strategy_version_label", "")),
                "contract_id": str(row.get("contract_id", "")),
                "instrument_id": str(row.get("instrument_id", "")),
                "timeframe": str(row.get("timeframe", "")),
                "params_hash": str(row.get("params_hash", "")),
                "ranking_policy_id": str(row.get("ranking_policy_id", "")),
                "policy_pass": bool(policy_pass),
                "qualifies_for_projection": bool(projection_eligible),
                "paper_signal_ready": bool(paper_signal_ready),
                "paper_trade_ready": bool(paper_trade_ready),
                "live_candidate_ready": bool(live_candidate_ready),
                "verdict": verdict,
                "promotion_state": promotion_state,
                "blocker_reasons_json": blockers if not live_candidate_ready else [],
                "missing_data_json": missing_data if not live_candidate_ready else [],
                "evidence_snapshot_json": {
                    "rank": row.get("rank"),
                    "score_total": row.get("score_total"),
                    "objective_score": row.get("objective_score"),
                    "trade_count_total": row.get("trade_count_total"),
                    "fold_count": rank_reason.get("fold_count"),
                    "positive_fold_ratio": rank_reason.get("positive_fold_ratio"),
                    "validation_scheme": validation_scheme,
                    "walk_forward_reoptimization_pass": walk_forward_reoptimization_pass,
                    "latest_frozen_param_confirmation_pass": latest_frozen_param_confirmation_pass,
                    "strict_confirmation_required": strict_validation,
                    "worst_max_drawdown": max_drawdown,
                    "candidate_count": len(matched_candidates),
                    "active_instrument_count": active_instrument_count,
                    "evaluation_window_years": evaluation_window_years,
                    "annual_return_pct": annual_return_pct,
                    "sharpe_ratio": sharpe_ratio,
                    "negative_months_4y": negative_months_4y,
                    "gross_leverage": gross_leverage,
                    "risk_per_position_pct": risk_per_position,
                    "repeatable": repeatable,
                    "policy_failure_reasons": policy_failures,
                    "candidate_ids": [
                        str(candidate.get("candidate_id", "")) for candidate in matched_candidates
                    ],
                },
                "created_at": generated_at,
            }
        )
    return profiles
