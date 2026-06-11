from __future__ import annotations

from trading_advisor_3000.product_plane.research.backtests.evaluation import (
    build_strategy_evaluation_profiles,
)


def _ranking_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ranking_id": "RANK-1",
        "campaign_run_id": "CRUN-1",
        "backtest_run_id": "RUN-1",
        "strategy_instance_id": "SINST-1",
        "strategy_template_id": "STPL-1",
        "family_id": "SFAM-1",
        "family_key": "trend_mtf_pullback_v1",
        "strategy_version_label": "trend-mtf-pullback-v1",
        "dataset_version": "dataset-v1",
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "15m",
        "rank": 1,
        "score_total": 0.82,
        "objective_score": 0.76,
        "ranking_policy_id": "research_screen_strict_v1",
        "qualifies_for_projection": True,
        "policy_pass": 1,
        "policy_failure_reasons_json": [],
        "params_hash": "PARAM-1",
        "mean_total_return": 0.12,
        "trade_count_total": 24,
        "worst_max_drawdown": 0.12,
        "rank_reason_json": {
            "fold_count": 4,
            "positive_fold_ratio": 1.0,
            "policy_scores": {
                "parameter_stability_score": 0.8,
                "slippage_sensitivity_score": 0.9,
            },
        },
    }
    row.update(overrides)
    return row


def _candidate_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "candidate_id": "CAND-1",
        "campaign_run_id": "CRUN-1",
        "ranking_id": "RANK-1",
        "backtest_run_id": "RUN-1",
        "strategy_instance_id": "SINST-1",
        "strategy_template_id": "STPL-1",
        "family_id": "SFAM-1",
        "family_key": "trend_mtf_pullback_v1",
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "15m",
        "ts_signal": "2026-04-29T12:00:00Z",
        "side": "long",
        "entry_ref": 100.0,
        "stop_ref": 98.5,
        "target_ref": 104.0,
        "score": 0.78,
        "estimated_commission": 0.0,
        "estimated_slippage": 0.0,
    }
    row.update(overrides)
    return row


def test_policy_pass_only_unlocks_paper_signal_not_live_candidate() -> None:
    profiles = build_strategy_evaluation_profiles(
        ranking_rows=[
            _ranking_row(
                worst_max_drawdown=0.24,
                trade_count_total=40,
            )
        ],
        candidate_rows=[_candidate_row()],
    )

    profile = profiles[0]
    blockers = set(profile["blocker_reasons_json"])
    assert profile["verdict"] == "paper-signal"
    assert profile["promotion_state"] == "paper_signal_ready"
    assert profile["paper_signal_ready"] is True
    assert profile["paper_trade_ready"] is False
    assert profile["live_candidate_ready"] is False
    assert "missing_risk_per_position" in blockers
    assert "live_candidate_max_drawdown_gt_20pct" in blockers
    assert "live_candidate_min_instruments" in blockers


def test_projection_eligible_without_candidate_stays_research_only() -> None:
    profiles = build_strategy_evaluation_profiles(ranking_rows=[_ranking_row()])

    profile = profiles[0]
    assert profile["verdict"] == "research-only"
    assert profile["promotion_state"] == "not_promoted"
    assert profile["paper_signal_ready"] is False
    assert "missing_projected_candidate" in profile["blocker_reasons_json"]


def test_nested_confirmation_failure_blocks_paper_signal_readiness() -> None:
    profiles = build_strategy_evaluation_profiles(
        ranking_rows=[
            _ranking_row(
                rank_reason_json={
                    "validation_scheme": "nested_walk_forward_v1",
                    "fold_count": 4,
                    "positive_fold_ratio": 1.0,
                    "walk_forward_reoptimization_pass": True,
                    "latest_frozen_param_confirmation_pass": False,
                },
            )
        ],
        candidate_rows=[_candidate_row()],
    )

    profile = profiles[0]
    evidence = profile["evidence_snapshot_json"]
    assert profile["verdict"] == "research-only"
    assert profile["paper_signal_ready"] is False
    assert "blind_confirmation_failed" in profile["blocker_reasons_json"]
    assert evidence["walk_forward_reoptimization_pass"] is True
    assert evidence["latest_frozen_param_confirmation_pass"] is False
    assert evidence["strict_confirmation_required"] is True


def test_reject_reports_research_policy_blockers_without_live_candidate_noise() -> None:
    profiles = build_strategy_evaluation_profiles(
        ranking_rows=[
            _ranking_row(
                policy_pass=False,
                qualifies_for_projection=False,
                policy_failure_reasons_json=["min_fold_count"],
                worst_max_drawdown=0.42,
            )
        ]
    )

    profile = profiles[0]
    blockers = set(profile["blocker_reasons_json"])
    assert profile["verdict"] == "reject"
    assert "research_ranking_policy_failed" in blockers
    assert "ranking_policy:min_fold_count" in blockers
    assert "live_candidate_min_instruments" not in blockers
    assert "live_candidate_max_drawdown_gt_20pct" not in blockers


def test_live_candidate_requires_profile_metrics_and_three_instruments() -> None:
    ranking_rows = [
        _ranking_row(
            ranking_id=f"RANK-{instrument}",
            strategy_instance_id=f"SINST-{instrument}",
            contract_id=f"{instrument}-6.26",
            instrument_id=instrument,
            annual_return_pct=35.0,
            sharpe_ratio=1.2,
            negative_months_4y=0,
            evaluation_window_years=4.0,
            gross_leverage=1.1,
            risk_per_position_pct=0.012,
            repeatable=True,
            no_trade_states_json=["stale_data", "market_closed"],
        )
        for instrument in ("BR", "Si", "RI")
    ]
    candidate_rows = [
        _candidate_row(
            ranking_id=str(row["ranking_id"]),
            strategy_instance_id=str(row["strategy_instance_id"]),
            contract_id=str(row["contract_id"]),
            instrument_id=str(row["instrument_id"]),
            risk_per_position_pct=0.012,
            gross_leverage=1.1,
            capital_model_json={"capital_rub": 1_000_000},
            exposure_model_json={"max_gross_leverage": 1.2},
        )
        for row in ranking_rows
    ]

    profiles = build_strategy_evaluation_profiles(
        ranking_rows=ranking_rows,
        candidate_rows=candidate_rows,
    )

    assert {profile["verdict"] for profile in profiles} == {"live-candidate"}
    assert all(profile["live_candidate_ready"] is True for profile in profiles)
    assert all(profile["blocker_reasons_json"] == [] for profile in profiles)
