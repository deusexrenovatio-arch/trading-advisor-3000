from __future__ import annotations

from pathlib import Path

import yaml

from trading_advisor_3000.product_plane.research.scoring import (
    build_strategy_promotion_decisions,
    build_strategy_scorecards,
    load_strategy_scoring_profile,
)


def _projection_rows(*, strategy_version_id: str, count: int, score: float, assets: tuple[str, ...]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(count):
        instrument_id = assets[index % len(assets)]
        rows.append(
            {
                "candidate_projection_id": f"CP-{strategy_version_id}-{index}",
                "candidate_id": f"CAND-{strategy_version_id}-{index}",
                "backtest_run_id": f"BTRUN-{strategy_version_id}",
                "strategy_version_id": strategy_version_id,
                "strategy_family": "trend-following",
                "contract_id": f"{instrument_id}-9.26",
                "instrument_id": instrument_id,
                "timeframe": "1d",
                "ts_signal": f"2026-03-{(index % 27) + 1:02d}T00:00:00Z",
                "side": "long",
                "entry_ref": 100.0,
                "stop_ref": 99.0,
                "target_ref": 102.0,
                "score": score,
                "window_id": "wf-1",
                "estimated_commission": 0.25,
                "estimated_slippage": 0.1,
                "capital_rub": 1_000_000,
                "reproducibility_fingerprint": "RFP-test",
            }
        )
    return rows


def test_load_strategy_scoring_profile_from_versioned_config() -> None:
    profile = load_strategy_scoring_profile(Path("configs/moex_canonicalization/strategy_scoring_profile.v1.yaml"))
    assert profile.profile_version == "strategy-scoring-profile.v1"
    assert profile.capital_rub == 1_000_000
    assert "1d" in profile.research_timeframes
    assert "5m" not in profile.research_timeframes


def test_scoring_profile_rejects_non_medium_timeframe_profile(tmp_path: Path) -> None:
    payload = {
        "profile_version": "strategy-scoring-profile.v1",
        "capital_rub": 1_000_000,
        "research_timeframes": ["5m", "15m", "1h", "4h", "1d", "1w"],
        "strategy_families": ["trend-following", "mean-reversion", "breakout-volatility"],
        "criteria": {
            "min_annual_return_pct": 30.0,
            "max_negative_months_4y": 0,
            "min_active_assets_count": 2,
            "min_signals_per_week": 2.0,
            "require_repeatable": True,
            "min_sharpe_ratio": 1.2,
            "max_drawdown_pct": 25.0,
        },
    }
    path = tmp_path / "profile.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    try:
        load_strategy_scoring_profile(path)
    except ValueError as exc:
        assert "must not include `5m`" in str(exc)
    else:
        raise AssertionError("expected ValueError for non-medium-timeframe profile")


def test_scorecard_and_promotion_decision_apply_governed_criteria() -> None:
    profile = load_strategy_scoring_profile(Path("configs/moex_canonicalization/strategy_scoring_profile.v1.yaml"))

    passing_rows = _projection_rows(
        strategy_version_id="trend-follow-v1",
        count=460,
        score=0.86,
        assets=("BR", "Si", "RI"),
    )
    blocked_rows = _projection_rows(
        strategy_version_id="mean-revert-v1",
        count=80,
        score=0.32,
        assets=("BR",),
    )

    scorecards = build_strategy_scorecards(
        projection_rows=passing_rows + blocked_rows,
        profile=profile,
        repeatable=True,
    )
    by_strategy = {row["strategy_version_id"]: row for row in scorecards}

    assert by_strategy["trend-follow-v1"]["verdict"] == "PASS"
    assert by_strategy["mean-revert-v1"]["verdict"] == "BLOCKED"
    assert "frequency_pass" in by_strategy["mean-revert-v1"]["blocked_reasons"]

    decisions = build_strategy_promotion_decisions(scorecards)
    decision_by_strategy = {row["strategy_version_id"]: row for row in decisions}
    assert decision_by_strategy["trend-follow-v1"]["promotion_state"] == "PROMOTED"
    assert decision_by_strategy["mean-revert-v1"]["promotion_state"] == "REJECTED"
