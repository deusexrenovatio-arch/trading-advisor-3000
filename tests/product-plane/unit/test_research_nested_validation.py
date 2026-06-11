from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.research.validation import (
    build_nested_walk_forward_plan,
)


def test_nested_walk_forward_plan_uses_quarterly_blind_confirmation() -> None:
    plan = build_nested_walk_forward_plan(
        start_ts="2021-04-01T00:00:00Z",
        end_ts="2026-03-31T23:59:59Z",
        backtest_timeframe="15m",
        warmup_bars=300,
        purge_bars=32,
        embargo_bars=16,
    )

    confirmation = [row for row in plan["windows"] if row["fold_role"] == "confirmation"]
    optimization = [row for row in plan["windows"] if row["fold_role"] == "optimization_validation"]

    assert len(confirmation) == 14
    assert len(optimization) == 42
    assert {row["optimizer_visible"] for row in optimization} == {True}
    assert {row["optimizer_visible"] for row in confirmation} == {False}
    assert confirmation[0]["outer_fold_id"] == "outer-01"
    assert confirmation[0]["analysis_start_ts"] == "2021-04-01T00:00:00Z"
    assert confirmation[0]["analysis_end_ts"] == "2022-10-01T00:00:00Z"
    assert confirmation[0]["score_start_ts"] == confirmation[0]["purge_end_ts"]
    assert confirmation[0]["score_end_ts"] == "2023-01-01T00:00:00Z"
    assert confirmation[-1]["outer_fold_id"] == "outer-14"
    assert confirmation[-1]["score_end_ts"] == "2026-04-01T00:00:00Z"


def test_nested_walk_forward_plan_rejects_missing_leakage_controls() -> None:
    with pytest.raises(ValueError, match="purge_bars"):
        build_nested_walk_forward_plan(
            start_ts="2021-04-01T00:00:00Z",
            end_ts="2026-03-31T23:59:59Z",
            backtest_timeframe="15m",
            warmup_bars=300,
            purge_bars=None,
            embargo_bars=16,
        )


def test_nested_walk_forward_plan_requires_enough_history() -> None:
    with pytest.raises(ValueError, match="insufficient history"):
        build_nested_walk_forward_plan(
            start_ts="2021-01-01T00:00:00Z",
            end_ts="2021-12-31T23:59:59Z",
            backtest_timeframe="15m",
            warmup_bars=300,
            purge_bars=32,
            embargo_bars=16,
        )
