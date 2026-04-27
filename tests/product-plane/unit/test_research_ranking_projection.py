from __future__ import annotations

from trading_advisor_3000.product_plane.research.backtests import RankingPolicy, default_ranking_policy, rank_backtest_results
from trading_advisor_3000.product_plane.research.backtests.projection import (
    CandidateProjectionRequest,
    _select_rows,
    supported_selection_policies,
)


def _run_row(
    *,
    run_id: str,
    params_hash: str,
    params_json: dict[str, object],
    window_id: str,
) -> dict[str, object]:
    return {
        "backtest_run_id": run_id,
        "backtest_batch_id": "BTBATCH-STAGE6",
        "strategy_version": "ma-cross-v1",
        "strategy_family": "ma_cross",
        "dataset_version": "dataset-v5",
        "indicator_set_version": "indicators-v1",
        "derived_indicator_set_version": "derived-v1",
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "15m",
        "window_id": window_id,
        "params_hash": params_hash,
        "params_json": params_json,
        "execution_mode": "signals",
        "engine_name": "vectorbt",
        "row_count": 12,
        "trade_count": 2,
        "started_at": "2026-03-16T09:00:00Z",
        "finished_at": "2026-03-16T11:45:00Z",
    }


def _stat_row(
    *,
    run_id: str,
    params_hash: str,
    window_id: str,
    total_return: float,
    sharpe: float,
    profit_factor: float,
    max_drawdown: float,
    trade_count: int,
) -> dict[str, object]:
    return {
        "backtest_run_id": run_id,
        "backtest_batch_id": "BTBATCH-STAGE6",
        "strategy_version": "ma-cross-v1",
        "strategy_family": "ma_cross",
        "dataset_version": "dataset-v5",
        "indicator_set_version": "indicators-v1",
        "derived_indicator_set_version": "derived-v1",
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "15m",
        "window_id": window_id,
        "params_hash": params_hash,
        "total_return": total_return,
        "annualized_return": total_return * 2.0,
        "sharpe": sharpe,
        "sortino": sharpe + 0.2,
        "calmar": 1.1,
        "max_drawdown": max_drawdown,
        "win_rate": 0.6,
        "profit_factor": profit_factor,
        "expectancy": 0.1,
        "trade_count": trade_count,
        "exposure": 0.4,
        "avg_trade_duration_bars": 3.0,
        "fees_total": 2.0,
        "slippage_total": 1.0,
        "created_at": "2026-03-16T12:00:00Z",
    }


def _trade_row(
    *,
    run_id: str,
    trade_id: str,
    pnl: float,
    entry_price: float = 100.0,
    exit_price: float = 102.0,
) -> dict[str, object]:
    return {
        "backtest_run_id": run_id,
        "backtest_batch_id": "BTBATCH-STAGE6",
        "strategy_version": "ma-cross-v1",
        "strategy_family": "ma_cross",
        "dataset_version": "dataset-v5",
        "indicator_set_version": "indicators-v1",
        "derived_indicator_set_version": "derived-v1",
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "15m",
        "window_id": "wf-01",
        "trade_id": trade_id,
        "position_id": 1,
        "direction": "long",
        "status": "closed",
        "entry_ts": "2026-03-16T09:15:00Z",
        "exit_ts": "2026-03-16T09:45:00Z",
        "entry_price": entry_price,
        "exit_price": exit_price,
        "size": 1.0,
        "pnl": pnl,
        "return": pnl / entry_price,
        "fees_total": 0.2,
        "duration_bars": 2,
    }


def test_default_ranking_policy_is_declared_for_stage6() -> None:
    policy = default_ranking_policy()
    assert policy.policy_id == "robust_oos_v1"
    assert policy.metric_order == ("total_return", "profit_factor", "max_drawdown")


def test_ranking_orders_robust_parameter_sets_and_marks_weak_ones() -> None:
    run_rows = [
        _run_row(run_id="RUN-A-1", params_hash="PA", params_json={"fast_window": 10, "slow_window": 20}, window_id="wf-01"),
        _run_row(run_id="RUN-A-2", params_hash="PA", params_json={"fast_window": 10, "slow_window": 20}, window_id="wf-02"),
        _run_row(run_id="RUN-B-1", params_hash="PB", params_json={"fast_window": 20, "slow_window": 20}, window_id="wf-01"),
        _run_row(run_id="RUN-B-2", params_hash="PB", params_json={"fast_window": 20, "slow_window": 20}, window_id="wf-02"),
        _run_row(run_id="RUN-C-1", params_hash="PC", params_json={"fast_window": 10, "slow_window": 50}, window_id="wf-01"),
        _run_row(run_id="RUN-C-2", params_hash="PC", params_json={"fast_window": 10, "slow_window": 50}, window_id="wf-02"),
    ]
    stat_rows = [
        _stat_row(run_id="RUN-A-1", params_hash="PA", window_id="wf-01", total_return=0.12, sharpe=1.6, profit_factor=1.8, max_drawdown=0.10, trade_count=3),
        _stat_row(run_id="RUN-A-2", params_hash="PA", window_id="wf-02", total_return=0.08, sharpe=1.4, profit_factor=1.7, max_drawdown=0.12, trade_count=4),
        _stat_row(run_id="RUN-B-1", params_hash="PB", window_id="wf-01", total_return=-0.03, sharpe=0.4, profit_factor=0.9, max_drawdown=0.42, trade_count=1),
        _stat_row(run_id="RUN-B-2", params_hash="PB", window_id="wf-02", total_return=0.01, sharpe=0.5, profit_factor=0.95, max_drawdown=0.39, trade_count=1),
        _stat_row(run_id="RUN-C-1", params_hash="PC", window_id="wf-01", total_return=0.10, sharpe=1.3, profit_factor=1.5, max_drawdown=0.14, trade_count=3),
        _stat_row(run_id="RUN-C-2", params_hash="PC", window_id="wf-02", total_return=0.07, sharpe=1.2, profit_factor=1.4, max_drawdown=0.16, trade_count=3),
    ]
    trade_rows = [
        _trade_row(run_id="RUN-A-1", trade_id="TRD-A1", pnl=120.0),
        _trade_row(run_id="RUN-A-1", trade_id="TRD-A2", pnl=80.0),
        _trade_row(run_id="RUN-A-2", trade_id="TRD-A3", pnl=70.0),
        _trade_row(run_id="RUN-A-2", trade_id="TRD-A4", pnl=65.0),
        _trade_row(run_id="RUN-B-1", trade_id="TRD-B1", pnl=-40.0, entry_price=100.0, exit_price=99.2),
        _trade_row(run_id="RUN-B-2", trade_id="TRD-B2", pnl=5.0, entry_price=100.0, exit_price=100.1),
        _trade_row(run_id="RUN-C-1", trade_id="TRD-C1", pnl=90.0),
        _trade_row(run_id="RUN-C-1", trade_id="TRD-C2", pnl=70.0),
        _trade_row(run_id="RUN-C-2", trade_id="TRD-C3", pnl=60.0),
        _trade_row(run_id="RUN-C-2", trade_id="TRD-C4", pnl=55.0),
    ]
    policy = RankingPolicy(
        policy_id="stage6-test-policy",
        metric_order=("total_return", "profit_factor", "max_drawdown"),
        min_trade_count=2,
        max_drawdown_cap=0.35,
        min_positive_fold_ratio=0.5,
        min_parameter_stability=0.0,
        min_slippage_score=0.0,
    )

    report = rank_backtest_results(
        batch_rows=[{"backtest_batch_id": "BTBATCH-STAGE6"}],
        run_rows=run_rows,
        stat_rows=stat_rows,
        trade_rows=trade_rows,
        policy=policy,
    )

    ranking_rows = sorted(report["ranking_rows"], key=lambda row: row["selected_rank"])
    finding_rows = report["finding_rows"]
    assert len(ranking_rows) == 3
    assert ranking_rows[0]["policy_pass"] == 1
    assert ranking_rows[0]["robust_score"] > ranking_rows[1]["robust_score"]
    weak = next(row for row in ranking_rows if row["params_hash"] == "PB")
    assert weak["policy_pass"] == 0
    assert weak["worst_max_drawdown"] > policy.max_drawdown_cap
    assert any(row["strategy_instance_id"] == weak["strategy_instance_id"] for row in finding_rows)
    assert {row["finding_type"] for row in finding_rows} == {"ranking_policy_reject"}
    stable = next(row for row in ranking_rows if row["params_hash"] == "PA")
    assert 0.0 <= stable["parameter_stability_score"] <= 1.0


def test_metric_order_really_changes_ranking_priority() -> None:
    run_rows = [
        _run_row(run_id="RUN-R-1", params_hash="PA", params_json={"fast_window": 10}, window_id="wf-01"),
        _run_row(run_id="RUN-R-2", params_hash="PB", params_json={"fast_window": 20}, window_id="wf-01"),
    ]
    stat_rows = [
        _stat_row(run_id="RUN-R-1", params_hash="PA", window_id="wf-01", total_return=0.14, sharpe=1.0, profit_factor=1.05, max_drawdown=0.16, trade_count=3),
        _stat_row(run_id="RUN-R-2", params_hash="PB", window_id="wf-01", total_return=0.09, sharpe=1.0, profit_factor=1.90, max_drawdown=0.10, trade_count=3),
    ]
    trade_rows = [
        _trade_row(run_id="RUN-R-1", trade_id="TRD-R1", pnl=70.0),
        _trade_row(run_id="RUN-R-2", trade_id="TRD-R2", pnl=60.0),
    ]
    return_first = RankingPolicy(
        policy_id="return-first",
        metric_order=("total_return", "profit_factor", "max_drawdown"),
        min_trade_count=1,
        max_drawdown_cap=0.5,
        min_positive_fold_ratio=0.0,
        min_parameter_stability=0.0,
        min_slippage_score=0.0,
    )
    pf_first = RankingPolicy(
        policy_id="pf-first",
        metric_order=("profit_factor", "total_return", "max_drawdown"),
        min_trade_count=1,
        max_drawdown_cap=0.5,
        min_positive_fold_ratio=0.0,
        min_parameter_stability=0.0,
        min_slippage_score=0.0,
    )

    return_ranked = rank_backtest_results(
        batch_rows=[{"backtest_batch_id": "BTBATCH-STAGE6"}],
        run_rows=run_rows,
        stat_rows=stat_rows,
        trade_rows=trade_rows,
        policy=return_first,
    )["ranking_rows"]
    pf_ranked = rank_backtest_results(
        batch_rows=[{"backtest_batch_id": "BTBATCH-STAGE6"}],
        run_rows=run_rows,
        stat_rows=stat_rows,
        trade_rows=trade_rows,
        policy=pf_first,
    )["ranking_rows"]

    top_return = min(return_ranked, key=lambda row: row["selected_rank"])
    top_pf = min(pf_ranked, key=lambda row: row["selected_rank"])
    assert top_return["policy_metric_order_json"] != top_pf["policy_metric_order_json"]
    assert top_return["objective_score"] != top_pf["objective_score"]


def test_ranking_keeps_family_first_survivors_before_cross_family_selection() -> None:
    run_rows: list[dict[str, object]] = []
    stat_rows: list[dict[str, object]] = []
    for run_id, label, family, params_hash, total_return in (
        ("RUN-MA-A", "ma-cross-v1", "ma_cross", "MA-A", 0.14),
        ("RUN-MA-B", "ma-cross-v1", "ma_cross", "MA-B", 0.12),
        ("RUN-BR-A", "breakout-v1", "breakout", "BR-A", 0.08),
    ):
        run_rows.append(
            {
                **_run_row(run_id=run_id, params_hash=params_hash, params_json={"slot": params_hash}, window_id="wf-01"),
                "strategy_version_label": label,
                "family_key": family,
                "strategy_template_id": f"stpl_{family}",
            }
        )
        stat_rows.append(
            {
                **_stat_row(
                    run_id=run_id,
                    params_hash=params_hash,
                    window_id="wf-01",
                    total_return=total_return,
                    sharpe=1.2,
                    profit_factor=1.4,
                    max_drawdown=0.10,
                    trade_count=4,
                ),
                "strategy_version_label": label,
                "family_key": family,
                "strategy_template_id": f"stpl_{family}",
            }
        )
    report = rank_backtest_results(
        batch_rows=[{"backtest_batch_id": "BTBATCH-STAGE6"}],
        run_rows=run_rows,
        stat_rows=stat_rows,
        trade_rows=[_trade_row(run_id=row["backtest_run_id"], trade_id=f"TRD-{row['backtest_run_id']}", pnl=50.0) for row in run_rows],
        policy=RankingPolicy(
            policy_id="family-first-test",
            metric_order=("total_return", "profit_factor", "max_drawdown"),
            min_trade_count=1,
            max_drawdown_cap=0.5,
            min_positive_fold_ratio=0.0,
            min_parameter_stability=0.0,
            min_slippage_score=0.0,
        ),
    )

    ranked = sorted(report["ranking_rows"], key=lambda row: row["selected_rank"])
    assert [row["family_rank"] for row in ranked] == [1, 1, 2]
    assert ranked[1]["family_key"] == "breakout"
    assert ranked[2]["params_hash"] == "MA-B"


def test_projection_selection_policy_really_changes_selected_rows() -> None:
    rows = [
        {
            "ranking_policy_id": "robust_oos_v1",
            "policy_pass": 1,
            "robust_score": 0.80,
            "policy_metric_score": 0.60,
            "selected_rank": 1,
            "strategy_family": "ma_cross",
            "representative_backtest_run_id": "RUN-A",
            "dataset_version": "dataset-v5",
            "contract_id": "BR-6.26",
            "timeframe": "15m",
        },
        {
            "ranking_policy_id": "robust_oos_v1",
            "policy_pass": 1,
            "robust_score": 0.66,
            "policy_metric_score": 0.92,
            "selected_rank": 2,
            "strategy_family": "breakout",
            "representative_backtest_run_id": "RUN-B",
            "dataset_version": "dataset-v5",
            "contract_id": "BR-6.26",
            "timeframe": "15m",
        },
        {
            "ranking_policy_id": "robust_oos_v1",
            "policy_pass": 1,
            "robust_score": 0.64,
            "policy_metric_score": 0.58,
            "selected_rank": 3,
            "strategy_family": "ma_cross",
            "representative_backtest_run_id": "RUN-C",
            "dataset_version": "dataset-v5",
            "contract_id": "BR-6.26",
            "timeframe": "15m",
        },
    ]

    robust_selected = _select_rows(
        rows,
        request=CandidateProjectionRequest(selection_policy="top_robust_per_series"),
    )
    policy_selected = _select_rows(
        rows,
        request=CandidateProjectionRequest(selection_policy="top_policy_per_series"),
    )
    family_selected = _select_rows(
        rows,
        request=CandidateProjectionRequest(selection_policy="top_by_family_per_series", max_candidates_per_partition=2),
    )
    all_selected = _select_rows(
        rows,
        request=CandidateProjectionRequest(selection_policy="all_policy_pass"),
    )

    assert supported_selection_policies() == (
        "top_robust_per_series",
        "top_policy_per_series",
        "top_by_family_per_series",
        "all_policy_pass",
    )
    assert [row["representative_backtest_run_id"] for row in robust_selected] == ["RUN-A"]
    assert [row["representative_backtest_run_id"] for row in policy_selected] == ["RUN-B"]
    assert {row["strategy_family"] for row in family_selected} == {"ma_cross", "breakout"}
    assert len(all_selected) == 3
