from __future__ import annotations

from trading_advisor_3000.app.runtime.analytics.review import (
    build_instrument_metrics_dashboard,
    build_phase5_review_report,
    build_strategy_metrics_dashboard,
)


def _outcome(
    *,
    signal_id: str,
    closed_at: str,
    pnl_r: float,
    mfe_r: float,
    mae_r: float,
    strategy_version_id: str = "trend-follow-v1",
    contract_id: str = "BR-6.26",
    mode: str = "shadow",
) -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "strategy_version_id": strategy_version_id,
        "contract_id": contract_id,
        "mode": mode,
        "opened_at": "2026-03-17T10:00:00Z",
        "closed_at": closed_at,
        "pnl_r": pnl_r,
        "mfe_r": mfe_r,
        "mae_r": mae_r,
        "close_reason": "closed_profit" if pnl_r > 0 else "closed_loss",
    }


def test_phase5_strategy_and_instrument_dashboards_compute_r_metrics() -> None:
    outcomes = [
        _outcome(signal_id="SIG-1", closed_at="2026-03-17T10:10:00Z", pnl_r=1.0, mfe_r=1.5, mae_r=-0.2),
        _outcome(signal_id="SIG-2", closed_at="2026-03-17T10:20:00Z", pnl_r=-0.5, mfe_r=0.4, mae_r=-0.8),
        _outcome(signal_id="SIG-3", closed_at="2026-03-17T10:30:00Z", pnl_r=2.0, mfe_r=2.2, mae_r=-0.1),
    ]

    strategy_rows = build_strategy_metrics_dashboard(outcomes)
    instrument_rows = build_instrument_metrics_dashboard(outcomes)

    assert len(strategy_rows) == 1
    strategy = strategy_rows[0]
    assert strategy.signals_count == 3
    assert strategy.wins_count == 2
    assert strategy.losses_count == 1
    assert round(strategy.win_rate, 6) == round(2 / 3, 6)
    assert round(strategy.sum_r, 6) == 2.5
    assert round(strategy.avg_r, 6) == round(2.5 / 3, 6)
    assert round(strategy.max_dd_r, 6) == 0.5

    assert len(instrument_rows) == 1
    instrument = instrument_rows[0]
    assert instrument.instrument_id == "BR"
    assert instrument.contract_id == "BR-6.26"
    assert instrument.signals_count == 3
    assert round(instrument.sum_r, 6) == 2.5


def test_phase5_review_report_handles_empty_outcomes() -> None:
    report = build_phase5_review_report(outcomes=[], signal_events=[])
    assert report.strategy_dashboard == []
    assert report.instrument_dashboard == []
    assert report.summary["strategy_rows"] == 0
    assert report.summary["instrument_rows"] == 0
    assert report.summary["latency_rows"] == 0
