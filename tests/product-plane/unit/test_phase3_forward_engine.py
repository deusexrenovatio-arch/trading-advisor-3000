from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import (
    CanonicalBar,
    DecisionCandidate,
    FeatureSnapshotRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.product_plane.research.forward import build_forward_observations


def _bar(*, ts: str, close: float, high: float, low: float) -> CanonicalBar:
    return CanonicalBar(
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe=Timeframe.M15,
        ts=ts,
        open=close,
        high=high,
        low=low,
        close=close,
        volume=1000,
        open_interest=21000,
    )


def _candidate(*, signal_id: str, side: TradeSide, ts_decision: str) -> DecisionCandidate:
    return DecisionCandidate(
        signal_id=signal_id,
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="trend-follow-v1",
        mode=Mode.SHADOW,
        side=side,
        entry_ref=100.0,
        stop_ref=99.0 if side == TradeSide.LONG else 101.0,
        target_ref=102.0 if side == TradeSide.LONG else 98.0,
        confidence=0.7,
        ts_decision=ts_decision,
        feature_snapshot=FeatureSnapshotRef(
            dataset_version="bars-whitelist-v1",
            snapshot_id="FS-PHASE3-0001",
        ),
    )


def test_forward_engine_computes_long_and_short_r_metrics() -> None:
    bars = [
        _bar(ts="2026-03-16T09:00:00Z", close=100.0, high=101.0, low=99.5),
        _bar(ts="2026-03-16T09:15:00Z", close=102.0, high=103.0, low=99.0),
        _bar(ts="2026-03-16T09:30:00Z", close=103.0, high=104.0, low=101.0),
    ]
    observations = build_forward_observations(
        candidates=[
            _candidate(signal_id="SIG-LONG", side=TradeSide.LONG, ts_decision="2026-03-16T09:00:00Z"),
            _candidate(signal_id="SIG-SHORT", side=TradeSide.SHORT, ts_decision="2026-03-16T09:00:00Z"),
        ],
        bars=bars,
        horizon_bars=2,
        risk_unit_fraction=0.01,
    )

    long_row = next(row for row in observations if row.result_state == "closed_profit")
    short_row = next(row for row in observations if row.result_state == "closed_loss")

    assert round(long_row.pnl_r, 6) == 3.0
    assert round(long_row.mfe_r, 6) == 4.0
    assert round(long_row.mae_r, 6) == -1.0

    assert round(short_row.pnl_r, 6) == -3.0
    assert round(short_row.mfe_r, 6) == 1.0
    assert round(short_row.mae_r, 6) == -4.0


def test_forward_engine_marks_no_forward_window_without_future_bars() -> None:
    bars = [_bar(ts="2026-03-16T09:30:00Z", close=103.0, high=104.0, low=101.0)]
    observation = build_forward_observations(
        candidates=[_candidate(signal_id="SIG-LAST", side=TradeSide.LONG, ts_decision="2026-03-16T09:30:00Z")],
        bars=bars,
        horizon_bars=2,
    )[0]

    assert observation.result_state == "no_forward_window"
    assert observation.pnl_r == 0.0
    assert observation.mfe_r == 0.0
    assert observation.mae_r == 0.0
