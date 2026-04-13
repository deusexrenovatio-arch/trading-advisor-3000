from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.research.features import build_feature_snapshots, phase2b_feature_store_contract
from trading_advisor_3000.product_plane.research.strategies import sample_strategy_ids


def _bar(*, ts_open: str, ts_close: str, close: float, volume: int) -> CanonicalBar:
    return CanonicalBar(
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe=Timeframe.M15,
        ts=ts_open,
        open=close - 0.2,
        high=close + 0.3,
        low=close - 0.4,
        close=close,
        volume=volume,
        open_interest=20000,
    )


def test_point_in_time_features_do_not_use_future_bar() -> None:
    base_bars = [
        _bar(ts_open="2026-03-16T09:00:00Z", ts_close="2026-03-16T09:15:00Z", close=82.2, volume=1200),
        _bar(ts_open="2026-03-16T09:15:00Z", ts_close="2026-03-16T09:30:00Z", close=82.4, volume=1300),
        _bar(ts_open="2026-03-16T09:30:00Z", ts_close="2026-03-16T09:45:00Z", close=82.7, volume=1400),
    ]
    with_future = base_bars + [
        _bar(ts_open="2026-03-16T09:45:00Z", ts_close="2026-03-16T10:00:00Z", close=85.5, volume=5000),
    ]

    base = build_feature_snapshots(
        base_bars,
        feature_set_version="feature-set-v1",
        instrument_by_contract={"BR-6.26": "BR"},
    )
    extended = build_feature_snapshots(
        with_future,
        feature_set_version="feature-set-v1",
        instrument_by_contract={"BR-6.26": "BR"},
    )

    base_rows = [item.to_dict() for item in base]
    extended_rows = [item.to_dict() for item in extended[: len(base)]]
    assert base_rows == extended_rows


def test_feature_store_contract_contains_research_delta_tables() -> None:
    manifest = phase2b_feature_store_contract()
    assert {"research_feature_frames", "feature_snapshots", "research_backtest_runs", "research_signal_candidates"} <= set(manifest)
    assert all(item["format"] == "delta" for item in manifest.values())


def test_sample_strategies_are_declared() -> None:
    assert sample_strategy_ids() == ("mean-revert-v1", "trend-follow-v1")
