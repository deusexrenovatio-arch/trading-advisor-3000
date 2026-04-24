from __future__ import annotations

from trading_advisor_3000.product_plane.contracts import CanonicalBar, Timeframe
from trading_advisor_3000.product_plane.research.features import (
    build_feature_snapshots_from_indicators,
    build_feature_snapshots,
    build_indicator_snapshots,
    medium_term_timeframes,
    research_feature_store_contract,
    run_indicator_feature_quality_gates,
)
from trading_advisor_3000.product_plane.research.strategies import sample_strategy_ids


def _bar(*, timeframe: Timeframe, ts_open: str, close: float, volume: int) -> CanonicalBar:
    return CanonicalBar(
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe=timeframe,
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
        _bar(timeframe=Timeframe.M15, ts_open="2026-03-16T09:00:00Z", close=82.2, volume=1200),
        _bar(timeframe=Timeframe.M15, ts_open="2026-03-16T09:15:00Z", close=82.4, volume=1300),
        _bar(timeframe=Timeframe.M15, ts_open="2026-03-16T09:30:00Z", close=82.7, volume=1400),
    ]
    with_future = base_bars + [
        _bar(timeframe=Timeframe.M15, ts_open="2026-03-16T09:45:00Z", close=85.5, volume=5000),
    ]

    base_indicators = build_indicator_snapshots(
        base_bars,
        indicator_set_version="pandas-ta-v1",
        computed_at_utc="2026-03-16T10:00:00Z",
    )
    extended_indicators = build_indicator_snapshots(
        with_future,
        indicator_set_version="pandas-ta-v1",
        computed_at_utc="2026-03-16T10:00:00Z",
    )
    base = build_feature_snapshots(
        base_bars,
        feature_set_version="gold-v1",
        instrument_by_contract={"BR-6.26": "BR"},
        computed_at_utc="2026-03-16T10:00:00Z",
    )
    extended = build_feature_snapshots(
        with_future,
        feature_set_version="gold-v1",
        instrument_by_contract={"BR-6.26": "BR"},
        computed_at_utc="2026-03-16T10:00:00Z",
    )

    assert [item.to_dict() for item in base_indicators] == [
        item.to_dict() for item in extended_indicators[: len(base_indicators)]
    ]
    base_rows = [item.to_dict() for item in base]
    extended_rows = [item.to_dict() for item in extended[: len(base)]]
    assert base_rows == extended_rows


def test_feature_builder_can_scope_to_medium_term_timeframes() -> None:
    bars = [
        _bar(timeframe=Timeframe.M5, ts_open="2026-03-16T09:00:00Z", close=82.1, volume=900),
        _bar(timeframe=Timeframe.M15, ts_open="2026-03-16T09:00:00Z", close=82.2, volume=1200),
    ]

    snapshots = build_feature_snapshots(
        bars,
        feature_set_version="gold-v1",
        instrument_by_contract={"BR-6.26": "BR"},
        allowed_timeframes=set(medium_term_timeframes()),
    )

    assert snapshots
    assert all(item.timeframe in set(medium_term_timeframes()) for item in snapshots)
    assert all(item.timeframe != Timeframe.M5 for item in snapshots)


def test_pandasta_indicator_layer_builds_rich_indicators_and_features() -> None:
    bars = [
        _bar(timeframe=Timeframe.M5, ts_open=f"2026-03-16T09:{minute:02d}:00Z", close=82.0 + minute * 0.1, volume=900 + minute)
        for minute in range(12)
    ]

    indicators = build_indicator_snapshots(
        bars,
        indicator_set_version="pandas-ta-v1",
        computed_at_utc="2026-03-16T10:00:00Z",
    )
    snapshots = build_feature_snapshots_from_indicators(indicators, feature_set_version="gold-v1")
    qc_report = run_indicator_feature_quality_gates(indicators=indicators, features=snapshots)

    assert len(indicators) == len(bars)
    assert len(snapshots) == len(bars)
    latest = indicators[-1]
    assert latest.indicator_values_json["source"] == "pandas-ta-classic"
    assert latest.rsi_14 >= 0.0
    assert latest.rvol_20 >= 0.0
    latest_features = snapshots[-1].to_dict()
    assert "trend_score" in snapshots[-1].features_json
    assert latest_features["trend_score"] == snapshots[-1].features_json["trend_score"]
    assert latest_features["breakout_state"] == snapshots[-1].features_json["breakout_state"]
    assert qc_report["status"] == "PASS"


def test_feature_store_contract_contains_governed_research_delta_tables() -> None:
    manifest = research_feature_store_contract()
    assert {
        "technical_indicator_snapshot",
        "gold_feature_snapshot",
        "research_runtime_candidate_projection",
        "strategy_scorecard",
        "strategy_promotion_decision",
        "promoted_strategy_registry",
    } <= set(manifest)
    gold_columns = set(manifest["gold_feature_snapshot"]["columns"])
    assert {"trend_score", "momentum_score", "volatility_score", "breakout_state"} <= gold_columns
    assert all(item["format"] == "delta" for item in manifest.values())


def test_sample_strategies_are_declared() -> None:
    assert sample_strategy_ids() == (
        "breakout-volatility-v1",
        "mean-revert-v1",
        "trend-follow-v1",
    )
