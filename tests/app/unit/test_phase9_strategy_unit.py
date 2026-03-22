from __future__ import annotations

from trading_advisor_3000.app.contracts import Timeframe, TradeSide
from trading_advisor_3000.app.research.features.snapshot import FeatureSnapshot
from trading_advisor_3000.app.research.strategies import (
    PHASE9_FEATURE_SET_VERSION,
    PHASE9_PRODUCTION_STRATEGY_ID,
    assess_phase9_production_pilot_readiness,
    evaluate_strategy,
    phase9_production_backtest_config,
    phase9_production_strategy_spec,
    production_strategy_ids,
)


def _snapshot(
    *,
    contract_id: str,
    instrument_id: str,
    ts: str,
    atr: float,
    ema_fast: float,
    ema_slow: float,
    donchian_high: float,
    donchian_low: float,
    rvol: float,
    last_close: float,
) -> FeatureSnapshot:
    return FeatureSnapshot(
        snapshot_id=f"FS-{contract_id}-{ts}",
        contract_id=contract_id,
        instrument_id=instrument_id,
        timeframe=Timeframe.M15,
        ts=ts,
        feature_set_version=PHASE9_FEATURE_SET_VERSION,
        regime="trend",
        atr=atr,
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        donchian_high=donchian_high,
        donchian_low=donchian_low,
        rvol=rvol,
        features_json={
            "atr": atr,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
            "donchian_high": donchian_high,
            "donchian_low": donchian_low,
            "rvol": rvol,
            "last_close": last_close,
        },
    )


def test_phase9_production_strategy_freezes_sources_and_backtest_defaults() -> None:
    spec = phase9_production_strategy_spec()

    assert production_strategy_ids() == (PHASE9_PRODUCTION_STRATEGY_ID,)
    assert spec.historical_source == "MOEX"
    assert spec.live_feed == "QUIK"
    assert spec.feature_set_version == PHASE9_FEATURE_SET_VERSION
    assert spec.allowed_modes == ("shadow",)
    assert phase9_production_backtest_config() == {
        "walk_forward_windows": 2,
        "commission_per_trade": 0.25,
        "slippage_bps": 4.0,
        "session_hours_utc": (7, 21),
    }


def test_phase9_production_strategy_emits_long_and_short_signals() -> None:
    long_snapshot = _snapshot(
        contract_id="BR-6.26",
        instrument_id="BR",
        ts="2026-03-16T09:15:00Z",
        atr=0.5,
        ema_fast=82.7,
        ema_slow=82.4,
        donchian_high=82.9,
        donchian_low=81.9,
        rvol=1.10,
        last_close=82.76,
    )
    short_snapshot = _snapshot(
        contract_id="Si-6.26",
        instrument_id="Si",
        ts="2026-03-16T09:30:00Z",
        atr=100.0,
        ema_fast=91890.0,
        ema_slow=92010.0,
        donchian_high=92120.0,
        donchian_low=91800.0,
        rvol=1.15,
        last_close=91820.0,
    )

    assert evaluate_strategy(
        strategy_version_id=PHASE9_PRODUCTION_STRATEGY_ID,
        snapshot=long_snapshot,
    ) == TradeSide.LONG
    assert evaluate_strategy(
        strategy_version_id=PHASE9_PRODUCTION_STRATEGY_ID,
        snapshot=short_snapshot,
    ) == TradeSide.SHORT


def test_phase9_pilot_readiness_blocks_missing_coverage_and_degraded_live_smoke() -> None:
    readiness = assess_phase9_production_pilot_readiness(
        covered_contract_ids={"BR-6.26"},
        research_report={
            "signal_contracts": 0,
            "strategy_metrics": {
                "long_count": 0,
                "short_count": 0,
            },
        },
        replay_report={"runtime_signal_candidates": 0},
        live_smoke_status="degraded",
    )

    assert readiness["status"] == "blocked"
    assert "dataset coverage is missing one or more pilot contracts" in readiness["blockers"]
    assert "backtest did not emit any signal candidates" in readiness["blockers"]
    assert "system replay did not accept any runtime candidates" in readiness["blockers"]
    assert "QUIK live-smoke evidence is degraded" in readiness["blockers"]
