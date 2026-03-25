from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.app.contracts import Mode, Timeframe, TradeSide
from trading_advisor_3000.app.research.features.snapshot import FeatureSnapshot


PHASE9_PRODUCTION_STRATEGY_ID = "phase9-moex-breakout-v1"
PHASE9_FEATURE_SET_VERSION = "feature-set-v1"


@dataclass(frozen=True)
class StrategyRiskTemplate:
    sizing_model: str
    max_parallel_signals: int
    exposure_caps: tuple[str, ...]
    cooldown_bars: int
    walk_forward_windows: int
    commission_per_trade: float
    slippage_bps: float
    session_hours_utc: tuple[int, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "sizing_model": self.sizing_model,
            "max_parallel_signals": self.max_parallel_signals,
            "exposure_caps": list(self.exposure_caps),
            "cooldown_bars": self.cooldown_bars,
            "walk_forward_windows": self.walk_forward_windows,
            "commission_per_trade": self.commission_per_trade,
            "slippage_bps": self.slippage_bps,
            "session_hours_utc": [self.session_hours_utc[0], self.session_hours_utc[1]],
        }


@dataclass(frozen=True)
class ProductionStrategySpec:
    strategy_version_id: str
    title: str
    status: str
    pilot_universe: tuple[str, ...]
    allowed_timeframes: tuple[Timeframe, ...]
    allowed_modes: tuple[Mode, ...]
    historical_source: str
    live_feed: str
    feature_set_version: str
    required_features: tuple[str, ...]
    target_signal_frequency: str
    acceptable_dry_periods: str
    rejection_criteria: tuple[str, ...]
    risk_template: StrategyRiskTemplate

    def to_dict(self) -> dict[str, object]:
        return {
            "strategy_version_id": self.strategy_version_id,
            "title": self.title,
            "status": self.status,
            "pilot_universe": list(self.pilot_universe),
            "allowed_timeframes": [item.value for item in self.allowed_timeframes],
            "allowed_modes": [item.value for item in self.allowed_modes],
            "historical_source": self.historical_source,
            "live_feed": self.live_feed,
            "feature_set_version": self.feature_set_version,
            "required_features": list(self.required_features),
            "target_signal_frequency": self.target_signal_frequency,
            "acceptable_dry_periods": self.acceptable_dry_periods,
            "rejection_criteria": list(self.rejection_criteria),
            "risk_template": self.risk_template.to_dict(),
        }


def _last_close(snapshot: FeatureSnapshot) -> float:
    raw = snapshot.features_json.get("last_close", snapshot.ema_fast)
    if isinstance(raw, str):
        return snapshot.ema_fast
    return float(raw)


def _breakout_signal(snapshot: FeatureSnapshot) -> TradeSide:
    atr = snapshot.atr if snapshot.atr > 0 else 1.0
    breakout_buffer = atr * 0.30
    last_close = _last_close(snapshot)
    long_ready = (
        snapshot.ema_fast > snapshot.ema_slow
        and snapshot.rvol >= 1.0
        and last_close >= (snapshot.donchian_high - breakout_buffer)
    )
    short_ready = (
        snapshot.ema_fast < snapshot.ema_slow
        and snapshot.rvol >= 1.0
        and last_close <= (snapshot.donchian_low + breakout_buffer)
    )
    if long_ready:
        return TradeSide.LONG
    if short_ready:
        return TradeSide.SHORT
    return TradeSide.FLAT


def phase9_production_strategy_spec() -> ProductionStrategySpec:
    return ProductionStrategySpec(
        strategy_version_id=PHASE9_PRODUCTION_STRATEGY_ID,
        title="Phase 9 MOEX intraday breakout",
        status="shadow",
        pilot_universe=("BR-6.26", "Si-6.26"),
        allowed_timeframes=(Timeframe.M15,),
        allowed_modes=(Mode.SHADOW,),
        historical_source="MOEX",
        live_feed="QUIK",
        feature_set_version=PHASE9_FEATURE_SET_VERSION,
        required_features=(
            "atr",
            "ema_fast",
            "ema_slow",
            "donchian_high",
            "donchian_low",
            "rvol",
            "last_close",
        ),
        target_signal_frequency="1-6 signals per trading day across the pilot universe",
        acceptable_dry_periods="up to 3 consecutive sessions without a signal",
        rejection_criteria=(
            "feature_set_version drift from replay contract",
            "MOEX historical bootstrap inconsistency",
            "QUIK freshness failures during pilot smoke",
            "strategy emits only one-sided signals for 5 consecutive sessions",
        ),
        risk_template=StrategyRiskTemplate(
            sizing_model="fixed-1-unit shadow sizing",
            max_parallel_signals=2,
            exposure_caps=("1 signal per contract", "2 active signals across pilot universe"),
            cooldown_bars=2,
            walk_forward_windows=2,
            commission_per_trade=0.25,
            slippage_bps=4.0,
            session_hours_utc=(7, 21),
        ),
    )


def production_strategy_ids() -> tuple[str, ...]:
    return (PHASE9_PRODUCTION_STRATEGY_ID,)


def phase9_production_backtest_config() -> dict[str, object]:
    risk_template = phase9_production_strategy_spec().risk_template
    return {
        "walk_forward_windows": risk_template.walk_forward_windows,
        "commission_per_trade": risk_template.commission_per_trade,
        "slippage_bps": risk_template.slippage_bps,
        "session_hours_utc": risk_template.session_hours_utc,
    }


def assess_phase9_production_pilot_readiness(
    *,
    covered_contract_ids: set[str],
    research_report: dict[str, object],
    replay_report: dict[str, object],
    live_smoke_status: str | None = None,
) -> dict[str, object]:
    spec = phase9_production_strategy_spec()
    metrics = research_report.get("strategy_metrics", {})
    long_count = int(metrics.get("long_count", 0))
    short_count = int(metrics.get("short_count", 0))
    candidate_count = int(research_report.get("signal_contracts", 0))
    runtime_candidate_count = int(replay_report.get("runtime_signal_candidates", 0))
    missing_contract_ids = sorted(set(spec.pilot_universe) - covered_contract_ids)

    blockers: list[str] = []
    warnings: list[str] = []
    if missing_contract_ids:
        blockers.append("dataset coverage is missing one or more pilot contracts")
    if candidate_count <= 0:
        blockers.append("backtest did not emit any signal candidates")
    if runtime_candidate_count <= 0:
        blockers.append("system replay did not accept any runtime candidates")
    if live_smoke_status is None:
        warnings.append("QUIK live-smoke evidence was not attached to this replay run")
    elif live_smoke_status != "ok":
        blockers.append("QUIK live-smoke evidence is degraded")
    if long_count == 0 or short_count == 0:
        warnings.append("the evidence window is one-sided and still needs multi-session follow-up")

    return {
        "status": "ready_for_shadow_pilot" if not blockers else "blocked",
        "blockers": blockers,
        "warnings": warnings,
        "missing_contract_ids": missing_contract_ids,
        "covered_contract_ids": sorted(covered_contract_ids),
        "candidate_count": candidate_count,
        "runtime_candidate_count": runtime_candidate_count,
        "strategy_version_id": spec.strategy_version_id,
        "historical_source": spec.historical_source,
        "live_feed": spec.live_feed,
    }


def evaluate_production_strategy(*, strategy_version_id: str, snapshot: FeatureSnapshot) -> TradeSide:
    if strategy_version_id != PHASE9_PRODUCTION_STRATEGY_ID:
        raise ValueError(f"unsupported production strategy_version_id: {strategy_version_id}")
    return _breakout_signal(snapshot)
