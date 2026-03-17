from __future__ import annotations

from trading_advisor_3000.app.contracts import (
    DecisionCandidate,
    FeatureSnapshotRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.app.research.forward import ForwardObservation, candidate_id_from_signal
from trading_advisor_3000.app.runtime.analytics import build_signal_outcomes, phase3_outcome_store_contract


def _candidate() -> DecisionCandidate:
    return DecisionCandidate(
        signal_id="SIG-20260317-0001",
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="trend-follow-v1",
        mode=Mode.SHADOW,
        side=TradeSide.LONG,
        confidence=0.81,
        ts_decision="2026-03-17T09:30:00Z",
        feature_snapshot=FeatureSnapshotRef(
            dataset_version="bars-whitelist-v1",
            snapshot_id="FS-20260317-0001",
        ),
    )


def test_phase3_outcomes_contract_has_required_tables_and_columns() -> None:
    manifest = phase3_outcome_store_contract()
    assert {"research_forward_observations", "analytics_signal_outcomes"} <= set(manifest)
    forward_columns = set(manifest["research_forward_observations"]["columns"])
    analytics_columns = set(manifest["analytics_signal_outcomes"]["columns"])
    assert {"forward_obs_id", "candidate_id", "result_state", "pnl_r", "mfe_r", "mae_r"} <= forward_columns
    assert {"signal_id", "strategy_version_id", "contract_id", "mode", "pnl_r", "close_reason"} <= analytics_columns


def test_build_signal_outcomes_maps_forward_state_to_close_reason() -> None:
    candidate = _candidate()
    observation = ForwardObservation(
        forward_obs_id="FWD-TEST-0001",
        candidate_id=candidate_id_from_signal(candidate),
        mode="shadow",
        opened_at="2026-03-17T09:30:00Z",
        closed_at="2026-03-17T10:15:00Z",
        result_state="closed_profit",
        pnl_r=1.25,
        mfe_r=1.7,
        mae_r=-0.4,
    )
    outcomes = build_signal_outcomes(candidates=[candidate], forward_observations=[observation])

    assert len(outcomes) == 1
    payload = outcomes[0].to_dict()
    assert payload["signal_id"] == candidate.signal_id
    assert payload["close_reason"] == "forward_window_profit"
    assert payload["mode"] == "shadow"
