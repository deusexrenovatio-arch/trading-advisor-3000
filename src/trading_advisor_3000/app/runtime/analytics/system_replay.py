from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.app.contracts import CanonicalBar, DecisionCandidate, Mode, OrderIntent
from trading_advisor_3000.app.execution.intents import PaperBrokerEngine
from trading_advisor_3000.app.interfaces.api import RuntimeAPI
from trading_advisor_3000.app.research import (
    build_forward_observations,
    run_research_from_bars,
)
from trading_advisor_3000.app.runtime.analytics.outcomes import (
    build_signal_outcomes,
    phase3_outcome_store_contract,
)
from trading_advisor_3000.app.runtime.analytics.review import (
    build_loki_event_lines,
    build_phase5_review_report,
    export_prometheus_metrics,
    phase5_review_store_contract,
)
from trading_advisor_3000.app.runtime.config import StrategyVersion
from trading_advisor_3000.app.runtime.pipeline import build_runtime_stack


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_lines(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows) + ("\n" if rows else ""), encoding="utf-8")


def _bar_close_index(bars: list[CanonicalBar]) -> dict[tuple[str, str, str], float]:
    index: dict[tuple[str, str, str], float] = {}
    for bar in bars:
        index[(bar.contract_id, bar.timeframe.value, bar.ts)] = bar.close
    return index


def run_system_shadow_replay(
    *,
    bars: list[CanonicalBar],
    instrument_by_contract: dict[str, str],
    strategy_version_id: str,
    dataset_version: str,
    output_dir: Path,
    telegram_channel: str,
    horizon_bars: int = 3,
    runtime_allowed_contracts: set[str] | None = None,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    research_report = run_research_from_bars(
        bars=bars,
        instrument_by_contract=instrument_by_contract,
        strategy_version_id=strategy_version_id,
        dataset_version=dataset_version,
        output_dir=output_dir,
    )
    candidates = [DecisionCandidate.from_dict(item) for item in research_report["signal_contract_rows"]]

    runtime_stack = build_runtime_stack(telegram_channel=telegram_channel)
    activated_from = min((item.ts_decision for item in candidates), default="1970-01-01T00:00:00Z")
    allowed_contracts = runtime_allowed_contracts or {item.contract_id for item in candidates}
    runtime_stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id=strategy_version_id,
            status="active",
            allowed_contracts=frozenset(allowed_contracts),
            allowed_timeframes=frozenset(item.timeframe for item in candidates),
            allowed_modes=frozenset(item.mode for item in candidates),
            activated_from=activated_from,
        )
    )
    runtime_api = RuntimeAPI(runtime_stack=runtime_stack)
    runtime_payload = runtime_api.replay_candidates(candidates)
    accepted_candidates = {
        str(item["signal_id"]): str(item["candidate_id"])
        for item in runtime_payload["replay_report"].get("accepted_candidates", [])
    }
    accepted_signal_ids = set(str(item) for item in runtime_payload["replay_report"]["accepted_signal_ids"])
    publication_signal_ids = {str(item["signal_id"]) for item in runtime_payload["publications"]}
    runtime_signal_ids = sorted(accepted_signal_ids & publication_signal_ids)
    runtime_candidates = [item for item in candidates if item.signal_id in runtime_signal_ids]
    runtime_candidate_ids = sorted({accepted_candidates[item.signal_id] for item in runtime_candidates})

    forward_observations = build_forward_observations(
        candidates=runtime_candidates,
        bars=bars,
        horizon_bars=horizon_bars,
        candidate_ids_by_signal=accepted_candidates,
    )
    observation_by_candidate_id = {row.candidate_id: row for row in forward_observations}
    close_index = _bar_close_index(bars)

    paper_broker = PaperBrokerEngine(account_id="PAPER-REPLAY")
    for candidate in runtime_candidates:
        candidate_id = accepted_candidates[candidate.signal_id]
        observation = observation_by_candidate_id.get(candidate_id)
        if observation is None:
            continue

        open_action = "buy" if candidate.side.value == "long" else "sell"
        close_action = "sell" if open_action == "buy" else "buy"
        open_intent = OrderIntent(
            intent_id=f"{candidate.signal_id}:OPEN",
            signal_id=candidate.signal_id,
            mode=Mode.PAPER,
            broker_adapter="stocksharp-sidecar-stub",
            action=open_action,
            contract_id=candidate.contract_id,
            qty=1,
            price=candidate.entry_ref,
            stop_price=candidate.stop_ref,
            created_at=candidate.ts_decision,
        )
        open_result = paper_broker.execute_intent(
            open_intent,
            fill_price=candidate.entry_ref,
            fill_ts=candidate.ts_decision,
            fee=0.0,
        )
        runtime_stack.signal_store.record_execution_fill(
            signal_id=candidate.signal_id,
            event_ts=candidate.ts_decision,
            fill_id=open_result.broker_fill.fill_id,
            role="open",
            contract_id=candidate.contract_id,
            qty=open_result.broker_fill.qty,
            price=open_result.broker_fill.price,
        )

        close_price = close_index.get(
            (candidate.contract_id, candidate.timeframe.value, observation.closed_at),
            candidate.entry_ref,
        )
        close_intent = OrderIntent(
            intent_id=f"{candidate.signal_id}:CLOSE",
            signal_id=candidate.signal_id,
            mode=Mode.PAPER,
            broker_adapter="stocksharp-sidecar-stub",
            action=close_action,
            contract_id=candidate.contract_id,
            qty=1,
            price=close_price,
            stop_price=candidate.stop_ref,
            created_at=observation.closed_at,
        )
        close_result = paper_broker.execute_intent(
            close_intent,
            fill_price=close_price,
            fill_ts=observation.closed_at,
            fee=0.0,
        )
        runtime_stack.signal_store.record_execution_fill(
            signal_id=candidate.signal_id,
            event_ts=observation.closed_at,
            fill_id=close_result.broker_fill.fill_id,
            role="close",
            contract_id=candidate.contract_id,
            qty=close_result.broker_fill.qty,
            price=close_result.broker_fill.price,
        )
        runtime_api.close_signal(
            signal_id=candidate.signal_id,
            closed_at=observation.closed_at,
            reason_code=observation.result_state,
        )

    outcomes = build_signal_outcomes(
        signal_events=runtime_api.list_signal_events(),
        broker_fills=[item.to_dict() for item in paper_broker.list_broker_fills()],
        positions=[item.to_dict() for item in paper_broker.list_position_snapshots()],
    )
    phase5_report = build_phase5_review_report(
        outcomes=[item.to_dict() for item in outcomes],
        signal_events=runtime_api.list_signal_events(),
    )
    prometheus_metrics = export_prometheus_metrics(phase5_report)
    loki_lines = build_loki_event_lines(phase5_report)

    forward_rows = [item.to_dict() for item in forward_observations]
    outcome_rows = [item.to_dict() for item in outcomes]
    strategy_rows = [item.to_dict() for item in phase5_report.strategy_dashboard]
    instrument_rows = [item.to_dict() for item in phase5_report.instrument_dashboard]
    latency_rows = [item.to_dict() for item in phase5_report.latency_metrics]
    forward_path = output_dir / "research.forward_observations.sample.jsonl"
    outcomes_path = output_dir / "analytics.signal_outcomes.sample.jsonl"
    strategy_metrics_path = output_dir / "analytics.strategy_metrics_daily.sample.jsonl"
    instrument_metrics_path = output_dir / "analytics.instrument_metrics_daily.sample.jsonl"
    latency_metrics_path = output_dir / "observability.latency_metrics.sample.jsonl"
    prometheus_path = output_dir / "observability.prometheus.metrics.txt"
    loki_path = output_dir / "observability.loki.events.jsonl"
    _write_jsonl(forward_path, forward_rows)
    _write_jsonl(outcomes_path, outcome_rows)
    _write_jsonl(strategy_metrics_path, strategy_rows)
    _write_jsonl(instrument_metrics_path, instrument_rows)
    _write_jsonl(latency_metrics_path, latency_rows)
    prometheus_path.write_text(prometheus_metrics, encoding="utf-8")
    _write_lines(loki_path, loki_lines)

    return {
        "bars_processed": len(bars),
        "signal_candidates": len(candidates),
        "runtime_signal_candidates": len(runtime_candidates),
        "runtime_candidate_ids": runtime_candidate_ids,
        "runtime_report": runtime_payload["replay_report"],
        "forward_observations": len(forward_observations),
        "analytics_outcomes": len(outcomes),
        "runtime_signal_ids": runtime_signal_ids,
        "runtime_payload": runtime_payload,
        "phase5_report": phase5_report.to_dict(),
        "signal_events": runtime_api.list_signal_events(),
        "broker_fills": [item.to_dict() for item in paper_broker.list_broker_fills()],
        "positions": [item.to_dict() for item in paper_broker.list_position_snapshots()],
        "forward_rows": forward_rows,
        "analytics_rows": outcome_rows,
        "strategy_rows": strategy_rows,
        "instrument_rows": instrument_rows,
        "latency_rows": latency_rows,
        "prometheus_metrics": prometheus_metrics,
        "loki_lines": loki_lines,
        "output_paths": {
            **research_report["output_paths"],
            "research_forward_observations": forward_path.as_posix(),
            "analytics_signal_outcomes": outcomes_path.as_posix(),
            "analytics_strategy_metrics_daily": strategy_metrics_path.as_posix(),
            "analytics_instrument_metrics_daily": instrument_metrics_path.as_posix(),
            "observability_latency_metrics": latency_metrics_path.as_posix(),
            "observability_prometheus_metrics": prometheus_path.as_posix(),
            "observability_loki_events": loki_path.as_posix(),
        },
        "delta_manifest": {
            **research_report["delta_manifest"],
            **phase3_outcome_store_contract(),
            **phase5_review_store_contract(),
        },
    }
