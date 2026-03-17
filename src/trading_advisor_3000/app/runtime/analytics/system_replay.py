from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.app.contracts import CanonicalBar, DecisionCandidate
from trading_advisor_3000.app.interfaces.api import RuntimeAPI
from trading_advisor_3000.app.research import build_forward_observations, run_research_from_bars
from trading_advisor_3000.app.runtime.analytics.outcomes import (
    build_signal_outcomes,
    phase3_outcome_store_contract,
)
from trading_advisor_3000.app.runtime.config import StrategyVersion
from trading_advisor_3000.app.runtime.pipeline import build_runtime_stack


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def run_system_shadow_replay(
    *,
    bars: list[CanonicalBar],
    instrument_by_contract: dict[str, str],
    strategy_version_id: str,
    dataset_version: str,
    output_dir: Path,
    telegram_channel: str,
    horizon_bars: int = 3,
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
    runtime_stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id=strategy_version_id,
            status="active",
            allowed_contracts=frozenset(item.contract_id for item in candidates),
            allowed_timeframes=frozenset(item.timeframe for item in candidates),
            allowed_modes=frozenset(item.mode for item in candidates),
            activated_from=activated_from,
        )
    )
    runtime_api = RuntimeAPI(runtime_stack=runtime_stack)
    runtime_payload = runtime_api.replay_candidates(candidates)

    forward_observations = build_forward_observations(
        candidates=candidates,
        bars=bars,
        horizon_bars=horizon_bars,
    )
    outcomes = build_signal_outcomes(
        candidates=candidates,
        forward_observations=forward_observations,
    )

    forward_rows = [item.to_dict() for item in forward_observations]
    outcome_rows = [item.to_dict() for item in outcomes]
    forward_path = output_dir / "research.forward_observations.sample.jsonl"
    outcomes_path = output_dir / "analytics.signal_outcomes.sample.jsonl"
    _write_jsonl(forward_path, forward_rows)
    _write_jsonl(outcomes_path, outcome_rows)

    return {
        "bars_processed": len(bars),
        "signal_candidates": len(candidates),
        "runtime_report": runtime_payload["replay_report"],
        "forward_observations": len(forward_observations),
        "analytics_outcomes": len(outcomes),
        "runtime_payload": runtime_payload,
        "forward_rows": forward_rows,
        "analytics_rows": outcome_rows,
        "output_paths": {
            **research_report["output_paths"],
            "research_forward_observations": forward_path.as_posix(),
            "analytics_signal_outcomes": outcomes_path.as_posix(),
        },
        "delta_manifest": {
            **research_report["delta_manifest"],
            **phase3_outcome_store_contract(),
        },
    }
