from __future__ import annotations

import hashlib
import json
from pathlib import Path

from trading_advisor_3000.app.contracts import DecisionCandidate, FeatureSnapshotRef, Mode, TradeSide
from trading_advisor_3000.app.research.features import FeatureSnapshot, phase2b_feature_store_contract
from trading_advisor_3000.app.research.strategies import evaluate_strategy


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _score(snapshot: FeatureSnapshot) -> float:
    spread = abs(snapshot.ema_fast - snapshot.ema_slow)
    denominator = snapshot.atr if snapshot.atr > 0 else 1.0
    return min(1.0, max(0.0, spread / denominator))


def _candidate_id(*, strategy_version_id: str, snapshot: FeatureSnapshot) -> str:
    return "CAND-" + _stable_hash(
        f"{strategy_version_id}|{snapshot.contract_id}|{snapshot.timeframe.value}|{snapshot.ts}"
    )


def _signal_id(*, strategy_version_id: str, snapshot: FeatureSnapshot) -> str:
    return "SIG-" + _stable_hash(
        f"{strategy_version_id}|{snapshot.snapshot_id}|{snapshot.contract_id}|{snapshot.ts}"
    )


def _build_run_id(
    *,
    strategy_version_id: str,
    dataset_version: str,
    snapshots: list[FeatureSnapshot],
) -> str:
    seed = "|".join(snapshot.snapshot_id for snapshot in snapshots)
    return "BTRUN-" + _stable_hash(f"{strategy_version_id}|{dataset_version}|{seed}")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def run_backtest(
    snapshots: list[FeatureSnapshot],
    *,
    strategy_version_id: str,
    dataset_version: str,
    output_dir: Path | None = None,
) -> dict[str, object]:
    ordered = sorted(snapshots, key=lambda item: (item.contract_id, item.timeframe.value, item.ts))
    backtest_run_id = _build_run_id(
        strategy_version_id=strategy_version_id,
        dataset_version=dataset_version,
        snapshots=ordered,
    )
    started_at = ordered[0].ts if ordered else "1970-01-01T00:00:00Z"
    finished_at = ordered[-1].ts if ordered else started_at
    params_hash = _stable_hash(f"{strategy_version_id}|{dataset_version}|v1")

    signal_contracts: list[DecisionCandidate] = []
    research_rows: list[dict[str, object]] = []
    for snapshot in ordered:
        side = evaluate_strategy(strategy_version_id=strategy_version_id, snapshot=snapshot)
        if side == TradeSide.FLAT:
            continue
        confidence = _score(snapshot)
        signal_contract = DecisionCandidate(
            signal_id=_signal_id(strategy_version_id=strategy_version_id, snapshot=snapshot),
            contract_id=snapshot.contract_id,
            timeframe=snapshot.timeframe,
            strategy_version_id=strategy_version_id,
            mode=Mode.SHADOW,
            side=side,
            confidence=confidence,
            ts_decision=snapshot.ts,
            feature_snapshot=FeatureSnapshotRef(
                dataset_version=dataset_version,
                snapshot_id=snapshot.snapshot_id,
            ),
        )
        signal_contracts.append(signal_contract)
        research_rows.append(
            {
                "candidate_id": _candidate_id(strategy_version_id=strategy_version_id, snapshot=snapshot),
                "backtest_run_id": backtest_run_id,
                "strategy_version_id": strategy_version_id,
                "contract_id": snapshot.contract_id,
                "timeframe": snapshot.timeframe.value,
                "ts_signal": snapshot.ts,
                "side": side.value,
                "score": confidence,
                "signal_contract_json": signal_contract.to_dict(),
            }
        )

    backtest_row = {
        "backtest_run_id": backtest_run_id,
        "strategy_version_id": strategy_version_id,
        "dataset_version": dataset_version,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": "completed",
        "params_hash": params_hash,
        "candidate_count": len(research_rows),
    }

    output_paths: dict[str, str] = {}
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        run_path = output_dir / "research.backtest_runs.sample.jsonl"
        candidate_path = output_dir / "research.signal_candidates.sample.jsonl"
        snapshot_path = output_dir / "feature.snapshots.sample.jsonl"

        _write_jsonl(run_path, [backtest_row])
        _write_jsonl(candidate_path, research_rows)
        _write_jsonl(snapshot_path, [snapshot.to_dict() for snapshot in ordered])
        output_paths = {
            "backtest_runs": run_path.as_posix(),
            "signal_candidates": candidate_path.as_posix(),
            "feature_snapshots": snapshot_path.as_posix(),
        }

    return {
        "backtest_run": backtest_row,
        "signal_contracts": [contract.to_dict() for contract in signal_contracts],
        "research_candidates": research_rows,
        "output_paths": output_paths,
        "delta_manifest": phase2b_feature_store_contract(),
    }
