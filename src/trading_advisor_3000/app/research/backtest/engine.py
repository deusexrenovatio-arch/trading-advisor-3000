from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

from trading_advisor_3000.app.contracts import DecisionCandidate, FeatureSnapshotRef, Mode, TradeSide
from trading_advisor_3000.app.research.features import FeatureSnapshot, phase2b_feature_store_contract
from trading_advisor_3000.app.research.ids import candidate_id
from trading_advisor_3000.app.research.strategies import evaluate_strategy


def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12].upper()


def _score(snapshot: FeatureSnapshot) -> float:
    spread = abs(snapshot.ema_fast - snapshot.ema_slow)
    denominator = snapshot.atr if snapshot.atr > 0 else 1.0
    return min(1.0, max(0.0, spread / denominator))


def _refs(*, snapshot: FeatureSnapshot, side: TradeSide) -> tuple[float, float, float]:
    entry_ref = snapshot.ema_fast
    stop_distance = snapshot.atr if snapshot.atr > 0 else 1.0
    if side == TradeSide.LONG:
        return entry_ref, entry_ref - stop_distance, entry_ref + (2.0 * stop_distance)
    if side == TradeSide.SHORT:
        return entry_ref, entry_ref + stop_distance, entry_ref - (2.0 * stop_distance)
    return entry_ref, entry_ref, entry_ref


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


def _hour_utc(ts: str) -> int:
    return int(ts[11:13])


def _in_session(*, ts: str, session_hours_utc: tuple[int, int] | None) -> bool:
    if session_hours_utc is None:
        return True
    start_hour, end_hour = session_hours_utc
    hour = _hour_utc(ts)
    if start_hour <= end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def _window_id(*, index: int, total: int, windows: int) -> str:
    if windows <= 1 or total <= 1:
        return "wf-1"
    effective_windows = max(1, min(windows, total))
    window_size = max(1, math.ceil(total / effective_windows))
    return f"wf-{(index // window_size) + 1}"


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    lines = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def run_backtest(
    snapshots: list[FeatureSnapshot],
    *,
    strategy_version_id: str,
    dataset_version: str,
    output_dir: Path | None = None,
    walk_forward_windows: int = 1,
    commission_per_trade: float = 0.0,
    slippage_bps: float = 0.0,
    session_hours_utc: tuple[int, int] | None = None,
) -> dict[str, object]:
    if walk_forward_windows <= 0:
        raise ValueError("walk_forward_windows must be positive")
    if commission_per_trade < 0:
        raise ValueError("commission_per_trade must be non-negative")
    if slippage_bps < 0:
        raise ValueError("slippage_bps must be non-negative")

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
    session_filtered_out = 0
    commission_total = 0.0
    slippage_total = 0.0
    rr_values: list[float] = []

    for index, snapshot in enumerate(ordered):
        side = evaluate_strategy(strategy_version_id=strategy_version_id, snapshot=snapshot)
        if side == TradeSide.FLAT:
            continue
        if not _in_session(ts=snapshot.ts, session_hours_utc=session_hours_utc):
            session_filtered_out += 1
            continue

        confidence = _score(snapshot)
        entry_ref, stop_ref, target_ref = _refs(snapshot=snapshot, side=side)
        slippage_abs = entry_ref * (slippage_bps / 10_000.0)
        if side == TradeSide.LONG:
            entry_ref = entry_ref + slippage_abs
            stop_ref = stop_ref + slippage_abs
            target_ref = target_ref + slippage_abs
        elif side == TradeSide.SHORT:
            entry_ref = entry_ref - slippage_abs
            stop_ref = stop_ref - slippage_abs
            target_ref = target_ref - slippage_abs

        commission_total += commission_per_trade
        slippage_total += abs(slippage_abs)
        window_id = _window_id(index=index, total=len(ordered), windows=walk_forward_windows)
        risk_denominator = abs(entry_ref - stop_ref)
        reward_numerator = abs(target_ref - entry_ref)
        rr_ratio = reward_numerator / risk_denominator if risk_denominator > 0 else 0.0
        rr_values.append(rr_ratio)

        signal_contract = DecisionCandidate(
            signal_id=_signal_id(strategy_version_id=strategy_version_id, snapshot=snapshot),
            contract_id=snapshot.contract_id,
            timeframe=snapshot.timeframe,
            strategy_version_id=strategy_version_id,
            mode=Mode.SHADOW,
            side=side,
            entry_ref=entry_ref,
            stop_ref=stop_ref,
            target_ref=target_ref,
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
                "candidate_id": candidate_id(
                    strategy_version_id=strategy_version_id,
                    contract_id=snapshot.contract_id,
                    timeframe=snapshot.timeframe.value,
                    ts_signal=snapshot.ts,
                ),
                "backtest_run_id": backtest_run_id,
                "strategy_version_id": strategy_version_id,
                "contract_id": snapshot.contract_id,
                "timeframe": snapshot.timeframe.value,
                "ts_signal": snapshot.ts,
                "side": side.value,
                "entry_ref": entry_ref,
                "stop_ref": stop_ref,
                "target_ref": target_ref,
                "score": confidence,
                "window_id": window_id,
                "estimated_commission": commission_per_trade,
                "estimated_slippage": abs(slippage_abs),
            }
        )

    strategy_metrics = {
        "walk_forward_windows": walk_forward_windows,
        "session_hours_utc": None if session_hours_utc is None else [session_hours_utc[0], session_hours_utc[1]],
        "session_filtered_out": session_filtered_out,
        "candidate_count": len(research_rows),
        "long_count": sum(1 for row in research_rows if row["side"] == "long"),
        "short_count": sum(1 for row in research_rows if row["side"] == "short"),
        "avg_score": (sum(float(row["score"]) for row in research_rows) / len(research_rows)) if research_rows else 0.0,
        "avg_risk_reward": (sum(rr_values) / len(rr_values)) if rr_values else 0.0,
        "commission_total": commission_total,
        "slippage_total": slippage_total,
    }

    backtest_row = {
        "backtest_run_id": backtest_run_id,
        "strategy_version_id": strategy_version_id,
        "dataset_version": dataset_version,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": "completed",
        "params_hash": params_hash,
        "candidate_count": len(research_rows),
        "walk_forward_windows": walk_forward_windows,
        "commission_total": commission_total,
        "slippage_total": slippage_total,
        "session_filtered_out": session_filtered_out,
    }

    output_paths: dict[str, str] = {}
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        run_path = output_dir / "research.backtest_runs.sample.jsonl"
        candidate_path = output_dir / "research.signal_candidates.sample.jsonl"
        snapshot_path = output_dir / "feature.snapshots.sample.jsonl"
        strategy_metrics_path = output_dir / "research.strategy_metrics.sample.jsonl"

        _write_jsonl(run_path, [backtest_row])
        _write_jsonl(candidate_path, research_rows)
        _write_jsonl(snapshot_path, [snapshot.to_dict() for snapshot in ordered])
        _write_jsonl(strategy_metrics_path, [strategy_metrics])
        output_paths = {
            "backtest_runs": run_path.as_posix(),
            "signal_candidates": candidate_path.as_posix(),
            "feature_snapshots": snapshot_path.as_posix(),
            "strategy_metrics": strategy_metrics_path.as_posix(),
        }

    return {
        "backtest_run": backtest_row,
        "strategy_metrics": strategy_metrics,
        "signal_contracts": [contract.to_dict() for contract in signal_contracts],
        "research_candidates": research_rows,
        "output_paths": output_paths,
        "delta_manifest": phase2b_feature_store_contract(),
    }
