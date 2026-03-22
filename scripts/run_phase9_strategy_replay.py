from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.app.contracts import CanonicalBar
from trading_advisor_3000.app.data_plane import (
    evaluate_phase9_live_smoke,
    load_phase9_live_snapshot,
    load_phase9_live_snapshot_from_url,
)
from trading_advisor_3000.app.research import run_research_from_bars
from trading_advisor_3000.app.research.strategies import (
    assess_phase9_production_pilot_readiness,
    phase9_production_backtest_config,
    phase9_production_strategy_spec,
    production_strategy_ids,
)
from trading_advisor_3000.app.runtime.analytics import run_system_shadow_replay


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _load_bars(path: Path) -> list[CanonicalBar]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [CanonicalBar.from_dict(row) for row in rows]


def _bootstrap_context(path: Path) -> tuple[Path, str]:
    payload = _load_json(path)
    output_paths = payload.get("output_paths")
    if not isinstance(output_paths, dict):
        raise ValueError("bootstrap report must contain output_paths")
    bars_path = output_paths.get("canonical_bars")
    dataset_version = payload.get("dataset_version")
    if not isinstance(bars_path, str) or not bars_path.strip():
        raise ValueError("bootstrap report must contain output_paths.canonical_bars")
    if not isinstance(dataset_version, str) or not dataset_version.strip():
        raise ValueError("bootstrap report must contain dataset_version")
    bars_candidate = Path(bars_path)
    if not bars_candidate.is_absolute() and not bars_candidate.exists():
        bars_candidate = (path.parent / bars_candidate).resolve()
    return bars_candidate, dataset_version.strip()


def _live_provider_id(*, live_feed: str) -> str:
    if live_feed == "QUIK":
        return "quik-live"
    return live_feed.lower()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 9 strategy backtest and shadow replay on pilot data.")
    parser.add_argument("--strategy", default=None)
    parser.add_argument("--bootstrap-report", default=None)
    parser.add_argument("--bars-path", default=None)
    parser.add_argument("--dataset-version", default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--telegram-channel", default="@ta3000_phase9_shadow")
    parser.add_argument("--horizon-bars", type=int, default=3)
    parser.add_argument("--snapshot-path", default=None)
    parser.add_argument("--snapshot-url", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--as-of-ts", default=None)
    parser.add_argument("--max-lag-seconds", type=int, default=None)
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    strategy_id = args.strategy or phase9_production_strategy_spec().strategy_version_id
    if strategy_id not in production_strategy_ids():
        raise SystemExit(f"unsupported Phase 9 production strategy: {strategy_id}")

    if bool(args.bootstrap_report) == bool(args.bars_path and args.dataset_version):
        if not args.bootstrap_report and not (args.bars_path and args.dataset_version):
            raise SystemExit("provide either --bootstrap-report or both --bars-path and --dataset-version")
        raise SystemExit("choose one input mode: --bootstrap-report or --bars-path with --dataset-version")

    if args.bootstrap_report:
        bars_path, dataset_version = _bootstrap_context(Path(args.bootstrap_report))
    else:
        bars_path = Path(str(args.bars_path))
        dataset_version = str(args.dataset_version)

    if bool(args.snapshot_path) and bool(args.snapshot_url):
        raise SystemExit("provide at most one of --snapshot-path or --snapshot-url")

    spec = phase9_production_strategy_spec()
    live_provider_id = _live_provider_id(live_feed=spec.live_feed)
    bars = _load_bars(bars_path)
    instrument_by_contract = {bar.contract_id: bar.instrument_id for bar in bars}
    covered_contract_ids = {bar.contract_id for bar in bars}

    output_dir = Path(args.output_dir)
    backtest_dir = output_dir / "backtest"
    replay_dir = output_dir / "replay"
    research_report = run_research_from_bars(
        bars=bars,
        instrument_by_contract=instrument_by_contract,
        strategy_version_id=strategy_id,
        dataset_version=dataset_version,
        output_dir=backtest_dir,
        backtest_config=phase9_production_backtest_config(),
    )
    replay_report = run_system_shadow_replay(
        bars=bars,
        instrument_by_contract=instrument_by_contract,
        strategy_version_id=strategy_id,
        dataset_version=dataset_version,
        output_dir=replay_dir,
        telegram_channel=args.telegram_channel,
        horizon_bars=args.horizon_bars,
        runtime_allowed_contracts=set(spec.pilot_universe),
    )

    live_smoke_report: dict[str, object] | None = None
    if args.snapshot_path:
        snapshot_provider_id, rows = load_phase9_live_snapshot(Path(args.snapshot_path))
        if snapshot_provider_id and snapshot_provider_id != live_provider_id:
            raise SystemExit(
                f"snapshot provider_id mismatch: expected {live_provider_id}, got {snapshot_provider_id}"
            )
        live_smoke_report = evaluate_phase9_live_smoke(
            provider_id=live_provider_id,
            snapshot_rows=rows,
            as_of_ts=args.as_of_ts or _utc_now(),
            max_lag_seconds=args.max_lag_seconds,
        )
        live_smoke_report["source_kind"] = "file"
        live_smoke_report["snapshot_path"] = str(Path(args.snapshot_path))
    elif args.snapshot_url:
        snapshot_provider_id, rows = load_phase9_live_snapshot_from_url(
            args.snapshot_url,
            timeout_seconds=args.timeout_seconds,
        )
        if snapshot_provider_id and snapshot_provider_id != live_provider_id:
            raise SystemExit(
                f"snapshot provider_id mismatch: expected {live_provider_id}, got {snapshot_provider_id}"
            )
        live_smoke_report = evaluate_phase9_live_smoke(
            provider_id=live_provider_id,
            snapshot_rows=rows,
            as_of_ts=args.as_of_ts or _utc_now(),
            max_lag_seconds=args.max_lag_seconds,
        )
        live_smoke_report["source_kind"] = "url"
        live_smoke_report["snapshot_url"] = args.snapshot_url

    readiness = assess_phase9_production_pilot_readiness(
        covered_contract_ids=covered_contract_ids,
        research_report=research_report,
        replay_report=replay_report,
        live_smoke_status=None if live_smoke_report is None else str(live_smoke_report.get("status")),
    )
    report = {
        "strategy_spec": spec.to_dict(),
        "dataset_version": dataset_version,
        "bars_path": bars_path.as_posix(),
        "covered_contract_ids": sorted(covered_contract_ids),
        "backtest_config": phase9_production_backtest_config(),
        "research_summary": {
            "bars_processed": research_report["bars_processed"],
            "feature_snapshots": research_report["feature_snapshots"],
            "signal_contracts": research_report["signal_contracts"],
            "strategy_metrics": research_report["strategy_metrics"],
            "backtest_run": research_report["backtest_run"],
            "output_paths": research_report["output_paths"],
        },
        "replay_summary": {
            "signal_candidates": replay_report["signal_candidates"],
            "runtime_signal_candidates": replay_report["runtime_signal_candidates"],
            "forward_observations": replay_report["forward_observations"],
            "runtime_signal_ids": replay_report["runtime_signal_ids"],
            "runtime_report": replay_report["runtime_report"],
            "output_paths": replay_report["output_paths"],
        },
        "live_smoke": live_smoke_report,
        "pilot_readiness": readiness,
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_out:
        report_path = Path(args.report_out)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if readiness["status"] == "ready_for_shadow_pilot" else 1


if __name__ == "__main__":
    raise SystemExit(main())
