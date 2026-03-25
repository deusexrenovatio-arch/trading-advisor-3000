from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.app.phase9 import run_phase9_strategy_replay_workflow
from trading_advisor_3000.app.research.strategies import phase9_production_strategy_spec, production_strategy_ids


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

    report = run_phase9_strategy_replay_workflow(
        strategy_id=strategy_id,
        bootstrap_report=None if not args.bootstrap_report else Path(args.bootstrap_report),
        bars_path=None if not args.bars_path else Path(str(args.bars_path)),
        dataset_version=None if not args.dataset_version else str(args.dataset_version),
        output_dir=Path(args.output_dir),
        telegram_channel=args.telegram_channel,
        horizon_bars=args.horizon_bars,
        snapshot_path=None if not args.snapshot_path else Path(args.snapshot_path),
        snapshot_url=args.snapshot_url,
        timeout_seconds=args.timeout_seconds,
        as_of_ts=args.as_of_ts,
        max_lag_seconds=args.max_lag_seconds,
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_out:
        report_path = Path(args.report_out)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if report["pilot_readiness"]["status"] == "ready_for_shadow_pilot" else 1


if __name__ == "__main__":
    raise SystemExit(main())
