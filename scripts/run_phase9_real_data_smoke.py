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

from trading_advisor_3000.app.data_plane import evaluate_phase9_live_smoke, load_phase9_live_snapshot


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 9 live market-data smoke for the frozen QUIK pilot feed.")
    parser.add_argument("--provider", default="quik-live")
    parser.add_argument("--snapshot-path", required=True)
    parser.add_argument("--as-of-ts", default=None)
    parser.add_argument("--max-lag-seconds", type=int, default=None)
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    snapshot_provider_id, rows = load_phase9_live_snapshot(Path(args.snapshot_path))
    if snapshot_provider_id and snapshot_provider_id != args.provider:
        raise SystemExit(f"snapshot provider_id mismatch: expected {args.provider}, got {snapshot_provider_id}")

    report = evaluate_phase9_live_smoke(
        provider_id=args.provider,
        snapshot_rows=rows,
        as_of_ts=args.as_of_ts or _utc_now(),
        max_lag_seconds=args.max_lag_seconds,
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_out:
        report_path = Path(args.report_out)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
