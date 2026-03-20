from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.app.data_plane import run_phase9_historical_bootstrap


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 9 historical provider bootstrap for the frozen pilot universe.")
    parser.add_argument("--provider", default="moex-history")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    report = run_phase9_historical_bootstrap(
        source_path=Path(args.source_path),
        output_dir=Path(args.output_dir),
        provider_id=args.provider,
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_out:
        report_path = Path(args.report_out)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
