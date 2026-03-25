from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.app.execution import (
    evaluate_phase9_sidecar_preflight,
    load_phase9_sidecar_delivery_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 9 WS-D sidecar delivery preflight.")
    parser.add_argument(
        "--manifest-path",
        default=str(ROOT / "deployment" / "stocksharp-sidecar" / "phase9-sidecar-delivery-manifest.json"),
    )
    parser.add_argument("--base-url", default=os.environ.get("TA3000_SIDECAR_BASE_URL", ""))
    parser.add_argument("--skip-rollout-dry-run", action="store_true")
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    manifest = load_phase9_sidecar_delivery_manifest(Path(args.manifest_path))
    report = evaluate_phase9_sidecar_preflight(
        env=dict(os.environ),
        delivery_spec=manifest,
        base_url=args.base_url or None,
        include_rollout_dry_run=not args.skip_rollout_dry_run,
    )
    payload = json.dumps(report.to_dict(), ensure_ascii=False, indent=2)
    if args.report_out:
        path = Path(args.report_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if report.is_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
