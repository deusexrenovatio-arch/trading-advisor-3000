"""Run inside the isolated seed container; default is metadata-only planning."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from trading_advisor_3000.product_plane.common.staging_seed import (  # noqa: E402
    apply_seed,
    plan_seed,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "target", "data-manifest", "research-manifest"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--max-bytes", type=int, default=0)
    args = parser.parse_args()
    plan = plan_seed(
        source=args.source,
        target=args.target,
        logical_root=args.target,
        data=args.data_manifest,
        research=args.research_manifest,
    )
    result = (
        apply_seed(plan, max_bytes=args.max_bytes)
        if args.apply
        else {
            "status": "PLANNED",
            "file_count": len(plan["files"]),
            "total_bytes": plan["total_bytes"],
            "heads": plan["heads"],
            "source_current_data_snapshot_id": plan["source_current_data_snapshot_id"],
        }
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
