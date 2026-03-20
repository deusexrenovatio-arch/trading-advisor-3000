from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.app.data_plane import (
    default_phase9_quik_connector_config,
    write_phase9_quik_connector_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Phase 9 local QUIK Lua market-data connector bundle.")
    parser.add_argument(
        "--export-path",
        default=str((ROOT / "artifacts" / "phase9" / "quik_live_snapshot.json").resolve()),
    )
    parser.add_argument(
        "--output-script",
        default=str((ROOT / "deployment" / "quik-live-feed" / "phase9_quik_live_export.lua").resolve()),
    )
    parser.add_argument(
        "--output-config",
        default=str((ROOT / "deployment" / "quik-live-feed" / "phase9_quik_live_export.config.json").resolve()),
    )
    parser.add_argument("--poll-interval-ms", type=int, default=1000)
    parser.add_argument("--class-code", default="SPBFUT")
    args = parser.parse_args()

    export_path = Path(args.export_path)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    config = default_phase9_quik_connector_config(
        export_path=str(export_path),
        class_code=args.class_code,
        poll_interval_ms=args.poll_interval_ms,
    )
    script_path = Path(args.output_script)
    config_path = Path(args.output_config)
    write_phase9_quik_connector_bundle(
        script_path=script_path,
        config_path=config_path,
        config=config,
    )
    payload = {
        "status": "ok",
        "script_path": str(script_path),
        "config_path": str(config_path),
        "export_path": str(export_path),
        "poll_interval_ms": args.poll_interval_ms,
        "class_code": args.class_code,
        "bindings": config.to_dict()["bindings"],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
