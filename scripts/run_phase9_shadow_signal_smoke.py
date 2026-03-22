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

from trading_advisor_3000.app.phase9 import (
    build_phase9_env_with_overrides,
    run_phase9_shadow_signal_smoke_workflow,
)
from trading_advisor_3000.app.runtime.config import (
    DEFAULT_PHASE9_BATTLE_RUN_PROFILE,
    DEFAULT_PHASE9_SIGNAL_STORE_BACKEND,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Phase 9 WS-C Telegram/Postgres shadow lifecycle smoke.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--dsn", default=os.environ.get("TA3000_APP_DSN", ""))
    parser.add_argument("--telegram-bot-token", default=os.environ.get("TA3000_TELEGRAM_BOT_TOKEN", ""))
    parser.add_argument(
        "--telegram-shadow-channel",
        default=os.environ.get("TA3000_TELEGRAM_SHADOW_CHANNEL", ""),
    )
    parser.add_argument(
        "--telegram-advisory-channel",
        default=os.environ.get("TA3000_TELEGRAM_ADVISORY_CHANNEL", ""),
    )
    parser.add_argument("--signal-store-schema", default=os.environ.get("TA3000_SIGNAL_STORE_SCHEMA", "signal"))
    parser.add_argument("--runtime-profile", default=DEFAULT_PHASE9_BATTLE_RUN_PROFILE)
    parser.add_argument("--signal-store-backend", default=DEFAULT_PHASE9_SIGNAL_STORE_BACKEND)
    parser.add_argument("--prometheus-base-url", default=os.environ.get("TA3000_PROMETHEUS_BASE_URL", ""))
    parser.add_argument("--loki-base-url", default=os.environ.get("TA3000_LOKI_BASE_URL", ""))
    parser.add_argument("--grafana-dashboard-url", default=os.environ.get("TA3000_GRAFANA_DASHBOARD_URL", ""))
    parser.add_argument("--min-lifecycle-events", type=int, default=10)
    parser.add_argument("--mode", choices=("shadow", "advisory"), default="shadow")
    parser.add_argument("--skip-migrations", action="store_true")
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    env = build_phase9_env_with_overrides(
        runtime_profile=args.runtime_profile,
        signal_store_backend=args.signal_store_backend,
        signal_store_schema=args.signal_store_schema,
        dsn=args.dsn,
        telegram_bot_token=args.telegram_bot_token,
        telegram_shadow_channel=args.telegram_shadow_channel,
        telegram_advisory_channel=args.telegram_advisory_channel,
        prometheus_base_url=args.prometheus_base_url,
        loki_base_url=args.loki_base_url,
        grafana_dashboard_url=args.grafana_dashboard_url,
    )
    report = run_phase9_shadow_signal_smoke_workflow(
        env=env,
        output_dir=Path(args.output_dir),
        skip_migrations=args.skip_migrations,
        min_lifecycle_events=args.min_lifecycle_events,
        mode=args.mode,
    )
    if args.report_out:
        report_path = Path(args.report_out)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    return 0 if report.get("ready_for_battle_run") else 1


if __name__ == "__main__":
    raise SystemExit(main())
