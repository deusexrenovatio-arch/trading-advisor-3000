from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.app.data_plane import (
    run_phase9_historical_bootstrap,
    run_phase9_moex_historical_bootstrap,
)
from trading_advisor_3000.app.execution import (
    evaluate_phase9_sidecar_preflight,
    load_phase9_sidecar_delivery_manifest,
)
from trading_advisor_3000.app.phase9 import (
    build_phase9_battle_run_report,
    build_phase9_env_with_overrides,
    render_phase9_evidence_markdown,
    run_phase9_shadow_signal_smoke_workflow,
    run_phase9_strategy_replay_workflow,
)
from trading_advisor_3000.app.research.strategies import phase9_production_strategy_spec


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _phase8_report(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _run_phase8_proving(*, report_path: Path) -> dict[str, object]:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase8_operational_proving.py",
            "--from-git",
            "--git-ref",
            "HEAD",
            "--output",
            str(report_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    payload = _phase8_report(report_path)
    return {
        "status": "ok" if result.returncode == 0 and payload is not None and payload.get("status") == "ok" else "failed",
        "returncode": result.returncode,
        "report_path": report_path.as_posix(),
        "report": payload,
    }


def _resolve_bootstrap(
    *,
    output_dir: Path,
    bootstrap_report: Path | None,
    source_path: Path | None,
    from_date: str | None,
    till_date: str | None,
    timeframe: str,
) -> tuple[dict[str, object], Path]:
    if bootstrap_report is not None:
        payload = json.loads(bootstrap_report.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            raise ValueError("bootstrap report must be a JSON object")
        return payload, bootstrap_report
    if source_path is not None:
        payload = run_phase9_historical_bootstrap(
            source_path=source_path,
            output_dir=output_dir,
            provider_id="moex-history",
        )
    else:
        if not from_date or not till_date:
            raise ValueError("provide bootstrap_report, source_path, or both from_date and till_date")
        payload = run_phase9_moex_historical_bootstrap(
            output_dir=output_dir,
            from_date=from_date,
            till_date=till_date,
            timeframe=timeframe,
            provider_id="moex-history",
        )
    report_path = output_dir / "phase9.bootstrap.report.json"
    _write_json(report_path, payload)
    return payload, report_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the integrated Phase 9A battle-run workflow.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--bootstrap-report", default=None)
    parser.add_argument("--source-path", default=None)
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--till-date", default=None)
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--strategy", default=phase9_production_strategy_spec().strategy_version_id)
    parser.add_argument("--snapshot-path", default=None)
    parser.add_argument("--snapshot-url", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--as-of-ts", default=None)
    parser.add_argument("--max-lag-seconds", type=int, default=None)
    parser.add_argument("--telegram-channel", default="@ta3000_phase9_shadow")
    parser.add_argument("--mode", choices=("shadow", "advisory"), default="shadow")
    parser.add_argument("--horizon-bars", type=int, default=3)
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--telegram-bot-token", default=None)
    parser.add_argument("--telegram-shadow-channel", default=None)
    parser.add_argument("--telegram-advisory-channel", default=None)
    parser.add_argument("--signal-store-schema", default="signal")
    parser.add_argument("--prometheus-base-url", default=None)
    parser.add_argument("--loki-base-url", default=None)
    parser.add_argument("--grafana-dashboard-url", default=None)
    parser.add_argument("--skip-migrations", action="store_true")
    parser.add_argument("--phase8-proving-mode", choices=("skip", "reuse", "run"), default="reuse")
    parser.add_argument("--phase8-report-path", default="artifacts/phase8-operational-proving.json")
    parser.add_argument("--sidecar-base-url", default=None)
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = output_dir / "data"
    strategy_dir = output_dir / "strategy"
    runtime_dir = output_dir / "runtime"

    bootstrap_payload, bootstrap_report_path = _resolve_bootstrap(
        output_dir=data_dir,
        bootstrap_report=None if not args.bootstrap_report else Path(args.bootstrap_report),
        source_path=None if not args.source_path else Path(args.source_path),
        from_date=args.from_date,
        till_date=args.till_date,
        timeframe=args.timeframe,
    )

    strategy_report = run_phase9_strategy_replay_workflow(
        strategy_id=args.strategy,
        bootstrap_report=bootstrap_report_path,
        bars_path=None,
        dataset_version=None,
        output_dir=strategy_dir,
        telegram_channel=args.telegram_channel,
        horizon_bars=args.horizon_bars,
        snapshot_path=None if not args.snapshot_path else Path(args.snapshot_path),
        snapshot_url=args.snapshot_url,
        timeout_seconds=args.timeout_seconds,
        as_of_ts=args.as_of_ts,
        max_lag_seconds=args.max_lag_seconds,
    )
    strategy_report_path = output_dir / "phase9.strategy.report.json"
    _write_json(strategy_report_path, strategy_report)

    env = build_phase9_env_with_overrides(
        dsn=args.dsn,
        telegram_bot_token=args.telegram_bot_token,
        telegram_shadow_channel=args.telegram_shadow_channel,
        telegram_advisory_channel=args.telegram_advisory_channel,
        signal_store_schema=args.signal_store_schema,
        prometheus_base_url=args.prometheus_base_url,
        loki_base_url=args.loki_base_url,
        grafana_dashboard_url=args.grafana_dashboard_url,
    )
    runtime_report = run_phase9_shadow_signal_smoke_workflow(
        env=env,
        output_dir=runtime_dir,
        skip_migrations=args.skip_migrations,
        min_lifecycle_events=10,
        mode=args.mode,
    )
    runtime_report_path = output_dir / "phase9.runtime.report.json"
    _write_json(runtime_report_path, runtime_report)

    phase8_report: dict[str, object] | None = None
    phase8_report_path = Path(args.phase8_report_path)
    if args.phase8_proving_mode == "run":
        phase8_report = _run_phase8_proving(report_path=phase8_report_path)
    elif args.phase8_proving_mode == "reuse":
        existing = _phase8_report(phase8_report_path)
        if existing is not None:
            phase8_report = {
                "status": str(existing.get("status", "unknown")),
                "report_path": phase8_report_path.as_posix(),
                "report": existing,
            }

    sidecar_preflight: dict[str, object] | None = None
    if args.sidecar_base_url:
        manifest = load_phase9_sidecar_delivery_manifest(
            ROOT / "deployment" / "stocksharp-sidecar" / "phase9-sidecar-delivery-manifest.json"
        )
        sidecar_report = evaluate_phase9_sidecar_preflight(
            env=env,
            delivery_spec=manifest,
            base_url=args.sidecar_base_url,
            include_rollout_dry_run=True,
        )
        sidecar_preflight = sidecar_report.to_dict()

    report = build_phase9_battle_run_report(
        bootstrap_report=bootstrap_payload,
        strategy_report=strategy_report,
        runtime_report=runtime_report,
        output_paths={
            "bootstrap_report": bootstrap_report_path.as_posix(),
            "strategy_report": strategy_report_path.as_posix(),
            "runtime_report": runtime_report_path.as_posix(),
        },
        git_ref="HEAD",
        publication_posture=args.mode,
        phase8_proving=phase8_report,
        sidecar_preflight=sidecar_preflight,
    )
    evidence_text = render_phase9_evidence_markdown(report)
    evidence_path = output_dir / "phase9.evidence.md"
    evidence_path.write_text(evidence_text, encoding="utf-8")
    report["output_paths"]["evidence_markdown"] = evidence_path.as_posix()

    if args.report_out:
        _write_json(Path(args.report_out), report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["phase9a_status"] == "ready_for_review" else 1


if __name__ == "__main__":
    raise SystemExit(main())
