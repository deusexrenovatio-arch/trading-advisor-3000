from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.app.contracts import (
    DecisionCandidate,
    DecisionPublication,
    FeatureSnapshotRef,
    Mode,
    RuntimeSignal,
    SignalEvent,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.app.interfaces.api import RuntimeAPI
from trading_advisor_3000.app.research.strategies import phase9_production_strategy_spec
from trading_advisor_3000.app.runtime import build_phase9_battle_run_stack
from trading_advisor_3000.app.runtime.analytics import (
    build_phase9_battle_run_audit,
    build_phase9_battle_run_loki_lines,
    export_phase9_battle_run_prometheus,
)
from trading_advisor_3000.app.runtime.config import (
    DEFAULT_PHASE9_BATTLE_RUN_PROFILE,
    DEFAULT_PHASE9_SIGNAL_STORE_BACKEND,
    StrategyVersion,
    evaluate_phase9_battle_run_preflight,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_lines(path: Path, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(rows) + ("\n" if rows else ""), encoding="utf-8")


def _env_with_overrides(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    env["TA3000_RUNTIME_PROFILE"] = args.runtime_profile
    env["TA3000_SIGNAL_STORE_BACKEND"] = args.signal_store_backend
    if args.signal_store_schema:
        env["TA3000_SIGNAL_STORE_SCHEMA"] = args.signal_store_schema
    if args.dsn:
        env["TA3000_APP_DSN"] = args.dsn
    if args.telegram_bot_token:
        env["TA3000_TELEGRAM_BOT_TOKEN"] = args.telegram_bot_token
    if args.telegram_shadow_channel:
        env["TA3000_TELEGRAM_SHADOW_CHANNEL"] = args.telegram_shadow_channel
    if args.telegram_advisory_channel:
        env["TA3000_TELEGRAM_ADVISORY_CHANNEL"] = args.telegram_advisory_channel
    if args.prometheus_base_url:
        env["TA3000_PROMETHEUS_BASE_URL"] = args.prometheus_base_url
    if args.loki_base_url:
        env["TA3000_LOKI_BASE_URL"] = args.loki_base_url
    if args.grafana_dashboard_url:
        env["TA3000_GRAFANA_DASHBOARD_URL"] = args.grafana_dashboard_url
    return env


def _run_migrations(*, dsn: str) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/apply_app_migrations.py",
            "--dsn",
            dsn,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(result.stdout + result.stderr)


def _candidate(
    *,
    signal_id: str,
    contract_id: str,
    side: TradeSide,
    ts_decision: str,
    confidence: float,
    entry_ref: float,
    stop_ref: float,
    target_ref: float,
) -> DecisionCandidate:
    instrument_id = contract_id.split("-", maxsplit=1)[0]
    return DecisionCandidate(
        signal_id=signal_id,
        contract_id=contract_id,
        timeframe=Timeframe.M15,
        strategy_version_id=phase9_production_strategy_spec().strategy_version_id,
        mode=Mode.SHADOW,
        side=side,
        entry_ref=entry_ref,
        stop_ref=stop_ref,
        target_ref=target_ref,
        confidence=confidence,
        ts_decision=ts_decision,
        feature_snapshot=FeatureSnapshotRef(
            dataset_version="phase9-ws-c-shadow-smoke",
            snapshot_id=f"FS-{instrument_id}-{signal_id}",
        ),
    )


def _build_api(*, env: dict[str, str]) -> RuntimeAPI:
    stack = build_phase9_battle_run_stack(env=env)
    spec = phase9_production_strategy_spec()
    stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id=spec.strategy_version_id,
            status="active",
            allowed_contracts=frozenset(spec.pilot_universe),
            allowed_timeframes=frozenset(spec.allowed_timeframes),
            allowed_modes=frozenset(spec.allowed_modes),
            activated_from="2026-03-22T07:00:00Z",
        )
    )
    return RuntimeAPI(runtime_stack=stack)


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
    parser.add_argument("--skip-migrations", action="store_true")
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    env = _env_with_overrides(args)
    preflight = evaluate_phase9_battle_run_preflight(env)
    if not preflight.is_ready:
        report = {"preflight": preflight.to_dict()}
        if args.report_out:
            _write_json(Path(args.report_out), report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1

    if not args.skip_migrations:
        _run_migrations(dsn=preflight.config.app_dsn)

    initial_candidates = [
        _candidate(
            signal_id="SIG-PHASE9-WSC-0001",
            contract_id="BR-6.26",
            side=TradeSide.LONG,
            ts_decision="2026-03-22T07:00:00Z",
            confidence=0.71,
            entry_ref=82.55,
            stop_ref=81.90,
            target_ref=83.85,
        ),
        _candidate(
            signal_id="SIG-PHASE9-WSC-0002",
            contract_id="Si-6.26",
            side=TradeSide.SHORT,
            ts_decision="2026-03-22T07:01:00Z",
            confidence=0.69,
            entry_ref=91820.0,
            stop_ref=91960.0,
            target_ref=91540.0,
        ),
        _candidate(
            signal_id="SIG-PHASE9-WSC-0003",
            contract_id="BR-6.26",
            side=TradeSide.LONG,
            ts_decision="2026-03-22T07:02:00Z",
            confidence=0.74,
            entry_ref=82.70,
            stop_ref=82.05,
            target_ref=84.00,
        ),
        _candidate(
            signal_id="SIG-PHASE9-WSC-0004",
            contract_id="Si-6.26",
            side=TradeSide.SHORT,
            ts_decision="2026-03-22T07:03:00Z",
            confidence=0.73,
            entry_ref=91790.0,
            stop_ref=91940.0,
            target_ref=91490.0,
        ),
    ]

    api_first = _build_api(env=env)
    first_batch = api_first.replay_candidates(initial_candidates)

    api_second = _build_api(env=env)
    restart_batch = api_second.replay_candidates(initial_candidates)

    edited_candidates = [
        DecisionCandidate.from_dict(
            {
                **initial_candidates[0].to_dict(),
                "entry_ref": 82.68,
                "stop_ref": 82.02,
                "target_ref": 84.02,
                "confidence": 0.79,
                "ts_decision": "2026-03-22T07:05:00Z",
            }
        ),
        DecisionCandidate.from_dict(
            {
                **initial_candidates[2].to_dict(),
                "entry_ref": 82.84,
                "stop_ref": 82.16,
                "target_ref": 84.18,
                "confidence": 0.81,
                "ts_decision": "2026-03-22T07:06:00Z",
            }
        ),
    ]
    edit_batch = api_second.replay_candidates(edited_candidates)

    close_results = [
        api_second.close_signal(
            signal_id="SIG-PHASE9-WSC-0001",
            closed_at="2026-03-22T07:10:00Z",
            reason_code="shadow_take_profit",
        ),
        api_second.close_signal(
            signal_id="SIG-PHASE9-WSC-0003",
            closed_at="2026-03-22T07:12:00Z",
            reason_code="shadow_time_exit",
        ),
    ]
    cancel_results = [
        api_second.cancel_signal(
            signal_id="SIG-PHASE9-WSC-0002",
            canceled_at="2026-03-22T07:11:00Z",
            reason_code="operator_cancel",
        ),
        api_second.cancel_signal(
            signal_id="SIG-PHASE9-WSC-0004",
            canceled_at="2026-03-22T07:13:00Z",
            reason_code="operator_cancel",
        ),
    ]

    api_final = _build_api(env=env)
    publication_events = [DecisionPublication.from_dict(row) for row in api_final.list_publication_events()]
    signal_events = [SignalEvent.from_dict(row) for row in api_final.list_signal_events()]
    active_signals = [RuntimeSignal.from_dict(row) for row in api_final.list_active_signals()]
    observability_targets = {
        "prometheus_base_url": preflight.config.prometheus_base_url,
        "loki_base_url": preflight.config.loki_base_url,
        "grafana_dashboard_url": preflight.config.grafana_dashboard_url,
    }
    audit = build_phase9_battle_run_audit(
        publication_events=publication_events,
        signal_events=signal_events,
        active_signals=active_signals,
        restart_published_delta=int(restart_batch["replay_report"]["published"]),
        preflight_ready=preflight.is_ready,
        warnings=preflight.warnings,
        observability_targets=observability_targets,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    preflight_path = output_dir / "phase9.preflight.json"
    publication_events_path = output_dir / "runtime.telegram.publication_events.sample.jsonl"
    signal_events_path = output_dir / "runtime.signal_events.sample.jsonl"
    prometheus_path = output_dir / "observability.prometheus.metrics.txt"
    loki_path = output_dir / "observability.loki.events.jsonl"

    _write_json(preflight_path, preflight.to_dict())
    _write_jsonl(publication_events_path, [item.to_dict() for item in publication_events])
    _write_jsonl(signal_events_path, [item.to_dict() for item in signal_events])
    _write_lines(loki_path, build_phase9_battle_run_loki_lines(
        publication_events=publication_events,
        signal_events=signal_events,
        audit=audit,
    ))
    prometheus_path.write_text(export_phase9_battle_run_prometheus(audit), encoding="utf-8")

    report = {
        "preflight": preflight.to_dict(),
        "initial_batch": first_batch,
        "restart_probe": {
            "accepted": restart_batch["replay_report"]["accepted"],
            "published_delta": restart_batch["replay_report"]["published"],
            "edited_delta": restart_batch["replay_report"]["edited"],
        },
        "edit_batch": edit_batch,
        "close_results": close_results,
        "cancel_results": cancel_results,
        "publication_audit": audit,
        "output_paths": {
            "preflight_report": preflight_path.as_posix(),
            "publication_events": publication_events_path.as_posix(),
            "signal_events": signal_events_path.as_posix(),
            "observability_prometheus_metrics": prometheus_path.as_posix(),
            "observability_loki_events": loki_path.as_posix(),
        },
    }
    if args.report_out:
        _write_json(Path(args.report_out), report)
    print(json.dumps(report, ensure_ascii=False, indent=2))

    return 0 if audit["status"] == "ok" and int(audit["lifecycle_total"]) >= args.min_lifecycle_events else 1


if __name__ == "__main__":
    raise SystemExit(main())
