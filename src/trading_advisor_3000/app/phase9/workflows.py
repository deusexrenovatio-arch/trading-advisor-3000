from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from trading_advisor_3000.app.contracts import (
    CanonicalBar,
    DecisionCandidate,
    DecisionPublication,
    FeatureSnapshotRef,
    Mode,
    RuntimeSignal,
    SignalEvent,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.app.data_plane import (
    evaluate_phase9_live_smoke,
    load_phase9_live_snapshot,
    load_phase9_live_snapshot_from_url,
)
from trading_advisor_3000.app.interfaces.api import RuntimeAPI
from trading_advisor_3000.app.research import run_research_from_bars
from trading_advisor_3000.app.research.strategies import (
    assess_phase9_production_pilot_readiness,
    phase9_production_backtest_config,
    phase9_production_strategy_spec,
    production_strategy_ids,
)
from trading_advisor_3000.app.runtime import build_phase9_battle_run_stack
from trading_advisor_3000.app.runtime.analytics import (
    build_phase9_battle_run_audit,
    build_phase9_battle_run_loki_lines,
    export_phase9_battle_run_prometheus,
    run_system_shadow_replay,
)
from trading_advisor_3000.app.runtime.config import (
    DEFAULT_PHASE9_BATTLE_RUN_PROFILE,
    DEFAULT_PHASE9_SIGNAL_STORE_BACKEND,
    StrategyVersion,
    evaluate_phase9_battle_run_preflight,
)


ROOT = Path(__file__).resolve().parents[4]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def run_phase9_strategy_replay_workflow(
    *,
    strategy_id: str,
    bootstrap_report: Path | None,
    bars_path: Path | None,
    dataset_version: str | None,
    output_dir: Path,
    telegram_channel: str,
    horizon_bars: int,
    snapshot_path: Path | None,
    snapshot_url: str | None,
    timeout_seconds: float,
    as_of_ts: str | None,
    max_lag_seconds: int | None,
) -> dict[str, object]:
    if strategy_id not in production_strategy_ids():
        raise ValueError(f"unsupported Phase 9 production strategy: {strategy_id}")
    if bootstrap_report is None and (bars_path is None or dataset_version is None):
        raise ValueError("provide bootstrap_report or both bars_path and dataset_version")

    if bootstrap_report is not None:
        resolved_bars_path, resolved_dataset_version = _bootstrap_context(bootstrap_report)
    else:
        assert bars_path is not None
        assert dataset_version is not None
        resolved_bars_path, resolved_dataset_version = bars_path, dataset_version

    if snapshot_path is not None and snapshot_url is not None:
        raise ValueError("provide at most one of snapshot_path or snapshot_url")

    spec = phase9_production_strategy_spec()
    live_provider_id = _live_provider_id(live_feed=spec.live_feed)
    bars = _load_bars(resolved_bars_path)
    instrument_by_contract = {bar.contract_id: bar.instrument_id for bar in bars}
    covered_contract_ids = {bar.contract_id for bar in bars}

    backtest_dir = output_dir / "backtest"
    replay_dir = output_dir / "replay"
    research_report = run_research_from_bars(
        bars=bars,
        instrument_by_contract=instrument_by_contract,
        strategy_version_id=strategy_id,
        dataset_version=resolved_dataset_version,
        output_dir=backtest_dir,
        backtest_config=phase9_production_backtest_config(),
    )
    replay_report = run_system_shadow_replay(
        bars=bars,
        instrument_by_contract=instrument_by_contract,
        strategy_version_id=strategy_id,
        dataset_version=resolved_dataset_version,
        output_dir=replay_dir,
        telegram_channel=telegram_channel,
        horizon_bars=horizon_bars,
        runtime_allowed_contracts=set(spec.pilot_universe),
    )

    live_smoke_report: dict[str, object] | None = None
    if snapshot_path is not None:
        snapshot_provider_id, rows = load_phase9_live_snapshot(snapshot_path)
        if snapshot_provider_id and snapshot_provider_id != live_provider_id:
            raise ValueError(
                f"snapshot provider_id mismatch: expected {live_provider_id}, got {snapshot_provider_id}"
            )
        live_smoke_report = evaluate_phase9_live_smoke(
            provider_id=live_provider_id,
            snapshot_rows=rows,
            as_of_ts=as_of_ts or _utc_now(),
            max_lag_seconds=max_lag_seconds,
        )
        live_smoke_report["source_kind"] = "file"
        live_smoke_report["snapshot_path"] = str(snapshot_path)
    elif snapshot_url is not None:
        snapshot_provider_id, rows = load_phase9_live_snapshot_from_url(
            snapshot_url,
            timeout_seconds=timeout_seconds,
        )
        if snapshot_provider_id and snapshot_provider_id != live_provider_id:
            raise ValueError(
                f"snapshot provider_id mismatch: expected {live_provider_id}, got {snapshot_provider_id}"
            )
        live_smoke_report = evaluate_phase9_live_smoke(
            provider_id=live_provider_id,
            snapshot_rows=rows,
            as_of_ts=as_of_ts or _utc_now(),
            max_lag_seconds=max_lag_seconds,
        )
        live_smoke_report["source_kind"] = "url"
        live_smoke_report["snapshot_url"] = snapshot_url

    readiness = assess_phase9_production_pilot_readiness(
        covered_contract_ids=covered_contract_ids,
        research_report=research_report,
        replay_report=replay_report,
        live_smoke_status=None if live_smoke_report is None else str(live_smoke_report.get("status")),
    )
    return {
        "strategy_spec": spec.to_dict(),
        "dataset_version": resolved_dataset_version,
        "bars_path": resolved_bars_path.as_posix(),
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


def build_phase9_env_with_overrides(
    *,
    base_env: Mapping[str, str] | None = None,
    runtime_profile: str = DEFAULT_PHASE9_BATTLE_RUN_PROFILE,
    signal_store_backend: str = DEFAULT_PHASE9_SIGNAL_STORE_BACKEND,
    signal_store_schema: str | None = None,
    dsn: str | None = None,
    telegram_bot_token: str | None = None,
    telegram_shadow_channel: str | None = None,
    telegram_advisory_channel: str | None = None,
    prometheus_base_url: str | None = None,
    loki_base_url: str | None = None,
    grafana_dashboard_url: str | None = None,
) -> dict[str, str]:
    env = dict(base_env or os.environ)
    env["TA3000_RUNTIME_PROFILE"] = runtime_profile
    env["TA3000_SIGNAL_STORE_BACKEND"] = signal_store_backend
    if signal_store_schema:
        env["TA3000_SIGNAL_STORE_SCHEMA"] = signal_store_schema
    if dsn:
        env["TA3000_APP_DSN"] = dsn
    if telegram_bot_token:
        env["TA3000_TELEGRAM_BOT_TOKEN"] = telegram_bot_token
    if telegram_shadow_channel:
        env["TA3000_TELEGRAM_SHADOW_CHANNEL"] = telegram_shadow_channel
    if telegram_advisory_channel:
        env["TA3000_TELEGRAM_ADVISORY_CHANNEL"] = telegram_advisory_channel
    if prometheus_base_url:
        env["TA3000_PROMETHEUS_BASE_URL"] = prometheus_base_url
    if loki_base_url:
        env["TA3000_LOKI_BASE_URL"] = loki_base_url
    if grafana_dashboard_url:
        env["TA3000_GRAFANA_DASHBOARD_URL"] = grafana_dashboard_url
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
        raise RuntimeError(result.stdout + result.stderr)


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


def _select_publication_channel(*, mode: str, preflight_config) -> tuple[str, list[str]]:
    warnings: list[str] = []
    channel = str(preflight_config.telegram_shadow_channel)
    if mode == "advisory":
        advisory = preflight_config.telegram_advisory_channel
        if advisory:
            channel = advisory
            warnings.append("runtime signals remain shadow mode while publication posture uses advisory destination")
        else:
            warnings.append("advisory mode requested but advisory channel is not configured; using shadow channel")
    return channel, warnings


def _build_api(*, env: dict[str, str], telegram_channel: str) -> RuntimeAPI:
    stack = build_phase9_battle_run_stack(env=env, telegram_channel_override=telegram_channel)
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


def run_phase9_shadow_signal_smoke_workflow(
    *,
    env: Mapping[str, str],
    output_dir: Path,
    skip_migrations: bool,
    min_lifecycle_events: int,
    mode: str = "shadow",
) -> dict[str, object]:
    if mode not in {"shadow", "advisory"}:
        raise ValueError("mode must be one of: shadow, advisory")
    preflight = evaluate_phase9_battle_run_preflight(env)
    if not preflight.is_ready:
        return {"preflight": preflight.to_dict()}

    channel, posture_warnings = _select_publication_channel(mode=mode, preflight_config=preflight.config)
    if not skip_migrations:
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

    env_dict = dict(env)
    api_first = _build_api(env=env_dict, telegram_channel=channel)
    first_batch = api_first.replay_candidates(initial_candidates)

    api_second = _build_api(env=env_dict, telegram_channel=channel)
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

    api_final = _build_api(env=env_dict, telegram_channel=channel)
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
        warnings=[*preflight.warnings, *posture_warnings],
        observability_targets=observability_targets,
    )
    audit["publication_posture"] = mode
    audit["publisher_channel"] = channel

    output_dir.mkdir(parents=True, exist_ok=True)
    preflight_path = output_dir / "phase9.preflight.json"
    publication_events_path = output_dir / "runtime.telegram.publication_events.sample.jsonl"
    signal_events_path = output_dir / "runtime.signal_events.sample.jsonl"
    prometheus_path = output_dir / "observability.prometheus.metrics.txt"
    loki_path = output_dir / "observability.loki.events.jsonl"

    _write_json(preflight_path, preflight.to_dict())
    _write_jsonl(publication_events_path, [item.to_dict() for item in publication_events])
    _write_jsonl(signal_events_path, [item.to_dict() for item in signal_events])
    _write_lines(
        loki_path,
        build_phase9_battle_run_loki_lines(
            publication_events=publication_events,
            signal_events=signal_events,
            audit=audit,
        ),
    )
    prometheus_path.write_text(export_phase9_battle_run_prometheus(audit), encoding="utf-8")

    report = {
        "preflight": preflight.to_dict(),
        "publication_posture": mode,
        "publisher_channel": channel,
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
        "ready_for_battle_run": audit["status"] == "ok" and int(audit["lifecycle_total"]) >= min_lifecycle_events,
    }
    return report


def render_phase9_evidence_markdown(report: dict[str, object]) -> str:
    strategy = report.get("strategy_replay", {})
    runtime = report.get("runtime_smoke", {})
    bootstrap = report.get("bootstrap", {})
    phase8 = report.get("phase8_proving")
    sidecar = report.get("sidecar_preflight")
    output_paths = report.get("output_paths", {})
    lines = [
        "# Phase 9 Evidence Package",
        "",
        "## Baseline",
        f"- git ref: `{report.get('git_ref', 'HEAD')}`",
        f"- phase 9 surface: `{report.get('phase9_surface', '9A')}`",
        f"- publication posture: `{report.get('publication_posture', 'shadow')}`",
        "",
        "## Data evidence",
        f"- MOEX dataset version: `{bootstrap.get('dataset_version', '')}`",
        f"- bootstrap report: `{output_paths.get('bootstrap_report', '')}`",
        f"- live smoke status: `{((strategy.get('live_smoke') or {}).get('status', 'missing'))}`",
        "",
        "## Strategy evidence",
        f"- strategy id: `{((strategy.get('strategy_spec') or {}).get('strategy_version_id', ''))}`",
        f"- pilot readiness: `{((strategy.get('pilot_readiness') or {}).get('status', 'unknown'))}`",
        f"- strategy report: `{output_paths.get('strategy_report', '')}`",
        "",
        "## Runtime and Telegram evidence",
        f"- publisher channel: `{runtime.get('publisher_channel', '')}`",
        f"- lifecycle total: `{((runtime.get('publication_audit') or {}).get('lifecycle_total', 0))}`",
        f"- runtime report: `{output_paths.get('runtime_report', '')}`",
        "",
        "## Observability evidence",
        f"- battle-run metrics: `{((runtime.get('output_paths') or {}).get('observability_prometheus_metrics', ''))}`",
        f"- battle-run logs: `{((runtime.get('output_paths') or {}).get('observability_loki_events', ''))}`",
        "",
        "## Phase 8 proving",
        f"- phase8 proving status: `{((phase8 or {}).get('status', 'not_attached'))}`",
        f"- phase8 proving artifact: `{((phase8 or {}).get('report_path', ''))}`",
        "",
        "## Optional 9B boundary",
        f"- sidecar preflight status: `{((sidecar or {}).get('status', 'not_run'))}`",
        "",
        "## Verdict",
        f"- phase9a integration status: `{report.get('phase9a_status', 'unknown')}`",
    ]
    warnings = report.get("warnings", [])
    if isinstance(warnings, list) and warnings:
        lines.extend(["", "## Warnings"])
        for item in warnings:
            lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def build_phase9_battle_run_report(
    *,
    bootstrap_report: dict[str, object],
    strategy_report: dict[str, object],
    runtime_report: dict[str, object],
    output_paths: dict[str, str],
    git_ref: str,
    publication_posture: str,
    phase8_proving: dict[str, object] | None = None,
    sidecar_preflight: dict[str, object] | None = None,
) -> dict[str, object]:
    warnings: list[str] = []
    strategy_readiness = ((strategy_report.get("pilot_readiness") or {}).get("status") == "ready_for_shadow_pilot")
    live_smoke_ok = ((strategy_report.get("live_smoke") or {}).get("status") == "ok")
    runtime_ready = bool(runtime_report.get("ready_for_battle_run"))
    if not live_smoke_ok:
        warnings.append("QUIK live smoke is not green in the integrated battle-run report")
    if not strategy_readiness:
        warnings.append("strategy replay is not ready for pilot according to WS-B readiness rules")
    if not runtime_ready:
        warnings.append("Telegram/PostgreSQL battle-run smoke is not ready according to WS-C audit")
    if phase8_proving is None or str(phase8_proving.get("status", "")) != "ok":
        warnings.append("Phase 8 proving is not attached as green evidence in this integrated report")
    if publication_posture == "advisory":
        warnings.append("advisory publication posture still uses shadow runtime signals under the current runtime enum surface")

    phase9a_status = "ready_for_review" if strategy_readiness and live_smoke_ok and runtime_ready else "blocked"
    return {
        "git_ref": git_ref,
        "phase9_surface": "9A",
        "publication_posture": publication_posture,
        "bootstrap": bootstrap_report,
        "strategy_replay": strategy_report,
        "runtime_smoke": runtime_report,
        "phase8_proving": phase8_proving,
        "sidecar_preflight": sidecar_preflight,
        "warnings": warnings,
        "phase9a_status": phase9a_status,
        "output_paths": output_paths,
    }
