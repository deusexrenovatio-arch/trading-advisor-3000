from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib import request as urllib_request


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.contracts import Mode, OrderIntent
from trading_advisor_3000.product_plane.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionFeatureFlags,
    LiveExecutionRetryPolicy,
    StockSharpHTTPTransport,
    StockSharpHTTPTransportConfig,
)
from trading_advisor_3000.product_plane.execution.broker_sync import BrokerSyncEngine, ControlledLiveExecutionEngine
from trading_advisor_3000.product_plane.runtime.config import DEFAULT_REQUIRED_LIVE_SECRETS


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_SYNTHETIC_MARKER_TOKENS = {
    "stub",
    "mock",
    "simulated",
    "simulation",
    "synthetic",
    "memory",
    "inmemory",
    "local",
    "dummy",
    "fake",
    "test",
    "sandbox",
}


def _has_synthetic_marker(value: str) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return True
    compact = normalized.replace("-", "").replace("_", "").replace(".", "")
    if compact in {"inmemory", "memory"}:
        return True
    tokens = {token for token in normalized.replace(".", "-").replace("_", "-").split("-") if token}
    return bool(tokens.intersection(_SYNTHETIC_MARKER_TOKENS))


def _env_bool(env: dict[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(env: dict[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_float(env: dict[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _required_secret_names(env: dict[str, str]) -> tuple[str, ...]:
    raw = str(env.get("TA3000_REQUIRED_LIVE_SECRETS", "")).strip()
    if not raw:
        return DEFAULT_REQUIRED_LIVE_SECRETS
    names = tuple(item.strip() for item in raw.split(",") if item.strip())
    return names or DEFAULT_REQUIRED_LIVE_SECRETS


@dataclass(frozen=True)
class StageResult:
    stage: str
    status: str
    details: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage,
            "status": self.status,
            "details": self.details,
        }


def _build_engine(
    *,
    base_url: str,
    env: dict[str, str],
    account_id: str,
) -> tuple[BrokerSyncEngine, ControlledLiveExecutionEngine, LiveExecutionBridge]:
    flags = LiveExecutionFeatureFlags(
        enable_live_execution=_env_bool(env, "TA3000_ENABLE_LIVE_EXECUTION", False),
        enable_stocksharp_bridge=_env_bool(env, "TA3000_ENABLE_STOCKSHARP_BRIDGE", False),
        enable_quik_connector=_env_bool(env, "TA3000_ENABLE_QUIK_CONNECTOR", False),
        enable_finam_transport=_env_bool(env, "TA3000_ENABLE_FINAM_TRANSPORT", False),
        enforce_live_secrets=_env_bool(env, "TA3000_ENFORCE_LIVE_SECRETS", True),
        enforce_secret_age=_env_bool(env, "TA3000_ENFORCE_SECRET_AGE", False),
        max_secret_age_days=_env_int(env, "TA3000_SECRET_MAX_AGE_DAYS", 90),
        required_live_secret_env_vars=_required_secret_names(env),
        environment=str(env.get("TA3000_ENVIRONMENT", "staging-real-transport")),
    )
    retry_policy = LiveExecutionRetryPolicy(
        max_attempts=_env_int(env, "TA3000_RETRY_MAX_ATTEMPTS", 3),
        backoff_seconds=_env_float(env, "TA3000_RETRY_BACKOFF_SECONDS", 0.05),
    )
    http_transport = StockSharpHTTPTransport(
        config=StockSharpHTTPTransportConfig(
            base_url=base_url,
            timeout_seconds=_env_float(env, "TA3000_SIDECAR_TIMEOUT_SECONDS", 3.0),
            stream_batch_size=_env_int(env, "TA3000_SIDECAR_STREAM_BATCH_SIZE", 500),
            api_prefix=str(env.get("TA3000_SIDECAR_API_PREFIX", "v1")),
        )
    )
    bridge = LiveExecutionBridge(
        sidecar=http_transport,
        flags=flags,
        retry_policy=retry_policy,
        env=env,
    )
    sync = BrokerSyncEngine(account_id=account_id, broker_adapter="stocksharp-sidecar")
    return sync, ControlledLiveExecutionEngine(bridge=bridge, sync_engine=sync), bridge


def _intent(*, intent_id: str, qty: int, created_at: str) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id=f"SIG-{intent_id}",
        mode=Mode.LIVE,
        broker_adapter="stocksharp-sidecar",
        action="buy",
        contract_id="BR-6.26",
        qty=qty,
        price=82.5,
        stop_price=81.8,
        created_at=created_at,
    )


def _toggle_kill_switch(*, base_url: str, active: bool) -> dict[str, object]:
    payload = json.dumps({"active": active}).encode("utf-8")
    request = urllib_request.Request(
        url=f"{base_url.rstrip('/')}/v1/admin/kill-switch",
        method="POST",
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    with urllib_request.urlopen(request, timeout=2.0) as response:
        body = response.read().decode("utf-8")
    parsed = json.loads(body) if body.strip() else {}
    if not isinstance(parsed, dict):
        return {"ok": False, "error": "kill-switch response is not object"}
    return parsed


def _run_connectivity_stage(
    *,
    bridge: LiveExecutionBridge,
) -> StageResult:
    health = bridge.health()
    bridge_status = str(health.get("status", "unknown"))
    transport_health = health.get("transport_health", {})
    degraded_transports: list[str] = []
    if isinstance(transport_health, dict):
        for key, value in transport_health.items():
            status = str(value.get("status", "unknown")) if isinstance(value, dict) else "unknown"
            if status != "ok":
                degraded_transports.append(str(key))
    connector_mode = ""
    connector_backend = ""
    connector_ready = False
    connector_session_id = ""
    connector_binding_source = ""
    connector_last_heartbeat = ""
    sidecar = health.get("sidecar")
    if isinstance(sidecar, dict):
        remote = sidecar.get("remote")
        if isinstance(remote, dict):
            connector_mode = str(remote.get("connector_mode", "")).strip().lower()
            connector_backend = str(remote.get("connector_backend", "")).strip().lower()
            connector_ready = bool(remote.get("connector_ready", False))
            connector_session_id = str(remote.get("connector_session_id", "")).strip()
            connector_binding_source = str(remote.get("connector_binding_source", "")).strip().lower()
            connector_last_heartbeat = str(remote.get("connector_last_heartbeat", "")).strip()

    connector_errors: list[str] = []
    if connector_mode not in {"staging-real", "real-staging", "real"}:
        connector_errors.append("missing_or_invalid_connector_mode")
    if _has_synthetic_marker(connector_backend):
        connector_errors.append("missing_or_invalid_connector_backend")
    if connector_ready is not True:
        connector_errors.append("connector_not_ready")
    if not connector_session_id:
        connector_errors.append("missing_connector_session_id")
    if _has_synthetic_marker(connector_binding_source):
        connector_errors.append("missing_or_invalid_connector_binding_source")
    if not connector_last_heartbeat:
        connector_errors.append("missing_connector_last_heartbeat")

    if bridge_status != "ok" or degraded_transports or connector_errors:
        return StageResult(
            stage="connectivity",
            status="failed",
            details={
                "bridge_status": bridge_status,
                "degraded_transports": degraded_transports,
                "preflight_errors": health.get("preflight_errors", []),
                "connector_mode": connector_mode,
                "connector_backend": connector_backend,
                "connector_ready": connector_ready,
                "connector_session_id": connector_session_id,
                "connector_binding_source": connector_binding_source,
                "connector_last_heartbeat": connector_last_heartbeat,
                "connector_errors": connector_errors,
            },
        )
    return StageResult(
        stage="connectivity",
        status="ok",
        details={
            "bridge_status": bridge_status,
            "degraded_transports": degraded_transports,
            "preflight_errors": health.get("preflight_errors", []),
            "connector_mode": connector_mode,
            "connector_backend": connector_backend,
            "connector_ready": connector_ready,
            "connector_session_id": connector_session_id,
            "connector_binding_source": connector_binding_source,
            "connector_last_heartbeat": connector_last_heartbeat,
            "connector_errors": connector_errors,
        },
    )


def _run_canary_stage(
    *,
    engine: ControlledLiveExecutionEngine,
    sync: BrokerSyncEngine,
    canary_intent_id: str,
    dry_run: bool,
) -> StageResult:
    if dry_run:
        return StageResult(stage="canary", status="planned", details={"intent_id": canary_intent_id})

    submitted_at = _utc_now()
    acks = engine.submit_intents(
        [_intent(intent_id=canary_intent_id, qty=1, created_at=submitted_at)],
        submitted_at=submitted_at,
    )
    sync_stats = engine.poll_broker_sync()
    snapshot = sync.to_dict()
    registered = int(snapshot["stats"]["registered_intents"])
    orders_synced = int(snapshot["stats"]["orders_synced"])
    if registered != 1 or orders_synced < 1:
        return StageResult(
            stage="canary",
            status="failed",
            details={
                "acks": acks,
                "sync_stats": sync_stats,
                "registered_intents": registered,
                "orders_synced": orders_synced,
            },
        )
    return StageResult(
        stage="canary",
        status="ok",
        details={
            "acks": acks,
            "sync_stats": sync_stats,
            "registered_intents": registered,
            "orders_synced": orders_synced,
        },
    )


def _run_batch_stage(
    *,
    engine: ControlledLiveExecutionEngine,
    sync: BrokerSyncEngine,
    batch_size: int,
    dry_run: bool,
    kill_switch_on_failure: bool,
    kill_switch_toggler: Callable[[bool], dict[str, object] | None],
) -> StageResult:
    if dry_run:
        return StageResult(stage="batch", status="planned", details={"batch_size": batch_size})

    incidents_before = len(sync.list_incidents())
    submitted_at = _utc_now()
    intents = [
        _intent(
            intent_id=f"INT-STAGING-BATCH-{index+1}",
            qty=1,
            created_at=submitted_at,
        )
        for index in range(batch_size)
    ]
    acks = engine.submit_intents(intents, submitted_at=submitted_at)
    sync_stats = engine.poll_broker_sync()
    reconciliation = engine.reconcile(expected_positions=sync.list_positions())
    observability = engine.observability_snapshot()
    sync_incidents_total = len(sync.list_incidents())
    sync_incidents_delta = max(0, sync_incidents_total - incidents_before)
    if reconciliation.incidents or sync_incidents_delta > 0:
        kill_switch_result: dict[str, object] | None = None
        cancel_results: list[dict[str, object]] = []
        if kill_switch_on_failure:
            try:
                kill_switch_result = kill_switch_toggler(True)
            except Exception as exc:  # pragma: no cover - endpoint might be unavailable in some gateways.
                kill_switch_result = {"ok": False, "error": str(exc)}
            for intent in intents:
                try:
                    cancel_results.append(
                        engine.cancel_intent(
                            intent_id=intent.intent_id,
                            canceled_at=_utc_now(),
                        )
                    )
                except Exception as exc:  # pragma: no cover - best effort cleanup
                    cancel_results.append({"intent_id": intent.intent_id, "cancel_error": str(exc)})
        return StageResult(
            stage="batch",
            status="failed",
            details={
                "batch_size": batch_size,
                "acks": acks,
                "sync_stats": sync_stats,
                "reconciliation_incidents": len(reconciliation.incidents),
                "reconciliation": reconciliation.to_dict(),
                "sync_incidents_total": sync_incidents_total,
                "sync_incidents_delta": sync_incidents_delta,
                "observability": observability,
                "kill_switch_result": kill_switch_result,
                "cancel_results": cancel_results,
            },
        )
    return StageResult(
        stage="batch",
        status="ok",
        details={
            "batch_size": batch_size,
            "acks": acks,
            "sync_stats": sync_stats,
            "reconciliation_incidents": 0,
            "sync_incidents_total": sync_incidents_total,
            "sync_incidents_delta": sync_incidents_delta,
            "observability": observability,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run staging-first real execution rollout procedure.")
    parser.add_argument("--base-url", default=os.environ.get("TA3000_SIDECAR_BASE_URL", "http://127.0.0.1:18081"))
    parser.add_argument("--account-id", default="STAGING-LIVE-ACC")
    parser.add_argument("--stage", choices=("connectivity", "canary", "batch", "all"), default="all")
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--canary-intent-id", default="INT-STAGING-CANARY-1")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--kill-switch-on-failure", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    env = dict(os.environ)
    sync, engine, bridge = _build_engine(base_url=args.base_url, env=env, account_id=args.account_id)
    stages = ["connectivity", "canary", "batch"] if args.stage == "all" else [args.stage]
    records: list[StageResult] = []

    for stage in stages:
        if stage == "connectivity":
            result = _run_connectivity_stage(bridge=bridge)
        elif stage == "canary":
            result = _run_canary_stage(
                engine=engine,
                sync=sync,
                canary_intent_id=args.canary_intent_id,
                dry_run=args.dry_run,
            )
        else:
            result = _run_batch_stage(
                engine=engine,
                sync=sync,
                batch_size=max(1, args.batch_size),
                dry_run=args.dry_run,
                kill_switch_on_failure=args.kill_switch_on_failure,
                kill_switch_toggler=lambda active: _toggle_kill_switch(base_url=args.base_url, active=active),
            )
        records.append(result)
        if result.status == "failed":
            break

    status = "ok" if all(item.status in {"ok", "planned"} for item in records) else "failed"
    payload = {
        "generated_at": _utc_now(),
        "status": status,
        "base_url": args.base_url,
        "account_id": args.account_id,
        "dry_run": args.dry_run,
        "stages": [item.to_dict() for item in records],
    }

    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"staging rollout: {payload['status']} (stages={len(records)})")
        for item in records:
            print(f"- {item.stage}: {item.status}")
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
