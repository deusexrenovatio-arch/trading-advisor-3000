from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Mapping

from trading_advisor_3000.app.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionFeatureFlags,
    LiveExecutionRetryPolicy,
    StockSharpHTTPTransport,
    StockSharpHTTPTransportConfig,
    StockSharpSidecarStub,
)
from trading_advisor_3000.app.runtime.config import DEFAULT_REQUIRED_LIVE_SECRETS


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def _env_text(env: Mapping[str, str], name: str, default: str) -> str:
    raw = env.get(name)
    if raw is None:
        return default
    trimmed = raw.strip()
    return trimmed if trimmed else default


def _required_secret_names(env: Mapping[str, str]) -> tuple[str, ...]:
    raw = env.get("TA3000_REQUIRED_LIVE_SECRETS", "")
    if not raw.strip():
        return DEFAULT_REQUIRED_LIVE_SECRETS
    names = tuple(item.strip() for item in raw.split(",") if item.strip())
    return names or DEFAULT_REQUIRED_LIVE_SECRETS


def _build_http_sidecar_transport(env: Mapping[str, str]) -> StockSharpHTTPTransport:
    return StockSharpHTTPTransport(
        config=StockSharpHTTPTransportConfig(
            base_url=_env_text(env, "TA3000_SIDECAR_BASE_URL", "http://127.0.0.1:18081"),
            timeout_seconds=_env_float(env, "TA3000_SIDECAR_TIMEOUT_SECONDS", 3.0),
            stream_batch_size=_env_int(env, "TA3000_SIDECAR_STREAM_BATCH_SIZE", 500),
            api_prefix=_env_text(env, "TA3000_SIDECAR_API_PREFIX", "v1"),
        )
    )


def build_live_bridge_from_env(env: Mapping[str, str] | None = None) -> LiveExecutionBridge:
    source = env or os.environ
    sidecar_transport_mode = _env_text(source, "TA3000_SIDECAR_TRANSPORT", "stub").lower()
    if sidecar_transport_mode not in {"stub", "http"}:
        raise ValueError("TA3000_SIDECAR_TRANSPORT must be one of: stub, http")

    flags = LiveExecutionFeatureFlags(
        enable_live_execution=_env_bool(source, "TA3000_ENABLE_LIVE_EXECUTION", False),
        enable_stocksharp_bridge=_env_bool(source, "TA3000_ENABLE_STOCKSHARP_BRIDGE", False),
        enable_quik_connector=_env_bool(source, "TA3000_ENABLE_QUIK_CONNECTOR", False),
        enable_finam_transport=_env_bool(source, "TA3000_ENABLE_FINAM_TRANSPORT", False),
        enforce_live_secrets=_env_bool(source, "TA3000_ENFORCE_LIVE_SECRETS", True),
        enforce_secret_age=_env_bool(source, "TA3000_ENFORCE_SECRET_AGE", False),
        max_secret_age_days=_env_int(source, "TA3000_SECRET_MAX_AGE_DAYS", 90),
        required_live_secret_env_vars=_required_secret_names(source),
        environment=source.get("TA3000_ENVIRONMENT", "staging-live-sim").strip() or "staging-live-sim",
    )
    retry_policy = LiveExecutionRetryPolicy(
        max_attempts=_env_int(source, "TA3000_RETRY_MAX_ATTEMPTS", 3),
        backoff_seconds=_env_float(source, "TA3000_RETRY_BACKOFF_SECONDS", 0.0),
    )
    adapter_transports: dict[str, object] = {}
    if sidecar_transport_mode == "http":
        adapter_transports["stocksharp-sidecar"] = _build_http_sidecar_transport(source)
    return LiveExecutionBridge(
        sidecar=StockSharpSidecarStub(),
        flags=flags,
        retry_policy=retry_policy,
        adapter_transports=adapter_transports,
        env=source,
    )


def build_runtime_operational_snapshot(env: Mapping[str, str] | None = None) -> dict[str, object]:
    bridge = build_live_bridge_from_env(env)
    bridge_health = bridge.health()
    preflight_errors = bridge_health.get("preflight_errors", [])
    preflight_count = len(preflight_errors) if isinstance(preflight_errors, list) else 0
    transport_health = bridge_health.get("transport_health", {})
    transport_degraded_keys: list[str] = []
    if isinstance(transport_health, dict):
        for transport_key, status_payload in transport_health.items():
            if not isinstance(status_payload, dict):
                transport_degraded_keys.append(str(transport_key))
                continue
            status = str(status_payload.get("status", "")).strip().lower()
            if status not in {"ok", "ready"}:
                transport_degraded_keys.append(str(transport_key))
    ready = bridge_health.get("status") == "ok" and not transport_degraded_keys
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "service": "ta3000-runtime-operational-profile",
        "ready": ready,
        "preflight_error_count": preflight_count + len(transport_degraded_keys),
        "transport_degraded_keys": transport_degraded_keys,
        "bridge": bridge_health,
    }


def render_runtime_operational_metrics(snapshot: dict[str, object]) -> str:
    bridge = snapshot.get("bridge", {})
    bridge_obj = bridge if isinstance(bridge, dict) else {}
    secrets = bridge_obj.get("secrets_policy", {})
    secrets_obj = secrets if isinstance(secrets, dict) else {}
    retry = bridge_obj.get("retry_policy", {})
    retry_obj = retry if isinstance(retry, dict) else {}
    telemetry = bridge_obj.get("execution_telemetry", {})
    telemetry_obj = telemetry if isinstance(telemetry, dict) else {}
    submit_latency_obj = telemetry_obj.get("submit_latency_ms", {})
    submit_latency = submit_latency_obj if isinstance(submit_latency_obj, dict) else {}
    sync_lag_obj = telemetry_obj.get("sync_lag_ms", {})
    sync_lag = sync_lag_obj if isinstance(sync_lag_obj, dict) else {}
    sidecar_errors_obj = telemetry_obj.get("sidecar_errors_by_code", {})
    sidecar_errors = sidecar_errors_obj if isinstance(sidecar_errors_obj, dict) else {}
    status = str(bridge_obj.get("status", "unknown"))
    preflight_errors = bridge_obj.get("preflight_errors", [])
    preflight_count = len(preflight_errors) if isinstance(preflight_errors, list) else 0
    missing_count = int(secrets_obj.get("missing_count", 0))
    stale_count = int(secrets_obj.get("stale_count", 0))
    max_attempts = int(retry_obj.get("max_attempts", 0))
    retry_exhausted_total = int(telemetry_obj.get("retry_exhausted_total", 0))
    submit_p95 = float(submit_latency.get("p95", 0.0))
    sync_lag_p95 = float(sync_lag.get("p95", 0.0))
    ready = 1 if bool(snapshot.get("ready")) else 0
    status_value = 1 if status == "ok" else 0

    lines = [
        "# HELP ta3000_runtime_operational_profile_up Runtime operational profile exporter health.",
        "# TYPE ta3000_runtime_operational_profile_up gauge",
        "ta3000_runtime_operational_profile_up 1",
        "# HELP ta3000_live_bridge_ready Live bridge readiness according to preflight policy.",
        "# TYPE ta3000_live_bridge_ready gauge",
        f"ta3000_live_bridge_ready {ready}",
        "# HELP ta3000_live_bridge_status Live bridge status as binary (ok=1).",
        "# TYPE ta3000_live_bridge_status gauge",
        f'ta3000_live_bridge_status{{status="{status}"}} {status_value}',
        "# HELP ta3000_live_bridge_preflight_errors_total Preflight error count.",
        "# TYPE ta3000_live_bridge_preflight_errors_total gauge",
        f"ta3000_live_bridge_preflight_errors_total {preflight_count}",
        "# HELP ta3000_live_bridge_missing_secrets_total Missing required secret count.",
        "# TYPE ta3000_live_bridge_missing_secrets_total gauge",
        f"ta3000_live_bridge_missing_secrets_total {missing_count}",
        "# HELP ta3000_live_bridge_stale_secrets_total Stale required secret count by age policy.",
        "# TYPE ta3000_live_bridge_stale_secrets_total gauge",
        f"ta3000_live_bridge_stale_secrets_total {stale_count}",
        "# HELP ta3000_live_bridge_retry_max_attempts Configured retry attempts for bridge operations.",
        "# TYPE ta3000_live_bridge_retry_max_attempts gauge",
        f"ta3000_live_bridge_retry_max_attempts {max_attempts}",
        "# HELP ta3000_live_submit_latency_p95_ms P95 submit latency in milliseconds.",
        "# TYPE ta3000_live_submit_latency_p95_ms gauge",
        f"ta3000_live_submit_latency_p95_ms {submit_p95}",
        "# HELP ta3000_live_sync_lag_p95_ms P95 sync lag between broker event timestamp and ingest time.",
        "# TYPE ta3000_live_sync_lag_p95_ms gauge",
        f"ta3000_live_sync_lag_p95_ms {sync_lag_p95}",
        "# HELP ta3000_live_retry_exhausted_total Retry-exhausted operation count.",
        "# TYPE ta3000_live_retry_exhausted_total gauge",
        f"ta3000_live_retry_exhausted_total {retry_exhausted_total}",
        "# HELP ta3000_live_sidecar_errors_total Sidecar transport errors by normalized code.",
        "# TYPE ta3000_live_sidecar_errors_total gauge",
    ]
    for error_code, total in sorted(sidecar_errors.items()):
        safe_code = str(error_code).replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'ta3000_live_sidecar_errors_total{{code="{safe_code}"}} {int(total)}')
    return "\n".join(lines) + "\n"


def run_profile_server(*, host: str = "0.0.0.0", port: int = 8088, env: Mapping[str, str] | None = None) -> None:
    source = env or os.environ

    class _Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_text(self, status: int, body_text: str, *, content_type: str) -> None:
            body = body_text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            snapshot = build_runtime_operational_snapshot(source)
            if self.path == "/health":
                self._send_json(200, snapshot)
                return
            if self.path == "/ready":
                status = 200 if bool(snapshot.get("ready")) else 503
                self._send_json(status, snapshot)
                return
            if self.path == "/metrics":
                metrics = render_runtime_operational_metrics(snapshot)
                self._send_text(status=200, body_text=metrics, content_type="text/plain; version=0.0.4")
                return
            self._send_json(404, {"error": "not_found", "path": self.path})

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), _Handler)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


if __name__ == "__main__":
    current_env = os.environ
    run_profile_server(
        host=current_env.get("TA3000_PROFILE_HOST", "0.0.0.0"),
        port=_env_int(current_env, "TA3000_PROFILE_PORT", 8088),
        env=current_env,
    )
