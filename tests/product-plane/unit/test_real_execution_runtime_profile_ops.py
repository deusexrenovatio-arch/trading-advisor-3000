from __future__ import annotations

import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator

from trading_advisor_3000.product_plane.runtime.ops import (
    build_live_bridge_from_env,
    build_runtime_operational_snapshot,
    render_runtime_operational_metrics,
)


@contextmanager
def _http_sidecar_probe_server() -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, payload: str) -> None:
            body = payload.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/v1/stream/updates?cursor=&limit=500" or self.path.startswith("/v1/stream/updates"):
                self._json(200, '{"updates":[],"next_cursor":"0"}')
                return
            if self.path == "/v1/stream/fills?cursor=&limit=500" or self.path.startswith("/v1/stream/fills"):
                self._json(200, '{"fills":[],"next_cursor":"0"}')
                return
            if self.path == "/health":
                self._json(200, '{"status":"ok"}')
                return
            if self.path == "/ready":
                self._json(200, '{"ready":true,"reason":"ok"}')
                return
            if self.path == "/metrics":
                body = b"gateway_metric 1\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._json(404, '{"error_code":"not_found","message":"unknown path"}')

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_runtime_profile_supports_http_sidecar_transport_mode() -> None:
    with _http_sidecar_probe_server() as base_url:
        env = {
            "TA3000_ENVIRONMENT": "staging-real-transport",
            "TA3000_ENABLE_LIVE_EXECUTION": "1",
            "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
            "TA3000_ENABLE_QUIK_CONNECTOR": "1",
            "TA3000_ENABLE_FINAM_TRANSPORT": "1",
            "TA3000_ENFORCE_LIVE_SECRETS": "1",
            "TA3000_SIDECAR_TRANSPORT": "http",
            "TA3000_SIDECAR_BASE_URL": base_url,
            "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
            "TA3000_FINAM_API_TOKEN": "finam-secret-001",
        }
        snapshot = build_runtime_operational_snapshot(env)
        bridge = build_live_bridge_from_env(env)
        health = bridge.health()

        assert snapshot["ready"] is True
        assert snapshot["transport_degraded_keys"] == []
        assert health["transport_health"]
        assert any(item["status"] == "ok" for item in health["transport_health"].values())


def test_runtime_profile_secret_age_policy_is_fail_closed_when_stale() -> None:
    env = {
        "TA3000_ENVIRONMENT": "staging-real-transport",
        "TA3000_ENABLE_LIVE_EXECUTION": "1",
        "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
        "TA3000_ENABLE_QUIK_CONNECTOR": "1",
        "TA3000_ENABLE_FINAM_TRANSPORT": "1",
        "TA3000_ENFORCE_LIVE_SECRETS": "1",
        "TA3000_ENFORCE_SECRET_AGE": "1",
        "TA3000_SECRET_MAX_AGE_DAYS": "30",
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_STOCKSHARP_API_KEY_ROTATED_AT": "2020-01-01T00:00:00Z",
        "TA3000_FINAM_API_TOKEN": "finam-secret-001",
        "TA3000_FINAM_API_TOKEN_ROTATED_AT": "2020-01-01T00:00:00Z",
    }
    snapshot = build_runtime_operational_snapshot(env)
    bridge = snapshot["bridge"]

    assert snapshot["ready"] is False
    assert bridge["secrets_policy"]["check_age"] is True
    assert bridge["secrets_policy"]["stale_count"] == 2
    assert bridge["secrets_policy"]["is_ready"] is False


def test_runtime_operational_metrics_include_real_transport_observability_series() -> None:
    env = {
        "TA3000_ENVIRONMENT": "staging-real-transport",
        "TA3000_ENABLE_LIVE_EXECUTION": "1",
        "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
        "TA3000_ENABLE_QUIK_CONNECTOR": "1",
        "TA3000_ENABLE_FINAM_TRANSPORT": "1",
        "TA3000_ENFORCE_LIVE_SECRETS": "1",
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-secret-001",
    }
    snapshot = build_runtime_operational_snapshot(env)
    metrics = render_runtime_operational_metrics(snapshot)

    assert "ta3000_live_submit_latency_p95_ms" in metrics
    assert "ta3000_live_sync_lag_p95_ms" in metrics
    assert "ta3000_live_retry_exhausted_total" in metrics
    assert "ta3000_live_bridge_stale_secrets_total" in metrics
