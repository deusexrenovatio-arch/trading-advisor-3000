from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator


ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = ROOT / "deployment" / "stocksharp-sidecar" / "phase9-sidecar-delivery-manifest.json"


@contextmanager
def _sidecar_gateway_server(*, route: str = "stocksharp->quik->finam") -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._json(200, {"status": "ok", "route": route, "service": "mock-sidecar-gateway"})
                return
            if self.path == "/ready":
                self._json(200, {"ready": True, "reason": "ok", "route": route})
                return
            if self.path == "/metrics":
                body = b"gateway_metric 1\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._json(404, {"error_code": "not_found", "message": "unknown path"})

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


def _script_env(*, base_url: str) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": str(ROOT / "src"),
            "TA3000_ENABLE_LIVE_EXECUTION": "1",
            "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
            "TA3000_ENABLE_QUIK_CONNECTOR": "1",
            "TA3000_ENABLE_FINAM_TRANSPORT": "1",
            "TA3000_SIDECAR_TRANSPORT": "http",
            "TA3000_SIDECAR_BASE_URL": base_url,
            "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
            "TA3000_FINAM_API_TOKEN": "finam-secret-001",
        }
    )
    return env


def test_phase9_sidecar_preflight_script_produces_frozen_delivery_report() -> None:
    with _sidecar_gateway_server() as base_url:
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_phase9_sidecar_preflight.py",
                "--manifest-path",
                str(MANIFEST_PATH),
                "--base-url",
                base_url,
            ],
            cwd=ROOT,
            env=_script_env(base_url=base_url),
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["status"] == "ok"
        assert payload["delivery_spec"]["delivery_mode"] == "pinned-staging-gateway-bundle"
        assert [item["path"] for item in payload["endpoint_probes"]] == ["/health", "/ready", "/metrics"]
        assert all(item["ok"] is True for item in payload["endpoint_probes"])
        assert payload["rollout_dry_run"]["status"] == "ok"
        stage_statuses = {item["stage"]: item["status"] for item in payload["rollout_dry_run"]["stages"]}
        assert stage_statuses == {"connectivity": "ok", "canary": "planned", "batch": "planned"}


def test_phase9_sidecar_preflight_blocks_route_mismatch() -> None:
    with _sidecar_gateway_server(route="stocksharp->sandbox->finam") as base_url:
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_phase9_sidecar_preflight.py",
                "--manifest-path",
                str(MANIFEST_PATH),
                "--base-url",
                base_url,
                "--skip-rollout-dry-run",
            ],
            cwd=ROOT,
            env=_script_env(base_url=base_url),
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 1, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["status"] == "blocked"
        route_probe = [item for item in payload["endpoint_probes"] if item["path"] == "/health"][0]
        assert route_probe["details"]["route_matches"] is False
