from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[3]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@contextmanager
def _gateway_server(*, unsupported_batch_state: bool) -> Iterator[str]:
    state = {
        "kill_switch": False,
        "intents": {},
        "updates": [],
        "fills": [],
    }

    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict[str, object]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            raw = self.rfile.read(max(0, length)).decode("utf-8")
            if not raw.strip():
                return {}
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else {}

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._json(200, {"status": "ok", "service": "mock-gateway"})
                return
            if parsed.path == "/ready":
                self._json(
                    200 if not state["kill_switch"] else 503,
                    {
                        "ready": not state["kill_switch"],
                        "reason": "kill_switch_active" if state["kill_switch"] else "ok",
                    },
                )
                return
            if parsed.path == "/v1/stream/updates":
                query = parse_qs(parsed.query)
                start = int((query.get("cursor") or ["0"])[0] or "0")
                limit = int((query.get("limit") or ["500"])[0] or "500")
                end = min(len(state["updates"]), start + max(1, limit))
                self._json(200, {"updates": state["updates"][start:end], "next_cursor": str(end)})
                return
            if parsed.path == "/v1/stream/fills":
                query = parse_qs(parsed.query)
                start = int((query.get("cursor") or ["0"])[0] or "0")
                limit = int((query.get("limit") or ["500"])[0] or "500")
                end = min(len(state["fills"]), start + max(1, limit))
                self._json(200, {"fills": state["fills"][start:end], "next_cursor": str(end)})
                return
            self._json(404, {"error_code": "not_found", "message": "unknown path"})

        def do_POST(self) -> None:  # noqa: N802
            payload = self._read_json()
            path = self.path
            if path == "/v1/admin/kill-switch":
                state["kill_switch"] = bool(payload.get("active", True))
                self._json(200, {"ok": True, "kill_switch_active": state["kill_switch"]})
                return
            if state["kill_switch"] and path == "/v1/intents/submit":
                self._json(503, {"error_code": "kill_switch_active", "message": "kill-switch active"})
                return
            if path == "/v1/intents/submit":
                intent_id = str(payload.get("intent_id", "")).strip()
                intent = payload.get("intent", {})
                if not intent_id or not isinstance(intent, dict):
                    self._json(400, {"error_code": "invalid_submit_payload", "message": "intent_id required"})
                    return
                external_order_id = f"gw-{intent_id.lower()}"
                state["intents"][intent_id] = {"external_order_id": external_order_id}
                update_state = "submitted"
                if unsupported_batch_state and intent_id.startswith("INT-STAGING-BATCH"):
                    update_state = "unsupported_custom_state"
                state["updates"].append(
                    {
                        "external_order_id": external_order_id,
                        "state": update_state,
                        "event_ts": str(intent.get("created_at", _utc_now())),
                        "payload": {"intent_id": intent_id},
                    }
                )
                self._json(
                    200,
                    {
                        "ack": {
                            "intent_id": intent_id,
                            "external_order_id": external_order_id,
                            "accepted": True,
                            "broker_adapter": "stocksharp-sidecar",
                            "state": "submitted",
                        }
                    },
                )
                return
            if path.startswith("/v1/intents/") and path.endswith("/cancel"):
                intent_id = path.removeprefix("/v1/intents/").removesuffix("/cancel").strip("/")
                existing = state["intents"].get(intent_id)
                if existing is None:
                    self._json(404, {"error_code": "unknown_intent", "message": "unknown intent"})
                    return
                canceled_at = str(payload.get("canceled_at", _utc_now()))
                state["updates"].append(
                    {
                        "external_order_id": existing["external_order_id"],
                        "state": "canceled",
                        "event_ts": canceled_at,
                        "payload": {"intent_id": intent_id},
                    }
                )
                self._json(
                    200,
                    {
                        "ack": {
                            "intent_id": intent_id,
                            "external_order_id": existing["external_order_id"],
                            "state": "canceled",
                            "canceled_at": canceled_at,
                        }
                    },
                )
                return
            if path.startswith("/v1/intents/") and path.endswith("/replace"):
                intent_id = path.removeprefix("/v1/intents/").removesuffix("/replace").strip("/")
                existing = state["intents"].get(intent_id)
                if existing is None:
                    self._json(404, {"error_code": "unknown_intent", "message": "unknown intent"})
                    return
                replaced_at = str(payload.get("replaced_at", _utc_now()))
                self._json(
                    200,
                    {
                        "ack": {
                            "intent_id": intent_id,
                            "external_order_id": existing["external_order_id"],
                            "state": "replaced",
                            "replaced_at": replaced_at,
                            "new_qty": int(payload.get("new_qty", 1)),
                            "new_price": float(payload.get("new_price", 1.0)),
                        }
                    },
                )
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


def _script_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": str(ROOT / "src"),
            "TA3000_ENABLE_LIVE_EXECUTION": "1",
            "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
            "TA3000_ENABLE_QUIK_CONNECTOR": "1",
            "TA3000_ENABLE_FINAM_TRANSPORT": "1",
            "TA3000_ENFORCE_LIVE_SECRETS": "1",
            "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
            "TA3000_FINAM_API_TOKEN": "finam-secret-001",
        }
    )
    return env


def test_staging_rollout_script_passes_all_stages_with_mock_gateway() -> None:
    with _gateway_server(unsupported_batch_state=False) as base_url:
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_staging_real_execution_rollout.py",
                "--base-url",
                base_url,
                "--stage",
                "all",
                "--batch-size",
                "2",
                "--format",
                "json",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            env=_script_env(),
        )
        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["status"] == "ok"
        statuses = {item["stage"]: item["status"] for item in payload["stages"]}
        assert statuses == {"connectivity": "ok", "canary": "ok", "batch": "ok"}


def test_staging_rollout_script_activates_kill_switch_when_batch_incident_detected() -> None:
    with _gateway_server(unsupported_batch_state=True) as base_url:
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_staging_real_execution_rollout.py",
                "--base-url",
                base_url,
                "--stage",
                "all",
                "--batch-size",
                "2",
                "--kill-switch-on-failure",
                "--format",
                "json",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            env=_script_env(),
        )
        assert result.returncode == 1, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["status"] == "failed"
        batch_stage = [item for item in payload["stages"] if item["stage"] == "batch"][0]
        assert batch_stage["status"] == "failed"
        kill_switch = batch_stage["details"]["kill_switch_result"]
        assert kill_switch["kill_switch_active"] is True
