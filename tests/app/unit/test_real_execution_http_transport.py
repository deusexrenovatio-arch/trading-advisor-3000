from __future__ import annotations

import hashlib
import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator
from urllib.parse import parse_qs, urlparse

import pytest

from trading_advisor_3000.app.contracts import Mode, OrderIntent
from trading_advisor_3000.app.execution.adapters import (
    LiveExecutionBridge,
    LiveExecutionFeatureFlags,
    SidecarTransportPermanentError,
    SidecarTransportRetryableError,
    StockSharpHTTPTransport,
    StockSharpHTTPTransportConfig,
    StockSharpSidecarStub,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _intent(intent_id: str) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id=f"SIG-{intent_id}",
        mode=Mode.LIVE,
        broker_adapter="stocksharp-sidecar",
        action="buy",
        contract_id="BR-6.26",
        qty=1,
        price=82.5,
        stop_price=81.8,
        created_at="2026-03-18T13:00:00Z",
    )


def _full_flags() -> LiveExecutionFeatureFlags:
    return LiveExecutionFeatureFlags(
        enable_live_execution=True,
        enable_stocksharp_bridge=True,
        enable_quik_connector=True,
        enable_finam_transport=True,
        enforce_live_secrets=True,
        environment="staging-real-transport",
    )


def _secrets_env() -> dict[str, str]:
    return {
        "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
        "TA3000_FINAM_API_TOKEN": "finam-secret-001",
    }


def _hash_external_order_id(intent_id: str) -> str:
    return "gw-" + hashlib.sha256(intent_id.encode("utf-8")).hexdigest()[:12]


@contextmanager
def _mock_sidecar_server(
    *,
    fail_mode: str = "ok",
) -> Iterator[str]:
    state = {
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
                self._json(200, {"status": "ok", "service": "mock-sidecar"})
                return
            if parsed.path == "/ready":
                self._json(200, {"ready": True, "reason": "ok"})
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
            if fail_mode == "retryable":
                self._json(503, {"error_code": "sidecar_unavailable", "message": "temporary outage"})
                return
            if fail_mode == "permanent":
                self._json(400, {"error_code": "invalid_request", "message": "bad payload"})
                return
            if fail_mode == "protocol":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"NOT_JSON")
                return

            payload = self._read_json()
            path = self.path
            if path == "/v1/intents/submit":
                intent_id = str(payload.get("intent_id", "")).strip()
                intent_obj = payload.get("intent", {})
                if not intent_id or not isinstance(intent_obj, dict):
                    self._json(400, {"error_code": "invalid_submit_payload", "message": "intent_id required"})
                    return
                external_order_id = _hash_external_order_id(intent_id)
                state["intents"][intent_id] = {
                    "external_order_id": external_order_id,
                    "state": "submitted",
                }
                state["updates"].append(
                    {
                        "external_order_id": external_order_id,
                        "state": "submitted",
                        "event_ts": str(intent_obj.get("created_at", _utc_now())),
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
                existing["state"] = "canceled"
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
                new_qty = int(payload.get("new_qty", 1))
                new_price = float(payload.get("new_price", 1.0))
                existing["state"] = "replaced"
                state["updates"].append(
                    {
                        "external_order_id": existing["external_order_id"],
                        "state": "replaced",
                        "event_ts": replaced_at,
                        "payload": {"intent_id": intent_id, "new_qty": new_qty, "new_price": new_price},
                    }
                )
                self._json(
                    200,
                    {
                        "ack": {
                            "intent_id": intent_id,
                            "external_order_id": existing["external_order_id"],
                            "state": "replaced",
                            "replaced_at": replaced_at,
                            "new_qty": new_qty,
                            "new_price": new_price,
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


def test_http_transport_supports_submit_replace_cancel_and_cursor_streams() -> None:
    with _mock_sidecar_server() as base_url:
        transport = StockSharpHTTPTransport(
            config=StockSharpHTTPTransportConfig(base_url=base_url, timeout_seconds=2.0, stream_batch_size=100)
        )
        intent = _intent("INT-HTTP-1")

        first_ack = transport.submit_order_intent(intent)
        second_ack = transport.submit_order_intent(intent)
        replace_ack = transport.replace_order_intent(
            intent_id=intent.intent_id,
            new_qty=2,
            new_price=82.7,
            replaced_at="2026-03-18T13:00:01Z",
        )
        cancel_ack = transport.cancel_order_intent(
            intent_id=intent.intent_id,
            canceled_at="2026-03-18T13:00:02Z",
        )
        first_updates = transport.list_broker_updates()
        second_updates = transport.list_broker_updates()

        assert first_ack["accepted"] is True
        assert first_ack["external_order_id"] == second_ack["external_order_id"]
        assert replace_ack["state"] == "replaced"
        assert cancel_ack["state"] == "canceled"
        assert len(first_updates) >= 3
        assert len(second_updates) == len(first_updates)
        assert {item["state"] for item in second_updates} >= {"submitted", "replaced", "canceled"}


def test_http_transport_classifies_retryable_http_errors() -> None:
    with _mock_sidecar_server(fail_mode="retryable") as base_url:
        transport = StockSharpHTTPTransport(
            config=StockSharpHTTPTransportConfig(base_url=base_url, timeout_seconds=1.0)
        )
        with pytest.raises(SidecarTransportRetryableError):
            transport.submit_order_intent(_intent("INT-HTTP-RETRY"))


def test_http_transport_classifies_permanent_http_errors() -> None:
    with _mock_sidecar_server(fail_mode="permanent") as base_url:
        transport = StockSharpHTTPTransport(
            config=StockSharpHTTPTransportConfig(base_url=base_url, timeout_seconds=1.0)
        )
        with pytest.raises(SidecarTransportPermanentError):
            transport.submit_order_intent(_intent("INT-HTTP-PERMANENT"))


def test_http_transport_rejects_protocol_violation_response() -> None:
    with _mock_sidecar_server(fail_mode="protocol") as base_url:
        transport = StockSharpHTTPTransport(
            config=StockSharpHTTPTransportConfig(base_url=base_url, timeout_seconds=1.0)
        )
        with pytest.raises(SidecarTransportPermanentError):
            transport.submit_order_intent(_intent("INT-HTTP-PROTOCOL"))


def test_live_bridge_routes_stocksharp_sidecar_to_http_transport_and_exposes_correlation_logs() -> None:
    with _mock_sidecar_server() as base_url:
        transport = StockSharpHTTPTransport(
            config=StockSharpHTTPTransportConfig(base_url=base_url, timeout_seconds=1.0)
        )
        bridge = LiveExecutionBridge(
            sidecar=StockSharpSidecarStub(),
            flags=_full_flags(),
            adapter_transports={"stocksharp-sidecar": transport},
            env=_secrets_env(),
        )
        intent = _intent("INT-HTTP-BRIDGE-1")

        ack = bridge.submit_order_intent(intent, accepted_at="2026-03-18T13:10:00Z")
        bridge.replace_order_intent(
            intent_id=intent.intent_id,
            new_qty=2,
            new_price=82.8,
            replaced_at="2026-03-18T13:10:01Z",
        )
        bridge.cancel_order_intent(intent_id=intent.intent_id, canceled_at="2026-03-18T13:10:02Z")
        streams = bridge.drain_broker_streams()
        health = bridge.health()

        assert ack["transport_key"] != ""
        assert ack["external_order_id"] is not None
        assert len(streams["updates"]) >= 3
        telemetry = health["execution_telemetry"]
        assert telemetry["submit_latency_ms"]["count"] == 1
        assert telemetry["sync_lag_ms"]["count"] >= 1
        log_tail = health["operation_log_tail"]
        assert any(item["intent_id"] == intent.intent_id for item in log_tail)
        assert any(item["external_order_id"] == ack["external_order_id"] for item in log_tail)
