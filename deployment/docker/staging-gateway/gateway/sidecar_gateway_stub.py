from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_or_now(value: object) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return _utc_now()


def _error_payload(*, error_code: str, message: str) -> dict[str, object]:
    return {"error_code": error_code, "message": message}


def _hash_order_id(intent_id: str) -> str:
    return "gw-" + hashlib.sha256(intent_id.encode("utf-8")).hexdigest()[:16]


@dataclass
class GatewayState:
    broker_route: str
    intents: dict[str, dict[str, object]] = field(default_factory=dict)
    submit_cache: dict[str, dict[str, object]] = field(default_factory=dict)
    cancel_cache: dict[str, dict[str, object]] = field(default_factory=dict)
    replace_cache: dict[str, dict[str, object]] = field(default_factory=dict)
    updates: list[dict[str, object]] = field(default_factory=list)
    fills: list[dict[str, object]] = field(default_factory=list)

    def _build_ack(self, *, intent_id: str, broker_adapter: str, state: str) -> dict[str, object]:
        current = self.intents[intent_id]
        return {
            "intent_id": intent_id,
            "external_order_id": current["external_order_id"],
            "accepted": True,
            "broker_adapter": broker_adapter,
            "state": state,
        }

    def submit(self, *, intent_id: str, intent: dict[str, object], idempotency_key: str | None) -> dict[str, object]:
        cache_key = idempotency_key or intent_id
        cached = self.submit_cache.get(cache_key)
        if cached is not None:
            return cached
        broker_adapter = str(intent.get("broker_adapter", "stocksharp-sidecar"))
        external_order_id = _hash_order_id(intent_id)
        self.intents[intent_id] = {
            "intent": intent,
            "state": "submitted",
            "external_order_id": external_order_id,
        }
        ack = self._build_ack(intent_id=intent_id, broker_adapter=broker_adapter, state="submitted")
        self.updates.append(
            {
                "external_order_id": external_order_id,
                "state": "submitted",
                "event_ts": _iso_or_now(intent.get("created_at")),
                "payload": {"intent_id": intent_id},
            }
        )
        self.submit_cache[cache_key] = ack
        return ack

    def cancel(self, *, intent_id: str, canceled_at: str, idempotency_key: str | None) -> dict[str, object]:
        cache_key = idempotency_key or f"cancel:{intent_id}:{canceled_at}"
        cached = self.cancel_cache.get(cache_key)
        if cached is not None:
            return cached
        existing = self.intents.get(intent_id)
        if existing is None:
            raise KeyError("unknown_intent_id")
        existing["state"] = "canceled"
        ack = {
            **self._build_ack(intent_id=intent_id, broker_adapter="stocksharp-sidecar", state="canceled"),
            "canceled_at": canceled_at,
        }
        self.updates.append(
            {
                "external_order_id": existing["external_order_id"],
                "state": "canceled",
                "event_ts": canceled_at,
                "payload": {"intent_id": intent_id},
            }
        )
        self.cancel_cache[cache_key] = ack
        return ack

    def replace(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
        idempotency_key: str | None,
    ) -> dict[str, object]:
        cache_key = idempotency_key or f"replace:{intent_id}:{replaced_at}"
        cached = self.replace_cache.get(cache_key)
        if cached is not None:
            return cached
        existing = self.intents.get(intent_id)
        if existing is None:
            raise KeyError("unknown_intent_id")
        if new_qty <= 0 or new_price <= 0:
            raise ValueError("invalid_replace_payload")
        intent_payload = existing["intent"]
        if isinstance(intent_payload, dict):
            intent_payload["qty"] = new_qty
            intent_payload["price"] = new_price
        existing["state"] = "replaced"
        ack = {
            **self._build_ack(intent_id=intent_id, broker_adapter="stocksharp-sidecar", state="replaced"),
            "new_qty": new_qty,
            "new_price": new_price,
            "replaced_at": replaced_at,
        }
        self.updates.append(
            {
                "external_order_id": existing["external_order_id"],
                "state": "replaced",
                "event_ts": replaced_at,
                "payload": {"intent_id": intent_id, "new_qty": new_qty, "new_price": new_price},
            }
        )
        self.replace_cache[cache_key] = ack
        return ack

    @staticmethod
    def _slice_stream(rows: list[dict[str, object]], *, cursor: str, limit: int) -> tuple[list[dict[str, object]], str]:
        try:
            start = int(cursor) if cursor.strip() else 0
        except ValueError:
            start = 0
        if start < 0:
            start = 0
        if start > len(rows):
            start = len(rows)
        end = min(len(rows), start + max(1, limit))
        return rows[start:end], str(end)


def run_server() -> None:
    host = os.environ.get("TA3000_GATEWAY_HOST", "0.0.0.0")
    port = int(os.environ.get("TA3000_GATEWAY_PORT", "18081"))
    route = os.environ.get("TA3000_GATEWAY_ROUTE", "stocksharp->quik->finam").strip() or "stocksharp->quik->finam"
    connector_mode = os.environ.get("TA3000_CONNECTOR_MODE", "staging-real").strip() or "staging-real"
    connector_backend = os.environ.get("TA3000_CONNECTOR_BACKEND", "stocksharp-quik-finam").strip() or "stocksharp-quik-finam"
    connector_binding_source = (
        os.environ.get("TA3000_CONNECTOR_BINDING_SOURCE", "staging-gateway-stub").strip()
        or "staging-gateway-stub"
    )
    connector_session_id = os.environ.get("TA3000_CONNECTOR_SESSION_ID", "").strip() or f"stub-session-{port}"
    kill_switch = os.environ.get("TA3000_GATEWAY_KILL_SWITCH", "0").strip().lower() in {"1", "true", "yes", "on"}
    kill_switch_state = {"active": kill_switch}
    state = GatewayState(broker_route=route)

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_text(self, status: int, body_text: str) -> None:
            body = body_text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
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
            path = parsed.path
            query = parse_qs(parsed.query)
            if path == "/health":
                self._send_json(
                    200,
                    {
                        "service": "stocksharp-sidecar-gateway-stub",
                        "status": "ok",
                        "route": state.broker_route,
                        "kill_switch": bool(kill_switch_state["active"]),
                        "queued_intents": len(state.intents),
                        "connector_mode": connector_mode,
                        "connector_backend": connector_backend,
                        "connector_ready": True,
                        "connector_session_id": connector_session_id,
                        "connector_binding_source": connector_binding_source,
                        "connector_last_heartbeat": _utc_now(),
                    },
                )
                return
            if path == "/ready":
                status = 200 if not bool(kill_switch_state["active"]) else 503
                self._send_json(
                    status,
                    {
                        "ready": not bool(kill_switch_state["active"]),
                        "reason": "kill_switch_active" if bool(kill_switch_state["active"]) else "ok",
                        "route": state.broker_route,
                    },
                )
                return
            if path == "/metrics":
                lines = [
                    "# HELP ta3000_sidecar_gateway_up Sidecar gateway process health.",
                    "# TYPE ta3000_sidecar_gateway_up gauge",
                    "ta3000_sidecar_gateway_up 1",
                    "# HELP ta3000_sidecar_gateway_queued_intents Total queued intents.",
                    "# TYPE ta3000_sidecar_gateway_queued_intents gauge",
                    f"ta3000_sidecar_gateway_queued_intents {len(state.intents)}",
                    "# HELP ta3000_sidecar_gateway_kill_switch Kill switch state.",
                    "# TYPE ta3000_sidecar_gateway_kill_switch gauge",
                    f"ta3000_sidecar_gateway_kill_switch {1 if bool(kill_switch_state['active']) else 0}",
                ]
                self._send_text(200, "\n".join(lines) + "\n")
                return
            if path == "/v1/stream/updates":
                cursor = str((query.get("cursor") or [""])[0])
                limit_raw = str((query.get("limit") or ["500"])[0])
                try:
                    limit = int(limit_raw)
                except ValueError:
                    limit = 500
                rows, next_cursor = state._slice_stream(state.updates, cursor=cursor, limit=limit)
                self._send_json(200, {"updates": rows, "next_cursor": next_cursor})
                return
            if path == "/v1/stream/fills":
                cursor = str((query.get("cursor") or [""])[0])
                limit_raw = str((query.get("limit") or ["500"])[0])
                try:
                    limit = int(limit_raw)
                except ValueError:
                    limit = 500
                rows, next_cursor = state._slice_stream(state.fills, cursor=cursor, limit=limit)
                self._send_json(200, {"fills": rows, "next_cursor": next_cursor})
                return
            self._send_json(404, _error_payload(error_code="not_found", message=f"unknown path: {path}"))

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            payload = self._read_json()
            idempotency_key = self.headers.get("X-Idempotency-Key")

            if bool(kill_switch_state["active"]) and path == "/v1/intents/submit":
                self._send_json(
                    503,
                    _error_payload(
                        error_code="kill_switch_active",
                        message="gateway kill-switch is active",
                    ),
                )
                return

            if path == "/v1/intents/submit":
                intent_id = str(payload.get("intent_id", "")).strip()
                intent = payload.get("intent", {})
                if not intent_id or not isinstance(intent, dict):
                    self._send_json(
                        400,
                        _error_payload(
                            error_code="invalid_submit_payload",
                            message="intent_id and intent object are required",
                        ),
                    )
                    return
                ack = state.submit(intent_id=intent_id, intent=intent, idempotency_key=idempotency_key)
                self._send_json(200, {"ack": ack})
                return

            if path.startswith("/v1/intents/") and path.endswith("/cancel"):
                intent_id = path.removeprefix("/v1/intents/").removesuffix("/cancel").strip("/")
                canceled_at = _iso_or_now(payload.get("canceled_at"))
                try:
                    ack = state.cancel(intent_id=intent_id, canceled_at=canceled_at, idempotency_key=idempotency_key)
                except KeyError:
                    self._send_json(
                        404,
                        _error_payload(error_code="unknown_intent_id", message=f"unknown intent_id: {intent_id}"),
                    )
                    return
                self._send_json(200, {"ack": ack})
                return

            if path.startswith("/v1/intents/") and path.endswith("/replace"):
                intent_id = path.removeprefix("/v1/intents/").removesuffix("/replace").strip("/")
                replaced_at = _iso_or_now(payload.get("replaced_at"))
                try:
                    new_qty = int(payload.get("new_qty"))
                    new_price = float(payload.get("new_price"))
                except (TypeError, ValueError):
                    self._send_json(
                        400,
                        _error_payload(
                            error_code="invalid_replace_payload",
                            message="new_qty and new_price must be numeric",
                        ),
                    )
                    return
                try:
                    ack = state.replace(
                        intent_id=intent_id,
                        new_qty=new_qty,
                        new_price=new_price,
                        replaced_at=replaced_at,
                        idempotency_key=idempotency_key,
                    )
                except KeyError:
                    self._send_json(
                        404,
                        _error_payload(error_code="unknown_intent_id", message=f"unknown intent_id: {intent_id}"),
                    )
                    return
                except ValueError:
                    self._send_json(
                        400,
                        _error_payload(
                            error_code="invalid_replace_payload",
                            message="new_qty/new_price must be positive",
                        ),
                    )
                    return
                self._send_json(200, {"ack": ack})
                return

            if path == "/v1/admin/kill-switch":
                kill_switch_state["active"] = bool(payload.get("active", True))
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "kill_switch_active": bool(kill_switch_state["active"]),
                    },
                )
                return

            self._send_json(404, _error_payload(error_code="not_found", message=f"unknown path: {path}"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), Handler)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
