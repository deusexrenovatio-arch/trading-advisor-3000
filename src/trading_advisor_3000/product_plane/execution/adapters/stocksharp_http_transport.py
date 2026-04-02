from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from typing import Mapping
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from trading_advisor_3000.product_plane.contracts import OrderIntent


class SidecarTransportError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        retryable: bool,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable
        self.status_code = status_code


class SidecarTransportRetryableError(SidecarTransportError):
    def __init__(self, message: str, *, error_code: str, status_code: int | None = None) -> None:
        super().__init__(
            message,
            error_code=error_code,
            retryable=True,
            status_code=status_code,
        )


class SidecarTransportPermanentError(SidecarTransportError):
    def __init__(self, message: str, *, error_code: str, status_code: int | None = None) -> None:
        super().__init__(
            message,
            error_code=error_code,
            retryable=False,
            status_code=status_code,
        )


@dataclass(frozen=True)
class StockSharpHTTPTransportConfig:
    base_url: str
    timeout_seconds: float = 3.0
    stream_batch_size: int = 500
    api_prefix: str = "v1"

    def __post_init__(self) -> None:
        if not isinstance(self.base_url, str) or not self.base_url.strip():
            raise ValueError("base_url must be non-empty string")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.stream_batch_size <= 0:
            raise ValueError("stream_batch_size must be positive")
        if not isinstance(self.api_prefix, str) or not self.api_prefix.strip():
            raise ValueError("api_prefix must be non-empty string")

    @property
    def normalized_base_url(self) -> str:
        return self.base_url.rstrip("/")

    @property
    def normalized_api_prefix(self) -> str:
        return self.api_prefix.strip().strip("/")


def _as_dict(value: object, *, error_code: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise SidecarTransportPermanentError(
            "sidecar response must be JSON object",
            error_code=error_code,
        )
    normalized: dict[str, object] = {}
    for key, item in value.items():
        normalized[str(key)] = item
    return normalized


def _as_non_empty_text(value: object, *, field: str, error_code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SidecarTransportPermanentError(
            f"sidecar response field {field} must be non-empty string",
            error_code=error_code,
        )
    return value.strip()


def _response_error_fields(payload: object) -> tuple[str, str]:
    if not isinstance(payload, dict):
        return "sidecar_http_error", "sidecar returned error response"
    code = str(payload.get("error_code", "sidecar_http_error")).strip() or "sidecar_http_error"
    message = str(payload.get("message", "sidecar returned error response")).strip() or "sidecar returned error response"
    return code, message


def _stream_rows(payload: dict[str, object], *, key: str, error_code: str) -> list[dict[str, object]]:
    raw = payload.get(key, [])
    if not isinstance(raw, list):
        raise SidecarTransportPermanentError(
            f"sidecar response field {key} must be list",
            error_code=error_code,
        )
    rows: list[dict[str, object]] = []
    for item in raw:
        rows.append(_as_dict(item, error_code=error_code))
    return rows


class StockSharpHTTPTransport:
    def __init__(
        self,
        *,
        config: StockSharpHTTPTransportConfig,
        static_headers: Mapping[str, str] | None = None,
    ) -> None:
        self._config = config
        self._base_url = config.normalized_base_url
        self._api_prefix = config.normalized_api_prefix
        self._timeout_seconds = config.timeout_seconds
        self._stream_batch_size = config.stream_batch_size
        self._static_headers = {str(key): str(value) for key, value in (static_headers or {}).items()}

        self._submit_ack_by_intent_id: dict[str, dict[str, object]] = {}
        self._updates_cursor: str | None = None
        self._fills_cursor: str | None = None
        self._updates_cache: list[dict[str, object]] = []
        self._fills_cache: list[dict[str, object]] = []
        self._seen_stream_rows: set[str] = set()

    def _endpoint(self, path: str, *, include_api_prefix: bool = True) -> str:
        suffix = path if path.startswith("/") else f"/{path}"
        if include_api_prefix:
            return f"{self._base_url}/{self._api_prefix}{suffix}"
        return f"{self._base_url}{suffix}"

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        query: Mapping[str, object] | None = None,
        idempotency_key: str | None = None,
        include_api_prefix: bool = True,
    ) -> dict[str, object]:
        url = self._endpoint(path, include_api_prefix=include_api_prefix)
        if query:
            query_pairs: list[tuple[str, str]] = []
            for key, value in query.items():
                if value is None:
                    continue
                query_pairs.append((str(key), str(value)))
            if query_pairs:
                url = f"{url}?{urllib_parse.urlencode(query_pairs)}"

        body: bytes | None = None
        headers = {
            "Accept": "application/json",
            "User-Agent": "ta3000-stocksharp-http-transport/1.0",
            **self._static_headers,
        }
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        if idempotency_key is not None:
            headers["X-Idempotency-Key"] = idempotency_key

        request = urllib_request.Request(url=url, data=body, method=method.upper(), headers=headers)
        try:
            with urllib_request.urlopen(request, timeout=self._timeout_seconds) as response:
                raw = response.read().decode("utf-8").strip()
        except urllib_error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            parsed: object
            try:
                parsed = json.loads(response_body) if response_body.strip() else {}
            except json.JSONDecodeError:
                parsed = {}
            error_code, error_message = _response_error_fields(parsed)
            status_code = int(getattr(exc, "code", 0))
            if status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
                raise SidecarTransportRetryableError(
                    f"sidecar request failed: {error_message}",
                    error_code=error_code,
                    status_code=status_code,
                ) from exc
            raise SidecarTransportPermanentError(
                f"sidecar request failed: {error_message}",
                error_code=error_code,
                status_code=status_code,
            ) from exc
        except (urllib_error.URLError, TimeoutError, socket.timeout) as exc:
            raise SidecarTransportRetryableError(
                f"sidecar request unavailable: {type(exc).__name__}: {exc}",
                error_code="sidecar_unreachable",
            ) from exc

        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SidecarTransportPermanentError(
                "sidecar returned non-json response",
                error_code="sidecar_protocol_error",
            ) from exc
        return _as_dict(parsed, error_code="sidecar_protocol_error")

    @staticmethod
    def _normalize_submit_ack(payload: dict[str, object], *, intent: OrderIntent) -> dict[str, object]:
        ack_obj = payload.get("ack", payload)
        ack = _as_dict(ack_obj, error_code="sidecar_protocol_error")
        external_order_id = _as_non_empty_text(
            ack.get("external_order_id"),
            field="external_order_id",
            error_code="sidecar_protocol_error",
        )
        state = str(ack.get("state", "submitted")).strip() or "submitted"
        return {
            "intent_id": str(ack.get("intent_id", intent.intent_id)),
            "external_order_id": external_order_id,
            "accepted": bool(ack.get("accepted", True)),
            "broker_adapter": str(ack.get("broker_adapter", intent.broker_adapter)),
            "state": state,
        }

    def submit_order_intent(self, intent: OrderIntent) -> dict[str, object]:
        cached = self._submit_ack_by_intent_id.get(intent.intent_id)
        if cached is not None:
            return dict(cached)
        payload = self._request_json(
            method="POST",
            path="/intents/submit",
            payload={
                "intent_id": intent.intent_id,
                "intent": intent.to_dict(),
            },
            idempotency_key=intent.intent_id,
        )
        normalized = self._normalize_submit_ack(payload, intent=intent)
        self._submit_ack_by_intent_id[intent.intent_id] = dict(normalized)
        return normalized

    def cancel_order_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        payload = self._request_json(
            method="POST",
            path=f"/intents/{urllib_parse.quote(intent_id, safe='')}/cancel",
            payload={"intent_id": intent_id, "canceled_at": canceled_at},
            idempotency_key=f"cancel:{intent_id}:{canceled_at}",
        )
        ack_obj = payload.get("ack", payload)
        ack = _as_dict(ack_obj, error_code="sidecar_protocol_error")
        return {
            "intent_id": str(ack.get("intent_id", intent_id)),
            "state": str(ack.get("state", "canceled")),
            "canceled_at": str(ack.get("canceled_at", canceled_at)),
            "external_order_id": ack.get("external_order_id"),
        }

    def replace_order_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        payload = self._request_json(
            method="POST",
            path=f"/intents/{urllib_parse.quote(intent_id, safe='')}/replace",
            payload={
                "intent_id": intent_id,
                "new_qty": new_qty,
                "new_price": new_price,
                "replaced_at": replaced_at,
            },
            idempotency_key=f"replace:{intent_id}:{replaced_at}",
        )
        ack_obj = payload.get("ack", payload)
        ack = _as_dict(ack_obj, error_code="sidecar_protocol_error")
        return {
            "intent_id": str(ack.get("intent_id", intent_id)),
            "state": str(ack.get("state", "replaced")),
            "new_qty": int(ack.get("new_qty", new_qty)),
            "new_price": float(ack.get("new_price", new_price)),
            "replaced_at": str(ack.get("replaced_at", replaced_at)),
            "external_order_id": ack.get("external_order_id"),
        }

    def _append_unique_stream_rows(
        self,
        *,
        rows: list[dict[str, object]],
        cache: list[dict[str, object]],
        row_kind: str,
    ) -> None:
        for item in rows:
            dedup_key = json.dumps(
                {"kind": row_kind, "payload": item},
                ensure_ascii=False,
                sort_keys=True,
            )
            if dedup_key in self._seen_stream_rows:
                continue
            self._seen_stream_rows.add(dedup_key)
            cache.append(item)

    def list_broker_updates(self) -> list[dict[str, object]]:
        payload = self._request_json(
            method="GET",
            path="/stream/updates",
            query={
                "cursor": self._updates_cursor or "",
                "limit": self._stream_batch_size,
            },
        )
        rows = _stream_rows(payload, key="updates", error_code="sidecar_protocol_error")
        next_cursor = payload.get("next_cursor")
        if isinstance(next_cursor, str) and next_cursor.strip():
            self._updates_cursor = next_cursor.strip()
        self._append_unique_stream_rows(rows=rows, cache=self._updates_cache, row_kind="update")
        return list(self._updates_cache)

    def list_broker_fills(self) -> list[dict[str, object]]:
        payload = self._request_json(
            method="GET",
            path="/stream/fills",
            query={
                "cursor": self._fills_cursor or "",
                "limit": self._stream_batch_size,
            },
        )
        rows = _stream_rows(payload, key="fills", error_code="sidecar_protocol_error")
        next_cursor = payload.get("next_cursor")
        if isinstance(next_cursor, str) and next_cursor.strip():
            self._fills_cursor = next_cursor.strip()
        self._append_unique_stream_rows(rows=rows, cache=self._fills_cache, row_kind="fill")
        return list(self._fills_cache)

    def health(self) -> dict[str, object]:
        try:
            remote = self._request_json(method="GET", path="/health", include_api_prefix=False)
            status = "ok"
            error_code = None
        except SidecarTransportError as exc:
            remote = {}
            status = "degraded"
            error_code = exc.error_code
        return {
            "adapter": "stocksharp-sidecar-http",
            "status": status,
            "base_url": self._base_url,
            "api_prefix": self._api_prefix,
            "updates_cursor": self._updates_cursor,
            "fills_cursor": self._fills_cursor,
            "updates_cached": len(self._updates_cache),
            "fills_cached": len(self._fills_cache),
            "error_code": error_code,
            "remote": remote,
        }

    def readiness(self) -> dict[str, object]:
        try:
            remote = self._request_json(method="GET", path="/ready", include_api_prefix=False)
        except SidecarTransportError as exc:
            return {
                "adapter": "stocksharp-sidecar-http",
                "ready": False,
                "reason": exc.error_code,
                "base_url": self._base_url,
            }
        return {
            "adapter": "stocksharp-sidecar-http",
            "ready": bool(remote.get("ready", True)),
            "reason": str(remote.get("reason", "ok")),
            "base_url": self._base_url,
        }
