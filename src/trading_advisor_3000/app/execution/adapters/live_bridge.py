from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping

from trading_advisor_3000.app.contracts import Mode, OrderIntent
from trading_advisor_3000.app.runtime.config import DEFAULT_REQUIRED_LIVE_SECRETS, evaluate_secrets_policy

from .catalog import ExecutionAdapterCatalog, default_execution_adapter_catalog
from .stocksharp_http_transport import SidecarTransportError, SidecarTransportRetryableError
from .stocksharp_sidecar_stub import TransientSidecarError
from .transport import ExecutionAdapterTransport


class LiveExecutionBridgeError(RuntimeError):
    pass


class LiveExecutionDisabledError(LiveExecutionBridgeError):
    pass


class UnsupportedExecutionModeError(LiveExecutionBridgeError):
    pass


class UnsupportedBrokerAdapterError(LiveExecutionBridgeError):
    pass


class AdapterTransportNotConfiguredError(LiveExecutionBridgeError):
    pass


class LiveExecutionRetryExhaustedError(LiveExecutionBridgeError):
    def __init__(self, *, operation: str, attempts: int, last_error: BaseException) -> None:
        super().__init__(
            f"{operation} failed after {attempts} attempts: {type(last_error).__name__}: {last_error}"
        )
        self.operation = operation
        self.attempts = attempts
        self.last_error = last_error


@dataclass(frozen=True)
class LiveExecutionRetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if self.backoff_seconds < 0:
            raise ValueError("backoff_seconds must be non-negative")


@dataclass(frozen=True)
class LiveExecutionFeatureFlags:
    enable_live_execution: bool = False
    enable_stocksharp_bridge: bool = False
    enable_quik_connector: bool = False
    enable_finam_transport: bool = False
    enforce_live_secrets: bool = False
    enforce_secret_age: bool = False
    max_secret_age_days: int = 90
    required_live_secret_env_vars: tuple[str, ...] = DEFAULT_REQUIRED_LIVE_SECRETS
    environment: str = "dev"

    def preflight_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.enable_live_execution:
            errors.append("live_execution_disabled")
        if not self.enable_stocksharp_bridge:
            errors.append("stocksharp_bridge_disabled")
        if not self.enable_quik_connector:
            errors.append("quik_connector_disabled")
        if not self.enable_finam_transport:
            errors.append("finam_transport_disabled")
        return errors

    @property
    def live_path(self) -> str:
        if self.preflight_errors():
            return "disabled"
        return "stocksharp->quik->finam"

    def to_dict(self) -> dict[str, object]:
        return {
            "enable_live_execution": self.enable_live_execution,
            "enable_stocksharp_bridge": self.enable_stocksharp_bridge,
            "enable_quik_connector": self.enable_quik_connector,
            "enable_finam_transport": self.enable_finam_transport,
            "enforce_live_secrets": self.enforce_live_secrets,
            "enforce_secret_age": self.enforce_secret_age,
            "max_secret_age_days": self.max_secret_age_days,
            "required_live_secret_env_vars": list(self.required_live_secret_env_vars),
            "environment": self.environment,
            "live_path": self.live_path,
        }


class LiveExecutionBridge:
    def __init__(
        self,
        *,
        sidecar: ExecutionAdapterTransport,
        flags: LiveExecutionFeatureFlags,
        retry_policy: LiveExecutionRetryPolicy | None = None,
        adapter_catalog: ExecutionAdapterCatalog | None = None,
        adapter_transports: Mapping[str, ExecutionAdapterTransport] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self._sidecar = sidecar
        self._flags = flags
        self._retry_policy = retry_policy or LiveExecutionRetryPolicy()
        self._adapter_catalog = adapter_catalog or default_execution_adapter_catalog()
        bindings: dict[str, ExecutionAdapterTransport] = {
            "stocksharp-sidecar-stub": sidecar,
            "stocksharp-sidecar": sidecar,
        }
        if adapter_transports is not None:
            bindings.update(adapter_transports)
        self._transport_key_by_adapter_id: dict[str, str] = {}
        self._transports_by_key: dict[str, ExecutionAdapterTransport] = {}
        object_key_by_id: dict[int, str] = {}
        for adapter_id, transport in sorted(bindings.items()):
            object_id = id(transport)
            transport_key = object_key_by_id.get(object_id)
            if transport_key is None:
                transport_key = f"transport-{len(object_key_by_id) + 1}"
                object_key_by_id[object_id] = transport_key
                self._transports_by_key[transport_key] = transport
            self._transport_key_by_adapter_id[adapter_id] = transport_key
        self._env = dict(env if env is not None else os.environ)
        self._seen_update_index_by_transport_key = {key: 0 for key in self._transports_by_key}
        self._seen_fill_index_by_transport_key = {key: 0 for key in self._transports_by_key}
        self._transport_key_by_intent_id: dict[str, str] = {}
        self._retryable_exceptions = (
            TransientSidecarError,
            SidecarTransportRetryableError,
            TimeoutError,
            ConnectionError,
        )
        self._submit_latency_ms: list[float] = []
        self._sync_lag_ms: list[float] = []
        self._sidecar_errors_by_code: dict[str, int] = {}
        self._retry_exhausted_total = 0
        self._operation_log: list[dict[str, object]] = []
        self._operation_log_limit = 500

    @staticmethod
    def _parse_utc(ts: object) -> datetime | None:
        if not isinstance(ts, str) or not ts.strip():
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _p95(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
        return round(ordered[index], 3)

    def _record_operation_event(
        self,
        *,
        event_type: str,
        status: str,
        intent_id: str | None = None,
        external_order_id: str | None = None,
        transport_key: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        event = {
            "event_type": event_type,
            "status": status,
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "intent_id": intent_id,
            "external_order_id": external_order_id,
            "transport_key": transport_key,
            "details": details or {},
        }
        self._operation_log.append(event)
        if len(self._operation_log) > self._operation_log_limit:
            overflow = len(self._operation_log) - self._operation_log_limit
            if overflow > 0:
                del self._operation_log[:overflow]

    def _record_sidecar_error(self, exc: BaseException) -> None:
        if isinstance(exc, SidecarTransportError):
            code = exc.error_code
            status_code = exc.status_code
        elif isinstance(exc, LiveExecutionRetryExhaustedError) and isinstance(exc.last_error, SidecarTransportError):
            code = exc.last_error.error_code
            status_code = exc.last_error.status_code
        else:
            code = type(exc).__name__
            status_code = None
        key = code if status_code is None else f"{code}:{status_code}"
        self._sidecar_errors_by_code[key] = self._sidecar_errors_by_code.get(key, 0) + 1

    def _observe_sync_lag(self, *, event_ts: object) -> float | None:
        event_dt = self._parse_utc(event_ts)
        if event_dt is None:
            return None
        lag_ms = max(
            0.0,
            (datetime.now(timezone.utc) - event_dt).total_seconds() * 1000.0,
        )
        self._sync_lag_ms.append(lag_ms)
        if len(self._sync_lag_ms) > 5000:
            del self._sync_lag_ms[: len(self._sync_lag_ms) - 5000]
        return round(lag_ms, 3)

    def _secret_rotation_by_name(self) -> dict[str, str]:
        rows: dict[str, str] = {}
        for secret_name in self._flags.required_live_secret_env_vars:
            rotation_name = f"{secret_name}_ROTATED_AT"
            raw = str(self._env.get(rotation_name, "")).strip()
            if raw:
                rows[secret_name] = raw
        return rows

    def _live_secrets_report(self) -> dict[str, object]:
        report = evaluate_secrets_policy(
            env=self._env,
            required_secret_names=self._flags.required_live_secret_env_vars,
            enforce=self._flags.enforce_live_secrets,
            check_age=self._flags.enforce_secret_age,
            max_age_days=self._flags.max_secret_age_days,
            secret_rotated_at_by_name=self._secret_rotation_by_name(),
        )
        return report.to_dict()

    def _ensure_supported_mode(self, intent: OrderIntent) -> None:
        if intent.mode not in {Mode.PAPER, Mode.LIVE}:
            raise UnsupportedExecutionModeError(
                f"execution bridge supports only paper/live intents, got mode={intent.mode.value}"
            )

    def _ensure_supported_adapter(self, intent: OrderIntent) -> None:
        adapter = self._adapter_catalog.get(intent.broker_adapter)
        if adapter is None:
            raise UnsupportedBrokerAdapterError(
                f"unsupported broker_adapter for controlled live bridge: {intent.broker_adapter}"
            )
        if not adapter.supports_mode(intent.mode):
            raise UnsupportedBrokerAdapterError(
                "broker_adapter does not support requested mode: "
                f"adapter={intent.broker_adapter}, mode={intent.mode.value}"
            )
        if intent.broker_adapter not in self._transport_key_by_adapter_id:
            raise AdapterTransportNotConfiguredError(
                "adapter transport is not configured: "
                f"adapter={intent.broker_adapter}"
            )

    def _transport_for_adapter(self, adapter_id: str) -> tuple[str, ExecutionAdapterTransport]:
        transport_key = self._transport_key_by_adapter_id.get(adapter_id)
        if transport_key is None:
            raise AdapterTransportNotConfiguredError(
                f"adapter transport is not configured: adapter={adapter_id}"
            )
        transport = self._transports_by_key[transport_key]
        return transport_key, transport

    def _transport_for_intent(self, intent_id: str) -> tuple[str, ExecutionAdapterTransport]:
        transport_key = self._transport_key_by_intent_id.get(intent_id)
        if transport_key is not None:
            transport = self._transports_by_key.get(transport_key)
            if transport is not None:
                return transport_key, transport
        if len(self._transports_by_key) == 1:
            only_key = next(iter(self._transports_by_key))
            return only_key, self._transports_by_key[only_key]
        raise AdapterTransportNotConfiguredError(
            "intent transport is ambiguous for multi-adapter bridge: "
            f"intent_id={intent_id}"
        )

    def _ensure_live_allowed(self, intent: OrderIntent) -> None:
        if intent.mode != Mode.LIVE:
            return
        errors = self._flags.preflight_errors()
        if errors:
            raise LiveExecutionDisabledError(
                "live execution blocked by feature flags: " + ", ".join(errors)
            )
        secrets_report = self._live_secrets_report()
        if self._flags.enforce_live_secrets and not bool(secrets_report["is_ready"]):
            missing_names = [str(item) for item in secrets_report.get("missing_secret_names", [])]
            stale_names = [str(item) for item in secrets_report.get("stale_secret_names", [])]
            if missing_names:
                raise LiveExecutionDisabledError(
                    "live execution blocked by missing secrets: " + ", ".join(missing_names)
                )
            if stale_names:
                raise LiveExecutionDisabledError(
                    "live execution blocked by stale secrets: " + ", ".join(stale_names)
                )
            raise LiveExecutionDisabledError("live execution blocked by secrets policy")

    def _call_with_retry(
        self,
        *,
        operation: str,
        fn: Callable[[], dict[str, object]],
    ) -> dict[str, object]:
        last_error: BaseException | None = None
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            try:
                return fn()
            except self._retryable_exceptions as exc:
                self._record_sidecar_error(exc)
                last_error = exc
                if attempt >= self._retry_policy.max_attempts:
                    break
                if self._retry_policy.backoff_seconds > 0:
                    time.sleep(self._retry_policy.backoff_seconds * attempt)
            except Exception:
                raise
        assert last_error is not None
        self._retry_exhausted_total += 1
        raise LiveExecutionRetryExhaustedError(
            operation=operation,
            attempts=self._retry_policy.max_attempts,
            last_error=last_error,
        ) from last_error

    def _telemetry_snapshot(self) -> dict[str, object]:
        submit_count = len(self._submit_latency_ms)
        submit_avg = round(sum(self._submit_latency_ms) / submit_count, 3) if submit_count else 0.0
        sync_count = len(self._sync_lag_ms)
        sync_avg = round(sum(self._sync_lag_ms) / sync_count, 3) if sync_count else 0.0
        sync_max = round(max(self._sync_lag_ms), 3) if sync_count else 0.0
        sync_latest = round(self._sync_lag_ms[-1], 3) if sync_count else 0.0
        return {
            "submit_latency_ms": {
                "count": submit_count,
                "avg": submit_avg,
                "p95": self._p95(self._submit_latency_ms),
            },
            "sync_lag_ms": {
                "count": sync_count,
                "avg": sync_avg,
                "p95": self._p95(self._sync_lag_ms),
                "max": sync_max,
                "latest": sync_latest,
            },
            "sidecar_errors_by_code": dict(sorted(self._sidecar_errors_by_code.items())),
            "retry_exhausted_total": self._retry_exhausted_total,
        }

    def operation_log_tail(self, *, limit: int = 50) -> list[dict[str, object]]:
        if limit <= 0:
            return []
        return list(self._operation_log[-limit:])

    def health(self) -> dict[str, object]:
        errors = self._flags.preflight_errors()
        secrets_report = self._live_secrets_report()
        if self._flags.enforce_live_secrets and not bool(secrets_report["is_ready"]):
            errors = [*errors, "missing_live_secrets"]
        return {
            "bridge": "controlled-live-execution-bridge",
            "status": "ok" if not errors else "degraded",
            "environment": self._flags.environment,
            "live_path": self._flags.live_path,
            "preflight_errors": errors,
            "retry_policy": {
                "max_attempts": self._retry_policy.max_attempts,
                "backoff_seconds": self._retry_policy.backoff_seconds,
            },
            "adapter_catalog": self._adapter_catalog.to_dict(),
            "adapter_transport_bindings": [
                {
                    "adapter_id": adapter_id,
                    "transport_key": self._transport_key_by_adapter_id[adapter_id],
                }
                for adapter_id in sorted(self._transport_key_by_adapter_id)
            ],
            "transport_health": {
                transport_key: (
                    transport.health()
                    if callable(getattr(transport, "health", None))
                    else {"status": "unknown"}
                )
                for transport_key, transport in sorted(self._transports_by_key.items())
            },
            "secrets_policy": secrets_report,
            "sidecar": (
                self._sidecar.health()
                if callable(getattr(self._sidecar, "health", None))
                else {"status": "unknown"}
            ),
            "execution_telemetry": self._telemetry_snapshot(),
            "operation_log_tail": self.operation_log_tail(limit=20),
        }

    def submit_order_intent(self, intent: OrderIntent, *, accepted_at: str) -> dict[str, object]:
        self._ensure_supported_mode(intent)
        self._ensure_supported_adapter(intent)
        self._ensure_live_allowed(intent)
        transport_key, transport = self._transport_for_adapter(intent.broker_adapter)
        started = time.perf_counter()
        try:
            ack = self._call_with_retry(
                operation="submit_order_intent",
                fn=lambda: transport.submit_order_intent(intent),
            )
        except Exception as exc:
            self._record_sidecar_error(exc)
            self._record_operation_event(
                event_type="submit_order_intent",
                status="failed",
                intent_id=intent.intent_id,
                transport_key=transport_key,
                details={
                    "broker_adapter": intent.broker_adapter,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            raise
        latency_ms = (time.perf_counter() - started) * 1000.0
        self._submit_latency_ms.append(latency_ms)
        if len(self._submit_latency_ms) > 5000:
            del self._submit_latency_ms[: len(self._submit_latency_ms) - 5000]
        self._transport_key_by_intent_id[intent.intent_id] = transport_key
        external_order_id = str(ack.get("external_order_id", "")).strip() or None
        self._record_operation_event(
            event_type="submit_order_intent",
            status="ok",
            intent_id=intent.intent_id,
            external_order_id=external_order_id,
            transport_key=transport_key,
            details={
                "broker_adapter": intent.broker_adapter,
                "accepted": bool(ack.get("accepted", False)),
                "latency_ms": round(latency_ms, 3),
            },
        )
        return {
            **ack,
            "accepted_at": accepted_at,
            "broker_adapter": intent.broker_adapter,
            "transport_key": transport_key,
        }

    def cancel_order_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        transport_key, transport = self._transport_for_intent(intent_id)
        try:
            ack = self._call_with_retry(
                operation="cancel_order_intent",
                fn=lambda: {
                    **transport.cancel_order_intent(intent_id=intent_id, canceled_at=canceled_at),
                    "transport_key": transport_key,
                },
            )
        except Exception as exc:
            self._record_sidecar_error(exc)
            self._record_operation_event(
                event_type="cancel_order_intent",
                status="failed",
                intent_id=intent_id,
                transport_key=transport_key,
                details={"error_type": type(exc).__name__, "error_message": str(exc)},
            )
            raise
        self._record_operation_event(
            event_type="cancel_order_intent",
            status="ok",
            intent_id=intent_id,
            external_order_id=str(ack.get("external_order_id", "")).strip() or None,
            transport_key=transport_key,
            details={"state": ack.get("state")},
        )
        return ack

    def replace_order_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        transport_key, transport = self._transport_for_intent(intent_id)
        try:
            ack = self._call_with_retry(
                operation="replace_order_intent",
                fn=lambda: {
                    **transport.replace_order_intent(
                        intent_id=intent_id,
                        new_qty=new_qty,
                        new_price=new_price,
                        replaced_at=replaced_at,
                    ),
                    "transport_key": transport_key,
                },
            )
        except Exception as exc:
            self._record_sidecar_error(exc)
            self._record_operation_event(
                event_type="replace_order_intent",
                status="failed",
                intent_id=intent_id,
                transport_key=transport_key,
                details={"error_type": type(exc).__name__, "error_message": str(exc)},
            )
            raise
        self._record_operation_event(
            event_type="replace_order_intent",
            status="ok",
            intent_id=intent_id,
            external_order_id=str(ack.get("external_order_id", "")).strip() or None,
            transport_key=transport_key,
            details={
                "state": ack.get("state"),
                "new_qty": ack.get("new_qty"),
                "new_price": ack.get("new_price"),
            },
        )
        return ack

    def drain_broker_streams(self) -> dict[str, list[dict[str, object]]]:
        new_updates: list[dict[str, object]] = []
        new_fills: list[dict[str, object]] = []
        for transport_key, transport in sorted(self._transports_by_key.items()):
            all_updates = transport.list_broker_updates()
            all_fills = transport.list_broker_fills()
            seen_updates = self._seen_update_index_by_transport_key.get(transport_key, 0)
            seen_fills = self._seen_fill_index_by_transport_key.get(transport_key, 0)
            if seen_updates > len(all_updates):
                seen_updates = 0
            if seen_fills > len(all_fills):
                seen_fills = 0
            self._seen_update_index_by_transport_key[transport_key] = len(all_updates)
            self._seen_fill_index_by_transport_key[transport_key] = len(all_fills)
            for row in all_updates[seen_updates:]:
                payload = row if isinstance(row, dict) else {"raw_payload": row}
                enriched = {**payload, "transport_key": transport_key}
                lag_ms = self._observe_sync_lag(event_ts=payload.get("event_ts"))
                if lag_ms is not None:
                    enriched["sync_lag_ms"] = lag_ms
                new_updates.append(enriched)
                self._record_operation_event(
                    event_type="broker_update",
                    status="ok",
                    intent_id=str(payload.get("payload", {}).get("intent_id")) if isinstance(payload.get("payload"), dict) else None,
                    external_order_id=str(payload.get("external_order_id", "")).strip() or None,
                    transport_key=transport_key,
                    details={
                        "state": payload.get("state"),
                        "event_ts": payload.get("event_ts"),
                        "sync_lag_ms": lag_ms,
                    },
                )
            for row in all_fills[seen_fills:]:
                payload = row if isinstance(row, dict) else {"raw_payload": row}
                enriched = {**payload, "transport_key": transport_key}
                lag_ms = self._observe_sync_lag(event_ts=payload.get("fill_ts"))
                if lag_ms is not None:
                    enriched["sync_lag_ms"] = lag_ms
                new_fills.append(enriched)
                self._record_operation_event(
                    event_type="broker_fill",
                    status="ok",
                    intent_id=None,
                    external_order_id=str(payload.get("external_order_id", "")).strip() or None,
                    transport_key=transport_key,
                    details={
                        "fill_id": payload.get("fill_id"),
                        "fill_ts": payload.get("fill_ts"),
                        "sync_lag_ms": lag_ms,
                    },
                )
        new_updates = sorted(
            new_updates,
            key=lambda item: (
                str(item.get("event_ts", "")),
                str(item.get("external_order_id", "")),
                str(item.get("state", "")),
                str(item.get("transport_key", "")),
            ),
        )
        new_fills = sorted(
            new_fills,
            key=lambda item: (
                str(item.get("fill_ts", "")),
                str(item.get("fill_id", "")),
                str(item.get("external_order_id", "")),
                str(item.get("transport_key", "")),
            ),
        )
        return {
            "updates": new_updates,
            "fills": new_fills,
        }
