from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Callable, Mapping

from trading_advisor_3000.app.contracts import Mode, OrderIntent
from trading_advisor_3000.app.runtime.config import DEFAULT_REQUIRED_LIVE_SECRETS, evaluate_secrets_policy

from .catalog import ExecutionAdapterCatalog, default_execution_adapter_catalog
from .stocksharp_sidecar_stub import StockSharpSidecarStub, TransientSidecarError


class LiveExecutionBridgeError(RuntimeError):
    pass


class LiveExecutionDisabledError(LiveExecutionBridgeError):
    pass


class UnsupportedExecutionModeError(LiveExecutionBridgeError):
    pass


class UnsupportedBrokerAdapterError(LiveExecutionBridgeError):
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
            "required_live_secret_env_vars": list(self.required_live_secret_env_vars),
            "environment": self.environment,
            "live_path": self.live_path,
        }


class LiveExecutionBridge:
    def __init__(
        self,
        *,
        sidecar: StockSharpSidecarStub,
        flags: LiveExecutionFeatureFlags,
        retry_policy: LiveExecutionRetryPolicy | None = None,
        adapter_catalog: ExecutionAdapterCatalog | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self._sidecar = sidecar
        self._flags = flags
        self._retry_policy = retry_policy or LiveExecutionRetryPolicy()
        self._adapter_catalog = adapter_catalog or default_execution_adapter_catalog()
        self._env = dict(env if env is not None else os.environ)
        self._seen_update_index = 0
        self._seen_fill_index = 0
        self._retryable_exceptions = (TransientSidecarError, TimeoutError, ConnectionError)

    def _live_secrets_report(self) -> dict[str, object]:
        report = evaluate_secrets_policy(
            env=self._env,
            required_secret_names=self._flags.required_live_secret_env_vars,
            enforce=self._flags.enforce_live_secrets,
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
            missing = ", ".join(str(item) for item in secrets_report["missing_secret_names"])
            raise LiveExecutionDisabledError(
                "live execution blocked by missing secrets: " + missing
            )

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
                last_error = exc
                if attempt >= self._retry_policy.max_attempts:
                    break
                if self._retry_policy.backoff_seconds > 0:
                    time.sleep(self._retry_policy.backoff_seconds * attempt)
            except Exception:
                raise
        assert last_error is not None
        raise LiveExecutionRetryExhaustedError(
            operation=operation,
            attempts=self._retry_policy.max_attempts,
            last_error=last_error,
        ) from last_error

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
            "secrets_policy": secrets_report,
            "sidecar": self._sidecar.health(),
        }

    def submit_order_intent(self, intent: OrderIntent, *, accepted_at: str) -> dict[str, object]:
        self._ensure_supported_mode(intent)
        self._ensure_supported_adapter(intent)
        self._ensure_live_allowed(intent)
        ack = self._call_with_retry(
            operation="submit_order_intent",
            fn=lambda: self._sidecar.submit_order_intent(intent),
        )
        return {**ack, "accepted_at": accepted_at}

    def cancel_order_intent(self, *, intent_id: str, canceled_at: str) -> dict[str, object]:
        return self._call_with_retry(
            operation="cancel_order_intent",
            fn=lambda: self._sidecar.cancel_order_intent(intent_id=intent_id, canceled_at=canceled_at),
        )

    def replace_order_intent(
        self,
        *,
        intent_id: str,
        new_qty: int,
        new_price: float,
        replaced_at: str,
    ) -> dict[str, object]:
        return self._call_with_retry(
            operation="replace_order_intent",
            fn=lambda: self._sidecar.replace_order_intent(
                intent_id=intent_id,
                new_qty=new_qty,
                new_price=new_price,
                replaced_at=replaced_at,
            ),
        )

    def drain_broker_streams(self) -> dict[str, list[dict[str, object]]]:
        all_updates = self._sidecar.list_broker_updates()
        all_fills = self._sidecar.list_broker_fills()

        new_updates = all_updates[self._seen_update_index :]
        new_fills = all_fills[self._seen_fill_index :]

        self._seen_update_index = len(all_updates)
        self._seen_fill_index = len(all_fills)

        return {
            "updates": new_updates,
            "fills": new_fills,
        }
