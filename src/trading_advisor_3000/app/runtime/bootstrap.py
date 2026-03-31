from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from .pipeline import RuntimeStack, build_runtime_stack
from .signal_store import InMemorySignalStore, PostgresSignalStore, SignalStore


_SUPPORTED_RUNTIME_PROFILES = frozenset({"dev", "test", "staging", "production"})
_DURABLE_RUNTIME_PROFILES = frozenset({"staging", "production"})
_SUPPORTED_SIGNAL_STORE_BACKENDS = frozenset({"inmemory", "postgres"})


class RuntimeConfigurationError(ValueError):
    """Raised when runtime bootstrap configuration is invalid."""


def _env_text(env: Mapping[str, str], name: str, default: str) -> str:
    raw = env.get(name)
    if raw is None:
        return default
    cleaned = raw.strip()
    return cleaned if cleaned else default


def _normalize_profile(value: str) -> str:
    profile = value.strip().lower()
    if profile not in _SUPPORTED_RUNTIME_PROFILES:
        allowed = ", ".join(sorted(_SUPPORTED_RUNTIME_PROFILES))
        raise RuntimeConfigurationError(f"unsupported TA3000_RUNTIME_PROFILE `{value}`; allowed: {allowed}")
    return profile


def _normalize_backend(value: str) -> str:
    backend = value.strip().lower()
    if backend not in _SUPPORTED_SIGNAL_STORE_BACKENDS:
        allowed = ", ".join(sorted(_SUPPORTED_SIGNAL_STORE_BACKENDS))
        raise RuntimeConfigurationError(f"unsupported TA3000_SIGNAL_STORE_BACKEND `{value}`; allowed: {allowed}")
    return backend


@dataclass(frozen=True)
class RuntimeBootstrapConfig:
    profile: str
    signal_store_backend: str
    telegram_channel: str
    postgres_dsn: str | None
    postgres_schema: str

    @property
    def durable_runtime_required(self) -> bool:
        return self.profile in _DURABLE_RUNTIME_PROFILES

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "signal_store_backend": self.signal_store_backend,
            "durable_runtime_required": self.durable_runtime_required,
            "postgres_dsn_configured": self.postgres_dsn is not None,
            "postgres_schema": self.postgres_schema,
            "telegram_channel": self.telegram_channel,
        }


@dataclass(frozen=True)
class RuntimeBootstrapResult:
    config: RuntimeBootstrapConfig
    runtime_stack: RuntimeStack


def read_runtime_bootstrap_config(env: Mapping[str, str] | None = None) -> RuntimeBootstrapConfig:
    source = env or os.environ
    profile = _normalize_profile(_env_text(source, "TA3000_RUNTIME_PROFILE", "staging"))
    default_backend = "postgres" if profile in _DURABLE_RUNTIME_PROFILES else "inmemory"
    signal_store_backend = _normalize_backend(
        _env_text(source, "TA3000_SIGNAL_STORE_BACKEND", default_backend),
    )
    dsn = _env_text(source, "TA3000_APP_DSN", "")
    dsn_value = dsn if dsn else None
    postgres_schema = _env_text(source, "TA3000_SIGNAL_STORE_SCHEMA", "signal")
    telegram_channel = _env_text(source, "TA3000_TELEGRAM_CHANNEL", "")
    if not telegram_channel:
        raise RuntimeConfigurationError(
            "runtime bootstrap requires TA3000_TELEGRAM_CHANNEL; "
            "no default or fallback channel is allowed",
        )

    if profile in _DURABLE_RUNTIME_PROFILES and signal_store_backend != "postgres":
        raise RuntimeConfigurationError(
            "durable profile requires PostgreSQL signal store: "
            "set TA3000_SIGNAL_STORE_BACKEND=postgres",
        )
    if signal_store_backend == "postgres" and not dsn_value:
        raise RuntimeConfigurationError(
            "PostgreSQL signal store requires TA3000_APP_DSN; "
            "no in-memory fallback is allowed for this runtime profile",
        )

    return RuntimeBootstrapConfig(
        profile=profile,
        signal_store_backend=signal_store_backend,
        telegram_channel=telegram_channel,
        postgres_dsn=dsn_value,
        postgres_schema=postgres_schema,
    )


def build_signal_store_from_config(config: RuntimeBootstrapConfig) -> SignalStore:
    if config.signal_store_backend == "postgres":
        if not config.postgres_dsn:
            raise RuntimeConfigurationError("postgres backend selected but postgres_dsn is empty")
        return PostgresSignalStore(
            dsn=config.postgres_dsn,
            schema_name=config.postgres_schema,
        )
    return InMemorySignalStore()


def build_runtime_stack_from_env(env: Mapping[str, str] | None = None) -> RuntimeBootstrapResult:
    config = read_runtime_bootstrap_config(env)
    source = env or os.environ
    telegram_bot_token = _env_text(source, "TA3000_TELEGRAM_BOT_TOKEN", "")
    signal_store = build_signal_store_from_config(config)
    runtime_stack = build_runtime_stack(
        telegram_channel=config.telegram_channel,
        telegram_bot_token=telegram_bot_token if telegram_bot_token else None,
        signal_store=signal_store,
    )
    return RuntimeBootstrapResult(config=config, runtime_stack=runtime_stack)
