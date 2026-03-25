from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .security import evaluate_secrets_policy, redact_secret


DEFAULT_PHASE9_BATTLE_RUN_PROFILE = "phase9-battle-run"
DEFAULT_PHASE9_SIGNAL_STORE_BACKEND = "postgres"
DEFAULT_PHASE9_SIGNAL_STORE_SCHEMA = "signal"
DEFAULT_REQUIRED_BATTLE_RUN_SECRETS = (
    "TA3000_APP_DSN",
    "TA3000_TELEGRAM_BOT_TOKEN",
)
DEFAULT_REQUIRED_BATTLE_RUN_ENV_NAMES = (
    "TA3000_RUNTIME_PROFILE",
    "TA3000_SIGNAL_STORE_BACKEND",
    "TA3000_APP_DSN",
    "TA3000_TELEGRAM_TRANSPORT",
    "TA3000_TELEGRAM_BOT_TOKEN",
    "TA3000_TELEGRAM_SHADOW_CHANNEL",
)
DEFAULT_OPTIONAL_BATTLE_RUN_ENV_NAMES = (
    "TA3000_SIGNAL_STORE_SCHEMA",
    "TA3000_TELEGRAM_API_BASE_URL",
    "TA3000_TELEGRAM_ADVISORY_CHANNEL",
    "TA3000_PROMETHEUS_BASE_URL",
    "TA3000_LOKI_BASE_URL",
    "TA3000_GRAFANA_DASHBOARD_URL",
)


def _env_text(env: Mapping[str, str], name: str, default: str = "") -> str:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip()


@dataclass(frozen=True)
class Phase9BattleRunConfig:
    runtime_profile: str
    signal_store_backend: str
    signal_store_schema: str
    app_dsn: str
    telegram_transport: str
    telegram_api_base_url: str | None
    telegram_shadow_channel: str
    telegram_advisory_channel: str | None
    prometheus_base_url: str | None
    loki_base_url: str | None
    grafana_dashboard_url: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "runtime_profile": self.runtime_profile,
            "signal_store_backend": self.signal_store_backend,
            "signal_store_schema": self.signal_store_schema,
            "app_dsn": redact_secret(self.app_dsn) if self.app_dsn else "<missing>",
            "telegram_transport": self.telegram_transport,
            "telegram_api_base_url": self.telegram_api_base_url,
            "telegram_shadow_channel": self.telegram_shadow_channel,
            "telegram_advisory_channel": self.telegram_advisory_channel,
            "prometheus_base_url": self.prometheus_base_url,
            "loki_base_url": self.loki_base_url,
            "grafana_dashboard_url": self.grafana_dashboard_url,
        }


@dataclass(frozen=True)
class Phase9BattleRunPreflightReport:
    config: Phase9BattleRunConfig
    required_env_names: tuple[str, ...]
    optional_env_names: tuple[str, ...]
    missing_config_names: list[str]
    invalid_config: list[str]
    warnings: list[str]
    secrets_policy: dict[str, object]

    @property
    def is_ready(self) -> bool:
        return not self.missing_config_names and not self.invalid_config and bool(self.secrets_policy.get("is_ready"))

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "ok" if self.is_ready else "blocked",
            "required_env_names": list(self.required_env_names),
            "optional_env_names": list(self.optional_env_names),
            "missing_config_names": list(self.missing_config_names),
            "invalid_config": list(self.invalid_config),
            "warnings": list(self.warnings),
            "config": self.config.to_dict(),
            "secrets_policy": self.secrets_policy,
            "is_ready": self.is_ready,
        }


def evaluate_phase9_battle_run_preflight(
    env: Mapping[str, str] | None = None,
) -> Phase9BattleRunPreflightReport:
    source = dict(env or {})
    config = Phase9BattleRunConfig(
        runtime_profile=_env_text(source, "TA3000_RUNTIME_PROFILE", DEFAULT_PHASE9_BATTLE_RUN_PROFILE),
        signal_store_backend=_env_text(
            source,
            "TA3000_SIGNAL_STORE_BACKEND",
            DEFAULT_PHASE9_SIGNAL_STORE_BACKEND,
        ).lower(),
        signal_store_schema=_env_text(
            source,
            "TA3000_SIGNAL_STORE_SCHEMA",
            DEFAULT_PHASE9_SIGNAL_STORE_SCHEMA,
        ),
        app_dsn=_env_text(source, "TA3000_APP_DSN"),
        telegram_transport=_env_text(source, "TA3000_TELEGRAM_TRANSPORT").lower(),
        telegram_api_base_url=_env_text(source, "TA3000_TELEGRAM_API_BASE_URL") or None,
        telegram_shadow_channel=_env_text(source, "TA3000_TELEGRAM_SHADOW_CHANNEL"),
        telegram_advisory_channel=_env_text(source, "TA3000_TELEGRAM_ADVISORY_CHANNEL") or None,
        prometheus_base_url=_env_text(source, "TA3000_PROMETHEUS_BASE_URL") or None,
        loki_base_url=_env_text(source, "TA3000_LOKI_BASE_URL") or None,
        grafana_dashboard_url=_env_text(source, "TA3000_GRAFANA_DASHBOARD_URL") or None,
    )

    missing_config_names: list[str] = []
    if not config.app_dsn:
        missing_config_names.append("TA3000_APP_DSN")
    if not config.telegram_transport:
        missing_config_names.append("TA3000_TELEGRAM_TRANSPORT")
    if not config.telegram_shadow_channel:
        missing_config_names.append("TA3000_TELEGRAM_SHADOW_CHANNEL")

    invalid_config: list[str] = []
    if config.runtime_profile != DEFAULT_PHASE9_BATTLE_RUN_PROFILE:
        invalid_config.append(
            "TA3000_RUNTIME_PROFILE must be phase9-battle-run for WS-C battle-run closure"
        )
    if config.signal_store_backend != DEFAULT_PHASE9_SIGNAL_STORE_BACKEND:
        invalid_config.append(
            "TA3000_SIGNAL_STORE_BACKEND must stay postgres for Phase 9 battle-run mode"
        )
    if config.telegram_transport != "bot-api":
        invalid_config.append(
            "TA3000_TELEGRAM_TRANSPORT must be bot-api for real Telegram Phase 9 battle-run closure"
        )
    if not config.signal_store_schema:
        invalid_config.append("TA3000_SIGNAL_STORE_SCHEMA must be non-empty when provided")

    warnings: list[str] = []
    if config.telegram_advisory_channel is None:
        warnings.append("TA3000_TELEGRAM_ADVISORY_CHANNEL is not set; shadow-only publication is in effect")
    if config.prometheus_base_url is None:
        warnings.append("TA3000_PROMETHEUS_BASE_URL is not set; evidence will rely on exported metrics artifacts")
    if config.loki_base_url is None:
        warnings.append("TA3000_LOKI_BASE_URL is not set; evidence will rely on exported log artifacts")
    if config.grafana_dashboard_url is None:
        warnings.append("TA3000_GRAFANA_DASHBOARD_URL is not set; dashboard export remains a manual attachment")

    secrets_policy = evaluate_secrets_policy(
        env=source,
        required_secret_names=DEFAULT_REQUIRED_BATTLE_RUN_SECRETS,
        enforce=True,
    ).to_dict()

    return Phase9BattleRunPreflightReport(
        config=config,
        required_env_names=DEFAULT_REQUIRED_BATTLE_RUN_ENV_NAMES,
        optional_env_names=DEFAULT_OPTIONAL_BATTLE_RUN_ENV_NAMES,
        missing_config_names=missing_config_names,
        invalid_config=invalid_config,
        warnings=warnings,
        secrets_policy=secrets_policy,
    )


def require_phase9_battle_run_config(env: Mapping[str, str] | None = None) -> Phase9BattleRunConfig:
    report = evaluate_phase9_battle_run_preflight(env)
    if report.is_ready:
        return report.config

    reasons = [
        *[f"missing:{item}" for item in report.missing_config_names],
        *report.invalid_config,
    ]
    missing_secrets = report.secrets_policy.get("missing_secret_names", [])
    if isinstance(missing_secrets, list):
        reasons.extend(f"missing_secret:{item}" for item in missing_secrets)
    raise ValueError("phase9 battle-run preflight failed: " + "; ".join(reasons))
