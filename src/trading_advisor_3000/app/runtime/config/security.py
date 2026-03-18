from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


DEFAULT_REQUIRED_LIVE_SECRETS = (
    "TA3000_STOCKSHARP_API_KEY",
    "TA3000_FINAM_API_TOKEN",
)


def redact_secret(value: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return "<empty>"
    if len(trimmed) <= 4:
        return "*" * len(trimmed)
    return f"{trimmed[:2]}***{trimmed[-2:]}"


@dataclass(frozen=True)
class SecretProbe:
    name: str
    present: bool
    redacted_value: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "present": self.present,
            "redacted_value": self.redacted_value,
        }


@dataclass(frozen=True)
class SecretsPolicyReport:
    required_secret_names: tuple[str, ...]
    probes: list[SecretProbe]
    missing_secret_names: list[str]
    enforce: bool

    @property
    def is_ready(self) -> bool:
        return not self.missing_secret_names

    def to_dict(self) -> dict[str, object]:
        return {
            "required_secret_names": list(self.required_secret_names),
            "missing_secret_names": list(self.missing_secret_names),
            "present_count": sum(1 for item in self.probes if item.present),
            "missing_count": len(self.missing_secret_names),
            "enforce": self.enforce,
            "is_ready": self.is_ready,
            "probes": [item.to_dict() for item in self.probes],
        }


def evaluate_secrets_policy(
    *,
    env: Mapping[str, str] | None,
    required_secret_names: tuple[str, ...] = DEFAULT_REQUIRED_LIVE_SECRETS,
    enforce: bool,
) -> SecretsPolicyReport:
    source = env or {}
    probes: list[SecretProbe] = []
    missing: list[str] = []
    for name in required_secret_names:
        raw = source.get(name)
        if isinstance(raw, str) and raw.strip():
            probes.append(SecretProbe(name=name, present=True, redacted_value=redact_secret(raw)))
            continue
        probes.append(SecretProbe(name=name, present=False, redacted_value=None))
        missing.append(name)
    return SecretsPolicyReport(
        required_secret_names=required_secret_names,
        probes=probes,
        missing_secret_names=missing,
        enforce=enforce,
    )
