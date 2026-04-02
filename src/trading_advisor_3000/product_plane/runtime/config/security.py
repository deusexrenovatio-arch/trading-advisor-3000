from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
    rotated_at: str | None = None
    age_days: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "present": self.present,
            "redacted_value": self.redacted_value,
            "rotated_at": self.rotated_at,
            "age_days": self.age_days,
        }


@dataclass(frozen=True)
class SecretsPolicyReport:
    required_secret_names: tuple[str, ...]
    probes: list[SecretProbe]
    missing_secret_names: list[str]
    stale_secret_names: list[str]
    enforce: bool
    check_age: bool
    max_age_days: int | None

    @property
    def is_ready(self) -> bool:
        return not self.missing_secret_names and not self.stale_secret_names

    def to_dict(self) -> dict[str, object]:
        return {
            "required_secret_names": list(self.required_secret_names),
            "missing_secret_names": list(self.missing_secret_names),
            "stale_secret_names": list(self.stale_secret_names),
            "present_count": sum(1 for item in self.probes if item.present),
            "missing_count": len(self.missing_secret_names),
            "stale_count": len(self.stale_secret_names),
            "enforce": self.enforce,
            "check_age": self.check_age,
            "max_age_days": self.max_age_days,
            "is_ready": self.is_ready,
            "probes": [item.to_dict() for item in self.probes],
        }


def _parse_utc(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def evaluate_secrets_policy(
    *,
    env: Mapping[str, str] | None,
    required_secret_names: tuple[str, ...] = DEFAULT_REQUIRED_LIVE_SECRETS,
    enforce: bool,
    check_age: bool = False,
    max_age_days: int | None = None,
    secret_rotated_at_by_name: Mapping[str, str] | None = None,
) -> SecretsPolicyReport:
    source = env or {}
    rotated_at_map = secret_rotated_at_by_name or {}
    probes: list[SecretProbe] = []
    missing: list[str] = []
    stale: list[str] = []
    now_utc = datetime.now(timezone.utc)
    for name in required_secret_names:
        raw = source.get(name)
        if isinstance(raw, str) and raw.strip():
            rotated_at_value = str(rotated_at_map.get(name, "")).strip()
            rotated_at = rotated_at_value if rotated_at_value else None
            age_days: float | None = None
            if check_age:
                if rotated_at is None:
                    stale.append(name)
                else:
                    rotated_dt = _parse_utc(rotated_at)
                    if rotated_dt is None:
                        stale.append(name)
                    else:
                        age_days = round(max(0.0, (now_utc - rotated_dt).total_seconds() / 86400.0), 3)
                        if max_age_days is not None and max_age_days > 0 and age_days > float(max_age_days):
                            stale.append(name)
            probes.append(
                SecretProbe(
                    name=name,
                    present=True,
                    redacted_value=redact_secret(raw),
                    rotated_at=rotated_at,
                    age_days=age_days,
                )
            )
            continue
        probes.append(
            SecretProbe(
                name=name,
                present=False,
                redacted_value=None,
                rotated_at=None,
                age_days=None,
            )
        )
        missing.append(name)
    return SecretsPolicyReport(
        required_secret_names=required_secret_names,
        probes=probes,
        missing_secret_names=missing,
        stale_secret_names=stale,
        enforce=enforce,
        check_age=check_age,
        max_age_days=max_age_days,
    )
