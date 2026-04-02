from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.product_plane.contracts import Timeframe


def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()


def _required_number(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return float(value)


def _required_non_negative(name: str, value: object) -> float:
    normalized = _required_number(name, value)
    if normalized < 0:
        raise ValueError(f"{name} must be non-negative")
    return normalized


def _required_features_json(value: object) -> dict[str, float | str]:
    if not isinstance(value, dict):
        raise ValueError("features_json must be an object")
    normalized: dict[str, float | str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("features_json keys must be non-empty strings")
        if isinstance(item, bool) or not isinstance(item, (int, float, str)):
            raise ValueError(f"features_json[{key}] must be number or string")
        normalized[key] = float(item) if isinstance(item, (int, float)) and not isinstance(item, bool) else item
    return normalized


def _require_keys(
    payload: dict[str, object],
    *,
    required: set[str],
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    allowed = required | optional
    extra = sorted(set(payload) - allowed)
    if extra:
        raise ValueError(f"unsupported fields: {', '.join(extra)}")
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")


@dataclass(frozen=True)
class FeatureSnapshot:
    snapshot_id: str
    contract_id: str
    instrument_id: str
    timeframe: Timeframe
    ts: str
    feature_set_version: str
    regime: str
    atr: float
    ema_fast: float
    ema_slow: float
    donchian_high: float
    donchian_low: float
    rvol: float
    features_json: dict[str, float | str]

    def __post_init__(self) -> None:
        if self.atr < 0:
            raise ValueError("atr must be non-negative")
        if self.rvol < 0:
            raise ValueError("rvol must be non-negative")
        if self.donchian_low > self.donchian_high:
            raise ValueError("donchian_low must be <= donchian_high")

    def to_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe.value,
            "ts": self.ts,
            "feature_set_version": self.feature_set_version,
            "regime": self.regime,
            "atr": self.atr,
            "ema_fast": self.ema_fast,
            "ema_slow": self.ema_slow,
            "donchian_high": self.donchian_high,
            "donchian_low": self.donchian_low,
            "rvol": self.rvol,
            "features_json": self.features_json,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "FeatureSnapshot":
        _require_keys(
            payload,
            required={
                "snapshot_id",
                "contract_id",
                "instrument_id",
                "timeframe",
                "ts",
                "feature_set_version",
                "regime",
                "atr",
                "ema_fast",
                "ema_slow",
                "donchian_high",
                "donchian_low",
                "rvol",
                "features_json",
            },
        )
        return cls(
            snapshot_id=_required_text("snapshot_id", payload.get("snapshot_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            instrument_id=_required_text("instrument_id", payload.get("instrument_id")),
            timeframe=Timeframe(_required_text("timeframe", payload.get("timeframe"))),
            ts=_required_text("ts", payload.get("ts")),
            feature_set_version=_required_text(
                "feature_set_version",
                payload.get("feature_set_version"),
            ),
            regime=_required_text("regime", payload.get("regime")),
            atr=_required_non_negative("atr", payload.get("atr")),
            ema_fast=_required_number("ema_fast", payload.get("ema_fast")),
            ema_slow=_required_number("ema_slow", payload.get("ema_slow")),
            donchian_high=_required_number("donchian_high", payload.get("donchian_high")),
            donchian_low=_required_number("donchian_low", payload.get("donchian_low")),
            rvol=_required_non_negative("rvol", payload.get("rvol")),
            features_json=_required_features_json(payload.get("features_json")),
        )
