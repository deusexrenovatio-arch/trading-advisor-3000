from __future__ import annotations

from dataclasses import dataclass

from .enums import Timeframe


def _require_non_empty(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()


def _require_number(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return float(value)


def _require_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _require_keys(payload: dict[str, object], *, required: set[str]) -> None:
    extra = sorted(set(payload) - required)
    if extra:
        raise ValueError(f"unsupported fields: {', '.join(extra)}")
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")


@dataclass(frozen=True)
class CanonicalBar:
    contract_id: str
    instrument_id: str
    timeframe: Timeframe
    ts: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: int

    def __post_init__(self) -> None:
        if self.volume < 0:
            raise ValueError("volume must be non-negative")
        if self.open_interest < 0:
            raise ValueError("open_interest must be non-negative")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= min(open, close)")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe.value,
            "ts": self.ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "open_interest": self.open_interest,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "CanonicalBar":
        _require_keys(
            payload,
            required={
                "contract_id",
                "instrument_id",
                "timeframe",
                "ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "open_interest",
            },
        )
        return cls(
            contract_id=_require_non_empty("contract_id", payload.get("contract_id")),
            instrument_id=_require_non_empty("instrument_id", payload.get("instrument_id")),
            timeframe=Timeframe(_require_non_empty("timeframe", payload.get("timeframe"))),
            ts=_require_non_empty("ts", payload.get("ts")),
            open=_require_number("open", payload.get("open")),
            high=_require_number("high", payload.get("high")),
            low=_require_number("low", payload.get("low")),
            close=_require_number("close", payload.get("close")),
            volume=_require_int("volume", payload.get("volume")),
            open_interest=_require_int("open_interest", payload.get("open_interest")),
        )
