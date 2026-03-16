from __future__ import annotations

from dataclasses import dataclass

from .enums import Timeframe



def _require_non_empty(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()



def _require_number(name: str, value: object) -> float:
    if not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return float(value)


@dataclass(frozen=True)
class CanonicalBar:
    contract_id: str
    timeframe: Timeframe
    ts_open: str
    ts_close: str
    open: float
    high: float
    low: float
    close: float
    volume: int

    def __post_init__(self) -> None:
        if self.volume < 0:
            raise ValueError("volume must be non-negative")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= min(open, close)")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "timeframe": self.timeframe.value,
            "ts_open": self.ts_open,
            "ts_close": self.ts_close,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "CanonicalBar":
        return cls(
            contract_id=_require_non_empty("contract_id", payload.get("contract_id")),
            timeframe=Timeframe(_require_non_empty("timeframe", payload.get("timeframe"))),
            ts_open=_require_non_empty("ts_open", payload.get("ts_open")),
            ts_close=_require_non_empty("ts_close", payload.get("ts_close")),
            open=_require_number("open", payload.get("open")),
            high=_require_number("high", payload.get("high")),
            low=_require_number("low", payload.get("low")),
            close=_require_number("close", payload.get("close")),
            volume=int(payload.get("volume", -1)),
        )
