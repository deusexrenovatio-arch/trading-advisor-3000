from __future__ import annotations

from dataclasses import dataclass

from .enums import Mode, TradeSide



def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    signal_id: str
    contract_id: str
    mode: Mode
    side: TradeSide
    quantity: int
    limit_price: float | None
    created_at: str

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.limit_price is not None and self.limit_price <= 0:
            raise ValueError("limit_price must be positive when set")

    def to_dict(self) -> dict[str, object]:
        return {
            "intent_id": self.intent_id,
            "signal_id": self.signal_id,
            "contract_id": self.contract_id,
            "mode": self.mode.value,
            "side": self.side.value,
            "quantity": self.quantity,
            "limit_price": self.limit_price,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "OrderIntent":
        limit_price_raw = payload.get("limit_price")
        if limit_price_raw is None:
            limit_price = None
        elif isinstance(limit_price_raw, (int, float)):
            limit_price = float(limit_price_raw)
        else:
            raise ValueError("limit_price must be a number or null")

        return cls(
            intent_id=_required_text("intent_id", payload.get("intent_id")),
            signal_id=_required_text("signal_id", payload.get("signal_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            mode=Mode(_required_text("mode", payload.get("mode"))),
            side=TradeSide(_required_text("side", payload.get("side"))),
            quantity=int(payload.get("quantity", 0)),
            limit_price=limit_price,
            created_at=_required_text("created_at", payload.get("created_at")),
        )


@dataclass(frozen=True)
class PositionSnapshot:
    account_id: str
    contract_id: str
    mode: Mode
    quantity: int
    avg_price: float
    broker_ts: str

    def __post_init__(self) -> None:
        if self.avg_price < 0:
            raise ValueError("avg_price must be non-negative")

    def to_dict(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "contract_id": self.contract_id,
            "mode": self.mode.value,
            "quantity": self.quantity,
            "avg_price": self.avg_price,
            "broker_ts": self.broker_ts,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "PositionSnapshot":
        avg_price_raw = payload.get("avg_price")
        if not isinstance(avg_price_raw, (int, float)):
            raise ValueError("avg_price must be a number")

        return cls(
            account_id=_required_text("account_id", payload.get("account_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            mode=Mode(_required_text("mode", payload.get("mode"))),
            quantity=int(payload.get("quantity", 0)),
            avg_price=float(avg_price_raw),
            broker_ts=_required_text("broker_ts", payload.get("broker_ts")),
        )
