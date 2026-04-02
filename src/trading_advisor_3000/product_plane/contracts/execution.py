from __future__ import annotations

from dataclasses import dataclass

from .enums import Mode


def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()


def _required_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _required_number(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    return float(value)


def _required_non_negative_number(name: str, value: object) -> float:
    normalized = _required_number(name, value)
    if normalized < 0:
        raise ValueError(f"{name} must be non-negative")
    return normalized


def _required_dict(name: str, value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    normalized: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{name} keys must be non-empty strings")
        normalized[key] = item
    return normalized


def _require_keys(payload: dict[str, object], *, required: set[str], optional: set[str] | None = None) -> None:
    optional = optional or set()
    allowed = required | optional
    extra = sorted(set(payload) - allowed)
    if extra:
        raise ValueError(f"unsupported fields: {', '.join(extra)}")
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    signal_id: str
    mode: Mode
    broker_adapter: str
    action: str
    contract_id: str
    qty: int
    price: float
    stop_price: float
    created_at: str

    def __post_init__(self) -> None:
        if not isinstance(self.broker_adapter, str) or not self.broker_adapter.strip():
            raise ValueError("broker_adapter must be non-empty string")
        if self.action not in {"buy", "sell"}:
            raise ValueError("action must be buy or sell")
        if self.qty <= 0:
            raise ValueError("qty must be positive")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.stop_price <= 0:
            raise ValueError("stop_price must be positive")

    def to_dict(self) -> dict[str, object]:
        return {
            "intent_id": self.intent_id,
            "signal_id": self.signal_id,
            "mode": self.mode.value,
            "broker_adapter": self.broker_adapter,
            "action": self.action,
            "contract_id": self.contract_id,
            "qty": self.qty,
            "price": self.price,
            "stop_price": self.stop_price,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "OrderIntent":
        _require_keys(
            payload,
            required={
                "intent_id",
                "signal_id",
                "mode",
                "broker_adapter",
                "action",
                "contract_id",
                "qty",
                "price",
                "stop_price",
                "created_at",
            },
        )
        return cls(
            intent_id=_required_text("intent_id", payload.get("intent_id")),
            signal_id=_required_text("signal_id", payload.get("signal_id")),
            mode=Mode(_required_text("mode", payload.get("mode"))),
            broker_adapter=_required_text("broker_adapter", payload.get("broker_adapter")),
            action=_required_text("action", payload.get("action")).lower(),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            qty=_required_int("qty", payload.get("qty")),
            price=_required_number("price", payload.get("price")),
            stop_price=_required_number("stop_price", payload.get("stop_price")),
            created_at=_required_text("created_at", payload.get("created_at")),
        )


@dataclass(frozen=True)
class PositionSnapshot:
    position_key: str
    account_id: str
    contract_id: str
    mode: Mode
    qty: int
    avg_price: float
    as_of_ts: str

    def __post_init__(self) -> None:
        if not isinstance(self.position_key, str) or not self.position_key.strip():
            raise ValueError("position_key must be non-empty string")
        if self.avg_price < 0:
            raise ValueError("avg_price must be non-negative")

    def to_dict(self) -> dict[str, object]:
        return {
            "position_key": self.position_key,
            "account_id": self.account_id,
            "contract_id": self.contract_id,
            "mode": self.mode.value,
            "qty": self.qty,
            "avg_price": self.avg_price,
            "as_of_ts": self.as_of_ts,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "PositionSnapshot":
        _require_keys(
            payload,
            required={"position_key", "account_id", "contract_id", "mode", "qty", "avg_price", "as_of_ts"},
        )

        return cls(
            position_key=_required_text("position_key", payload.get("position_key")),
            account_id=_required_text("account_id", payload.get("account_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            mode=Mode(_required_text("mode", payload.get("mode"))),
            qty=_required_int("qty", payload.get("qty")),
            avg_price=_required_number("avg_price", payload.get("avg_price")),
            as_of_ts=_required_text("as_of_ts", payload.get("as_of_ts")),
        )


BROKER_ORDER_STATES = {
    "new",
    "submitted",
    "replaced",
    "partially_filled",
    "filled",
    "canceled",
    "rejected",
}


@dataclass(frozen=True)
class BrokerOrder:
    broker_order_id: str
    intent_id: str
    external_order_id: str | None
    broker: str
    state: str
    submitted_at: str
    updated_at: str

    def __post_init__(self) -> None:
        if self.state not in BROKER_ORDER_STATES:
            raise ValueError(f"unsupported broker order state: {self.state}")

    def to_dict(self) -> dict[str, object]:
        return {
            "broker_order_id": self.broker_order_id,
            "intent_id": self.intent_id,
            "external_order_id": self.external_order_id,
            "broker": self.broker,
            "state": self.state,
            "submitted_at": self.submitted_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BrokerOrder":
        _require_keys(
            payload,
            required={"broker_order_id", "intent_id", "broker", "state", "submitted_at", "updated_at"},
            optional={"external_order_id"},
        )
        external_order_id_raw = payload.get("external_order_id")
        if external_order_id_raw is None:
            external_order_id = None
        else:
            external_order_id = _required_text("external_order_id", external_order_id_raw)

        return cls(
            broker_order_id=_required_text("broker_order_id", payload.get("broker_order_id")),
            intent_id=_required_text("intent_id", payload.get("intent_id")),
            external_order_id=external_order_id,
            broker=_required_text("broker", payload.get("broker")),
            state=_required_text("state", payload.get("state")),
            submitted_at=_required_text("submitted_at", payload.get("submitted_at")),
            updated_at=_required_text("updated_at", payload.get("updated_at")),
        )


@dataclass(frozen=True)
class BrokerFill:
    fill_id: str
    broker_order_id: str
    fill_ts: str
    qty: int
    price: float
    fee: float
    external_trade_id: str | None

    def __post_init__(self) -> None:
        if self.qty <= 0:
            raise ValueError("qty must be positive")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.fee < 0:
            raise ValueError("fee must be non-negative")

    def to_dict(self) -> dict[str, object]:
        return {
            "fill_id": self.fill_id,
            "broker_order_id": self.broker_order_id,
            "fill_ts": self.fill_ts,
            "qty": self.qty,
            "price": self.price,
            "fee": self.fee,
            "external_trade_id": self.external_trade_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BrokerFill":
        _require_keys(
            payload,
            required={"fill_id", "broker_order_id", "fill_ts", "qty", "price", "fee"},
            optional={"external_trade_id"},
        )
        external_trade_id_raw = payload.get("external_trade_id")
        if external_trade_id_raw is None:
            external_trade_id = None
        else:
            external_trade_id = _required_text("external_trade_id", external_trade_id_raw)

        return cls(
            fill_id=_required_text("fill_id", payload.get("fill_id")),
            broker_order_id=_required_text("broker_order_id", payload.get("broker_order_id")),
            fill_ts=_required_text("fill_ts", payload.get("fill_ts")),
            qty=_required_int("qty", payload.get("qty")),
            price=_required_number("price", payload.get("price")),
            fee=_required_non_negative_number("fee", payload.get("fee")),
            external_trade_id=external_trade_id,
        )


@dataclass(frozen=True)
class RiskSnapshot:
    account_id: str
    as_of_ts: str
    available_cash: float
    margin_required: float
    margin_free: float
    broker_limits_json: dict[str, object]

    def __post_init__(self) -> None:
        if self.available_cash < 0:
            raise ValueError("available_cash must be non-negative")
        if self.margin_required < 0:
            raise ValueError("margin_required must be non-negative")
        if self.margin_free < 0:
            raise ValueError("margin_free must be non-negative")

    def to_dict(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "as_of_ts": self.as_of_ts,
            "available_cash": self.available_cash,
            "margin_required": self.margin_required,
            "margin_free": self.margin_free,
            "broker_limits_json": self.broker_limits_json,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "RiskSnapshot":
        _require_keys(
            payload,
            required={
                "account_id",
                "as_of_ts",
                "available_cash",
                "margin_required",
                "margin_free",
                "broker_limits_json",
            },
        )
        return cls(
            account_id=_required_text("account_id", payload.get("account_id")),
            as_of_ts=_required_text("as_of_ts", payload.get("as_of_ts")),
            available_cash=_required_non_negative_number("available_cash", payload.get("available_cash")),
            margin_required=_required_non_negative_number("margin_required", payload.get("margin_required")),
            margin_free=_required_non_negative_number("margin_free", payload.get("margin_free")),
            broker_limits_json=_required_dict("broker_limits_json", payload.get("broker_limits_json")),
        )


@dataclass(frozen=True)
class BrokerEvent:
    event_id: str
    broker_adapter: str
    external_object_id: str
    event_type: str
    event_ts: str
    payload_json: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "broker_adapter": self.broker_adapter,
            "external_object_id": self.external_object_id,
            "event_type": self.event_type,
            "event_ts": self.event_ts,
            "payload_json": self.payload_json,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BrokerEvent":
        _require_keys(
            payload,
            required={
                "event_id",
                "broker_adapter",
                "external_object_id",
                "event_type",
                "event_ts",
                "payload_json",
            },
        )
        return cls(
            event_id=_required_text("event_id", payload.get("event_id")),
            broker_adapter=_required_text("broker_adapter", payload.get("broker_adapter")),
            external_object_id=_required_text("external_object_id", payload.get("external_object_id")),
            event_type=_required_text("event_type", payload.get("event_type")),
            event_ts=_required_text("event_ts", payload.get("event_ts")),
            payload_json=_required_dict("payload_json", payload.get("payload_json")),
        )
