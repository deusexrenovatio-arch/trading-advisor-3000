from __future__ import annotations

from dataclasses import dataclass

from .enums import Mode, TradeSide
from .signal import _require_keys, _required_number, _required_text, _required_unit_float


@dataclass(frozen=True)
class RuntimeSignal:
    signal_id: str
    strategy_version_id: str
    contract_id: str
    mode: Mode
    side: TradeSide
    entry_price: float
    stop_price: float
    target_price: float
    confidence: float
    state: str
    opened_at: str
    updated_at: str
    expires_at: str | None
    publication_message_id: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.state, str) or not self.state.strip():
            raise ValueError("state must be non-empty string")
        if self.side == TradeSide.FLAT:
            raise ValueError("runtime signal side cannot be flat")

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "strategy_version_id": self.strategy_version_id,
            "contract_id": self.contract_id,
            "mode": self.mode.value,
            "side": self.side.value,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "confidence": self.confidence,
            "state": self.state,
            "opened_at": self.opened_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "publication_message_id": self.publication_message_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "RuntimeSignal":
        _require_keys(
            payload,
            required={
                "signal_id",
                "strategy_version_id",
                "contract_id",
                "mode",
                "side",
                "entry_price",
                "stop_price",
                "target_price",
                "confidence",
                "state",
                "opened_at",
                "updated_at",
            },
            optional={"expires_at", "publication_message_id"},
        )
        expires_at_raw = payload.get("expires_at")
        publication_message_id_raw = payload.get("publication_message_id")
        return cls(
            signal_id=_required_text("signal_id", payload.get("signal_id")),
            strategy_version_id=_required_text("strategy_version_id", payload.get("strategy_version_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            mode=Mode(_required_text("mode", payload.get("mode"))),
            side=TradeSide(_required_text("side", payload.get("side"))),
            entry_price=_required_number("entry_price", payload.get("entry_price")),
            stop_price=_required_number("stop_price", payload.get("stop_price")),
            target_price=_required_number("target_price", payload.get("target_price")),
            confidence=_required_unit_float("confidence", payload.get("confidence")),
            state=_required_text("state", payload.get("state")),
            opened_at=_required_text("opened_at", payload.get("opened_at")),
            updated_at=_required_text("updated_at", payload.get("updated_at")),
            expires_at=None if expires_at_raw is None else _required_text("expires_at", expires_at_raw),
            publication_message_id=(
                None
                if publication_message_id_raw is None
                else _required_text("publication_message_id", publication_message_id_raw)
            ),
        )


@dataclass(frozen=True)
class SignalEvent:
    event_id: str
    signal_id: str
    event_ts: str
    event_type: str
    reason_code: str
    payload_json: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "signal_id": self.signal_id,
            "event_ts": self.event_ts,
            "event_type": self.event_type,
            "reason_code": self.reason_code,
            "payload_json": self.payload_json,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "SignalEvent":
        _require_keys(
            payload,
            required={
                "event_id",
                "signal_id",
                "event_ts",
                "event_type",
                "reason_code",
                "payload_json",
            },
        )
        payload_json = payload.get("payload_json")
        if not isinstance(payload_json, dict):
            raise ValueError("payload_json must be an object")
        return cls(
            event_id=_required_text("event_id", payload.get("event_id")),
            signal_id=_required_text("signal_id", payload.get("signal_id")),
            event_ts=_required_text("event_ts", payload.get("event_ts")),
            event_type=_required_text("event_type", payload.get("event_type")),
            reason_code=_required_text("reason_code", payload.get("reason_code")),
            payload_json={str(key): value for key, value in payload_json.items()},
        )
