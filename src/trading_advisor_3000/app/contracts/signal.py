from __future__ import annotations

from dataclasses import dataclass

from .enums import Mode, PublicationState, Timeframe, TradeSide



def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()



def _required_unit_float(name: str, value: object) -> float:
    if not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    normalized = float(value)
    if normalized < 0.0 or normalized > 1.0:
        raise ValueError(f"{name} must be in [0, 1]")
    return normalized


@dataclass(frozen=True)
class FeatureSnapshotRef:
    dataset_version: str
    snapshot_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "dataset_version": self.dataset_version,
            "snapshot_id": self.snapshot_id,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "FeatureSnapshotRef":
        return cls(
            dataset_version=_required_text("dataset_version", payload.get("dataset_version")),
            snapshot_id=_required_text("snapshot_id", payload.get("snapshot_id")),
        )


@dataclass(frozen=True)
class DecisionCandidate:
    signal_id: str
    contract_id: str
    timeframe: Timeframe
    model_id: str
    mode: Mode
    side: TradeSide
    confidence: float
    ts_decision: str
    feature_snapshot: FeatureSnapshotRef

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "contract_id": self.contract_id,
            "timeframe": self.timeframe.value,
            "model_id": self.model_id,
            "mode": self.mode.value,
            "side": self.side.value,
            "confidence": self.confidence,
            "ts_decision": self.ts_decision,
            "feature_snapshot": self.feature_snapshot.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "DecisionCandidate":
        snapshot_payload = payload.get("feature_snapshot")
        if not isinstance(snapshot_payload, dict):
            raise ValueError("feature_snapshot must be an object")
        return cls(
            signal_id=_required_text("signal_id", payload.get("signal_id")),
            contract_id=_required_text("contract_id", payload.get("contract_id")),
            timeframe=Timeframe(_required_text("timeframe", payload.get("timeframe"))),
            model_id=_required_text("model_id", payload.get("model_id")),
            mode=Mode(_required_text("mode", payload.get("mode"))),
            side=TradeSide(_required_text("side", payload.get("side"))),
            confidence=_required_unit_float("confidence", payload.get("confidence")),
            ts_decision=_required_text("ts_decision", payload.get("ts_decision")),
            feature_snapshot=FeatureSnapshotRef.from_dict(snapshot_payload),
        )


@dataclass(frozen=True)
class DecisionPublication:
    signal_id: str
    channel: str
    message_id: str
    status: PublicationState
    published_at: str

    def to_dict(self) -> dict[str, str]:
        return {
            "signal_id": self.signal_id,
            "channel": self.channel,
            "message_id": self.message_id,
            "status": self.status.value,
            "published_at": self.published_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "DecisionPublication":
        return cls(
            signal_id=_required_text("signal_id", payload.get("signal_id")),
            channel=_required_text("channel", payload.get("channel")),
            message_id=_required_text("message_id", payload.get("message_id")),
            status=PublicationState(_required_text("status", payload.get("status"))),
            published_at=_required_text("published_at", payload.get("published_at")),
        )
