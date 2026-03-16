from __future__ import annotations

import hashlib
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import DecisionPublication, PublicationState


def _message_id(signal_id: str) -> str:
    return "tg-" + hashlib.sha256(signal_id.encode("utf-8")).hexdigest()[:10]


@dataclass(frozen=True)
class TelegramOperation:
    operation: str
    signal_id: str
    at: str
    message_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "operation": self.operation,
            "signal_id": self.signal_id,
            "at": self.at,
            "message_id": self.message_id,
        }


class TelegramPublicationEngine:
    def __init__(self, *, channel: str) -> None:
        if not channel.strip():
            raise ValueError("channel must be non-empty")
        self._channel = channel
        self._publications: dict[str, DecisionPublication] = {}
        self._rendered_messages: dict[str, str] = {}
        self._operations: list[TelegramOperation] = []

    def publish(self, *, signal_id: str, rendered_message: str, published_at: str) -> tuple[DecisionPublication, bool]:
        existing = self._publications.get(signal_id)
        if existing is not None and existing.status == PublicationState.PUBLISHED:
            return existing, False

        publication = DecisionPublication(
            signal_id=signal_id,
            channel=self._channel,
            message_id=_message_id(signal_id),
            status=PublicationState.PUBLISHED,
            published_at=published_at,
        )
        self._publications[signal_id] = publication
        self._rendered_messages[signal_id] = rendered_message
        self._operations.append(
            TelegramOperation(
                operation="create",
                signal_id=signal_id,
                at=published_at,
                message_id=publication.message_id,
            )
        )
        return publication, True

    def edit(self, *, signal_id: str, rendered_message: str, edited_at: str) -> tuple[DecisionPublication, bool]:
        existing = self._publications.get(signal_id)
        if existing is None:
            raise ValueError(f"publication missing for signal: {signal_id}")
        if existing.status == PublicationState.CLOSED:
            return existing, False
        current = self._rendered_messages.get(signal_id)
        if current == rendered_message:
            return existing, False
        self._rendered_messages[signal_id] = rendered_message
        self._operations.append(
            TelegramOperation(
                operation="edit",
                signal_id=signal_id,
                at=edited_at,
                message_id=existing.message_id,
            )
        )
        return existing, True

    def close(self, *, signal_id: str, closed_at: str) -> tuple[DecisionPublication, bool]:
        existing = self._publications.get(signal_id)
        if existing is None:
            raise ValueError(f"publication missing for signal: {signal_id}")
        if existing.status == PublicationState.CLOSED:
            return existing, False
        closed = DecisionPublication(
            signal_id=existing.signal_id,
            channel=existing.channel,
            message_id=existing.message_id,
            status=PublicationState.CLOSED,
            published_at=closed_at,
        )
        self._publications[signal_id] = closed
        self._operations.append(
            TelegramOperation(
                operation="close",
                signal_id=signal_id,
                at=closed_at,
                message_id=closed.message_id,
            )
        )
        return closed, True

    def list_publications(self) -> list[DecisionPublication]:
        return sorted(
            self._publications.values(),
            key=lambda publication: (publication.signal_id, publication.published_at),
        )

    def list_operations(self) -> list[TelegramOperation]:
        return list(self._operations)
