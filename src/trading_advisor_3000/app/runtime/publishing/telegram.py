from __future__ import annotations

import hashlib
from dataclasses import dataclass

from trading_advisor_3000.app.contracts import (
    DecisionPublication,
    PublicationState,
    PublicationType,
)


def _message_id(signal_id: str) -> str:
    return "tg-" + hashlib.sha256(signal_id.encode("utf-8")).hexdigest()[:10]


def _publication_id(*, signal_id: str, publication_type: PublicationType, at: str) -> str:
    seed = f"{signal_id}|{publication_type.value}|{at}"
    return "PUB-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()


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
        self._history: list[DecisionPublication] = []
        self._rendered_messages: dict[str, str] = {}
        self._operations: list[TelegramOperation] = []

    def _emit_publication(
        self,
        *,
        signal_id: str,
        message_id: str,
        publication_type: PublicationType,
        status: PublicationState,
        at: str,
    ) -> DecisionPublication:
        publication = DecisionPublication(
            publication_id=_publication_id(
                signal_id=signal_id,
                publication_type=publication_type,
                at=f"{at}|{len(self._history)}",
            ),
            signal_id=signal_id,
            channel=self._channel,
            message_id=message_id,
            publication_type=publication_type,
            status=status,
            published_at=at,
        )
        self._publications[signal_id] = publication
        self._history.append(publication)
        return publication

    def publish(self, *, signal_id: str, rendered_message: str, published_at: str) -> tuple[DecisionPublication, bool]:
        existing = self._publications.get(signal_id)
        if existing is not None and existing.status == PublicationState.PUBLISHED:
            return existing, False

        publication = self._emit_publication(
            signal_id=signal_id,
            message_id=_message_id(signal_id),
            publication_type=PublicationType.CREATE,
            status=PublicationState.PUBLISHED,
            at=published_at,
        )
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
        if existing.status in {PublicationState.CLOSED, PublicationState.CANCELED}:
            return existing, False
        current = self._rendered_messages.get(signal_id)
        if current == rendered_message:
            return existing, False
        self._rendered_messages[signal_id] = rendered_message
        publication = self._emit_publication(
            signal_id=signal_id,
            message_id=existing.message_id,
            publication_type=PublicationType.EDIT,
            status=PublicationState.PUBLISHED,
            at=edited_at,
        )
        self._operations.append(
            TelegramOperation(
                operation="edit",
                signal_id=signal_id,
                at=edited_at,
                message_id=existing.message_id,
            )
        )
        return publication, True

    def close(self, *, signal_id: str, closed_at: str) -> tuple[DecisionPublication, bool]:
        existing = self._publications.get(signal_id)
        if existing is None:
            raise ValueError(f"publication missing for signal: {signal_id}")
        if existing.status == PublicationState.CLOSED:
            return existing, False
        closed = self._emit_publication(
            signal_id=existing.signal_id,
            message_id=existing.message_id,
            publication_type=PublicationType.CLOSE,
            status=PublicationState.CLOSED,
            at=closed_at,
        )
        self._operations.append(
            TelegramOperation(
                operation="close",
                signal_id=signal_id,
                at=closed_at,
                message_id=closed.message_id,
            )
        )
        return closed, True

    def cancel(self, *, signal_id: str, canceled_at: str) -> tuple[DecisionPublication, bool]:
        existing = self._publications.get(signal_id)
        if existing is None:
            raise ValueError(f"publication missing for signal: {signal_id}")
        if existing.status == PublicationState.CANCELED:
            return existing, False
        canceled = self._emit_publication(
            signal_id=existing.signal_id,
            message_id=existing.message_id,
            publication_type=PublicationType.CANCEL,
            status=PublicationState.CANCELED,
            at=canceled_at,
        )
        self._operations.append(
            TelegramOperation(
                operation="cancel",
                signal_id=signal_id,
                at=canceled_at,
                message_id=canceled.message_id,
            )
        )
        return canceled, True

    def list_publications(self) -> list[DecisionPublication]:
        return sorted(
            self._publications.values(),
            key=lambda publication: (publication.signal_id, publication.published_at),
        )

    def list_publication_events(self) -> list[DecisionPublication]:
        return list(self._history)

    def list_operations(self) -> list[TelegramOperation]:
        return list(self._operations)
