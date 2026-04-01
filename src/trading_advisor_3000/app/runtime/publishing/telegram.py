from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

from trading_advisor_3000.app.contracts import (
    DecisionPublication,
    PublicationState,
    PublicationType,
)


TelegramApiCall = Callable[[str, dict[str, str] | None], dict[str, Any]]


def _message_id(signal_id: str) -> str:
    return "tg-" + hashlib.sha256(signal_id.encode("utf-8")).hexdigest()[:10]


def _publication_id(*, signal_id: str, publication_type: PublicationType, at: str) -> str:
    seed = f"{signal_id}|{publication_type.value}|{at}"
    return "PUB-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()


def _telegram_bot_api_call(
    token: str,
    method: str,
    *,
    params: dict[str, str] | None = None,
) -> dict[str, Any]:
    query = urllib.parse.urlencode(params or {})
    url = f"https://api.telegram.org/bot{token}/{method}"
    if query:
        url = f"{url}?{query}"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return {
                "http_status": getattr(response, "status", None),
                "ok": bool(payload.get("ok")),
                "error_code": payload.get("error_code"),
                "description": payload.get("description", ""),
                "result": payload.get("result"),
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"description": body}
        return {
            "http_status": exc.code,
            "ok": bool(payload.get("ok")),
            "error_code": payload.get("error_code"),
            "description": payload.get("description", ""),
            "result": payload.get("result"),
        }
    except Exception as exc:  # pragma: no cover - defensive path
        return {
            "http_status": None,
            "ok": False,
            "error_code": None,
            "description": f"{type(exc).__name__}: {exc}",
            "result": None,
        }


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


@dataclass(frozen=True)
class TelegramApiReceipt:
    operation: str
    method: str
    signal_id: str
    at: str
    http_status: int | None
    ok: bool
    error_code: int | None
    description: str
    result_message_id: str | None
    result_chat_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "operation": self.operation,
            "method": self.method,
            "signal_id": self.signal_id,
            "at": self.at,
            "http_status": self.http_status,
            "ok": self.ok,
            "error_code": self.error_code,
            "description": self.description,
            "result_message_id": self.result_message_id,
            "result_chat_id": self.result_chat_id,
        }


class TelegramApiError(RuntimeError):
    def __init__(self, *, receipt: TelegramApiReceipt) -> None:
        super().__init__(
            "Telegram API call failed: "
            f"{receipt.method} for signal {receipt.signal_id} "
            f"(status={receipt.http_status}, error={receipt.error_code}, description={receipt.description})"
        )
        self.receipt = receipt


class TelegramPublicationEngine:
    def __init__(
        self,
        *,
        channel: str,
        bot_token: str | None = None,
        api_call: TelegramApiCall | None = None,
    ) -> None:
        if not channel.strip():
            raise ValueError("channel must be non-empty")
        self._channel = channel
        cleaned_token = (bot_token or "").strip()
        self._bot_token = cleaned_token if cleaned_token else None
        self._api_call = api_call
        self._transport = "telegram-bot-api" if self._bot_token else "memory"
        self._publications: dict[str, DecisionPublication] = {}
        self._history: list[DecisionPublication] = []
        self._rendered_messages: dict[str, str] = {}
        self._operations: list[TelegramOperation] = []
        self._api_receipts: list[TelegramApiReceipt] = []

    @property
    def transport(self) -> str:
        return self._transport

    def _call_telegram(
        self,
        *,
        method: str,
        operation: str,
        signal_id: str,
        at: str,
        params: dict[str, str],
    ) -> dict[str, Any]:
        if self._bot_token is None:
            raise RuntimeError("Telegram API transport requires bot_token")
        payload = (
            self._api_call(method, params)
            if self._api_call is not None
            else _telegram_bot_api_call(self._bot_token, method, params=params)
        )
        result = payload.get("result")
        result_payload = result if isinstance(result, dict) else {}
        message_id = result_payload.get("message_id")
        chat_payload = result_payload.get("chat")
        chat_id = None
        if isinstance(chat_payload, dict):
            candidate = chat_payload.get("id")
            if candidate is not None:
                chat_id = str(candidate)
        receipt = TelegramApiReceipt(
            operation=operation,
            method=method,
            signal_id=signal_id,
            at=at,
            http_status=payload.get("http_status"),
            ok=bool(payload.get("ok")),
            error_code=payload.get("error_code"),
            description=str(payload.get("description", "")),
            result_message_id=str(message_id) if message_id is not None else None,
            result_chat_id=chat_id,
        )
        self._api_receipts.append(receipt)
        if not receipt.ok:
            raise TelegramApiError(receipt=receipt)
        return payload

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

        message_id = _message_id(signal_id)
        if self._transport == "telegram-bot-api":
            payload = self._call_telegram(
                method="sendMessage",
                operation="create",
                signal_id=signal_id,
                at=published_at,
                params={
                    "chat_id": self._channel,
                    "text": rendered_message,
                    "disable_web_page_preview": "true",
                },
            )
            result = payload.get("result")
            if not isinstance(result, dict) or result.get("message_id") is None:
                raise RuntimeError("Telegram sendMessage response missing result.message_id")
            message_id = str(result["message_id"])

        publication = self._emit_publication(
            signal_id=signal_id,
            message_id=message_id,
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
        if self._transport == "telegram-bot-api":
            self._call_telegram(
                method="editMessageText",
                operation="edit",
                signal_id=signal_id,
                at=edited_at,
                params={
                    "chat_id": self._channel,
                    "message_id": existing.message_id,
                    "text": rendered_message,
                    "disable_web_page_preview": "true",
                },
            )
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
        if self._transport == "telegram-bot-api":
            current = self._rendered_messages.get(signal_id, "")
            close_marker = f"[CLOSED at {closed_at}]"
            close_message = current.strip()
            if close_message:
                close_message = f"{close_message}\n\n{close_marker}"
            else:
                close_message = close_marker
            self._call_telegram(
                method="editMessageText",
                operation="close",
                signal_id=signal_id,
                at=closed_at,
                params={
                    "chat_id": self._channel,
                    "message_id": existing.message_id,
                    "text": close_message,
                    "disable_web_page_preview": "true",
                },
            )
            self._rendered_messages[signal_id] = close_message
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
        if self._transport == "telegram-bot-api":
            self._call_telegram(
                method="deleteMessage",
                operation="cancel",
                signal_id=signal_id,
                at=canceled_at,
                params={
                    "chat_id": self._channel,
                    "message_id": existing.message_id,
                },
            )
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

    def list_api_receipts(self) -> list[TelegramApiReceipt]:
        return list(self._api_receipts)

    def restore(self, *, publications: list[DecisionPublication]) -> None:
        self._publications = {}
        self._history = []
        self._rendered_messages = {}
        self._operations = []
        self._api_receipts = []
        for publication in sorted(publications, key=lambda item: (item.published_at, item.publication_id)):
            self._publications[publication.signal_id] = publication
            self._history.append(publication)
