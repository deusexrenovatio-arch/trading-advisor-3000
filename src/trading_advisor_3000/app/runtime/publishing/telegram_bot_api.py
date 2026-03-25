from __future__ import annotations

import json
from dataclasses import dataclass
from urllib import error as urllib_error
from urllib import request as urllib_request


class TelegramTransportError(RuntimeError):
    pass


@dataclass(frozen=True)
class TelegramBotTransportConfig:
    bot_token: str
    api_base_url: str = "https://api.telegram.org"
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not isinstance(self.bot_token, str) or not self.bot_token.strip():
            raise ValueError("bot_token must be non-empty")
        if not isinstance(self.api_base_url, str) or not self.api_base_url.strip():
            raise ValueError("api_base_url must be non-empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


class TelegramBotTransport:
    def __init__(self, *, config: TelegramBotTransportConfig) -> None:
        self._config = config

    def _endpoint(self, method: str) -> str:
        base = self._config.api_base_url.rstrip("/")
        token = self._config.bot_token.strip()
        return f"{base}/bot{token}/{method}"

    def _call(self, *, method: str, payload: dict[str, object]) -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib_request.Request(
            url=self._endpoint(method),
            method="POST",
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        try:
            with urllib_request.urlopen(request, timeout=self._config.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib_error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise TelegramTransportError(f"telegram http error {getattr(exc, 'code', 'unknown')}: {raw}") from exc
        except urllib_error.URLError as exc:
            raise TelegramTransportError(f"telegram transport unavailable: {exc.reason}") from exc

        try:
            parsed = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError as exc:
            raise TelegramTransportError("telegram returned non-json response") from exc
        if not isinstance(parsed, dict):
            raise TelegramTransportError("telegram response must be an object")
        if not bool(parsed.get("ok", False)):
            raise TelegramTransportError(str(parsed.get("description", "telegram api returned not ok")))
        result = parsed.get("result")
        if not isinstance(result, dict):
            raise TelegramTransportError("telegram response missing result object")
        return result

    def send_message(self, *, channel: str, text: str) -> str:
        result = self._call(method="sendMessage", payload={"chat_id": channel, "text": text})
        message_id = result.get("message_id")
        if isinstance(message_id, bool) or not isinstance(message_id, (int, str)):
            raise TelegramTransportError("telegram sendMessage result missing message_id")
        return str(message_id)

    def edit_message(self, *, channel: str, message_id: str, text: str) -> str:
        payload_message_id: int | str
        try:
            payload_message_id = int(message_id)
        except ValueError:
            payload_message_id = message_id
        result = self._call(
            method="editMessageText",
            payload={"chat_id": channel, "message_id": payload_message_id, "text": text},
        )
        result_message_id = result.get("message_id", payload_message_id)
        if isinstance(result_message_id, bool) or not isinstance(result_message_id, (int, str)):
            raise TelegramTransportError("telegram editMessageText result missing message_id")
        return str(result_message_id)
