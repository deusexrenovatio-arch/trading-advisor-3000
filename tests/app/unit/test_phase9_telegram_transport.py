from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator

from trading_advisor_3000.app.runtime.publishing import (
    TelegramBotTransport,
    TelegramBotTransportConfig,
    TelegramPublicationEngine,
)


@contextmanager
def _telegram_server() -> Iterator[tuple[str, list[dict[str, object]]]]:
    request_log: list[dict[str, object]] = []
    next_message_id = 7000

    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            nonlocal next_message_id
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length).decode("utf-8")
            payload = json.loads(raw) if raw else {}
            request_log.append({"path": self.path, "payload": payload})
            if self.path.endswith("/sendMessage"):
                next_message_id += 1
                self._json(200, {"ok": True, "result": {"message_id": next_message_id}})
                return
            if self.path.endswith("/editMessageText"):
                self._json(200, {"ok": True, "result": {"message_id": payload.get("message_id")}})
                return
            self._json(404, {"ok": False, "description": "not found"})

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield base_url, request_log
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_telegram_bot_transport_sends_and_edits_messages_over_http() -> None:
    with _telegram_server() as (base_url, request_log):
        transport = TelegramBotTransport(
            config=TelegramBotTransportConfig(
                bot_token="telegram-bot-token-001",
                api_base_url=base_url,
            )
        )

        message_id = transport.send_message(channel="@ta3000_shadow", text="create")
        edited_message_id = transport.edit_message(
            channel="@ta3000_shadow",
            message_id=message_id,
            text="edit",
        )

    assert message_id == "7001"
    assert edited_message_id == "7001"
    assert [item["path"] for item in request_log] == [
        "/bottelegram-bot-token-001/sendMessage",
        "/bottelegram-bot-token-001/editMessageText",
    ]


def test_telegram_publication_engine_uses_transport_for_create_edit_close_and_cancel() -> None:
    with _telegram_server() as (base_url, request_log):
        transport = TelegramBotTransport(
            config=TelegramBotTransportConfig(
                bot_token="telegram-bot-token-001",
                api_base_url=base_url,
            )
        )
        publisher = TelegramPublicationEngine(channel="@ta3000_shadow", transport=transport)

        created, _ = publisher.publish(
            signal_id="SIG-1",
            rendered_message="create-1",
            published_at="2026-03-22T07:00:00Z",
        )
        publisher.edit(
            signal_id="SIG-1",
            rendered_message="edit-1",
            edited_at="2026-03-22T07:01:00Z",
        )
        publisher.close(
            signal_id="SIG-1",
            closed_at="2026-03-22T07:02:00Z",
            rendered_message="close-1",
        )

        second, _ = publisher.publish(
            signal_id="SIG-2",
            rendered_message="create-2",
            published_at="2026-03-22T07:03:00Z",
        )
        publisher.cancel(
            signal_id="SIG-2",
            canceled_at="2026-03-22T07:04:00Z",
            rendered_message="cancel-2",
        )

    assert created.message_id == "7001"
    assert second.message_id == "7002"
    assert [item["path"] for item in request_log] == [
        "/bottelegram-bot-token-001/sendMessage",
        "/bottelegram-bot-token-001/editMessageText",
        "/bottelegram-bot-token-001/editMessageText",
        "/bottelegram-bot-token-001/sendMessage",
        "/bottelegram-bot-token-001/editMessageText",
    ]
