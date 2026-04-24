from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import build_publication_lifecycle_evidence as module  # noqa: E402


def test_build_evidence_marks_live_real_ready_when_bindings_and_probes_are_green(
    monkeypatch,
) -> None:
    monkeypatch.setenv("TA3000_TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TA3000_TELEGRAM_CHANNEL", "@real_channel")

    def _fake_probe(token: str, method: str, *, params=None):  # type: ignore[no-untyped-def]
        if method == "getMe":
            return {"http_status": 200, "ok": True, "error_code": None, "description": ""}
        if method == "getChat":
            assert params == {"chat_id": "@real_channel"}
            return {"http_status": 200, "ok": True, "error_code": None, "description": ""}
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(module, "_telegram_api_call", _fake_probe)
    monkeypatch.setattr(
        module,
        "_build_lifecycle",
        lambda channel, *, at, credential_value: {  # type: ignore[no-untyped-call]
            "message_lifecycle": {},
            "operations": [{"operation": "create"}],
            "publication_events": [{"publication_type": "create"}],
            "api_receipts": [{"method": "sendMessage", "ok": True}],
            "real_lifecycle": {
                "ok": True,
                "transport": "telegram-bot-api",
                "failure": None,
            },
        },
    )

    artifact = module.build_evidence(
        phase="F1-B",
        attempt="run/attempt-01",
        publication_channel="",
        credential_env_names=["TA3000_TELEGRAM_BOT_TOKEN"],
        channel_env_names=["TA3000_TELEGRAM_CHANNEL"],
    )

    assert artifact["binding_probe"]["resolved_credential_env"] == "TA3000_TELEGRAM_BOT_TOKEN"
    assert artifact["binding_probe"]["resolved_channel_env"] == "TA3000_TELEGRAM_CHANNEL"
    assert artifact["binding_probe"]["credentials_present"] is True
    assert artifact["binding_probe"]["channel_configured"] is True
    assert artifact["live_probe"]["live_real_ready"] is True


def test_build_evidence_is_unready_without_channel_binding(monkeypatch) -> None:
    monkeypatch.setenv("TA3000_TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.delenv("TA3000_TELEGRAM_CHANNEL", raising=False)
    monkeypatch.delenv("TA3000_TELEGRAM_CHAT_ID", raising=False)

    def _fake_probe(token: str, method: str, *, params=None):  # type: ignore[no-untyped-def]
        if method == "getMe":
            return {"http_status": 200, "ok": True, "error_code": None, "description": ""}
        if method == "getChat":
            raise AssertionError("getChat must not run without an explicit channel binding")
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(module, "_telegram_api_call", _fake_probe)

    artifact = module.build_evidence(
        phase="F1-B",
        attempt="run/attempt-01",
        publication_channel="",
        credential_env_names=["TA3000_TELEGRAM_BOT_TOKEN"],
        channel_env_names=["TA3000_TELEGRAM_CHANNEL", "TA3000_TELEGRAM_CHAT_ID"],
    )

    assert artifact["publication_channel"] == ""
    assert artifact["binding_probe"]["resolved_channel_env"] is None
    assert artifact["binding_probe"]["fallback_channel_used"] is False
    assert artifact["binding_probe"]["channel_configured"] is False
    assert artifact["live_probe"]["get_chat"]["description"] == "channel binding missing"
    assert artifact["real_lifecycle"]["failure"]["type"] == "channel_missing"
    assert artifact["live_probe"]["live_real_ready"] is False
