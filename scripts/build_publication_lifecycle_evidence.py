from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.runtime.publishing import TelegramPublicationEngine


DEFAULT_CREDENTIAL_ENV_NAMES = (
    "TA3000_TELEGRAM_BOT_TOKEN",
    "TA3000_PUBLICATION_CREDENTIAL",
    "TA3000_PUBLICATION_BOT_TOKEN",
)
DEFAULT_CHANNEL_ENV_NAMES = (
    "TA3000_TELEGRAM_CHANNEL",
    "TA3000_TELEGRAM_CHAT_ID",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_first_non_empty(env_names: list[str]) -> tuple[str | None, str | None]:
    for name in env_names:
        raw = os.getenv(name)
        if raw and raw.strip():
            return name, raw.strip()
    return None, None


def _telegram_api_call(token: str, method: str, *, params: dict[str, str] | None = None) -> dict[str, Any]:
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
        }
    except Exception as exc:  # pragma: no cover - defensive path
        return {
            "http_status": None,
            "ok": False,
            "error_code": None,
            "description": f"{type(exc).__name__}: {exc}",
        }


def _build_lifecycle(channel: str, *, at: str, credential_value: str | None) -> dict[str, Any]:
    message_lifecycle: dict[str, Any] = {
        "create_idempotent": {
            "first_created": False,
            "second_created": False,
            "message_id_stable": False,
        },
        "edit": {
            "edited": False,
            "message_id": "",
        },
        "close": {
            "closed": False,
            "message_id": "",
        },
        "cancel": {
            "second_signal_created": False,
            "canceled": False,
            "message_id": "",
        },
    }
    if not credential_value:
        return {
            "message_lifecycle": message_lifecycle,
            "operations": [],
            "publication_events": [],
            "api_receipts": [],
            "real_lifecycle": {
                "ok": False,
                "transport": "memory",
                "failure": {
                    "type": "credential_missing",
                    "message": "real publication lifecycle skipped because credential binding is missing",
                },
            },
        }
    if not channel.strip():
        return {
            "message_lifecycle": message_lifecycle,
            "operations": [],
            "publication_events": [],
            "api_receipts": [],
            "real_lifecycle": {
                "ok": False,
                "transport": "none",
                "failure": {
                    "type": "channel_missing",
                    "message": "real publication lifecycle skipped because publication channel binding is missing",
                },
            },
        }

    engine = TelegramPublicationEngine(channel=channel, bot_token=credential_value)
    lifecycle_failure: dict[str, str] | None = None
    try:
        first, first_created = engine.publish(
            signal_id="SIG-F1B-PHASE02-001",
            rendered_message="F1-B publication contour probe #1",
            published_at=at,
        )
        second, second_created = engine.publish(
            signal_id="SIG-F1B-PHASE02-001",
            rendered_message="F1-B publication contour probe #1 duplicate",
            published_at=at,
        )
        edited, edited_changed = engine.edit(
            signal_id="SIG-F1B-PHASE02-001",
            rendered_message="F1-B publication contour probe #1 edited",
            edited_at=at,
        )
        closed, closed_changed = engine.close(
            signal_id="SIG-F1B-PHASE02-001",
            closed_at=at,
        )
        _, second_signal_created = engine.publish(
            signal_id="SIG-F1B-PHASE02-002",
            rendered_message="F1-B publication contour probe #2",
            published_at=at,
        )
        canceled, canceled_changed = engine.cancel(
            signal_id="SIG-F1B-PHASE02-002",
            canceled_at=at,
        )
        message_lifecycle = {
            "create_idempotent": {
                "first_created": first_created,
                "second_created": second_created,
                "message_id_stable": first.message_id == second.message_id,
            },
            "edit": {
                "edited": edited_changed,
                "message_id": edited.message_id,
            },
            "close": {
                "closed": closed_changed,
                "message_id": closed.message_id,
            },
            "cancel": {
                "second_signal_created": second_signal_created,
                "canceled": canceled_changed,
                "message_id": canceled.message_id,
            },
        }
    except Exception as exc:  # pragma: no cover - network/error path
        lifecycle_failure = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

    operations = [operation.to_dict() for operation in engine.list_operations()]
    publication_events = [publication.to_dict() for publication in engine.list_publication_events()]
    api_receipts = [receipt.to_dict() for receipt in engine.list_api_receipts()]
    real_lifecycle_ok = (
        lifecycle_failure is None
        and message_lifecycle["create_idempotent"]["first_created"] is True
        and message_lifecycle["create_idempotent"]["message_id_stable"] is True
        and message_lifecycle["edit"]["edited"] is True
        and message_lifecycle["close"]["closed"] is True
        and message_lifecycle["cancel"]["canceled"] is True
        and bool(api_receipts)
    )

    return {
        "message_lifecycle": message_lifecycle,
        "operations": operations,
        "publication_events": publication_events,
        "api_receipts": api_receipts,
        "real_lifecycle": {
            "ok": real_lifecycle_ok,
            "transport": engine.transport,
            "failure": lifecycle_failure,
        },
    }


def build_evidence(
    *,
    phase: str,
    attempt: str,
    publication_channel: str,
    credential_env_names: list[str],
    channel_env_names: list[str],
) -> dict[str, Any]:
    generated_at = _utc_now()
    resolved_credential_env, credential_value = _resolve_first_non_empty(credential_env_names)

    if publication_channel.strip():
        resolved_channel_env = "ARG:publication_channel"
        channel = publication_channel.strip()
    else:
        resolved_channel_env, channel = _resolve_first_non_empty(channel_env_names)
        if channel is None:
            resolved_channel_env = None
            channel = ""

    probe_get_me: dict[str, Any] = {
        "http_status": None,
        "ok": False,
        "error_code": None,
        "description": "credential binding missing",
    }
    probe_get_chat: dict[str, Any] = {
        "http_status": None,
        "ok": False,
        "error_code": None,
        "description": "channel binding missing",
    }
    if credential_value:
        probe_get_me = _telegram_api_call(credential_value, "getMe")
        if channel:
            probe_get_chat = _telegram_api_call(credential_value, "getChat", params={"chat_id": channel})
        else:
            probe_get_chat = {
                "http_status": None,
                "ok": False,
                "error_code": None,
                "description": "channel binding missing",
            }

    lifecycle = _build_lifecycle(channel, at=generated_at, credential_value=credential_value)
    credentials_present = bool(resolved_credential_env)
    channel_configured = bool(resolved_channel_env)
    replayable_publication_evidence = bool(lifecycle["api_receipts"]) and lifecycle["real_lifecycle"]["ok"] is True
    live_real_ready = (
        credentials_present
        and channel_configured
        and probe_get_me.get("ok") is True
        and probe_get_chat.get("ok") is True
        and replayable_publication_evidence
    )

    notes = [
        "Artifact is attempt-scoped and captures Telegram Bot API publication receipts.",
    ]
    if lifecycle["real_lifecycle"]["ok"] is False:
        notes.append("Real Telegram lifecycle probe did not complete create/edit/close/cancel successfully.")
    if not live_real_ready:
        notes.append(
            "Live-real contour is not closed: provide a configured reachable chat/channel and rerun this evidence.",
        )

    return {
        "generated_at": generated_at,
        "phase": phase,
        "attempt": attempt,
        "publication_channel": channel,
        "binding_probe": {
            "required_real_binding_names": [
                "configured real publication chat/channel",
                "real publication credentials",
                "replayable publication evidence",
            ],
            "resolved_credential_env": resolved_credential_env,
            "credentials_present": credentials_present,
            "checked_env_names": credential_env_names,
            "resolved_channel_env": resolved_channel_env,
            "channel_configured": channel_configured,
            "checked_channel_env_names": channel_env_names,
            "fallback_channel_used": False,
        },
        "live_probe": {
            "get_me": probe_get_me,
            "get_chat": probe_get_chat,
            "replayable_publication_evidence": replayable_publication_evidence,
            "live_real_ready": live_real_ready,
        },
        **lifecycle,
        "notes": notes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build attempt-scoped F1-B publication evidence and include real-binding probes "
            "without exposing secret values."
        )
    )
    parser.add_argument("--output", required=True, help="Destination JSON path.")
    parser.add_argument("--attempt", required=True, help="Attempt identifier, e.g. run-id/attempt-N.")
    parser.add_argument("--phase", default="F1-B", help="Phase label stored in the artifact.")
    parser.add_argument(
        "--publication-channel",
        default="",
        help="Explicit publication channel override; if empty, resolve from env names.",
    )
    parser.add_argument(
        "--credential-env-names",
        nargs="+",
        default=list(DEFAULT_CREDENTIAL_ENV_NAMES),
        help="Credential env names in resolution order.",
    )
    parser.add_argument(
        "--channel-env-names",
        nargs="+",
        default=list(DEFAULT_CHANNEL_ENV_NAMES),
        help="Publication channel env names in resolution order.",
    )
    parser.add_argument(
        "--fail-if-not-live-real",
        action="store_true",
        help="Exit with code 1 when live_real_ready is false.",
    )
    args = parser.parse_args()

    artifact = build_evidence(
        phase=args.phase,
        attempt=args.attempt,
        publication_channel=args.publication_channel,
        credential_env_names=[name.strip() for name in args.credential_env_names if name.strip()],
        channel_env_names=[name.strip() for name in args.channel_env_names if name.strip()],
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(artifact, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    if args.fail_if_not_live_real and not artifact["live_probe"]["live_real_ready"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
