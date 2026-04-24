from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.contracts import Mode, OrderIntent
from trading_advisor_3000.product_plane.execution.adapters import (
    SidecarTransportRetryableError,
    StockSharpHTTPTransport,
    StockSharpHTTPTransportConfig,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _intent(intent_id: str) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        signal_id=f"SIG-{intent_id}",
        mode=Mode.LIVE,
        broker_adapter="stocksharp-sidecar",
        action="buy",
        contract_id="BR-6.26",
        qty=1,
        price=82.5,
        stop_price=81.8,
        created_at=_utc_now(),
    )


def _compose_command(sidecar_binary: Path, dotnet_executable: str) -> list[str]:
    if not sidecar_binary.exists():
        raise FileNotFoundError(f"sidecar binary does not exist: {sidecar_binary}")
    suffix = sidecar_binary.suffix.lower()
    if suffix == ".dll":
        return [dotnet_executable, str(sidecar_binary)]
    if suffix == ".exe":
        return [str(sidecar_binary)]
    raise ValueError("sidecar binary must be .dll or .exe")


def _wait_for_health(base_url: str, *, timeout_seconds: float) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    health_url = f"{base_url.rstrip('/')}/health"
    last_error = "sidecar did not respond"

    while time.time() < deadline:
        try:
            with urllib_request.urlopen(health_url, timeout=1.0) as response:
                body = response.read().decode("utf-8")
            payload = json.loads(body) if body.strip() else {}
            if isinstance(payload, dict):
                status = str(payload.get("status", "")).strip().lower()
                connector_ready = bool(payload.get("connector_ready", False))
                connector_session_id = str(payload.get("connector_session_id", "")).strip()
                connector_binding_source = str(payload.get("connector_binding_source", "")).strip()
                connector_last_heartbeat = str(payload.get("connector_last_heartbeat", "")).strip()
                if (
                    status == "ok"
                    and connector_ready
                    and connector_session_id
                    and connector_binding_source
                    and connector_last_heartbeat
                ):
                    return payload
                last_error = (
                    "health is not ready for connector proof: "
                    f"status={status}, connector_ready={connector_ready}, "
                    f"session={bool(connector_session_id)}, binding={bool(connector_binding_source)}, "
                    f"heartbeat={bool(connector_last_heartbeat)}"
                )
                time.sleep(0.25)
                continue
            last_error = "health payload is not object"
        except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, socket.timeout, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.25)

    raise TimeoutError(f"timeout waiting for /health from compiled sidecar: {last_error}")


def _http_json(
    *,
    method: str,
    url: str,
    timeout_seconds: float,
    payload: dict[str, object] | None = None,
) -> tuple[int, dict[str, object]]:
    body: bytes | None = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib_request.Request(url=url, method=method.upper(), data=body, headers=headers)
    try:
        with urllib_request.urlopen(req, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", 200))
            raw = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        status = int(getattr(exc, "code", 0))
        raw = exc.read().decode("utf-8", errors="replace")

    parsed: object = {}
    if raw.strip():
        parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"expected JSON object from {method} {url}, got {type(parsed).__name__}")
    return status, parsed


def _http_text(*, url: str, timeout_seconds: float) -> tuple[int, str]:
    req = urllib_request.Request(url=url, method="GET")
    try:
        with urllib_request.urlopen(req, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", 200))
            text = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        status = int(getattr(exc, "code", 0))
        text = exc.read().decode("utf-8", errors="replace")
    return status, text


def _run_smoke(base_url: str) -> dict[str, object]:
    transport = StockSharpHTTPTransport(
        config=StockSharpHTTPTransportConfig(
            base_url=base_url,
            timeout_seconds=2.0,
            stream_batch_size=100,
            api_prefix="v1",
        )
    )
    intent = _intent("INT-DOTNET-SIDECAR-COMPILED-1")
    second_intent = _intent("INT-DOTNET-SIDECAR-COMPILED-2")
    blocked_intent = _intent("INT-DOTNET-SIDECAR-COMPILED-BLOCKED")

    metrics_before_status, metrics_before = _http_text(
        url=f"{base_url.rstrip('/')}/metrics",
        timeout_seconds=2.0,
    )
    if metrics_before_status != 200:
        raise RuntimeError(f"/metrics must return 200, got {metrics_before_status}")
    if "ta3000_sidecar_gateway_up" not in metrics_before:
        raise RuntimeError("/metrics response does not include sidecar gateway health metric")
    if "ta3000_sidecar_gateway_kill_switch 0" not in metrics_before:
        raise RuntimeError("/metrics does not show kill-switch disabled before admin toggle")

    submit_ack = transport.submit_order_intent(intent)
    replace_ack = transport.replace_order_intent(
        intent_id=intent.intent_id,
        new_qty=2,
        new_price=82.7,
        replaced_at=_utc_now(),
    )
    cancel_ack = transport.cancel_order_intent(
        intent_id=intent.intent_id,
        canceled_at=_utc_now(),
    )
    updates = transport.list_broker_updates()
    fills = transport.list_broker_fills()
    health = transport.health()
    remote_health = health.get("remote", {}) if isinstance(health, dict) else {}
    if not isinstance(remote_health, dict):
        raise RuntimeError("sidecar health remote payload must be object")
    connector_session_id = str(remote_health.get("connector_session_id", "")).strip()
    connector_binding_source = str(remote_health.get("connector_binding_source", "")).strip()
    connector_last_heartbeat = str(remote_health.get("connector_last_heartbeat", "")).strip()
    if not connector_session_id:
        raise RuntimeError("sidecar /health must expose connector_session_id for broker contour evidence")
    if not connector_binding_source:
        raise RuntimeError("sidecar /health must expose connector_binding_source for broker contour evidence")
    if not connector_last_heartbeat:
        raise RuntimeError("sidecar /health must expose connector_last_heartbeat for broker contour evidence")
    readiness = transport.readiness()

    kill_switch_on_status, kill_switch_on_payload = _http_json(
        method="POST",
        url=f"{base_url.rstrip('/')}/v1/admin/kill-switch",
        timeout_seconds=2.0,
        payload={"active": True},
    )
    if kill_switch_on_status != 200:
        raise RuntimeError(f"kill-switch enable must return 200, got {kill_switch_on_status}")
    if bool(kill_switch_on_payload.get("kill_switch_active")) is not True:
        raise RuntimeError("kill-switch enable did not set kill_switch_active=true")

    ready_under_kill_status, ready_under_kill_payload = _http_json(
        method="GET",
        url=f"{base_url.rstrip('/')}/ready",
        timeout_seconds=2.0,
    )
    if ready_under_kill_status != 503:
        raise RuntimeError(f"/ready under kill-switch must return 503, got {ready_under_kill_status}")
    if bool(ready_under_kill_payload.get("ready", True)) is not False:
        raise RuntimeError("/ready under kill-switch must report ready=false")
    if str(ready_under_kill_payload.get("reason", "")).strip() != "kill_switch_active":
        raise RuntimeError("/ready under kill-switch must report reason=kill_switch_active")

    blocked_submit_error: dict[str, object] | None = None
    try:
        transport.submit_order_intent(blocked_intent)
    except SidecarTransportRetryableError as exc:
        blocked_submit_error = {
            "error_code": exc.error_code,
            "status_code": exc.status_code,
            "message": str(exc),
        }
        if exc.error_code != "kill_switch_active":
            raise RuntimeError(
                "submit under kill-switch must fail with error_code=kill_switch_active, "
                f"got {exc.error_code!r}"
            ) from exc
        if int(exc.status_code or 0) != 503:
            raise RuntimeError(
                f"submit under kill-switch must fail with status_code=503, got {exc.status_code}"
            ) from exc
    else:
        raise RuntimeError("submit under kill-switch must fail with 503")

    metrics_kill_on_status, metrics_kill_on = _http_text(
        url=f"{base_url.rstrip('/')}/metrics",
        timeout_seconds=2.0,
    )
    if metrics_kill_on_status != 200:
        raise RuntimeError(f"/metrics after kill-switch enable must return 200, got {metrics_kill_on_status}")
    if "ta3000_sidecar_gateway_kill_switch 1" not in metrics_kill_on:
        raise RuntimeError("/metrics does not show kill-switch enabled after admin toggle")

    kill_switch_off_status, kill_switch_off_payload = _http_json(
        method="POST",
        url=f"{base_url.rstrip('/')}/v1/admin/kill-switch",
        timeout_seconds=2.0,
        payload={"active": False},
    )
    if kill_switch_off_status != 200:
        raise RuntimeError(f"kill-switch disable must return 200, got {kill_switch_off_status}")
    if bool(kill_switch_off_payload.get("kill_switch_active", True)) is not False:
        raise RuntimeError("kill-switch disable did not set kill_switch_active=false")

    readiness_after_restore = transport.readiness()
    if not bool(readiness_after_restore.get("ready", False)):
        raise RuntimeError(f"readiness after kill-switch disable must be ready=true: {readiness_after_restore}")

    second_submit_ack = transport.submit_order_intent(second_intent)
    if second_submit_ack.get("accepted") is not True:
        raise RuntimeError("submit after kill-switch restore must be accepted=true")

    metrics_restored_status, metrics_restored = _http_text(
        url=f"{base_url.rstrip('/')}/metrics",
        timeout_seconds=2.0,
    )
    if metrics_restored_status != 200:
        raise RuntimeError(f"/metrics after kill-switch disable must return 200, got {metrics_restored_status}")
    if "ta3000_sidecar_gateway_kill_switch 0" not in metrics_restored:
        raise RuntimeError("/metrics does not show kill-switch disabled after restore")

    update_states = {str(item.get("state", "")) for item in updates}
    if submit_ack.get("accepted") is not True:
        raise RuntimeError("submit ack must be accepted=true")
    if str(replace_ack.get("state", "")) != "replaced":
        raise RuntimeError("replace ack must have state=replaced")
    if str(cancel_ack.get("state", "")) != "canceled":
        raise RuntimeError("cancel ack must have state=canceled")
    if not {"submitted", "replaced", "canceled"}.issubset(update_states):
        raise RuntimeError("compiled sidecar update stream does not include required states")
    if not bool(readiness.get("ready", False)):
        raise RuntimeError(f"compiled sidecar readiness is not ready: {readiness}")

    return {
        "submit_ack": submit_ack,
        "replace_ack": replace_ack,
        "cancel_ack": cancel_ack,
        "updates_count": len(updates),
        "fills_count": len(fills),
        "health": health,
        "connector_session": {
            "connector_session_id": connector_session_id,
            "connector_binding_source": connector_binding_source,
            "connector_last_heartbeat": connector_last_heartbeat,
        },
        "readiness": readiness,
        "metrics": {
            "before_kill_switch": metrics_before,
            "while_kill_switch_enabled": metrics_kill_on,
            "after_kill_switch_restore": metrics_restored,
        },
        "kill_switch": {
            "enable_response": kill_switch_on_payload,
            "ready_under_kill_switch": ready_under_kill_payload,
            "blocked_submit": blocked_submit_error,
            "disable_response": kill_switch_off_payload,
            "readiness_after_restore": readiness_after_restore,
            "submit_after_restore": second_submit_ack,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Python transport smoke against a compiled StockSharp .NET sidecar binary."
    )
    parser.add_argument("--sidecar-binary", required=True, help="Path to compiled .dll or .exe")
    parser.add_argument("--dotnet", default="dotnet", help="dotnet executable used when sidecar binary is .dll")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18091)
    parser.add_argument("--startup-timeout", type=float, default=20.0)
    parser.add_argument("--output", default=None, help="Optional JSON output path")
    args = parser.parse_args()

    sidecar_binary = Path(args.sidecar_binary).resolve()
    base_url = f"http://{args.host}:{args.port}"
    command = _compose_command(sidecar_binary, dotnet_executable=args.dotnet)

    env = dict(os.environ)
    env["ASPNETCORE_URLS"] = base_url

    process = subprocess.Popen(
        command,
        cwd=sidecar_binary.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    logs_tail = ""
    try:
        boot_health = _wait_for_health(base_url, timeout_seconds=args.startup_timeout)
        smoke = _run_smoke(base_url)
        payload = {
            "generated_at": _utc_now(),
            "status": "ok",
            "route": "worker:phase-only",
            "sidecar_binary": str(sidecar_binary),
            "base_url": base_url,
            "boot_health": boot_health,
            "smoke": smoke,
        }
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.stdout is not None:
            logs_tail = process.stdout.read().strip()
        if logs_tail:
            tail_lines = logs_tail.splitlines()[-20:]
            print("compiled-sidecar-log-tail:")
            for line in tail_lines:
                print(line)


if __name__ == "__main__":
    raise SystemExit(main())
