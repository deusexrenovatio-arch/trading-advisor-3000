from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib import error as urllib_error
from urllib import request as urllib_request

from trading_advisor_3000.app.runtime.config import DEFAULT_REQUIRED_LIVE_SECRETS, evaluate_secrets_policy


DEFAULT_REQUIRED_SIDECAR_ENV_NAMES = (
    "TA3000_ENABLE_LIVE_EXECUTION",
    "TA3000_ENABLE_STOCKSHARP_BRIDGE",
    "TA3000_ENABLE_QUIK_CONNECTOR",
    "TA3000_ENABLE_FINAM_TRANSPORT",
    "TA3000_SIDECAR_TRANSPORT",
    "TA3000_SIDECAR_BASE_URL",
)


def _required_text(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be non-empty string")
    return value.strip()


def _required_text_list(payload: dict[str, object], key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{key} must be non-empty list")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{key} items must be non-empty strings")
        items.append(item.strip())
    return tuple(items)


def _env_flag_enabled(env: Mapping[str, str], name: str) -> bool:
    raw = str(env.get(name, "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SidecarEndpointProbe:
    path: str
    status_code: int | None
    ok: bool
    details: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "status_code": self.status_code,
            "ok": self.ok,
            "details": self.details,
        }


@dataclass(frozen=True)
class Phase9SidecarDeliverySpec:
    delivery_mode: str
    wire_api_version: str
    sidecar_route: str
    gateway_profile_dir: str
    gateway_entrypoint: str
    gateway_compose_file: str
    gateway_env_template: str
    readiness_endpoints: tuple[str, ...]
    dry_run_command: str
    canary_command: str
    kill_switch_env_name: str
    stocksharp_delivery_note: str

    def to_dict(self) -> dict[str, object]:
        return {
            "delivery_mode": self.delivery_mode,
            "wire_api_version": self.wire_api_version,
            "sidecar_route": self.sidecar_route,
            "gateway_profile_dir": self.gateway_profile_dir,
            "gateway_entrypoint": self.gateway_entrypoint,
            "gateway_compose_file": self.gateway_compose_file,
            "gateway_env_template": self.gateway_env_template,
            "readiness_endpoints": list(self.readiness_endpoints),
            "dry_run_command": self.dry_run_command,
            "canary_command": self.canary_command,
            "kill_switch_env_name": self.kill_switch_env_name,
            "stocksharp_delivery_note": self.stocksharp_delivery_note,
        }


@dataclass(frozen=True)
class Phase9SidecarPreflightReport:
    delivery_spec: Phase9SidecarDeliverySpec
    base_url: str
    missing_env_names: list[str]
    invalid_env: list[str]
    warnings: list[str]
    secrets_policy: dict[str, object]
    endpoint_probes: list[SidecarEndpointProbe]
    rollout_dry_run: dict[str, object] | None

    @property
    def is_ready(self) -> bool:
        dry_run_ready = self.rollout_dry_run is None or str(self.rollout_dry_run.get("status")) == "ok"
        return (
            not self.missing_env_names
            and not self.invalid_env
            and bool(self.secrets_policy.get("is_ready"))
            and all(item.ok for item in self.endpoint_probes)
            and dry_run_ready
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "ok" if self.is_ready else "blocked",
            "delivery_spec": self.delivery_spec.to_dict(),
            "base_url": self.base_url,
            "missing_env_names": list(self.missing_env_names),
            "invalid_env": list(self.invalid_env),
            "warnings": list(self.warnings),
            "secrets_policy": self.secrets_policy,
            "endpoint_probes": [item.to_dict() for item in self.endpoint_probes],
            "rollout_dry_run": self.rollout_dry_run,
            "is_ready": self.is_ready,
        }


def load_phase9_sidecar_delivery_manifest(path: Path) -> Phase9SidecarDeliverySpec:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("sidecar delivery manifest must be a JSON object")
    return Phase9SidecarDeliverySpec(
        delivery_mode=_required_text(payload, "delivery_mode"),
        wire_api_version=_required_text(payload, "wire_api_version"),
        sidecar_route=_required_text(payload, "sidecar_route"),
        gateway_profile_dir=_required_text(payload, "gateway_profile_dir"),
        gateway_entrypoint=_required_text(payload, "gateway_entrypoint"),
        gateway_compose_file=_required_text(payload, "gateway_compose_file"),
        gateway_env_template=_required_text(payload, "gateway_env_template"),
        readiness_endpoints=_required_text_list(payload, "readiness_endpoints"),
        dry_run_command=_required_text(payload, "dry_run_command"),
        canary_command=_required_text(payload, "canary_command"),
        kill_switch_env_name=_required_text(payload, "kill_switch_env_name"),
        stocksharp_delivery_note=_required_text(payload, "stocksharp_delivery_note"),
    )


def _probe_endpoint(*, base_url: str, path: str, expected_route: str) -> SidecarEndpointProbe:
    url = f"{base_url.rstrip('/')}{path}"
    try:
        with urllib_request.urlopen(url, timeout=3.0) as response:
            status_code = int(getattr(response, "status", 200))
            content_type = str(response.headers.get("Content-Type", "")).lower()
            body = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        return SidecarEndpointProbe(
            path=path,
            status_code=int(getattr(exc, "code", 0)),
            ok=False,
            details={"error": f"http_error:{getattr(exc, 'code', 'unknown')}"},
        )
    except urllib_error.URLError as exc:
        return SidecarEndpointProbe(
            path=path,
            status_code=None,
            ok=False,
            details={"error": f"url_error:{exc.reason}"},
        )

    details: dict[str, object] = {"content_type": content_type}
    ok = status_code == 200
    if path == "/metrics":
        details["non_empty_body"] = bool(body.strip())
        ok = ok and bool(body.strip())
        return SidecarEndpointProbe(path=path, status_code=status_code, ok=ok, details=details)

    try:
        parsed = json.loads(body) if body.strip() else {}
    except json.JSONDecodeError:
        parsed = {}
    if isinstance(parsed, dict):
        details["payload"] = parsed
        route = str(parsed.get("route", "")).strip()
        if route:
            details["route_matches"] = route == expected_route
            ok = ok and route == expected_route
        if path == "/ready":
            details["ready"] = bool(parsed.get("ready"))
            ok = ok and bool(parsed.get("ready"))
    else:
        ok = False
        details["error"] = "non_json_payload"
    return SidecarEndpointProbe(path=path, status_code=status_code, ok=ok, details=details)


def _run_rollout_dry_run(*, base_url: str, env: Mapping[str, str]) -> dict[str, object]:
    root = Path(__file__).resolve().parents[4]
    script = root / "scripts" / "run_staging_real_execution_rollout.py"
    process_env = dict(env)
    pythonpath = process_env.get("PYTHONPATH", "")
    src = str(root / "src")
    if src not in pythonpath.split(";"):
        process_env["PYTHONPATH"] = src if not pythonpath else src + ";" + pythonpath
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--base-url",
            base_url,
            "--stage",
            "all",
            "--dry-run",
            "--format",
            "json",
        ],
        cwd=root,
        env=process_env,
        check=False,
        capture_output=True,
        text=True,
    )
    stdout = result.stdout.strip()
    payload: dict[str, object]
    if stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = {"status": "failed", "stdout": stdout, "stderr": result.stderr}
    else:
        parsed = {"status": "failed", "stdout": "", "stderr": result.stderr}
    payload = parsed if isinstance(parsed, dict) else {"status": "failed", "stdout": stdout, "stderr": result.stderr}
    payload["returncode"] = result.returncode
    return payload


def evaluate_phase9_sidecar_preflight(
    *,
    env: Mapping[str, str] | None,
    delivery_spec: Phase9SidecarDeliverySpec,
    base_url: str | None = None,
    include_rollout_dry_run: bool = True,
) -> Phase9SidecarPreflightReport:
    source = dict(env or {})
    resolved_base_url = (base_url or str(source.get("TA3000_SIDECAR_BASE_URL", ""))).strip()
    missing_env_names = [
        name
        for name in DEFAULT_REQUIRED_SIDECAR_ENV_NAMES
        if not str(source.get(name, "")).strip()
    ]
    invalid_env: list[str] = []
    if str(source.get("TA3000_SIDECAR_TRANSPORT", "")).strip().lower() != "http":
        invalid_env.append("TA3000_SIDECAR_TRANSPORT must be http for the frozen sidecar delivery mode")
    for flag_name in (
        "TA3000_ENABLE_LIVE_EXECUTION",
        "TA3000_ENABLE_STOCKSHARP_BRIDGE",
        "TA3000_ENABLE_QUIK_CONNECTOR",
        "TA3000_ENABLE_FINAM_TRANSPORT",
    ):
        if str(source.get(flag_name, "")).strip() and not _env_flag_enabled(source, flag_name):
            invalid_env.append(f"{flag_name} must be explicitly enabled")

    warnings: list[str] = []
    if str(source.get(delivery_spec.kill_switch_env_name, "")).strip() == "1":
        warnings.append(f"{delivery_spec.kill_switch_env_name} is set; readiness may be intentionally blocked")

    secrets_policy = evaluate_secrets_policy(
        env=source,
        required_secret_names=DEFAULT_REQUIRED_LIVE_SECRETS,
        enforce=True,
    ).to_dict()
    endpoint_probes: list[SidecarEndpointProbe] = []
    if resolved_base_url:
        endpoint_probes = [
            _probe_endpoint(
                base_url=resolved_base_url,
                path=path,
                expected_route=delivery_spec.sidecar_route,
            )
            for path in delivery_spec.readiness_endpoints
        ]
    rollout_dry_run = None
    if include_rollout_dry_run and resolved_base_url:
        rollout_dry_run = _run_rollout_dry_run(base_url=resolved_base_url, env=source)

    return Phase9SidecarPreflightReport(
        delivery_spec=delivery_spec,
        base_url=resolved_base_url,
        missing_env_names=missing_env_names,
        invalid_env=invalid_env,
        warnings=warnings,
        secrets_policy=secrets_policy,
        endpoint_probes=endpoint_probes,
        rollout_dry_run=rollout_dry_run,
    )
