from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import platform
import re
import shlex
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "artifacts" / "f1" / "phase05" / "real-broker-process"
DEFAULT_PROFILE_CONTRACT = ROOT / "configs" / "broker_staging_connector_profile.v1.json"
SIDECAR_SCRIPT_DIR = ROOT / "deployment" / "stocksharp-sidecar" / "scripts"
SMOKE_SCRIPT = ROOT / "scripts" / "smoke_stocksharp_sidecar_binary.py"
ROLLOUT_SCRIPT = ROOT / "scripts" / "run_staging_real_execution_rollout.py"
PHASE_BRIEF = "docs/codex/modules/f1-full-closure.phase-05.md"
PHASE_NAME = "F1-E - Real Broker Process Closure"


@dataclass(frozen=True)
class StepResult:
    step_id: str
    command: tuple[str, ...]
    return_code: int
    started_at: str
    finished_at: str
    duration_sec: float
    stdout_path: str
    stderr_path: str

    def as_json(self) -> dict[str, object]:
        return {
            "step_id": self.step_id,
            "command": render_command(self.command),
            "return_code": self.return_code,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_sec": self.duration_sec,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
        }


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def render_command(command: Sequence[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(list(command))
    return shlex.join(command)


def rel(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_failure_artifact(
    *,
    run_dir: Path,
    run_id: str,
    git_sha: str,
    stage: str,
    message: str,
    context: Mapping[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "phase": PHASE_NAME,
        "phase_brief": PHASE_BRIEF,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "status": "failed",
        "generated_at": utc_now(),
        "run_id": run_id,
        "git_sha": git_sha,
        "failure_stage": stage,
        "error": message,
    }
    if context:
        payload["context"] = dict(context)
    write_json(run_dir / "failure.json", payload)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path.as_posix()}")
    return payload


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def collect_hash_entries(*, base_dir: Path, targets: Sequence[Path]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for path in sorted({item.resolve() for item in targets}):
        if not path.exists():
            raise FileNotFoundError(f"hash target is missing: {path.as_posix()}")
        if not path.is_file():
            raise RuntimeError(f"hash target is not a file: {path.as_posix()}")
        entries.append(
            {
                "path": path.relative_to(base_dir).as_posix(),
                "sha256": sha256_file(path),
                "size_bytes": int(path.stat().st_size),
            }
        )
    return entries


def write_hash_manifest(
    *,
    base_dir: Path,
    targets: Sequence[Path],
    output_json: Path,
    output_sha256: Path,
) -> list[dict[str, object]]:
    entries = collect_hash_entries(base_dir=base_dir, targets=targets)
    write_json(
        output_json,
        {
            "schema_version": 1,
            "generated_at": utc_now(),
            "entries": entries,
        },
    )
    sha_lines = [f"{entry['sha256']} *{entry['path']}" for entry in entries]
    output_sha256.write_text("\n".join(sha_lines) + "\n", encoding="utf-8")
    return entries


def verify_hash_manifest(*, base_dir: Path, entries: Sequence[dict[str, object]]) -> None:
    for entry in entries:
        path_text = str(entry.get("path", "")).strip()
        expected_hash = str(entry.get("sha256", "")).strip().lower()
        if not path_text or not expected_hash:
            raise ValueError("hash manifest entry must include path and sha256")
        target = (base_dir / path_text).resolve()
        if not target.exists():
            raise FileNotFoundError(f"hash verification target is missing: {path_text}")
        actual = sha256_file(target).lower()
        if actual != expected_hash:
            raise ValueError(
                f"hash mismatch for {path_text}: expected {expected_hash}, got {actual}"
            )


def git_head_sha() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"failed to resolve git HEAD SHA: {detail}")
    value = completed.stdout.strip()
    if len(value) < 12:
        raise RuntimeError(f"unexpected git SHA output: {value!r}")
    return value


def resolve_dotnet_executable(explicit: str) -> str:
    if explicit.strip():
        return explicit.strip()
    env_dotnet = os.environ.get("TA3000_DOTNET_BIN", "").strip()
    if env_dotnet:
        return env_dotnet
    return "dotnet"


def redact_secret(value: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return "<empty>"
    if len(trimmed) <= 4:
        return "*" * len(trimmed)
    return f"{trimmed[:2]}***{trimmed[-2:]}"


_PLACEHOLDER_SECRET_TOKENS = (
    "change_me",
    "changeme",
    "placeholder",
    "example",
    "dummy",
    "fake",
    "sample",
    "test-only",
    "secret-001",
    "token-001",
)


def is_placeholder_secret(value: str) -> bool:
    trimmed = value.strip()
    if len(trimmed) < 12:
        return True
    lowered = trimmed.lower()
    if any(token in lowered for token in _PLACEHOLDER_SECRET_TOKENS):
        return True
    if lowered.startswith(("demo-", "sample-", "test-", "fake-")):
        return True
    return False


_SYNTHETIC_MARKER_TOKENS = {
    "stub",
    "mock",
    "simulated",
    "simulation",
    "synthetic",
    "memory",
    "inmemory",
    "local",
    "remediation",
    "dummy",
    "fake",
    "test",
    "sandbox",
}

_LOCAL_CONNECTOR_HOSTS = {"localhost", "host.docker.internal"}


def has_synthetic_marker(value: str) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return True
    compact = normalized.replace("-", "").replace("_", "").replace(".", "")
    if compact in {"inmemory", "memory"}:
        return True
    tokens = {token for token in re.split(r"[^a-z0-9]+", normalized) if token}
    return bool(tokens.intersection(_SYNTHETIC_MARKER_TOKENS))


def validate_real_connector_base_url(raw_url: str) -> str:
    value = raw_url.strip()
    if not value:
        raise RuntimeError("Finam API base URL must be non-empty for real broker contour")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RuntimeError("Finam API base URL must be absolute http/https URL")
    host = parsed.hostname.strip().lower()
    if host in _LOCAL_CONNECTOR_HOSTS:
        raise RuntimeError("Finam API base URL must reference external host, not local host")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return value
    if ip.is_loopback or ip.is_unspecified:
        raise RuntimeError("Finam API base URL must not use loopback or unspecified host for real contour")
    return value


def validate_real_connector_health(payload: dict[str, object]) -> dict[str, object]:
    service_identity = str(payload.get("service", "")).strip().lower()
    connector_mode = str(payload.get("connector_mode", "")).strip().lower()
    connector_backend = str(payload.get("connector_backend", "")).strip().lower()
    connector_ready = payload.get("connector_ready")
    connector_session_id = str(payload.get("connector_session_id", "")).strip()
    connector_binding_source = str(payload.get("connector_binding_source", "")).strip().lower()
    connector_last_heartbeat = str(payload.get("connector_last_heartbeat", "")).strip()

    if service_identity and has_synthetic_marker(service_identity):
        raise RuntimeError("sidecar health service identity must not be stub/mock/sandbox for real contour")
    if connector_mode not in {"staging-real", "real-staging", "real"}:
        raise RuntimeError(
            "sidecar health must expose real connector_mode (`staging-real`/`real-staging`/`real`)"
        )
    if has_synthetic_marker(connector_backend):
        raise RuntimeError(
            "sidecar health reports non-real connector backend; real broker contour is not bound"
        )
    if connector_ready is not True:
        raise RuntimeError("sidecar health reports connector_ready!=true for real broker contour")
    if not connector_session_id:
        raise RuntimeError("sidecar health must include connector_session_id for real broker contour")
    if has_synthetic_marker(connector_binding_source):
        raise RuntimeError("sidecar health must include non-synthetic connector_binding_source")
    if not connector_last_heartbeat:
        raise RuntimeError("sidecar health must include connector_last_heartbeat for real broker contour")
    return {
        "connector_mode": connector_mode,
        "connector_backend": connector_backend,
        "connector_ready": True,
        "connector_session_id": connector_session_id,
        "connector_binding_source": connector_binding_source,
        "connector_last_heartbeat": connector_last_heartbeat,
    }


def validate_finam_session_details(payload: dict[str, object]) -> dict[str, object]:
    created_at = str(payload.get("created_at", "")).strip()
    expires_at = str(payload.get("expires_at", "")).strip()
    readonly = payload.get("readonly")
    account_ids_raw = payload.get("account_ids")
    md_permissions_raw = payload.get("md_permissions")
    if not created_at:
        raise RuntimeError("Finam session preflight must include created_at")
    if not expires_at:
        raise RuntimeError("Finam session preflight must include expires_at")
    if not isinstance(readonly, bool):
        raise RuntimeError("Finam session preflight must include readonly bool")
    if not isinstance(account_ids_raw, list):
        raise RuntimeError("Finam session preflight must include account_ids array")
    account_ids = [str(item).strip() for item in account_ids_raw if str(item).strip()]
    if not isinstance(md_permissions_raw, list):
        raise RuntimeError("Finam session preflight must include md_permissions array")
    md_permissions = [item for item in md_permissions_raw if isinstance(item, dict)]
    return {
        "created_at": created_at,
        "expires_at": expires_at,
        "readonly": readonly,
        "account_ids": account_ids,
        "md_permissions_count": len(md_permissions),
    }


def probe_finam_session_details(
    *,
    base_url: str,
    jwt_token: str,
    session_details_path: str,
    timeout_seconds: float,
) -> dict[str, object]:
    normalized_jwt = jwt_token.strip()
    normalized_path = session_details_path.strip()
    if not normalized_path.startswith("/"):
        normalized_path = "/" + normalized_path
    details_url = f"{base_url.rstrip('/')}{normalized_path}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": normalized_jwt,
    }
    request_payload = json.dumps({"token": normalized_jwt}).encode("utf-8")
    request = urllib_request.Request(url=details_url, method="POST", headers=headers, data=request_payload)
    status_code = 0
    body = ""
    try:
        with urllib_request.urlopen(request, timeout=max(1.0, timeout_seconds)) as response:
            status_code = int(getattr(response, "status", 200))
            body = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        status_code = int(getattr(exc, "code", 0))
        body = exc.read().decode("utf-8", errors="replace")
        body_hint = body.strip().replace("\n", " ")[:200]
        suffix = f"; body={body_hint}" if body_hint else ""
        raise RuntimeError(
            "Finam session preflight failed "
            f"({status_code}) at {details_url}; verify TA3000_FINAM_API_BASE_URL and "
            f"TA3000_FINAM_API_TOKEN bindings{suffix}"
        ) from exc
    except (urllib_error.URLError, TimeoutError, socket.timeout) as exc:
        raise RuntimeError(
            "Finam session preflight failed "
            f"at {details_url}: {type(exc).__name__}: {exc}"
        ) from exc

    try:
        payload = json.loads(body) if body.strip() else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Finam session preflight returned non-JSON payload; "
            f"contract is required at {details_url}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            "Finam session preflight returned non-object payload; "
            f"contract is required at {details_url}"
        )

    session_details = validate_finam_session_details(payload)
    return {
        "status_code": status_code,
        "details_url": details_url,
        "session_details": session_details,
        "raw_response": payload,
    }


def run_command(
    *,
    step_id: str,
    command: Sequence[str],
    logs_dir: Path,
    cwd: Path = ROOT,
    env: dict[str, str] | None = None,
    require_success: bool = True,
) -> StepResult:
    started_at = utc_now()
    started_clock = time.perf_counter()
    completed = subprocess.run(
        list(command),
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    finished_at = utc_now()
    duration_sec = round(time.perf_counter() - started_clock, 3)

    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = logs_dir / f"{step_id}.stdout.log"
    stderr_path = logs_dir / f"{step_id}.stderr.log"
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")

    result = StepResult(
        step_id=step_id,
        command=tuple(command),
        return_code=int(completed.returncode),
        started_at=started_at,
        finished_at=finished_at,
        duration_sec=duration_sec,
        stdout_path=rel(stdout_path),
        stderr_path=rel(stderr_path),
    )
    if require_success and completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"{step_id} failed ({completed.returncode}): {detail}")
    return result


def _compose_sidecar_command(sidecar_binary: Path, dotnet_executable: str) -> list[str]:
    if not sidecar_binary.exists():
        raise FileNotFoundError(f"sidecar binary does not exist: {sidecar_binary.as_posix()}")
    suffix = sidecar_binary.suffix.lower()
    if suffix == ".dll":
        return [dotnet_executable, str(sidecar_binary)]
    if suffix == ".exe":
        return [str(sidecar_binary)]
    raise ValueError("sidecar binary must be .dll or .exe")


def _wait_for_health(base_url: str, *, timeout_seconds: float) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    url = f"{base_url.rstrip('/')}/health"
    last_error = "sidecar did not respond"
    while time.time() < deadline:
        try:
            with urllib_request.urlopen(url, timeout=1.0) as response:
                body = response.read().decode("utf-8")
            payload = json.loads(body) if body.strip() else {}
            if isinstance(payload, dict):
                return payload
            last_error = "health payload is not object"
        except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, socket.timeout, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.25)
    raise TimeoutError(f"timeout waiting for sidecar /health: {last_error}")


@contextmanager
def running_sidecar(
    *,
    sidecar_binary: Path,
    dotnet_executable: str,
    base_url: str,
    startup_timeout: float,
    log_path: Path,
    extra_env: Mapping[str, str] | None = None,
) -> Iterator[dict[str, object]]:
    command = _compose_sidecar_command(sidecar_binary, dotnet_executable)
    env = dict(os.environ)
    env["ASPNETCORE_URLS"] = base_url
    if extra_env:
        for key, value in extra_env.items():
            env[str(key)] = str(value)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as stream:
        process = subprocess.Popen(
            command,
            cwd=sidecar_binary.parent,
            stdout=stream,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        try:
            health = _wait_for_health(base_url, timeout_seconds=startup_timeout)
            connector_health = validate_real_connector_health(health)
            yield {
                "health": health,
                "connector_health": connector_health,
                "command": render_command(command),
                "log_path": rel(log_path),
            }
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def validate_connector_profile(payload: dict[str, Any]) -> dict[str, Any]:
    if int(payload.get("schema_version", 0)) != 1:
        raise ValueError("connector profile must set schema_version=1")
    profile_id = str(payload.get("profile_id", "")).strip()
    if not profile_id:
        raise ValueError("connector profile must define non-empty profile_id")
    environment = str(payload.get("environment", "")).strip().lower()
    if environment != "staging-real":
        raise ValueError("connector profile environment must be `staging-real`")
    proof_class = str(payload.get("proof_class", "")).strip()
    if proof_class != "staging-real":
        raise ValueError("connector profile proof_class must be `staging-real`")

    transport = payload.get("transport")
    if not isinstance(transport, dict):
        raise ValueError("connector profile must include transport object")
    base_url = str(transport.get("base_url", "")).strip()
    api_prefix = str(transport.get("api_prefix", "")).strip()
    timeout_seconds = float(transport.get("timeout_seconds", 0))
    stream_batch_size = int(transport.get("stream_batch_size", 0))
    if not base_url:
        raise ValueError("connector profile transport.base_url must be non-empty")
    if not api_prefix:
        raise ValueError("connector profile transport.api_prefix must be non-empty")
    if timeout_seconds <= 0:
        raise ValueError("connector profile transport.timeout_seconds must be positive")
    if stream_batch_size <= 0:
        raise ValueError("connector profile transport.stream_batch_size must be positive")

    finam_session_binding = payload.get("finam_session_binding")
    if not isinstance(finam_session_binding, dict):
        raise ValueError("connector profile must include finam_session_binding object")
    finam_base_url_env_var = str(finam_session_binding.get("base_url_env_var", "")).strip()
    finam_jwt_env_var = str(finam_session_binding.get("jwt_env_var", "")).strip()
    finam_session_details_path = str(finam_session_binding.get("session_details_path", "")).strip()
    if not finam_base_url_env_var:
        raise ValueError("connector profile finam_session_binding.base_url_env_var must be non-empty")
    if not finam_jwt_env_var:
        raise ValueError("connector profile finam_session_binding.jwt_env_var must be non-empty")
    if not finam_session_details_path:
        raise ValueError("connector profile finam_session_binding.session_details_path must be non-empty")
    required_session_fields_raw = finam_session_binding.get("required_session_fields")
    if not isinstance(required_session_fields_raw, list):
        raise ValueError("connector profile finam_session_binding.required_session_fields must be list")
    required_session_fields = sorted(
        {str(item).strip() for item in required_session_fields_raw if str(item).strip()}
    )
    mandatory_session_fields = {"created_at", "expires_at", "readonly"}
    missing_session_fields = sorted(mandatory_session_fields.difference(required_session_fields))
    if missing_session_fields:
        raise ValueError(
            "connector profile finam_session_binding.required_session_fields missing: "
            + ", ".join(missing_session_fields)
        )

    required_flags = payload.get("required_feature_flags")
    if not isinstance(required_flags, dict) or not required_flags:
        raise ValueError("connector profile required_feature_flags must be non-empty object")
    for key in (
        "TA3000_ENABLE_LIVE_EXECUTION",
        "TA3000_ENABLE_STOCKSHARP_BRIDGE",
        "TA3000_ENABLE_QUIK_CONNECTOR",
        "TA3000_ENABLE_FINAM_TRANSPORT",
        "TA3000_ENFORCE_LIVE_SECRETS",
    ):
        value = str(required_flags.get(key, "")).strip()
        if value != "1":
            raise ValueError(f"connector profile requires `{key}=1`")

    secrets = payload.get("required_secret_env_vars")
    if not isinstance(secrets, list) or not secrets:
        raise ValueError("connector profile required_secret_env_vars must be non-empty list")
    secret_names = [str(item).strip() for item in secrets if str(item).strip()]
    if not secret_names:
        raise ValueError("connector profile required_secret_env_vars must contain names")

    recovery = payload.get("recovery")
    if not isinstance(recovery, dict):
        raise ValueError("connector profile must include recovery object")
    if bool(recovery.get("kill_switch_on_failure", False)) is not True:
        raise ValueError("connector profile recovery.kill_switch_on_failure must be true")
    retry_max_attempts = int(recovery.get("retry_max_attempts", 0))
    retry_backoff_seconds = float(recovery.get("retry_backoff_seconds", 0))
    if retry_max_attempts <= 0:
        raise ValueError("connector profile recovery.retry_max_attempts must be positive")
    if retry_backoff_seconds < 0:
        raise ValueError("connector profile recovery.retry_backoff_seconds must be non-negative")

    operations = payload.get("proof_operations")
    if not isinstance(operations, list):
        raise ValueError("connector profile proof_operations must be list")
    required_operations = {
        "boot",
        "readiness",
        "kill_switch",
        "submit",
        "replace",
        "cancel",
        "updates",
        "reconciliation",
    }
    present = {str(item).strip() for item in operations if str(item).strip()}
    missing = sorted(required_operations.difference(present))
    if missing:
        raise ValueError(
            "connector profile proof_operations missing required operations: "
            + ", ".join(missing)
        )

    return {
        "profile_id": profile_id,
        "environment": environment,
        "proof_class": proof_class,
        "connector_backend": "stocksharp-quik-finam",
        "transport": {
            "base_url": base_url,
            "api_prefix": api_prefix,
            "timeout_seconds": timeout_seconds,
            "stream_batch_size": stream_batch_size,
        },
        "finam_session_binding": {
            "base_url_env_var": finam_base_url_env_var,
            "jwt_env_var": finam_jwt_env_var,
            "session_details_path": finam_session_details_path,
            "required_session_fields": required_session_fields,
        },
        "required_feature_flags": {str(key): str(value) for key, value in required_flags.items()},
        "required_secret_env_vars": secret_names,
        "recovery": {
            "kill_switch_on_failure": True,
            "retry_max_attempts": retry_max_attempts,
            "retry_backoff_seconds": retry_backoff_seconds,
        },
        "proof_operations": sorted(present),
    }


def validate_smoke_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("status", "")).strip() != "ok":
        raise ValueError("smoke payload status must be ok")
    smoke = payload.get("smoke")
    if not isinstance(smoke, dict):
        raise ValueError("smoke payload must include `smoke` object")

    submit_ack = smoke.get("submit_ack")
    if not isinstance(submit_ack, dict) or submit_ack.get("accepted") is not True:
        raise ValueError("smoke payload must include accepted submit_ack")
    replace_ack = smoke.get("replace_ack")
    if not isinstance(replace_ack, dict) or str(replace_ack.get("state", "")).strip() != "replaced":
        raise ValueError("smoke payload replace_ack must include state=replaced")
    cancel_ack = smoke.get("cancel_ack")
    if not isinstance(cancel_ack, dict) or str(cancel_ack.get("state", "")).strip() != "canceled":
        raise ValueError("smoke payload cancel_ack must include state=canceled")
    if int(smoke.get("updates_count", 0)) < 3:
        raise ValueError("smoke payload updates_count must be >= 3")

    readiness = smoke.get("readiness")
    if not isinstance(readiness, dict) or readiness.get("ready") is not True:
        raise ValueError("smoke payload readiness must report ready=true")

    connector_session = smoke.get("connector_session")
    if not isinstance(connector_session, dict):
        raise ValueError("smoke payload must include connector_session object")
    if not str(connector_session.get("connector_session_id", "")).strip():
        raise ValueError("smoke payload connector_session must include connector_session_id")
    if not str(connector_session.get("connector_binding_source", "")).strip():
        raise ValueError("smoke payload connector_session must include connector_binding_source")
    if not str(connector_session.get("connector_last_heartbeat", "")).strip():
        raise ValueError("smoke payload connector_session must include connector_last_heartbeat")

    kill_switch = smoke.get("kill_switch")
    if not isinstance(kill_switch, dict):
        raise ValueError("smoke payload must include `kill_switch` object")
    ready_under_kill = kill_switch.get("ready_under_kill_switch")
    if not isinstance(ready_under_kill, dict):
        raise ValueError("smoke payload missing ready_under_kill_switch object")
    if ready_under_kill.get("ready") is not False:
        raise ValueError("kill-switch readiness check must report ready=false")
    if str(ready_under_kill.get("reason", "")).strip() != "kill_switch_active":
        raise ValueError("kill-switch readiness check must report reason=kill_switch_active")

    blocked_submit = kill_switch.get("blocked_submit")
    if not isinstance(blocked_submit, dict):
        raise ValueError("smoke payload missing blocked_submit object")
    if str(blocked_submit.get("error_code", "")).strip() != "kill_switch_active":
        raise ValueError("blocked_submit must expose error_code=kill_switch_active")
    if int(blocked_submit.get("status_code", 0)) != 503:
        raise ValueError("blocked_submit must expose status_code=503")

    submit_after_restore = kill_switch.get("submit_after_restore")
    if not isinstance(submit_after_restore, dict) or submit_after_restore.get("accepted") is not True:
        raise ValueError("submit_after_restore must be accepted=true")

    metrics = smoke.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("smoke payload missing metrics object")
    metrics_on = str(metrics.get("while_kill_switch_enabled", ""))
    metrics_restore = str(metrics.get("after_kill_switch_restore", ""))
    if "ta3000_sidecar_gateway_kill_switch 1" not in metrics_on:
        raise ValueError("metrics must include kill switch enabled marker")
    if "ta3000_sidecar_gateway_kill_switch 0" not in metrics_restore:
        raise ValueError("metrics must include kill switch restored marker")


def validate_rollout_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("status", "")).strip() != "ok":
        raise ValueError("rollout payload status must be ok")
    stages = payload.get("stages")
    if not isinstance(stages, list):
        raise ValueError("rollout payload must include stages list")
    by_stage: dict[str, dict[str, Any]] = {}
    for item in stages:
        if not isinstance(item, dict):
            continue
        stage_name = str(item.get("stage", "")).strip()
        by_stage[stage_name] = item
    for stage_name in ("connectivity", "canary", "batch"):
        row = by_stage.get(stage_name)
        if row is None:
            raise ValueError(f"rollout payload missing stage `{stage_name}`")
        if str(row.get("status", "")).strip() != "ok":
            raise ValueError(f"rollout stage `{stage_name}` must be ok")

    batch_details = by_stage["batch"].get("details")
    if not isinstance(batch_details, dict):
        raise ValueError("rollout batch stage must include details object")
    if int(batch_details.get("reconciliation_incidents", -1)) != 0:
        raise ValueError("rollout batch reconciliation_incidents must be 0")
    if int(batch_details.get("sync_incidents_delta", -1)) != 0:
        raise ValueError("rollout batch sync_incidents_delta must be 0")


def validate_miswire_failure(payload: dict[str, Any]) -> None:
    if str(payload.get("status", "")).strip() != "failed":
        raise ValueError("miswire disprover must produce failed rollout payload")
    stages = payload.get("stages")
    if not isinstance(stages, list) or not stages:
        raise ValueError("miswire disprover payload must include at least one stage")
    first = stages[0]
    if not isinstance(first, dict):
        raise ValueError("miswire disprover first stage must be object")
    if str(first.get("stage", "")).strip() != "connectivity":
        raise ValueError("miswire disprover must fail at connectivity stage")
    if str(first.get("status", "")).strip() != "failed":
        raise ValueError("miswire disprover connectivity stage must be failed")
    details = first.get("details")
    if not isinstance(details, dict):
        raise ValueError("miswire disprover connectivity stage must include details object")
    degraded = details.get("degraded_transports")
    if not isinstance(degraded, list) or not degraded:
        raise ValueError("miswire disprover must report degraded_transports")


def run_rollout(
    *,
    python_executable: str,
    base_url: str,
    output_path: Path,
    logs_dir: Path,
    env: dict[str, str],
    batch_size: int,
    step_id: str,
    require_success: bool,
) -> StepResult:
    return run_command(
        step_id=step_id,
        command=[
            python_executable,
            str(ROLLOUT_SCRIPT.resolve()),
            "--base-url",
            base_url,
            "--stage",
            "all",
            "--batch-size",
            str(batch_size),
            "--kill-switch-on-failure",
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
        logs_dir=logs_dir,
        env=env,
        require_success=require_success,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run F1-E staging-real broker process closure with fail-closed disprover and recovery evidence."
    )
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT.as_posix())
    parser.add_argument("--run-id", default="")
    parser.add_argument("--configuration", default="Release")
    parser.add_argument("--dotnet", default="")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18101)
    parser.add_argument("--startup-timeout", type=float, default=20.0)
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument("--profile-contract", default=DEFAULT_PROFILE_CONTRACT.as_posix())
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--skip-disprovers", action="store_true")
    args = parser.parse_args()

    git_sha = git_head_sha()
    dotnet_executable = resolve_dotnet_executable(args.dotnet)
    run_stamp = args.run_id.strip() or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{git_sha[:12]}"

    output_root = Path(args.output_root)
    if not output_root.is_absolute():
        output_root = (ROOT / output_root).resolve()
    run_dir = (output_root / run_stamp).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = run_dir / "logs"
    publish_dir = run_dir / "publish"

    profile_path = Path(args.profile_contract)
    if not profile_path.is_absolute():
        profile_path = (ROOT / profile_path).resolve()
    if not profile_path.exists():
        raise FileNotFoundError(f"profile contract does not exist: {profile_path.as_posix()}")
    profile_payload = read_json(profile_path)
    profile = validate_connector_profile(profile_payload)

    required_secrets = [str(item) for item in profile["required_secret_env_vars"]]
    secret_probes: list[dict[str, object]] = []
    missing_secrets: list[str] = []
    placeholder_secrets: list[str] = []
    for name in required_secrets:
        raw = str(os.environ.get(name, "")).strip()
        if not raw:
            missing_secrets.append(name)
            secret_probes.append({"name": name, "present": False, "redacted_value": None})
            continue
        placeholder = is_placeholder_secret(raw)
        if placeholder:
            placeholder_secrets.append(name)
        secret_probes.append(
            {
                "name": name,
                "present": True,
                "placeholder_like": placeholder,
                "redacted_value": redact_secret(raw),
            }
        )
    if missing_secrets:
        message = "required broker secrets are missing from environment: " + ", ".join(missing_secrets)
        write_failure_artifact(
            run_dir=run_dir,
            run_id=run_stamp,
            git_sha=git_sha,
            stage="secret_validation",
            message=message,
            context={
                "required_secret_env_vars": required_secrets,
                "missing_secret_env_vars": missing_secrets,
                "secret_probes": secret_probes,
            },
        )
        raise RuntimeError(message)
    if placeholder_secrets:
        message = (
            "placeholder-like broker secrets are forbidden for governed F1-E runs: "
            + ", ".join(placeholder_secrets)
        )
        write_failure_artifact(
            run_dir=run_dir,
            run_id=run_stamp,
            git_sha=git_sha,
            stage="secret_validation",
            message=message,
            context={
                "required_secret_env_vars": required_secrets,
                "placeholder_secret_env_vars": placeholder_secrets,
                "secret_probes": secret_probes,
            },
        )
        raise RuntimeError(message)

    finam_session_binding = dict(profile["finam_session_binding"])
    finam_base_url_env_var = str(finam_session_binding["base_url_env_var"]).strip()
    finam_jwt_env_var = str(finam_session_binding["jwt_env_var"]).strip()
    finam_session_details_path = str(finam_session_binding["session_details_path"]).strip()
    finam_api_base_url = str(os.environ.get(finam_base_url_env_var, "")).strip()
    finam_jwt = str(os.environ.get(finam_jwt_env_var, "")).strip()
    if not finam_api_base_url:
        message = f"required Finam API base URL env var is missing: {finam_base_url_env_var}"
        write_failure_artifact(
            run_dir=run_dir,
            run_id=run_stamp,
            git_sha=git_sha,
            stage="finam_binding_validation",
            message=message,
            context={
                "finam_base_url_env_var": finam_base_url_env_var,
                "finam_jwt_env_var": finam_jwt_env_var,
            },
        )
        raise RuntimeError(message)
    finam_api_base_url = validate_real_connector_base_url(finam_api_base_url)
    if not finam_jwt:
        message = f"required Finam JWT env var is missing: {finam_jwt_env_var}"
        write_failure_artifact(
            run_dir=run_dir,
            run_id=run_stamp,
            git_sha=git_sha,
            stage="finam_binding_validation",
            message=message,
            context={
                "finam_base_url_env_var": finam_base_url_env_var,
                "finam_jwt_env_var": finam_jwt_env_var,
                "finam_api_base_url": finam_api_base_url,
            },
        )
        raise RuntimeError(message)
    if is_placeholder_secret(finam_jwt):
        message = f"Finam JWT env var has placeholder-like value: {finam_jwt_env_var}"
        write_failure_artifact(
            run_dir=run_dir,
            run_id=run_stamp,
            git_sha=git_sha,
            stage="finam_binding_validation",
            message=message,
            context={
                "finam_base_url_env_var": finam_base_url_env_var,
                "finam_jwt_env_var": finam_jwt_env_var,
                "finam_api_base_url": finam_api_base_url,
                "finam_jwt_redacted": redact_secret(finam_jwt),
            },
        )
        raise RuntimeError(message)

    effective_base_url = f"http://{args.host}:{args.port}"
    miswire_base_url = f"http://{args.host}:{args.port + 37}"
    sidecar_binding_env = {
        "TA3000_GATEWAY_ROUTE": "stocksharp->quik->finam",
        "TA3000_CONNECTOR_MODE": str(profile["environment"]),
        "TA3000_CONNECTOR_BACKEND": str(profile["connector_backend"]),
        "TA3000_BROKER_CONNECTOR_BASE_URL": finam_api_base_url,
        "TA3000_BROKER_CONNECTOR_API_PREFIX": str(profile["transport"]["api_prefix"]),
        "TA3000_BROKER_CONNECTOR_AUTH_HEADER": "Authorization",
        "TA3000_BROKER_CONNECTOR_AUTH_TOKEN": finam_jwt,
        "TA3000_BROKER_CONNECTOR_TIMEOUT_SECONDS": str(profile["transport"]["timeout_seconds"]),
    }
    sidecar_binding_env_redacted = dict(sidecar_binding_env)
    sidecar_binding_env_redacted["TA3000_BROKER_CONNECTOR_AUTH_TOKEN"] = redact_secret(finam_jwt)

    dotnet_info = run_command(
        step_id="dotnet-info",
        command=[dotnet_executable, "--info"],
        logs_dir=logs_dir,
        require_success=False,
    )
    if dotnet_info.return_code != 0:
        message = (
            "dotnet --info failed; install .NET SDK or set TA3000_DOTNET_BIN to SDK-backed dotnet executable"
        )
        write_failure_artifact(
            run_dir=run_dir,
            run_id=run_stamp,
            git_sha=git_sha,
            stage="dotnet_preflight",
            message=message,
            context={
                "dotnet_executable": dotnet_executable,
                "stdout_log": dotnet_info.stdout_path,
                "stderr_log": dotnet_info.stderr_path,
                "return_code": dotnet_info.return_code,
            },
        )
        raise RuntimeError(message)

    environment_payload = {
        "schema_version": 1,
        "phase": PHASE_NAME,
        "phase_brief": PHASE_BRIEF,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "generated_at": utc_now(),
        "git_sha": git_sha,
        "python": {
            "executable": sys.executable,
            "version": sys.version,
            "platform": platform.platform(),
        },
        "dotnet": {
            "executable": dotnet_executable,
            "info_return_code": dotnet_info.return_code,
            "stdout_log": dotnet_info.stdout_path,
            "stderr_log": dotnet_info.stderr_path,
        },
        "connector_profile": {
            "path": rel(profile_path),
            "profile_id": profile["profile_id"],
            "environment": profile["environment"],
            "proof_class": profile["proof_class"],
            "connector_binding_env": sidecar_binding_env_redacted,
            "finam_session_binding": profile["finam_session_binding"],
            "transport": profile["transport"],
            "required_feature_flags": profile["required_feature_flags"],
            "required_secret_env_vars": required_secrets,
            "secret_probes": secret_probes,
            "connector_binding_probes": [
                {
                    "name": finam_base_url_env_var,
                    "present": bool(finam_api_base_url),
                    "value": finam_api_base_url,
                },
                {
                    "name": finam_jwt_env_var,
                    "present": bool(finam_jwt),
                    "redacted_value": redact_secret(finam_jwt),
                },
            ],
            "effective_base_url": effective_base_url,
            "miswire_base_url": miswire_base_url,
        },
    }
    write_json(run_dir / "environment.json", environment_payload)
    connector_preflight_path = run_dir / "connector-preflight.json"
    try:
        connector_preflight = probe_finam_session_details(
            base_url=finam_api_base_url,
            jwt_token=finam_jwt,
            session_details_path=finam_session_details_path,
            timeout_seconds=float(profile["transport"]["timeout_seconds"]),
        )
    except RuntimeError as exc:
        write_json(
            connector_preflight_path,
            {
                "status": "failed",
                "validated_at": utc_now(),
                "details_url": f"{finam_api_base_url.rstrip('/')}{finam_session_details_path}",
                "error": str(exc),
            },
        )
        write_failure_artifact(
            run_dir=run_dir,
            run_id=run_stamp,
            git_sha=git_sha,
            stage="finam_session_preflight",
            message=str(exc),
            context={
                "connector_preflight_artifact": rel(connector_preflight_path),
                "details_url": f"{finam_api_base_url.rstrip('/')}{finam_session_details_path}",
                "finam_base_url_env_var": finam_base_url_env_var,
                "finam_jwt_env_var": finam_jwt_env_var,
            },
        )
        raise
    write_json(
        connector_preflight_path,
        {
            "status": "ok",
            "validated_at": utc_now(),
            "details_url": connector_preflight["details_url"],
            "status_code": connector_preflight["status_code"],
            "session_details": connector_preflight["session_details"],
            "raw_response": connector_preflight["raw_response"],
        },
    )

    step_records: list[StepResult] = []

    build_step = run_command(
        step_id="build",
        command=[
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((SIDECAR_SCRIPT_DIR / "build.ps1").resolve()),
            "-Configuration",
            args.configuration,
            "-DotnetExecutable",
            dotnet_executable,
        ],
        logs_dir=logs_dir,
    )
    step_records.append(build_step)
    write_json(run_dir / "build.json", build_step.as_json())

    test_step = run_command(
        step_id="test",
        command=[
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((SIDECAR_SCRIPT_DIR / "test.ps1").resolve()),
            "-Configuration",
            args.configuration,
            "-DotnetExecutable",
            dotnet_executable,
        ],
        logs_dir=logs_dir,
    )
    step_records.append(test_step)
    write_json(run_dir / "test.json", test_step.as_json())

    publish_step = run_command(
        step_id="publish",
        command=[
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str((SIDECAR_SCRIPT_DIR / "publish.ps1").resolve()),
            "-Configuration",
            args.configuration,
            "-OutputDir",
            str(publish_dir),
            "-DotnetExecutable",
            dotnet_executable,
        ],
        logs_dir=logs_dir,
    )
    step_records.append(publish_step)
    write_json(run_dir / "publish.json", publish_step.as_json())

    sidecar_binary = publish_dir / "TradingAdvisor3000.StockSharpSidecar.dll"
    if not sidecar_binary.exists():
        raise RuntimeError(f"publish step did not produce compiled binary: {sidecar_binary.as_posix()}")

    smoke_output = run_dir / "smoke.json"
    smoke_step = run_command(
        step_id="smoke",
        command=[
            args.python_executable,
            str(SMOKE_SCRIPT.resolve()),
            "--sidecar-binary",
            str(sidecar_binary),
            "--dotnet",
            dotnet_executable,
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--output",
            str(smoke_output),
        ],
        logs_dir=logs_dir,
        env={**os.environ, **sidecar_binding_env},
    )
    step_records.append(smoke_step)
    smoke_payload = read_json(smoke_output)
    validate_smoke_payload(smoke_payload)
    write_json(
        run_dir / "smoke-validation.json",
        {
            "status": "ok",
            "validated_at": utc_now(),
            "checks": [
                "boot_health_ok",
                "kill_switch_fail_closed",
                "submit_replace_cancel_updates_proved",
                "readiness_recovered_after_kill_switch",
            ],
        },
    )

    rollout_env = dict(os.environ)
    rollout_env["TA3000_ENVIRONMENT"] = str(profile["environment"])
    rollout_env["TA3000_REQUIRED_LIVE_SECRETS"] = ",".join(required_secrets)
    rollout_env["TA3000_RETRY_MAX_ATTEMPTS"] = str(profile["recovery"]["retry_max_attempts"])
    rollout_env["TA3000_RETRY_BACKOFF_SECONDS"] = str(profile["recovery"]["retry_backoff_seconds"])
    rollout_env["TA3000_SIDECAR_API_PREFIX"] = str(profile["transport"]["api_prefix"])
    rollout_env["TA3000_SIDECAR_TIMEOUT_SECONDS"] = str(profile["transport"]["timeout_seconds"])
    rollout_env["TA3000_SIDECAR_STREAM_BATCH_SIZE"] = str(profile["transport"]["stream_batch_size"])
    rollout_env.update(sidecar_binding_env)
    for key, value in dict(profile["required_feature_flags"]).items():
        rollout_env[str(key)] = str(value)

    negative_tests: list[dict[str, object]] = []
    if not args.skip_disprovers:
        miswire_output = run_dir / "disprover-miswire-rollout.json"
        miswire_step = run_rollout(
            python_executable=args.python_executable,
            base_url=miswire_base_url,
            output_path=miswire_output,
            logs_dir=logs_dir,
            env=rollout_env,
            batch_size=max(1, args.batch_size),
            step_id="negative-miswire-connectivity",
            require_success=False,
        )
        step_records.append(miswire_step)
        if miswire_step.return_code == 0:
            raise RuntimeError("miswire disprover failed: rollout unexpectedly succeeded with unreachable connector")
        miswire_payload = read_json(miswire_output)
        validate_miswire_failure(miswire_payload)
        negative_tests.append(
            {
                "name": "miswire_unavailable_connector",
                "status": "expected_failure_observed",
                "return_code": miswire_step.return_code,
                "output_path": rel(miswire_output),
                "stdout_path": miswire_step.stdout_path,
                "stderr_path": miswire_step.stderr_path,
            }
        )

    rollout_output = run_dir / "rollout.json"
    sidecar_process_log = run_dir / "sidecar-process.log"
    connector_session_proof = run_dir / "connector-session-proof.json"
    with running_sidecar(
        sidecar_binary=sidecar_binary,
        dotnet_executable=dotnet_executable,
        base_url=effective_base_url,
        startup_timeout=max(1.0, float(args.startup_timeout)),
        log_path=sidecar_process_log,
        extra_env=sidecar_binding_env,
    ) as sidecar_runtime:
        write_json(
            run_dir / "connector-health-validation.json",
            {
                "status": "ok",
                "validated_at": utc_now(),
                "connector_health": sidecar_runtime["connector_health"],
                "sidecar_health": sidecar_runtime["health"],
            },
        )
        write_json(
            connector_session_proof,
            {
                "status": "ok",
                "validated_at": utc_now(),
                "required_session_fields": profile["finam_session_binding"]["required_session_fields"],
                "finam_session": connector_preflight["session_details"],
                "connector_session_id": sidecar_runtime["connector_health"]["connector_session_id"],
                "connector_binding_source": sidecar_runtime["connector_health"]["connector_binding_source"],
                "connector_last_heartbeat": sidecar_runtime["connector_health"]["connector_last_heartbeat"],
                "connector_backend": sidecar_runtime["connector_health"]["connector_backend"],
                "connector_mode": sidecar_runtime["connector_health"]["connector_mode"],
            },
        )
        rollout_step = run_rollout(
            python_executable=args.python_executable,
            base_url=effective_base_url,
            output_path=rollout_output,
            logs_dir=logs_dir,
            env=rollout_env,
            batch_size=max(1, args.batch_size),
            step_id="rollout-recovery",
            require_success=True,
        )
        step_records.append(rollout_step)
        rollout_payload = read_json(rollout_output)
        validate_rollout_payload(rollout_payload)
        recovery_payload = {
            "status": "ok",
            "validated_at": utc_now(),
            "connector_health_before_rollout": sidecar_runtime["health"],
            "rollout_status": rollout_payload.get("status"),
            "recovery_path": "miswire_failure_then_successful_rebind",
            "sidecar_command": sidecar_runtime["command"],
            "sidecar_log_path": sidecar_runtime["log_path"],
        }
        write_json(run_dir / "recovery-validation.json", recovery_payload)

    write_json(
        run_dir / "negative-tests.json",
        {
            "generated_at": utc_now(),
            "tests": negative_tests,
        },
    )

    hash_targets = [
        run_dir / "environment.json",
        connector_preflight_path,
        run_dir / "build.json",
        run_dir / "test.json",
        run_dir / "publish.json",
        run_dir / "smoke.json",
        run_dir / "smoke-validation.json",
        run_dir / "connector-health-validation.json",
        connector_session_proof,
        run_dir / "rollout.json",
        run_dir / "recovery-validation.json",
        run_dir / "negative-tests.json",
        run_dir / "sidecar-process.log",
    ]
    if (run_dir / "disprover-miswire-rollout.json").exists():
        hash_targets.append(run_dir / "disprover-miswire-rollout.json")
    hash_targets.extend(path for path in publish_dir.rglob("*") if path.is_file())
    hash_entries = write_hash_manifest(
        base_dir=run_dir,
        targets=hash_targets,
        output_json=run_dir / "hashes.json",
        output_sha256=run_dir / "hashes.sha256",
    )
    verify_hash_manifest(base_dir=run_dir, entries=hash_entries)
    write_json(
        run_dir / "hash-validation.json",
        {
            "status": "ok",
            "validated_at": utc_now(),
            "entries_count": len(hash_entries),
        },
    )

    manifest_payload = {
        "schema_version": 1,
        "phase": PHASE_NAME,
        "phase_brief": PHASE_BRIEF,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "generated_at": utc_now(),
        "run_id": run_stamp,
        "git_sha": git_sha,
        "commands": [item.as_json() for item in step_records],
        "artifacts": {
            "run_dir": rel(run_dir),
            "environment": rel(run_dir / "environment.json"),
            "connector_preflight": rel(connector_preflight_path),
            "build": rel(run_dir / "build.json"),
            "test": rel(run_dir / "test.json"),
            "publish": rel(run_dir / "publish.json"),
            "smoke": rel(run_dir / "smoke.json"),
            "smoke_validation": rel(run_dir / "smoke-validation.json"),
            "connector_health_validation": rel(run_dir / "connector-health-validation.json"),
            "connector_session_proof": rel(run_dir / "connector-session-proof.json"),
            "rollout": rel(run_dir / "rollout.json"),
            "recovery_validation": rel(run_dir / "recovery-validation.json"),
            "negative_tests": rel(run_dir / "negative-tests.json"),
            "hashes_json": rel(run_dir / "hashes.json"),
            "hashes_sha256": rel(run_dir / "hashes.sha256"),
            "hash_validation": rel(run_dir / "hash-validation.json"),
            "publish_dir": rel(publish_dir),
        },
        "real_bindings": [
            f"Finam API base URL via {finam_base_url_env_var}: {finam_api_base_url}",
            f"Finam JWT via {finam_jwt_env_var}",
            "compiled stocksharp sidecar binary with connector session proof (session_id/binding_source/heartbeat)",
        ],
    }
    write_json(run_dir / "manifest.json", manifest_payload)
    print(json.dumps(manifest_payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
