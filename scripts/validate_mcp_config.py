from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


DEFAULT_CONFIG = Path("deployment/mcp/config.template.toml")
DEFAULT_MATRIX = Path("deployment/mcp/mcp-rollout-matrix.yaml")
DEFAULT_MANIFEST = Path("deployment/mcp/rollout-manifest.yaml")
DEFAULT_RUNBOOK = Path("docs/runbooks/app/mcp-wave-rollout-runbook.md")
REQUIRED_PROFILES = ("base", "ops", "data_readonly")

_SUSPICIOUS_SECRET_PATTERNS = (
    re.compile(r"gh[pousr]_[A-Za-z0-9_]{10,}"),
    re.compile(r"sk-[A-Za-z0-9]{12,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN [A-Z ]+PRIVATE KEY-----"),
)


def _validate_url_contract(*, server_id: str, url_value: str) -> list[str]:
    errors: list[str] = []
    parsed = urlparse(url_value)
    if parsed.scheme not in {"http", "https"}:
        errors.append(f"mcp_servers.{server_id}.url must use http/https scheme")
    if not parsed.netloc:
        errors.append(f"mcp_servers.{server_id}.url must include hostname")
    if parsed.username or parsed.password:
        errors.append(f"mcp_servers.{server_id}.url must not embed credentials")
    return errors


def _load_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("matrix must be YAML object")
    return payload


def _check_secret_leaks(raw_text: str) -> list[str]:
    errors: list[str] = []
    for pattern in _SUSPICIOUS_SECRET_PATTERNS:
        if pattern.search(raw_text):
            errors.append(f"suspicious credential pattern matched: {pattern.pattern}")
    for line_number, line in enumerate(raw_text.splitlines(), start=1):
        normalized = line.strip().lower()
        if "${" in line:
            continue
        if any(token in normalized for token in ("token =", "password =", "secret =", "dsn =")):
            errors.append(f"potential inline secret assignment at line {line_number}")
    return errors


def validate(
    *,
    config_path: Path,
    matrix_path: Path,
    manifest_path: Path | None = None,
    runbook_path: Path | None = None,
) -> tuple[list[str], dict[str, Any]]:
    errors: list[str] = []
    if not config_path.exists():
        return [f"config template not found: {config_path.as_posix()}"], {}
    if not matrix_path.exists():
        return [f"rollout matrix not found: {matrix_path.as_posix()}"], {}

    config_text = config_path.read_text(encoding="utf-8")
    errors.extend(_check_secret_leaks(config_text))

    try:
        config = _load_toml(config_path)
    except Exception as exc:  # pragma: no cover - parse error path
        return [f"config parse error: {type(exc).__name__}: {exc}"], {}

    try:
        matrix = _load_yaml(matrix_path)
    except Exception as exc:  # pragma: no cover - parse error path
        return [f"matrix parse error: {type(exc).__name__}: {exc}"], {}
    manifest: dict[str, Any] = {}
    if manifest_path is not None:
        if not manifest_path.exists():
            errors.append(f"rollout manifest not found: {manifest_path.as_posix()}")
        else:
            try:
                manifest = _load_yaml(manifest_path)
            except Exception as exc:  # pragma: no cover - parse error path
                errors.append(f"manifest parse error: {type(exc).__name__}: {exc}")

    servers_obj = matrix.get("servers", {})
    if not isinstance(servers_obj, dict) or not servers_obj:
        errors.append("matrix.servers must be non-empty object")
        return errors, {}
    required_server_ids = sorted(str(item) for item in servers_obj.keys())

    project_obj = config.get("project", {})
    if not isinstance(project_obj, dict):
        errors.append("config.project must be object")
    else:
        if bool(project_obj.get("require_trusted")) is not True:
            errors.append("config.project.require_trusted must be true")

    profiles_obj = config.get("profiles", {})
    if not isinstance(profiles_obj, dict):
        errors.append("config.profiles must be object")
        profiles_obj = {}
    for profile_name in REQUIRED_PROFILES:
        profile_entry = profiles_obj.get(profile_name)
        if not isinstance(profile_entry, dict):
            errors.append(f"missing profile: profiles.{profile_name}")
            continue
        profile_servers = profile_entry.get("servers")
        if not isinstance(profile_servers, list) or not profile_servers:
            errors.append(f"profiles.{profile_name}.servers must be non-empty list")
            continue
        for item in profile_servers:
            if str(item) not in required_server_ids:
                errors.append(f"profiles.{profile_name} references unknown server: {item}")

    mcp_servers_obj = config.get("mcp_servers", {})
    if not isinstance(mcp_servers_obj, dict):
        errors.append("config.mcp_servers must be object")
        mcp_servers_obj = {}

    for server_id in required_server_ids:
        entry = mcp_servers_obj.get(server_id)
        if not isinstance(entry, dict):
            errors.append(f"missing mcp_servers.{server_id}")
            continue
        command = entry.get("command")
        url_value = entry.get("url")
        args = entry.get("args")
        required_env = entry.get("required_env")
        health_probe_args = entry.get("health_probe_args")
        if not isinstance(required_env, list) or not all(isinstance(item, str) for item in required_env):
            errors.append(f"mcp_servers.{server_id}.required_env must be list[str]")
        if not isinstance(health_probe_args, list) or not all(
            isinstance(item, str) for item in health_probe_args
        ):
            errors.append(f"mcp_servers.{server_id}.health_probe_args must be list[str]")
        has_command = isinstance(command, str) and bool(command.strip())
        has_url = isinstance(url_value, str) and bool(url_value.strip())
        if has_command == has_url:
            errors.append(
                f"mcp_servers.{server_id} must define exactly one transport entry: either command or url"
            )
            continue
        if has_command:
            if not isinstance(args, list) or not all(isinstance(item, str) for item in args):
                errors.append(f"mcp_servers.{server_id}.args must be list[str]")
            continue
        if args is not None and (not isinstance(args, list) or not all(isinstance(item, str) for item in args)):
            errors.append(f"mcp_servers.{server_id}.args must be list[str] when provided")
        errors.extend(_validate_url_contract(server_id=server_id, url_value=str(url_value)))

    if manifest:
        manifest_profiles = manifest.get("profiles", {})
        if not isinstance(manifest_profiles, dict):
            errors.append("manifest.profiles must be object")
            manifest_profiles = {}
        for profile_name in REQUIRED_PROFILES:
            manifest_profile = manifest_profiles.get(profile_name)
            if not isinstance(manifest_profile, dict):
                errors.append(f"manifest missing profile: profiles.{profile_name}")
                continue
            manifest_servers = manifest_profile.get("servers")
            if not isinstance(manifest_servers, list):
                errors.append(f"manifest profile servers must be list: profiles.{profile_name}")
                continue
            config_servers = profiles_obj.get(profile_name, {}).get("servers") if isinstance(profiles_obj, dict) else []
            if sorted(str(item) for item in manifest_servers) != sorted(str(item) for item in config_servers):
                errors.append(f"manifest profile mismatch: profiles.{profile_name}")
        wave_servers: set[str] = set()
        waves_obj = manifest.get("waves", {})
        if not isinstance(waves_obj, dict):
            errors.append("manifest.waves must be object")
            waves_obj = {}
        for wave_name, wave_payload in waves_obj.items():
            if not isinstance(wave_payload, dict):
                errors.append(f"manifest wave must be object: {wave_name}")
                continue
            servers = wave_payload.get("servers")
            if not isinstance(servers, list):
                errors.append(f"manifest wave servers must be list: {wave_name}")
                continue
            for item in servers:
                wave_servers.add(str(item))
        missing_from_waves = sorted(set(required_server_ids) - wave_servers)
        if missing_from_waves:
            errors.append("manifest waves missing servers: " + ", ".join(missing_from_waves))

    if runbook_path is not None:
        if not runbook_path.exists():
            errors.append(f"runbook not found: {runbook_path.as_posix()}")
        else:
            runbook_text = runbook_path.read_text(encoding="utf-8").lower()
            for server_id in required_server_ids:
                if server_id.lower() not in runbook_text:
                    errors.append(f"runbook does not mention server: {server_id}")

    report = {
        "config": config_path.as_posix(),
        "matrix": matrix_path.as_posix(),
        "manifest": manifest_path.as_posix() if manifest_path is not None else None,
        "required_server_ids": required_server_ids,
        "required_profiles": list(REQUIRED_PROFILES),
        "errors_total": len(errors),
    }
    return errors, report


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate MCP rollout config contract.")
    parser.add_argument("--config", default=DEFAULT_CONFIG.as_posix())
    parser.add_argument("--matrix", default=DEFAULT_MATRIX.as_posix())
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST.as_posix())
    parser.add_argument("--runbook", default=DEFAULT_RUNBOOK.as_posix())
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    runbook = Path(args.runbook) if args.runbook else None
    manifest = Path(args.manifest) if args.manifest else None
    errors, report = validate(
        config_path=Path(args.config),
        matrix_path=Path(args.matrix),
        manifest_path=manifest,
        runbook_path=runbook,
    )
    status = "ok" if not errors else "failed"
    payload = {**report, "status": status, "errors": errors}
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        if not errors:
            print(
                "mcp config validation: OK "
                f"(servers={len(report.get('required_server_ids', []))} "
                f"profiles={','.join(REQUIRED_PROFILES)})"
            )
        else:
            print("mcp config validation: FAILED")
            for item in errors:
                print(f"- {item}")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
