from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


DEFAULT_CONFIG = Path("deployment/mcp/config.template.toml")
DEFAULT_MATRIX = Path("deployment/mcp/mcp-rollout-matrix.yaml")
DEFAULT_PROFILE = "ops"


def _resolve_command_path(command: str) -> str | None:
    if os.name == "nt":
        command = command.strip()
        if not command:
            return None
        if "." in Path(command).name:
            return shutil.which(command)
        for ext in (".exe", ".cmd", ".bat", ".com", ""):
            candidate = f"{command}{ext}"
            resolved = shutil.which(candidate)
            if resolved:
                return resolved
        return None
    return shutil.which(command)


def _probe_url_reachability(url_value: str, timeout_sec: float) -> tuple[bool, str | None]:
    parsed = urlparse(url_value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False, "invalid url format"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        with socket.create_connection((parsed.hostname, port), timeout=timeout_sec):
            return True, None
    except OSError as exc:
        return False, str(exc)


def _load_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("matrix must be YAML object")
    return payload


def _profile_servers(config: dict[str, Any], profile: str) -> list[str]:
    profiles_obj = config.get("profiles", {})
    if not isinstance(profiles_obj, dict):
        raise ValueError("config.profiles must be object")
    profile_obj = profiles_obj.get(profile)
    if not isinstance(profile_obj, dict):
        raise ValueError(f"profile not found: {profile}")
    servers = profile_obj.get("servers")
    if not isinstance(servers, list) or not servers:
        raise ValueError(f"profile {profile} servers must be non-empty list")
    return [str(item) for item in servers]


def run_preflight(
    *,
    config_path: Path,
    matrix_path: Path,
    profile: str,
    env: dict[str, str],
    strict_env_check: bool,
    probe_commands: bool,
    command_timeout_sec: float,
) -> tuple[int, dict[str, Any]]:
    config = _load_toml(config_path)
    matrix = _load_yaml(matrix_path)
    servers_matrix = matrix.get("servers", {})
    if not isinstance(servers_matrix, dict):
        raise ValueError("matrix.servers must be object")
    selected_servers = _profile_servers(config, profile)
    mcp_servers = config.get("mcp_servers", {})
    if not isinstance(mcp_servers, dict):
        raise ValueError("config.mcp_servers must be object")

    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for server_id in selected_servers:
        row: dict[str, Any] = {
            "server_id": server_id,
            "status": "ok",
            "missing_env": [],
            "command_available": False,
            "resolved_command": None,
            "transport": "unknown",
            "url_reachable": None,
            "probe_executed": False,
            "probe_return_code": None,
        }
        config_entry = mcp_servers.get(server_id)
        matrix_entry = servers_matrix.get(server_id)
        if not isinstance(config_entry, dict):
            row["status"] = "failed"
            errors.append(f"missing config for server: {server_id}")
            rows.append(row)
            continue
        if not isinstance(matrix_entry, dict):
            row["status"] = "failed"
            errors.append(f"missing matrix entry for server: {server_id}")
            rows.append(row)
            continue

        command = str(config_entry.get("command", "")).strip()
        url_value = str(config_entry.get("url", "")).strip()
        args = config_entry.get("args", [])
        required_env = matrix_entry.get("required_env", [])
        has_command = bool(command)
        has_url = bool(url_value)
        if has_command == has_url:
            row["status"] = "failed"
            errors.append(f"{server_id}: expected exactly one transport configuration (command or url)")
            rows.append(row)
            continue
        if args is not None and (not isinstance(args, list) or not all(isinstance(item, str) for item in args)):
            row["status"] = "failed"
            errors.append(f"{server_id}: args must be list[str] when provided")
            rows.append(row)
            continue
        if not isinstance(required_env, list) or not all(isinstance(item, str) for item in required_env):
            row["status"] = "failed"
            errors.append(f"{server_id}: matrix.required_env must be list[str]")
            rows.append(row)
            continue

        missing_env = [name for name in required_env if not str(env.get(name, "")).strip()]
        row["missing_env"] = missing_env
        if strict_env_check and missing_env:
            row["status"] = "failed"
            errors.append(f"{server_id}: missing env vars: {', '.join(missing_env)}")

        if has_command:
            row["transport"] = "stdio"
            if not isinstance(args, list):
                row["status"] = "failed"
                errors.append(f"{server_id}: args must be list[str] for command transport")
                rows.append(row)
                continue
            resolved_command = _resolve_command_path(command)
            row["resolved_command"] = resolved_command
            row["command_available"] = resolved_command is not None
            if not row["command_available"]:
                row["status"] = "failed"
                errors.append(f"{server_id}: command not found in PATH: {command}")

            if probe_commands and row["command_available"]:
                probe_args = config_entry.get("health_probe_args", [])
                if not isinstance(probe_args, list) or not all(isinstance(item, str) for item in probe_args):
                    row["status"] = "failed"
                    errors.append(f"{server_id}: health_probe_args must be list[str]")
                else:
                    row["probe_executed"] = True
                    try:
                        completed = subprocess.run(
                            [str(resolved_command), *probe_args],
                            check=False,
                            capture_output=True,
                            text=True,
                            timeout=command_timeout_sec,
                        )
                        row["probe_return_code"] = int(completed.returncode)
                        if completed.returncode != 0:
                            row["status"] = "failed"
                            errors.append(f"{server_id}: probe command failed with code {completed.returncode}")
                    except subprocess.TimeoutExpired:
                        row["status"] = "failed"
                        row["probe_return_code"] = -1
                        errors.append(f"{server_id}: probe command timed out after {command_timeout_sec:.1f}s")
            rows.append(row)
            continue

        row["transport"] = "streamable_http"
        parsed_url = urlparse(url_value)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            row["status"] = "failed"
            errors.append(f"{server_id}: invalid url transport entry: {url_value}")
            rows.append(row)
            continue
        row["command_available"] = True
        if probe_commands:
            row["probe_executed"] = True
            reachable, reason = _probe_url_reachability(url_value, command_timeout_sec)
            row["url_reachable"] = reachable
            row["probe_return_code"] = 0 if reachable else 1
            if not reachable:
                row["status"] = "failed"
                errors.append(f"{server_id}: url probe failed: {reason}")

        rows.append(row)

    payload = {
        "config": config_path.as_posix(),
        "matrix": matrix_path.as_posix(),
        "profile": profile,
        "servers": rows,
        "status": "ok" if not errors else "failed",
        "errors": errors,
    }
    return (0 if not errors else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run MCP preflight smoke checks.")
    parser.add_argument("--config", default=DEFAULT_CONFIG.as_posix())
    parser.add_argument("--matrix", default=DEFAULT_MATRIX.as_posix())
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--strict-env-check", action="store_true")
    parser.add_argument("--probe-commands", action="store_true")
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    code, payload = run_preflight(
        config_path=Path(args.config),
        matrix_path=Path(args.matrix),
        profile=args.profile,
        env=dict(os.environ),
        strict_env_check=args.strict_env_check,
        probe_commands=args.probe_commands,
        command_timeout_sec=max(args.timeout_sec, 0.1),
    )
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            "mcp preflight smoke: "
            f"{payload['status']} "
            f"(profile={payload['profile']}, servers={len(payload['servers'])})"
        )
        if payload["errors"]:
            for item in payload["errors"]:
                print(f"- {item}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
