from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG = Path("deployment/mcp/config.template.toml")
DEFAULT_MATRIX = Path("deployment/mcp/mcp-rollout-matrix.yaml")
DEFAULT_PROFILE = "ops"


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
        args = config_entry.get("args", [])
        required_env = matrix_entry.get("required_env", [])
        if not command:
            row["status"] = "failed"
            errors.append(f"{server_id}: empty command")
            rows.append(row)
            continue
        if not isinstance(args, list) or not all(isinstance(item, str) for item in args):
            row["status"] = "failed"
            errors.append(f"{server_id}: args must be list[str]")
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

        row["command_available"] = shutil.which(command) is not None
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
                completed = subprocess.run(
                    [command, *probe_args],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=command_timeout_sec,
                )
                row["probe_return_code"] = int(completed.returncode)
                if completed.returncode != 0:
                    row["status"] = "failed"
                    errors.append(f"{server_id}: probe command failed with code {completed.returncode}")

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
