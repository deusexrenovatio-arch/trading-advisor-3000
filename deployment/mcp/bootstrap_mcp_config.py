from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any
import tomllib


DEFAULT_SOURCE = Path("deployment/mcp/config.template.toml")
DEFAULT_TARGET = Path(".codex/config.toml")
DEFAULT_HOME_TARGET = Path.home() / ".codex" / "config.toml"
_BARE_TOML_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _normalize_platform_commands(config_text: str) -> str:
    if os.name != "nt":
        return config_text

    normalized = config_text
    if shutil.which("npx.cmd"):
        normalized = normalized.replace('command = "npx"', 'command = "npx.cmd"')

    uvx_path = shutil.which("uvx") or shutil.which("uvx.exe")
    if not uvx_path:
        appdata = os.getenv("APPDATA", "")
        if appdata:
            candidate = (
                Path(appdata)
                / "Python"
                / f"Python{sys.version_info.major}{sys.version_info.minor}"
                / "Scripts"
                / "uvx.exe"
            )
            if candidate.exists():
                uvx_path = candidate.as_posix()

    if uvx_path:
        normalized_uvx = str(uvx_path).replace("\\", "/")
        normalized = normalized.replace('command = "uvx"', f'command = "{normalized_uvx}"')

    return normalized


def _load_toml_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8-sig"))


def _escape_toml_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _format_toml_key(key: str) -> str:
    if _BARE_TOML_KEY_RE.fullmatch(key):
        return key
    return f'"{_escape_toml_string(key)}"'


def _format_toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return f'"{_escape_toml_string(value)}"'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        items = [
            f"{_format_toml_key(str(key))} = {_format_toml_value(item)}"
            for key, item in value.items()
        ]
        return "{ " + ", ".join(items) + " }"
    if isinstance(value, list):
        return "[" + ", ".join(_format_toml_value(item) for item in value) + "]"
    raise TypeError(f"unsupported TOML value type: {type(value).__name__}")


def _dump_toml_document(document: dict[str, Any]) -> str:
    lines: list[str] = []

    def emit_table(path: list[str], payload: dict[str, Any]) -> None:
        scalar_items: list[tuple[str, Any]] = []
        nested_items: list[tuple[str, dict[str, Any]]] = []
        for key, value in payload.items():
            if isinstance(value, dict):
                nested_items.append((key, value))
            else:
                scalar_items.append((key, value))

        if path and scalar_items:
            if lines and lines[-1] != "":
                lines.append("")
            lines.append(f"[{'.'.join(_format_toml_key(part) for part in path)}]")
        for key, value in scalar_items:
            lines.append(f"{_format_toml_key(key)} = {_format_toml_value(value)}")
        for key, value in nested_items:
            emit_table([*path, key], value)

    emit_table([], document)
    return "\n".join(lines).strip() + "\n"


def merge_home_config_data(
    *,
    home_config: dict[str, Any],
    source_config: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(home_config)
    source_servers = source_config.get("mcp_servers", {})
    if not isinstance(source_servers, dict) or not source_servers:
        raise ValueError("source config must contain non-empty mcp_servers table")

    existing_servers = merged.get("mcp_servers", {})
    if existing_servers is None:
        existing_servers = {}
    if not isinstance(existing_servers, dict):
        raise ValueError("home config mcp_servers must be object when present")

    merged_servers = dict(existing_servers)
    for server_id, payload in source_servers.items():
        if not isinstance(payload, dict):
            raise ValueError(f"source mcp_servers.{server_id} must be object")
        merged_servers[str(server_id)] = dict(payload)
    merged["mcp_servers"] = merged_servers
    return merged


def run(
    *,
    source: Path,
    target: Path,
    force: bool,
    normalize_platform_commands: bool,
    merge_home_config: bool,
    home_target: Path,
) -> int:
    if not source.exists():
        raise FileNotFoundError(f"source config template not found: {source.as_posix()}")
    if target.exists() and not force:
        raise FileExistsError(
            f"target config already exists: {target.as_posix()} "
            "(use --force to overwrite)"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = source.read_text(encoding="utf-8")
    if normalize_platform_commands:
        payload = _normalize_platform_commands(payload)
    target.write_text(payload, encoding="utf-8")
    if merge_home_config:
        home_target.parent.mkdir(parents=True, exist_ok=True)
        merged_home = merge_home_config_data(
            home_config=_load_toml_document(home_target),
            source_config=tomllib.loads(payload),
        )
        home_target.write_text(_dump_toml_document(merged_home), encoding="utf-8")
    print(f"mcp config bootstrap completed: {target.as_posix()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap project-scoped MCP config from template.")
    parser.add_argument("--source", default=DEFAULT_SOURCE.as_posix())
    parser.add_argument("--target", default=DEFAULT_TARGET.as_posix())
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--no-platform-normalization",
        action="store_true",
        help="Disable platform-specific command normalization for generated config.",
    )
    parser.add_argument(
        "--merge-home-config",
        action="store_true",
        help="Merge MCP server entries into the active Codex user config.",
    )
    parser.add_argument(
        "--home-target",
        default=DEFAULT_HOME_TARGET.as_posix(),
        help="User-level Codex config path used with --merge-home-config.",
    )
    args = parser.parse_args()
    raise SystemExit(
        run(
            source=Path(args.source),
            target=Path(args.target),
            force=args.force,
            normalize_platform_commands=not args.no_platform_normalization,
            merge_home_config=args.merge_home_config,
            home_target=Path(args.home_target),
        )
    )


if __name__ == "__main__":
    main()
