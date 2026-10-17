"""Local reuse of successful, source-bound fast checks. CI always executes afresh."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from gate_common import CommandSpec, command_text, run_command

ROOT = Path(__file__).resolve().parents[1]
TTL_SECONDS = 600
SCHEMA = "local-fast-check.v1"


def _fingerprint(root: Path, scope: dict, command: str | CommandSpec) -> str:
    tracked = (
        subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=root,
            check=True,
            capture_output=True,
        )
        .stdout.decode("utf-8")
        .split("\0")
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    selectors = {}
    for name in ("base_ref", "head_ref", "git_ref"):
        if scope.get(name):
            selectors[name] = subprocess.run(
                ["git", "rev-parse", "--verify", scope[name]],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
    digest = hashlib.sha256()
    inputs = {
        "schema": SCHEMA,
        "root": str(root.resolve()),
        "head": head,
        "selectors": selectors,
        "scope": scope,
        "command": command_text(command),
        "stdin": command.stdin_text if isinstance(command, CommandSpec) else None,
        "python": [sys.executable, sys.version, platform.platform()],
        "packages": sorted(
            (d.metadata.get("Name", ""), d.version) for d in importlib.metadata.distributions()
        ),
        "environment": sorted((k, v) for k, v in os.environ.items() if k.upper() != "PROMPT"),
    }
    # Store only the digest, never environment values or credentials.
    digest.update(json.dumps(inputs, sort_keys=True).encode())
    for name in sorted(set(tracked) - {""}):
        path = root / name
        if not path.resolve().is_relative_to(root.resolve()) or path.is_symlink():
            raise ValueError("external or symbolic input")
        digest.update(name.encode())
        digest.update(str(path.stat().st_mode).encode() if path.exists() else b"missing")
        if path.is_dir():
            raise ValueError("directory or submodule input")
        if path.is_file():
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _eligible(command: str | CommandSpec) -> bool:
    # Explicit allowlist: numerical/native proofs, live checks and PR-only checks
    # are never reusable through this mechanism.
    try:
        parts = shlex.split(command_text(command), posix=os.name != "nt")
    except ValueError:
        return False
    parts = [p.strip('"').replace("\\", "/") for p in parts]
    if len(parts) < 2 or parts[1] != "scripts/run_boring_checks.py":
        return False
    if "--fix" in parts or any(
        token in command_text(command) for token in (";", "&&", "||", "|", ">", "\n")
    ):
        return False
    return (
        "scripts/run_boring_checks.py" in parts
        and parts.count("--profile") == 1
        and parts.count("--scope") == 1
        and "--profile" in parts
        and parts.index("--profile") + 1 < len(parts)
        and parts[parts.index("--profile") + 1] == "quick"
        and "--scope" in parts
        and parts.index("--scope") + 1 < len(parts)
        and parts[parts.index("--scope") + 1] == "changed"
    )


def _ci() -> bool:
    return any(
        os.environ.get(k, "").lower() not in ("", "0", "false", "no")
        for k in ("CI", "GITHUB_ACTIONS")
    )


def run_loop_commands(commands, *, scope: dict, reuse: bool = True, root: Path = ROOT):
    for command in commands:
        key = None
        receipt = None
        if reuse and not _ci() and _eligible(command):
            try:
                cache = root / ".tmp" / "gate-reuse"
                if not cache.resolve().is_relative_to(root.resolve()):
                    raise ValueError("external cache")
                key = _fingerprint(root, scope, command)
                receipt = cache / f"{key}.json"
                if receipt.exists():
                    saved = json.loads(receipt.read_text(encoding="utf-8"))
                    age = time.time() - saved["created_at"]
                    if (
                        saved.get("schema") == SCHEMA
                        and saved.get("key") == key
                        and saved.get("exit_code") == 0
                        and 0 <= age <= TTL_SECONDS
                    ):
                        print(f"loop reuse: HIT ({command_text(command)})", flush=True)
                        continue
            except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
                key = None
        code = run_command(command)
        if code:
            return code, command_text(command)
        if key and receipt:
            try:
                # A formatter/check that changes an input cannot certify the old key.
                if _fingerprint(root, scope, command) == key:
                    receipt.parent.mkdir(parents=True, exist_ok=True)
                    with tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        dir=receipt.parent,
                        delete=False,
                    ) as handle:
                        json.dump(
                            {
                                "schema": SCHEMA,
                                "key": key,
                                "exit_code": 0,
                                "created_at": time.time(),
                            },
                            handle,
                        )
                        temporary = Path(handle.name)
                    temporary.replace(receipt)
            except (OSError, ValueError, subprocess.SubprocessError):
                pass  # Missing/unwritable cache never prevents a fresh gate.
    return 0, None
