from __future__ import annotations

import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CommandSpec:
    command: str
    stdin_text: str | None = None


def collect_changed_files(
    *,
    base_ref: str | None,
    head_ref: str | None,
    git_ref: str | None,
    from_git: bool,
    changed_files: list[str],
    from_stdin: bool,
) -> list[str]:
    files = list(changed_files)
    if base_ref and head_ref:
        diff = subprocess.run(
            ["git", "diff", "--name-only", f"{base_ref}..{head_ref}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if diff.returncode == 0:
            files.extend(line.strip() for line in diff.stdout.splitlines() if line.strip())
    elif from_git:
        ref = git_ref or "HEAD"
        diff = subprocess.run(
            ["git", "diff", "--name-only", ref],
            check=False,
            capture_output=True,
            text=True,
        )
        if diff.returncode == 0:
            files.extend(line.strip() for line in diff.stdout.splitlines() if line.strip())
        untracked = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            check=False,
            capture_output=True,
            text=True,
        )
        if untracked.returncode == 0:
            files.extend(line.strip() for line in untracked.stdout.splitlines() if line.strip())
    if from_stdin:
        files.extend(line.strip() for line in sys.stdin.read().splitlines() if line.strip())

    normalized: list[str] = []
    seen: set[str] = set()
    for item in files:
        candidate = item.replace("\\", "/").strip()
        marker = candidate.lower()
        if not marker or marker in seen:
            continue
        seen.add(marker)
        normalized.append(candidate)
    return normalized


def command_text(command: str | CommandSpec) -> str:
    if isinstance(command, CommandSpec):
        return command.command
    return command


def _join_command(parts: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(parts)
    return shlex.join(parts)


def run_command(command: str | CommandSpec) -> int:
    rendered = command_text(command)
    print(f">>> {rendered}", flush=True)
    completed = subprocess.run(
        rendered,
        shell=True,
        check=False,
        text=True,
        input=command.stdin_text if isinstance(command, CommandSpec) else None,
    )
    return int(completed.returncode)


def run_commands(commands: list[str | CommandSpec]) -> tuple[int, str | None]:
    for command in commands:
        code = run_command(command)
        if code != 0:
            return code, command_text(command)
    return 0, None


def is_non_trivial_diff(paths: list[str]) -> bool:
    if not paths:
        return False
    for path in paths:
        normalized = path.replace("\\", "/").strip().lower()
        if normalized.startswith("docs/") and normalized.endswith((".md", ".txt", ".rst")):
            continue
        return True
    return False


def _stdin_payload(changed_files: list[str]) -> str:
    if not changed_files:
        return ""
    return "\n".join(changed_files) + "\n"


def scope_validate_command(
    command: str,
    *,
    base_sha: str | None,
    head_sha: str | None,
    changed_files: list[str],
) -> str | CommandSpec:
    try:
        parts = shlex.split(command, posix=os.name != "nt")
    except ValueError:
        return command
    normalized = [token.replace("\\", "/").strip().lower() for token in parts]
    scoped_scripts = (
        "scripts/validate_task_request_contract.py",
        "scripts/validate_phase_planning_contract.py",
        "scripts/validate_task_outcomes.py",
        "scripts/validate_solution_intent.py",
        "scripts/validate_critical_contour_closure.py",
    )
    if not any(any(token.endswith(script_name) for token in normalized) for script_name in scoped_scripts):
        skill_precommit = "scripts/skill_precommit_gate.py"
        if not any(token.endswith(skill_precommit) for token in normalized):
            return command
        if "--changed-files" in normalized:
            return command
        scoped_parts = list(parts)
        if changed_files:
            scoped_parts.append("--changed-files")
            scoped_parts.extend(changed_files)
        if "--strict" not in normalized:
            scoped_parts.append("--strict")
        return _join_command(scoped_parts)

    if "--base-sha" in normalized or "--changed-files" in normalized:
        return command

    scoped_parts = list(parts)
    if base_sha and head_sha:
        scoped_parts.extend(["--base-sha", base_sha, "--head-sha", head_sha])
    elif changed_files:
        scoped_parts.append("--stdin")
        return CommandSpec(
            command=_join_command(scoped_parts),
            stdin_text=_stdin_payload(changed_files),
        )
    else:
        return command

    return _join_command(scoped_parts)


def write_summary(
    *,
    summary_file: str | None,
    gate_name: str,
    surface_result: dict[str, Any],
    commands: list[str | CommandSpec],
) -> None:
    if not summary_file:
        return
    summary_path = Path(summary_file)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"## {gate_name}",
        "",
        f"- Primary surface: `{surface_result['primary_surface']}`",
        f"- Surfaces: `{', '.join(surface_result['surfaces'])}`",
        f"- Docs only: `{surface_result['docs_only']}`",
        f"- Changed files: `{len(surface_result['changed_files'])}`",
        "",
        "### Commands",
    ]
    for command in commands:
        lines.append(f"- `{command_text(command)}`")
    lines.append("")
    with summary_path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
