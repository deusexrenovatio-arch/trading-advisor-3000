from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


CANONICAL_PROJECT_YML = """project_name: "trading-advisor-3000"
languages:
- python
encoding: "utf-8"
ignore_all_files_in_gitignore: true
ignored_paths:
- codex_ai_delivery_shell_package/**
- docs/tasks/archive/**
- memory/**
- plans/**
- artifacts/**
- .runlogs/**
- .tmp*/**
- graphify-out/**
read_only: false
initial_prompt: "For non-trivial code changes or new code inside an existing subsystem, use Serena first for code discovery, local pattern learning, impact analysis, and reference checks before broad scans, whole-file reads, or implementation. For new isolated files, inspect the closest existing module or pattern first unless the task is truly standalone. Skip Serena only for docs-only work, tiny already-localized edits, generated/artifact paths, config/non-code-only tasks, unsupported file types, or Serena unavailability; state the fallback reason briefly. Keep cold paths out of default exploration: codex_ai_delivery_shell_package, docs/tasks/archive, memory, plans, artifacts, .runlogs, and temp folders. Respect the dual-surface boundary: shell governance stays separate from product-plane app/data code."
"""

SERENA_GITIGNORE_LINES = ("cache/", "logs/", "memories/", "project.local.yml")

CANONICAL_MARKERS = (
    'project_name: "trading-advisor-3000"',
    "codex_ai_delivery_shell_package/**",
    "Respect the dual-surface boundary",
)


@dataclass
class BootstrapResult:
    worktree: Path
    project_name: str
    actions: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _canonical_config(text: str) -> bool:
    return all(marker in text for marker in CANONICAL_MARKERS)


def _normalize_for_compare(value: str) -> str:
    return value.replace("/", "\\").rstrip("\\").casefold()


def _default_serena_config_path() -> Path:
    return Path.home() / ".serena" / "serena_config.yml"


def _resolve_default_worktree() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0 and completed.stdout.strip():
        return Path(completed.stdout.strip()).resolve()
    return Path.cwd().resolve()


def derive_project_name(worktree: Path) -> str:
    token = worktree.name
    if token.casefold() == "trading advisor 3000":
        token = worktree.parent.name or "main"
    normalized = re.sub(r"[^a-z0-9]+", "-", token.casefold()).strip("-")
    if not normalized or normalized in {"d", "worktrees"}:
        normalized = "main"
    if normalized.startswith("ta3000-"):
        return normalized
    return f"ta3000-{normalized}"


def _write_project_config(project_yml: Path, *, check_only: bool, result: BootstrapResult) -> None:
    if not project_yml.exists():
        if check_only:
            result.errors.append(f"missing Serena project config: {project_yml}")
            return
        _write_text(project_yml, CANONICAL_PROJECT_YML)
        result.actions.append("created .serena/project.yml")
        return

    text = _read_text(project_yml)
    if _canonical_config(text):
        result.actions.append("kept canonical .serena/project.yml")
        return

    if check_only:
        result.errors.append(f"non-canonical Serena project config: {project_yml}")
        return

    backup = project_yml.with_name("project.yml.bak")
    if not backup.exists():
        _write_text(backup, text)
        result.actions.append("backed up non-canonical .serena/project.yml")
    _write_text(project_yml, CANONICAL_PROJECT_YML)
    result.actions.append("repaired .serena/project.yml")


def _ensure_serena_gitignore(gitignore: Path, *, check_only: bool, result: BootstrapResult) -> None:
    if not gitignore.exists():
        if check_only:
            result.errors.append(f"missing Serena gitignore: {gitignore}")
            return
        _write_text(gitignore, "\n".join(SERENA_GITIGNORE_LINES) + "\n")
        result.actions.append("created .serena/.gitignore")
        return

    text = _read_text(gitignore)
    lines = {line.strip().lstrip("/") for line in text.splitlines() if line.strip()}
    missing = [line for line in SERENA_GITIGNORE_LINES if line.rstrip("/") not in lines and line not in lines]
    if not missing:
        result.actions.append("kept .serena/.gitignore")
        return

    if check_only:
        result.errors.append(f"incomplete Serena gitignore: {gitignore}")
        return

    suffix = "" if text.endswith("\n") or not text else "\n"
    _write_text(gitignore, text + suffix + "\n".join(missing) + "\n")
    result.actions.append("updated .serena/.gitignore")


def _local_project_text(existing: str | None, project_name: str) -> str:
    project_line = f'project_name: "{project_name}"'
    if existing is None:
        return (
            "# Local Serena overrides for this worktree. Do not commit.\n"
            f"{project_line}\n"
        )

    lines = existing.splitlines()
    for index, line in enumerate(lines):
        if line.strip().startswith("project_name:"):
            lines[index] = project_line
            return "\n".join(lines).rstrip() + "\n"
    return f"{project_line}\n" + existing.rstrip() + "\n"


def _ensure_project_local(
    project_local: Path,
    *,
    project_name: str,
    check_only: bool,
    result: BootstrapResult,
) -> None:
    existing = _read_text(project_local) if project_local.exists() else None
    desired = _local_project_text(existing, project_name)
    if existing == desired:
        result.actions.append("kept .serena/project.local.yml")
        return

    if check_only:
        result.errors.append(f"missing or stale local Serena project name: {project_local}")
        return

    _write_text(project_local, desired)
    result.actions.append("updated .serena/project.local.yml")


def _project_registered(config_text: str, project_path: Path) -> bool:
    expected = _normalize_for_compare(str(project_path))
    for line in config_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        candidate = stripped[2:].strip().strip("'\"")
        if _normalize_for_compare(candidate) == expected:
            return True
    return False


def _register_project(
    serena_config: Path,
    *,
    worktree: Path,
    check_only: bool,
    result: BootstrapResult,
) -> None:
    if not serena_config.exists():
        result.actions.append(f"skipped missing global Serena config: {serena_config}")
        return

    text = _read_text(serena_config)
    if _project_registered(text, worktree):
        result.actions.append("kept global Serena project registration")
        return

    if check_only:
        result.errors.append(f"worktree is not registered in global Serena config: {worktree}")
        return

    lines = text.splitlines()
    project_line_index = next((index for index, line in enumerate(lines) if line.strip() == "projects:"), None)
    project_entry = f"- {worktree}"
    if project_line_index is None:
        new_text = text.rstrip() + "\n\nprojects:\n" + project_entry + "\n"
    else:
        insert_at = project_line_index + 1
        lines.insert(insert_at, project_entry)
        new_text = "\n".join(lines).rstrip() + "\n"
    _write_text(serena_config, new_text)
    result.actions.append("registered worktree in global Serena config")


def bootstrap(
    *,
    worktree: Path,
    project_name: str | None = None,
    serena_config: Path | None = None,
    register: bool = True,
    check_only: bool = False,
) -> BootstrapResult:
    resolved_worktree = worktree.resolve()
    derived_name = project_name or derive_project_name(resolved_worktree)
    result = BootstrapResult(worktree=resolved_worktree, project_name=derived_name)

    if not resolved_worktree.exists() or not resolved_worktree.is_dir():
        result.errors.append(f"worktree not found: {resolved_worktree}")
        return result

    serena_dir = resolved_worktree / ".serena"
    if check_only and not serena_dir.exists():
        result.errors.append(f"missing Serena folder: {serena_dir}")
    elif not check_only:
        serena_dir.mkdir(parents=True, exist_ok=True)

    _write_project_config(serena_dir / "project.yml", check_only=check_only, result=result)
    _ensure_serena_gitignore(serena_dir / ".gitignore", check_only=check_only, result=result)
    _ensure_project_local(
        serena_dir / "project.local.yml",
        project_name=derived_name,
        check_only=check_only,
        result=result,
    )
    if register:
        _register_project(
            serena_config or _default_serena_config_path(),
            worktree=resolved_worktree,
            check_only=check_only,
            result=result,
        )
    else:
        result.actions.append("skipped global Serena project registration")
    return result


def _print_text(result: BootstrapResult) -> None:
    print(f"serena worktree bootstrap: {'OK' if result.ok else 'FAILED'}")
    print(f"- worktree: {result.worktree}")
    print(f"- local project name: {result.project_name}")
    for action in result.actions:
        print(f"- {action}")
    for error in result.errors:
        print(f"- ERROR: {error}")
    print(f"- activate Serena by path: {result.worktree}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap Serena metadata for a TA3000 worktree.")
    parser.add_argument("--worktree", default=None, help="Worktree root. Defaults to the current git root.")
    parser.add_argument("--name", default=None, help="Local Serena project name. Defaults to ta3000-<worktree-id>.")
    parser.add_argument("--serena-config", default=None, help="Global serena_config.yml path.")
    parser.add_argument("--no-register", action="store_true", help="Do not update the global Serena project list.")
    parser.add_argument("--check", action="store_true", help="Validate without writing files.")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    result = bootstrap(
        worktree=Path(args.worktree).resolve() if args.worktree else _resolve_default_worktree(),
        project_name=args.name,
        serena_config=Path(args.serena_config).resolve() if args.serena_config else None,
        register=not args.no_register,
        check_only=args.check,
    )
    if args.format == "json":
        print(
            json.dumps(
                {
                    "status": "ok" if result.ok else "failed",
                    "worktree": str(result.worktree),
                    "project_name": result.project_name,
                    "actions": result.actions,
                    "errors": result.errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        _print_text(result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
