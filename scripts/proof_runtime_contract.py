from __future__ import annotations

import os
import shlex
from pathlib import Path, PurePosixPath


DEFAULT_WORKSPACE_ROOT = "/workspace"


def resolve_repo_path(path: Path, *, repo_root: Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def normalize_runtime_root(raw_value: str, *, field_name: str) -> str:
    value = str(raw_value).strip()
    if not value:
        raise RuntimeError(f"{field_name} is required")
    normalized = value.replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    if not normalized.startswith("/"):
        raise RuntimeError(f"{field_name} must be an absolute container path: {normalized}")
    return normalized.rstrip("/") or "/"


def _is_within_runtime_workspace(*, normalized_path: str, runtime_workspace: str) -> bool:
    if runtime_workspace == "/":
        return normalized_path.startswith("/")
    return normalized_path == runtime_workspace or normalized_path.startswith(f"{runtime_workspace}/")


def host_to_container_path(path: Path, *, repo_root: Path, workspace_root: str = DEFAULT_WORKSPACE_ROOT) -> str:
    runtime_workspace = PurePosixPath(normalize_runtime_root(workspace_root, field_name="workspace root"))
    resolved = resolve_repo_path(path, repo_root=repo_root)
    try:
        relative = resolved.relative_to(repo_root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"path must stay inside repo for docker proof: {resolved.as_posix()}") from exc
    return runtime_workspace.joinpath(*relative.parts).as_posix()


def container_to_host_path(value: str, *, repo_root: Path, workspace_root: str = DEFAULT_WORKSPACE_ROOT) -> str:
    runtime_workspace = normalize_runtime_root(workspace_root, field_name="workspace root")
    normalized = str(value).strip().replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    if not _is_within_runtime_workspace(normalized_path=normalized, runtime_workspace=runtime_workspace):
        return value
    if runtime_workspace == "/":
        suffix = normalized.lstrip("/")
    else:
        suffix = normalized[len(runtime_workspace) :].lstrip("/")
    if not suffix:
        return repo_root.resolve().as_posix()
    parts = tuple(part for part in suffix.split("/") if part)
    return (repo_root.resolve() / Path(*parts)).resolve().as_posix()


def ensure_output_directory_writable(path: Path) -> None:
    if path.exists() and not path.is_dir():
        raise RuntimeError(f"output directory is not writable: expected directory, found file `{path.as_posix()}`")
    path.mkdir(parents=True, exist_ok=True)
    probe_path = path / ".write-probe"
    try:
        probe_path.write_text("probe\n", encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"output directory is not writable: `{path.as_posix()}` ({exc})") from exc
    finally:
        probe_path.unlink(missing_ok=True)


def ensure_output_file_writable(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.is_dir():
        raise RuntimeError(f"output path is not writable file target: `{path.as_posix()}`")
    try:
        with path.open("a", encoding="utf-8"):
            pass
    except OSError as exc:
        raise RuntimeError(f"output path is not writable: `{path.as_posix()}` ({exc})") from exc


def docker_host_owner() -> str:
    getuid = getattr(os, "getuid", None)
    getgid = getattr(os, "getgid", None)
    if not callable(getuid) or not callable(getgid):
        return ""
    return f"{getuid()}:{getgid()}"


def wrap_with_owner_normalization(
    *,
    command: list[str],
    owner: str,
    targets: list[str],
) -> list[str]:
    if not owner:
        return command
    python_text = " ".join(shlex.quote(part) for part in command)
    chown_targets = " ".join(shlex.quote(path_text) for path_text in targets)
    shell_text = (
        f"{python_text}; "
        "rc=$?; "
        f"chown -R {shlex.quote(owner)} {chown_targets} >/dev/null 2>&1 || true; "
        "exit $rc"
    )
    return ["/bin/sh", "-lc", shell_text]
