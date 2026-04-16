from __future__ import annotations

import os
from pathlib import Path


MOEX_HISTORICAL_DATA_ROOT_ENV = "TA3000_MOEX_HISTORICAL_DATA_ROOT"
PHASE01_STORAGE_DIRNAME = "moex-phase01"
PHASE02_STORAGE_DIRNAME = "moex-phase02"
NIGHTLY_STORAGE_DIRNAME = "moex-nightly"
PHASE03_STORAGE_DIRNAME = "moex-phase03"
PHASE03_STAGING_STORAGE_DIRNAME = "moex-phase03-staging"


def _ensure_external_path(*, path: Path, repo_root: Path, field_name: str) -> Path:
    candidate = path.expanduser()
    if not candidate.is_absolute():
        raise ValueError(f"`{field_name}` must be an absolute external path")
    resolved = candidate.resolve()
    normalized_repo_root = repo_root.resolve()
    if resolved == normalized_repo_root or normalized_repo_root in resolved.parents:
        raise ValueError(
            f"`{field_name}` must resolve outside the repository root: {normalized_repo_root.as_posix()}"
        )
    return resolved


def configured_moex_historical_data_root(*, repo_root: Path) -> Path:
    raw = os.environ.get(MOEX_HISTORICAL_DATA_ROOT_ENV, "").strip()
    if not raw:
        raise RuntimeError(
            "MOEX historical route requires an explicit external data root. "
            f"Set `{MOEX_HISTORICAL_DATA_ROOT_ENV}` to an absolute path outside the repository."
        )
    try:
        return _ensure_external_path(
            path=Path(raw),
            repo_root=repo_root,
            field_name=MOEX_HISTORICAL_DATA_ROOT_ENV,
        )
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc


def resolve_external_root(
    raw_path: str,
    *,
    repo_root: Path,
    field_name: str,
    default_subdir: str,
) -> Path:
    if str(raw_path).strip():
        return _ensure_external_path(
            path=Path(str(raw_path).strip()),
            repo_root=repo_root,
            field_name=field_name,
        )
    return configured_moex_historical_data_root(repo_root=repo_root) / default_subdir


def resolve_external_file_path(
    raw_path: str,
    *,
    repo_root: Path,
    field_name: str,
) -> Path:
    return _ensure_external_path(
        path=Path(str(raw_path).strip()),
        repo_root=repo_root,
        field_name=field_name,
    )
