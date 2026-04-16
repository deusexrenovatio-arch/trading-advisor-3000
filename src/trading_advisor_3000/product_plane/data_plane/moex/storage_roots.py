from __future__ import annotations

import os
from pathlib import Path


MOEX_HISTORICAL_DATA_ROOT_ENV = "TA3000_MOEX_HISTORICAL_DATA_ROOT"
RAW_INGEST_STORAGE_DIRNAME = "moex-raw-ingest"
CANONICAL_REFRESH_STORAGE_DIRNAME = "moex-canonical-refresh"
ROUTE_REFRESH_STORAGE_DIRNAME = "moex-route-refresh"
DAGSTER_ROUTE_STORAGE_DIRNAME = "moex-dagster-route"
RECONCILIATION_STORAGE_DIRNAME = "moex-reconciliation"
STAGING_BINDING_STORAGE_DIRNAME = "moex-staging-binding"
OPERATIONS_READINESS_STORAGE_DIRNAME = "moex-operations-readiness"

RAW_INGEST_SUMMARY_REPORT_FILENAME = "raw-ingest-summary-report.json"
CANONICAL_REFRESH_REPORT_FILENAME = "canonical-refresh-report.json"
ROUTE_REFRESH_REPORT_FILENAME = "route-refresh-report.json"
DAGSTER_ROUTE_REPORT_FILENAME = "dagster-route-report.json"
STAGING_BINDING_REPORT_FILENAME = "staging-binding-report.json"
RECONCILIATION_REPORT_FILENAME = "reconciliation-report.json"
OPERATIONS_READINESS_REPORT_FILENAME = "operations-readiness-report.json"
RAW_INGEST_ACCEPTANCE_FILENAME = "raw-ingest-acceptance.json"
CANONICAL_REFRESH_ACCEPTANCE_FILENAME = "canonical-refresh-acceptance.json"
RECONCILIATION_ACCEPTANCE_FILENAME = "reconciliation-acceptance.json"


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
