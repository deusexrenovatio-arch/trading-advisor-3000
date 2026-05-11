from __future__ import annotations

# ruff: noqa: E501
import os
from dataclasses import dataclass
from pathlib import Path

MOEX_HISTORICAL_DATA_ROOT_ENV = "TA3000_MOEX_HISTORICAL_DATA_ROOT"
MOEX_PRODUCT_RUNTIME_STAGING_ROOT_ENV = "TA3000_MOEX_PRODUCT_RUNTIME_STAGING_ROOT"
MOEX_VERIFICATION_STAGING_ROOT_ENV = "TA3000_MOEX_VERIFICATION_STAGING_ROOT"
RAW_INGEST_STORAGE_DIRNAME = "moex-raw-ingest"
CANONICAL_REFRESH_STORAGE_DIRNAME = "moex-canonical-refresh"
ROUTE_REFRESH_STORAGE_DIRNAME = "moex-route-refresh"
DAGSTER_ROUTE_STORAGE_DIRNAME = "moex-dagster-route"
BASELINE_UPDATE_STORAGE_DIRNAME = "moex-baseline-update"
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

RAW_BASELINE_TABLE_RELATIVE_PATH = (
    Path("raw") / "moex" / "baseline-4y-current" / "raw_moex_history.delta"
)
CANONICAL_BASELINE_ROOT_RELATIVE_PATH = Path("canonical") / "moex" / "baseline-4y-current"
CANONICAL_BASELINE_BARS_FILENAME = "canonical_bars.delta"
CANONICAL_BASELINE_PROVENANCE_FILENAME = "canonical_bar_provenance.delta"
CANONICAL_BASELINE_SESSION_CALENDAR_FILENAME = "canonical_session_calendar.delta"
CANONICAL_BASELINE_ROLL_MAP_FILENAME = "canonical_roll_map.delta"
PRODUCT_RUNTIME_STAGING_RELATIVE_PATH = Path("staging") / "product-runtime"
VERIFICATION_STAGING_RELATIVE_PATH = Path("staging") / "verification"


@dataclass(frozen=True)
class MoexRuntimeStagingRoots:
    product_runtime_root: Path
    verification_root: Path


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


def _resolve_optional_external_root(
    *,
    env_var: str,
    default_path: Path,
    repo_root: Path,
) -> Path:
    raw = os.environ.get(env_var, "").strip()
    candidate = Path(raw) if raw else default_path
    try:
        return _ensure_external_path(path=candidate, repo_root=repo_root, field_name=env_var)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc


def _ensure_non_overlapping_roots(
    *, left: Path, left_name: str, right: Path, right_name: str
) -> None:
    if left == right or left in right.parents or right in left.parents:
        raise RuntimeError(
            f"`{left_name}` and `{right_name}` must not overlap: "
            f"{left.as_posix()} vs {right.as_posix()}"
        )


def configured_moex_runtime_staging_roots(*, repo_root: Path) -> MoexRuntimeStagingRoots:
    data_root = configured_moex_historical_data_root(repo_root=repo_root)
    product_runtime_root = _resolve_optional_external_root(
        env_var=MOEX_PRODUCT_RUNTIME_STAGING_ROOT_ENV,
        default_path=data_root / PRODUCT_RUNTIME_STAGING_RELATIVE_PATH,
        repo_root=repo_root,
    )
    verification_root = _resolve_optional_external_root(
        env_var=MOEX_VERIFICATION_STAGING_ROOT_ENV,
        default_path=data_root / VERIFICATION_STAGING_RELATIVE_PATH,
        repo_root=repo_root,
    )
    _ensure_non_overlapping_roots(
        left=product_runtime_root,
        left_name=MOEX_PRODUCT_RUNTIME_STAGING_ROOT_ENV,
        right=verification_root,
        right_name=MOEX_VERIFICATION_STAGING_ROOT_ENV,
    )
    return MoexRuntimeStagingRoots(
        product_runtime_root=product_runtime_root,
        verification_root=verification_root,
    )


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
