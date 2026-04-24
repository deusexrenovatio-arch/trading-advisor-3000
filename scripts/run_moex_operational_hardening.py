from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex import run_phase04_production_hardening
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    CANONICAL_REFRESH_ACCEPTANCE_FILENAME,
    CANONICAL_REFRESH_REPORT_FILENAME,
    CANONICAL_REFRESH_STORAGE_DIRNAME,
    DAGSTER_ROUTE_STORAGE_DIRNAME,
    OPERATIONS_READINESS_REPORT_FILENAME,
    OPERATIONS_READINESS_STORAGE_DIRNAME,
    RAW_INGEST_ACCEPTANCE_FILENAME,
    RAW_INGEST_STORAGE_DIRNAME,
    RAW_INGEST_SUMMARY_REPORT_FILENAME,
    RECONCILIATION_STORAGE_DIRNAME,
    RECONCILIATION_ACCEPTANCE_FILENAME,
    RECONCILIATION_REPORT_FILENAME,
    resolve_external_file_path,
    resolve_external_root,
)

DEFAULT_SCHEDULER_POLICY = Path("configs/moex_phase04/scheduler_jobs.v1.yaml")
DEFAULT_MONITORING_POLICY = Path("configs/moex_phase04/monitoring_requirements.v1.yaml")
RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _pick_run_dir(root: Path, requested_run_id: str) -> Path:
    if requested_run_id.strip():
        run_dir = root / requested_run_id.strip()
        if not run_dir.exists():
            raise FileNotFoundError(f"run directory not found: {run_dir.as_posix()}")
        return run_dir

    candidates = [item for item in root.iterdir() if item.is_dir() and RUN_ID_PATTERN.match(item.name)]
    if not candidates:
        raise FileNotFoundError(
            "cannot auto-resolve run directory: no folders matching "
            f"{RUN_ID_PATTERN.pattern} under {root.as_posix()}"
        )
    return sorted(candidates, key=lambda item: item.name)[-1]


def _resolve_capability_report_path(
    *,
    explicit_report_path: str,
    root: Path,
    run_id: str,
    filename: str,
) -> tuple[Path, str]:
    if explicit_report_path.strip():
        return resolve_external_file_path(
            explicit_report_path,
            repo_root=ROOT,
            field_name=f"explicit {filename}",
        ), "explicit"

    run_dir = _pick_run_dir(root, run_id)
    return run_dir / filename, f"auto:{run_dir.name}"


def _require_source_path(raw: str, flag_name: str) -> Path:
    value = raw.strip()
    if not value:
        raise SystemExit(f"operations-readiness requires `{flag_name}`; no implicit source default is allowed")
    path = resolve_external_file_path(
        value,
        repo_root=ROOT,
        field_name=flag_name,
    )
    if not path.exists():
        raise FileNotFoundError(f"required source path does not exist: {path.as_posix()}")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Verify MOEX operations readiness: scheduler observability proof, "
            "recovery replay validation, monitoring evidence checks, and release decision bundle."
        )
    )

    parser.add_argument("--raw-ingest-report-path", default="")
    parser.add_argument("--raw-ingest-root", default="")
    parser.add_argument("--raw-ingest-run-id", default="")
    parser.add_argument("--raw-ingest-acceptance-path", default="")

    parser.add_argument("--canonical-report-path", default="")
    parser.add_argument("--canonical-root", default="")
    parser.add_argument("--canonical-run-id", default="")
    parser.add_argument("--canonical-acceptance-path", default="")

    parser.add_argument("--reconciliation-report-path", default="")
    parser.add_argument("--reconciliation-root", default="")
    parser.add_argument("--reconciliation-run-id", default="")
    parser.add_argument("--reconciliation-acceptance-path", default="")

    parser.add_argument("--scheduler-policy", default=DEFAULT_SCHEDULER_POLICY.as_posix())
    parser.add_argument("--monitoring-policy", default=DEFAULT_MONITORING_POLICY.as_posix())
    parser.add_argument("--scheduler-status-source-path", default="")
    parser.add_argument("--monitoring-evidence-source-path", default="")
    parser.add_argument("--recovery-drill-source-path", default="")
    parser.add_argument("--defects-source-path", default="")

    parser.add_argument("--output-root", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--route-signal", default="worker:capability-only")
    args = parser.parse_args()

    run_id = args.run_id.strip() or _default_run_id()

    phase01_path, phase01_resolution = _resolve_phase_report_path(
        explicit_report_path=args.raw_ingest_report_path,
        root=resolve_external_root(
            args.raw_ingest_root,
            repo_root=ROOT,
            field_name="--raw-ingest-root",
            default_subdir=RAW_INGEST_STORAGE_DIRNAME,
        ),
        run_id=args.raw_ingest_run_id,
        filename=RAW_INGEST_SUMMARY_REPORT_FILENAME,
    )
    phase02_path, phase02_resolution = _resolve_phase_report_path(
        explicit_report_path=args.canonical_report_path,
        root=resolve_external_root(
            args.canonical_root,
            repo_root=ROOT,
            field_name="--canonical-root",
            default_subdir=CANONICAL_REFRESH_STORAGE_DIRNAME,
        ),
        run_id=args.canonical_run_id,
        filename=CANONICAL_REFRESH_REPORT_FILENAME,
    )
    phase03_path, phase03_resolution = _resolve_phase_report_path(
        explicit_report_path=args.reconciliation_report_path,
        root=resolve_external_root(
            args.reconciliation_root,
            repo_root=ROOT,
            field_name="--reconciliation-root",
            default_subdir=RECONCILIATION_STORAGE_DIRNAME,
        ),
        run_id=args.reconciliation_run_id,
        filename=RECONCILIATION_REPORT_FILENAME,
    )
    phase01_acceptance_path = _require_source_path(
        args.raw_ingest_acceptance_path,
        "--raw-ingest-acceptance-path",
    )
    phase02_acceptance_path = _require_source_path(
        args.canonical_acceptance_path,
        "--canonical-acceptance-path",
    )
    phase03_acceptance_path = _require_source_path(
        args.reconciliation_acceptance_path,
        "--reconciliation-acceptance-path",
    )

    scheduler_policy_path = _resolve(Path(args.scheduler_policy))
    monitoring_policy_path = _resolve(Path(args.monitoring_policy))
    scheduler_status_source_path = _require_source_path(args.scheduler_status_source_path, "--scheduler-status-source-path")
    monitoring_evidence_source_path = _require_source_path(
        args.monitoring_evidence_source_path,
        "--monitoring-evidence-source-path",
    )
    recovery_drill_source_path = _require_source_path(args.recovery_drill_source_path, "--recovery-drill-source-path")
    defects_source_path = (
        resolve_external_file_path(
            args.defects_source_path.strip(),
            repo_root=ROOT,
            field_name="--defects-source-path",
        )
        if args.defects_source_path.strip()
        else None
    )
    if defects_source_path is not None and not defects_source_path.exists():
        raise FileNotFoundError(f"defects source path does not exist: {defects_source_path.as_posix()}")

    output_root = resolve_external_root(
        args.output_root,
        repo_root=ROOT,
        field_name="--output-root",
        default_subdir=OPERATIONS_READINESS_STORAGE_DIRNAME,
    )
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    report = run_moex_operational_hardening(
        raw_ingest_report_path=raw_ingest_path,
        canonicalization_report_path=canonicalization_path,
        reconciliation_report_path=reconciliation_path,
        raw_ingest_acceptance_path=raw_ingest_acceptance_path,
        canonicalization_acceptance_path=canonicalization_acceptance_path,
        reconciliation_acceptance_path=reconciliation_acceptance_path,
        scheduler_policy_path=scheduler_policy_path,
        scheduler_status_source_path=scheduler_status_source_path,
        monitoring_policy_path=monitoring_policy_path,
        monitoring_evidence_source_path=monitoring_evidence_source_path,
        recovery_drill_source_path=recovery_drill_source_path,
        defects_source_path=defects_source_path,
        output_dir=output_dir,
        run_id=run_id,
        route_signal=args.route_signal,
    )
    report["report_resolution"] = {
        "raw_ingest": phase01_resolution,
        "canonical_refresh": phase02_resolution,
        "reconciliation": phase03_resolution,
    }

    report_path = output_dir / OPERATIONS_READINESS_REPORT_FILENAME
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if report.get("status") != "PASS":
        raise SystemExit("operations-readiness verification blocked by fail-closed checks")


if __name__ == "__main__":
    main()
