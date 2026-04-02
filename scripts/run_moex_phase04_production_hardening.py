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


DEFAULT_PHASE01_ROOT = Path("artifacts/codex/moex-phase01")
DEFAULT_PHASE02_ROOT = Path("artifacts/codex/moex-phase02")
DEFAULT_PHASE03_ROOT = Path("artifacts/codex/moex-phase03")
DEFAULT_SCHEDULER_POLICY = Path("configs/moex_phase04/scheduler_jobs.v1.yaml")
DEFAULT_MONITORING_POLICY = Path("configs/moex_phase04/monitoring_requirements.v1.yaml")
DEFAULT_OUTPUT_ROOT = Path("artifacts/codex/moex-phase04")
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


def _resolve_phase_report_path(
    *,
    explicit_report_path: str,
    root: Path,
    run_id: str,
    filename: str,
) -> tuple[Path, str]:
    if explicit_report_path.strip():
        return _resolve(Path(explicit_report_path.strip())), "explicit"

    run_dir = _pick_run_dir(root, run_id)
    return run_dir / filename, f"auto:{run_dir.name}"


def _require_source_path(raw: str, flag_name: str) -> Path:
    value = raw.strip()
    if not value:
        raise SystemExit(f"phase-04 requires `{flag_name}`; no implicit fallback is allowed")
    path = _resolve(Path(value))
    if not path.exists():
        raise FileNotFoundError(f"required source path does not exist: {path.as_posix()}")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run MOEX Phase-04 Production Hardening contour: scheduler observability proof, "
            "recovery replay validation, monitoring evidence checks, and release decision bundle."
        )
    )

    parser.add_argument("--phase01-report-path", default="")
    parser.add_argument("--phase01-root", default=DEFAULT_PHASE01_ROOT.as_posix())
    parser.add_argument("--phase01-run-id", default="")
    parser.add_argument("--phase01-acceptance-path", default="")

    parser.add_argument("--phase02-report-path", default="")
    parser.add_argument("--phase02-root", default=DEFAULT_PHASE02_ROOT.as_posix())
    parser.add_argument("--phase02-run-id", default="")
    parser.add_argument("--phase02-acceptance-path", default="")

    parser.add_argument("--phase03-report-path", default="")
    parser.add_argument("--phase03-root", default=DEFAULT_PHASE03_ROOT.as_posix())
    parser.add_argument("--phase03-run-id", default="")
    parser.add_argument("--phase03-acceptance-path", default="")

    parser.add_argument("--scheduler-policy", default=DEFAULT_SCHEDULER_POLICY.as_posix())
    parser.add_argument("--monitoring-policy", default=DEFAULT_MONITORING_POLICY.as_posix())
    parser.add_argument("--scheduler-status-source-path", default="")
    parser.add_argument("--monitoring-evidence-source-path", default="")
    parser.add_argument("--recovery-drill-source-path", default="")
    parser.add_argument("--defects-source-path", default="")

    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT.as_posix())
    parser.add_argument("--run-id", default="")
    parser.add_argument("--route-signal", default="worker:phase-only")
    args = parser.parse_args()

    run_id = args.run_id.strip() or _default_run_id()

    phase01_path, phase01_resolution = _resolve_phase_report_path(
        explicit_report_path=args.phase01_report_path,
        root=_resolve(Path(args.phase01_root)),
        run_id=args.phase01_run_id,
        filename="phase01-foundation-report.json",
    )
    phase02_path, phase02_resolution = _resolve_phase_report_path(
        explicit_report_path=args.phase02_report_path,
        root=_resolve(Path(args.phase02_root)),
        run_id=args.phase02_run_id,
        filename="phase02-canonical-report.json",
    )
    phase03_path, phase03_resolution = _resolve_phase_report_path(
        explicit_report_path=args.phase03_report_path,
        root=_resolve(Path(args.phase03_root)),
        run_id=args.phase03_run_id,
        filename="phase03-reconciliation-report.json",
    )
    phase01_acceptance_path = _require_source_path(args.phase01_acceptance_path, "--phase01-acceptance-path")
    phase02_acceptance_path = _require_source_path(args.phase02_acceptance_path, "--phase02-acceptance-path")
    phase03_acceptance_path = _require_source_path(args.phase03_acceptance_path, "--phase03-acceptance-path")

    scheduler_policy_path = _resolve(Path(args.scheduler_policy))
    monitoring_policy_path = _resolve(Path(args.monitoring_policy))
    scheduler_status_source_path = _require_source_path(args.scheduler_status_source_path, "--scheduler-status-source-path")
    monitoring_evidence_source_path = _require_source_path(
        args.monitoring_evidence_source_path,
        "--monitoring-evidence-source-path",
    )
    recovery_drill_source_path = _require_source_path(args.recovery_drill_source_path, "--recovery-drill-source-path")
    defects_source_path = _resolve(Path(args.defects_source_path.strip())) if args.defects_source_path.strip() else None
    if defects_source_path is not None and not defects_source_path.exists():
        raise FileNotFoundError(f"defects source path does not exist: {defects_source_path.as_posix()}")

    output_root = _resolve(Path(args.output_root))
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    report = run_phase04_production_hardening(
        phase01_report_path=phase01_path,
        phase02_report_path=phase02_path,
        phase03_report_path=phase03_path,
        phase01_acceptance_path=phase01_acceptance_path,
        phase02_acceptance_path=phase02_acceptance_path,
        phase03_acceptance_path=phase03_acceptance_path,
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
    report["phase_report_resolution"] = {
        "phase01": phase01_resolution,
        "phase02": phase02_resolution,
        "phase03": phase03_resolution,
    }

    report_path = output_dir / "phase04-production-hardening-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if report.get("status") != "PASS":
        raise SystemExit("phase-04 production hardening contour blocked by fail-closed checks")


if __name__ == "__main__":
    main()
