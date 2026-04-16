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

from trading_advisor_3000.product_plane.data_plane.moex import run_phase03_reconciliation
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    PHASE02_STORAGE_DIRNAME,
    PHASE03_STORAGE_DIRNAME,
    resolve_external_file_path,
    resolve_external_root,
)

DEFAULT_MAPPING_REGISTRY = Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml")
DEFAULT_THRESHOLD_POLICY = Path("configs/moex_phase03/reconciliation_thresholds.v1.yaml")
RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _pick_phase02_run_dir(phase02_root: Path, requested_run_id: str) -> Path:
    if requested_run_id.strip():
        run_dir = phase02_root / requested_run_id.strip()
        if not run_dir.exists():
            raise FileNotFoundError(f"phase-02 run directory not found: {run_dir.as_posix()}")
        return run_dir

    candidates = [
        item
        for item in phase02_root.iterdir()
        if item.is_dir() and RUN_ID_PATTERN.match(item.name)
    ]
    if not candidates:
        raise FileNotFoundError(
            "cannot auto-resolve phase-02 run directory: no run folders matching "
            f"{RUN_ID_PATTERN.pattern} under {phase02_root.as_posix()}"
        )
    return sorted(candidates, key=lambda item: item.name)[-1]


def _resolve_phase02_paths(
    *,
    canonical_bars_path: str,
    canonical_provenance_path: str,
    phase02_root: Path,
    phase02_run_id: str,
) -> tuple[Path, Path, str]:
    if canonical_bars_path.strip() and canonical_provenance_path.strip():
        bars = resolve_external_file_path(
            canonical_bars_path,
            repo_root=ROOT,
            field_name="--canonical-bars-path",
        )
        provenance = resolve_external_file_path(
            canonical_provenance_path,
            repo_root=ROOT,
            field_name="--canonical-provenance-path",
        )
        return bars, provenance, "explicit"

    run_dir = _pick_phase02_run_dir(phase02_root, phase02_run_id)
    bars = run_dir / "delta" / "canonical_bars.delta"
    provenance = run_dir / "delta" / "canonical_bar_provenance.delta"
    return bars, provenance, f"phase02:{run_dir.name}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run MOEX Phase-03 Reconciliation contour: Finam archive ingest, overlap drift metrics, "
            "threshold-driven alert simulation, and fail-closed publish decision."
        )
    )
    parser.add_argument(
        "--finam-archive-source-path",
        default="",
        help="Path to Finam archive snapshots (.json/.csv/delta). Required for phase-03 run.",
    )
    parser.add_argument(
        "--canonical-bars-path",
        default="",
        help="Path to phase-02 canonical_bars.delta. If omitted, resolved from --phase02-root.",
    )
    parser.add_argument(
        "--canonical-provenance-path",
        default="",
        help="Path to phase-02 canonical_bar_provenance.delta. If omitted, resolved from --phase02-root.",
    )
    parser.add_argument(
        "--phase02-root",
        default="",
        help=(
            "Absolute external phase-02 artifact root used when explicit canonical paths are omitted. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument(
        "--phase02-run-id",
        default="",
        help="Specific phase-02 run folder name (YYYYMMDDTHHMMSSZ). If omitted, latest run is used.",
    )
    parser.add_argument(
        "--mapping-registry",
        default=DEFAULT_MAPPING_REGISTRY.as_posix(),
        help="Path to active MOEX mapping registry.",
    )
    parser.add_argument(
        "--threshold-policy",
        default=DEFAULT_THRESHOLD_POLICY.as_posix(),
        help="Path to phase-03 threshold policy file.",
    )
    parser.add_argument(
        "--output-root",
        default="",
        help=(
            "Absolute external root folder for phase-03 reconciliation artifacts. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; defaults to current UTC timestamp.",
    )
    parser.add_argument(
        "--allow-degraded-publish",
        action="store_true",
        help="Allow degraded publish with incident trace instead of hard block on threshold breaches.",
    )
    args = parser.parse_args()

    finam_source_raw = args.finam_archive_source_path.strip()
    if not finam_source_raw:
        raise SystemExit("phase-03 requires --finam-archive-source-path; no implicit synthetic fallback is allowed")

    run_id = args.run_id.strip() or _default_run_id()
    phase02_root = resolve_external_root(
        args.phase02_root,
        repo_root=ROOT,
        field_name="--phase02-root",
        default_subdir=PHASE02_STORAGE_DIRNAME,
    )
    canonical_bars_path, canonical_provenance_path, source_resolution = _resolve_phase02_paths(
        canonical_bars_path=args.canonical_bars_path,
        canonical_provenance_path=args.canonical_provenance_path,
        phase02_root=phase02_root,
        phase02_run_id=args.phase02_run_id,
    )
    mapping_registry = _resolve(Path(args.mapping_registry))
    threshold_policy = _resolve(Path(args.threshold_policy))
    finam_source = resolve_external_file_path(
        finam_source_raw,
        repo_root=ROOT,
        field_name="--finam-archive-source-path",
    )
    output_root = resolve_external_root(
        args.output_root,
        repo_root=ROOT,
        field_name="--output-root",
        default_subdir=PHASE03_STORAGE_DIRNAME,
    )
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    report = run_phase03_reconciliation(
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        finam_archive_source_path=finam_source,
        threshold_policy_path=threshold_policy,
        mapping_registry_path=mapping_registry,
        output_dir=output_dir,
        run_id=run_id,
        allow_degraded_publish=bool(args.allow_degraded_publish),
    )
    report["canonical_source_resolution"] = source_resolution

    report_path = output_dir / "phase03-reconciliation-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if str(report.get("status")) == "BLOCKED":
        raise SystemExit("phase-03 reconciliation contour blocked by fail-closed thresholds")


if __name__ == "__main__":
    main()

