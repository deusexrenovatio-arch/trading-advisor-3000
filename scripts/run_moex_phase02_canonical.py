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

from trading_advisor_3000.product_plane.data_plane.moex import run_phase02_canonical
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    PHASE01_STORAGE_DIRNAME,
    PHASE02_STORAGE_DIRNAME,
    resolve_external_file_path,
    resolve_external_root,
)

RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _pick_phase01_run_dir(phase01_root: Path, requested_run_id: str) -> Path:
    if requested_run_id.strip():
        run_dir = phase01_root / requested_run_id.strip()
        if not run_dir.exists():
            raise FileNotFoundError(f"raw-ingest run directory not found: {run_dir.as_posix()}")
        return run_dir

    candidates = [
        item
        for item in phase01_root.iterdir()
        if item.is_dir() and RUN_ID_PATTERN.match(item.name)
    ]
    if not candidates:
        raise FileNotFoundError(
            "cannot auto-resolve raw-ingest run directory: no run folders matching "
            f"{RUN_ID_PATTERN.pattern} under {phase01_root.as_posix()}"
        )
    return sorted(candidates, key=lambda item: item.name)[-1]


def _resolve_raw_table_path(
    *,
    raw_table_path: str,
    phase01_root: Path,
    phase01_run_id: str,
) -> tuple[Path, str]:
    if raw_table_path.strip():
        path = resolve_external_file_path(
            raw_table_path,
            repo_root=ROOT,
            field_name="--raw-table-path",
        )
        return path, "explicit"

    run_dir = _pick_phase01_run_dir(phase01_root, phase01_run_id)
    path = run_dir / "delta" / "raw_moex_history.delta"
    return path, f"phase01:{run_dir.name}"


def _resolve_raw_ingest_report_path(
    *,
    raw_ingest_report_path: str,
    phase01_root: Path,
    phase01_run_id: str,
) -> tuple[Path, str]:
    if raw_ingest_report_path.strip():
        path = resolve_external_file_path(
            raw_ingest_report_path,
            repo_root=ROOT,
            field_name="--raw-ingest-report-path",
        )
        return path, "explicit"

    run_dir = _pick_phase01_run_dir(phase01_root, phase01_run_id)
    candidates = (
        run_dir / "raw-ingest-report.pass1.json",
        run_dir / "raw-ingest-report.json",
        run_dir / "raw-ingest-report.pass2.json",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate, f"phase01:{run_dir.name}:{candidate.name}"
    raise FileNotFoundError(
        "canonicalization requires raw-ingest report; checked: "
        + ", ".join(item.as_posix() for item in candidates)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run MOEX Spark canonicalization contour as a legacy migration canonical rebuild/repair aid: "
            "Spark resampling (5m/15m/1h/4h/1d/1w), fail-closed QC, contract compatibility check, "
            "and runtime decoupling proof."
        )
    )
    parser.add_argument(
        "--raw-table-path",
        default="",
        help="Path to legacy raw-ingest raw_moex_history.delta. If omitted, resolved from --phase01-root.",
    )
    parser.add_argument(
        "--phase01-root",
        default="",
        help=(
            "Absolute external phase-01 artifact root used when --raw-table-path is omitted. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument(
        "--phase01-run-id",
        default="",
        help="Specific raw-ingest run folder name (YYYYMMDDTHHMMSSZ). If omitted, latest run is used.",
    )
    parser.add_argument(
        "--raw-ingest-report-path",
        default="",
        help="Path to raw-ingest report JSON. If omitted, resolved from --phase01-root run folder.",
    )
    parser.add_argument(
        "--output-root",
        default="",
        help=(
            "Absolute external root folder for canonicalization run artifacts. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; defaults to current UTC timestamp.",
    )
    args = parser.parse_args()

    print(
        "route-note: scripts/run_moex_phase02_canonical.py is a legacy Spark canonicalization rebuild/repair aid. "
        "It is not the canonical operator-facing scheduled route after Dagster cutover.",
        flush=True,
    )

    run_id = args.run_id.strip() or _default_run_id()
    phase01_root = resolve_external_root(
        args.phase01_root,
        repo_root=ROOT,
        field_name="--phase01-root",
        default_subdir=PHASE01_STORAGE_DIRNAME,
    )
    raw_table_path, raw_source = _resolve_raw_table_path(
        raw_table_path=args.raw_table_path,
        phase01_root=phase01_root,
        phase01_run_id=args.phase01_run_id,
    )
    raw_ingest_report_path, raw_ingest_source = _resolve_raw_ingest_report_path(
        raw_ingest_report_path=args.raw_ingest_report_path,
        phase01_root=phase01_root,
        phase01_run_id=args.phase01_run_id,
    )
    raw_ingest_report_payload = json.loads(raw_ingest_report_path.read_text(encoding="utf-8"))
    if not isinstance(raw_ingest_report_payload, dict):
        raise ValueError(
            f"canonicalization raw-ingest report must be JSON object: {raw_ingest_report_path.as_posix()}"
        )
    output_root = resolve_external_root(
        args.output_root,
        repo_root=ROOT,
        field_name="--output-root",
        default_subdir=PHASE02_STORAGE_DIRNAME,
    )
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    report = run_phase02_canonical(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id=run_id,
        raw_ingest_run_report=raw_ingest_report_payload,
        repo_root=ROOT,
    )
    report["raw_source_resolution"] = raw_source
    report["raw_ingest_report_source_resolution"] = raw_ingest_source
    report["raw_ingest_report_path"] = raw_ingest_report_path.as_posix()

    report_path = output_dir / "phase02-canonical-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if report.get("publish_decision") != "publish":
        raise SystemExit("canonicalization contour blocked by fail-closed checks")


if __name__ == "__main__":
    main()
