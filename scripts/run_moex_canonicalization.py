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

from trading_advisor_3000.product_plane.data_plane.moex import run_moex_canonicalization


DEFAULT_RAW_INGEST_ROOT = Path("artifacts/codex/moex-raw-ingest")
DEFAULT_OUTPUT_ROOT = Path("artifacts/codex/moex-canonicalization")
RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _pick_raw_ingest_run_dir(raw_ingest_root: Path, requested_run_id: str) -> Path:
    if requested_run_id.strip():
        run_dir = raw_ingest_root / requested_run_id.strip()
        if not run_dir.exists():
            raise FileNotFoundError(f"raw-ingest run directory not found: {run_dir.as_posix()}")
        return run_dir

    candidates = [
        item
        for item in raw_ingest_root.iterdir()
        if item.is_dir() and RUN_ID_PATTERN.match(item.name)
    ]
    if not candidates:
        raise FileNotFoundError(
            "cannot auto-resolve raw-ingest run directory: no run folders matching "
            f"{RUN_ID_PATTERN.pattern} under {raw_ingest_root.as_posix()}"
        )
    return sorted(candidates, key=lambda item: item.name)[-1]


def _resolve_raw_table_path(
    *,
    raw_table_path: str,
    raw_ingest_root: Path,
    raw_ingest_run_id: str,
) -> tuple[Path, str]:
    if raw_table_path.strip():
        path = _resolve(Path(raw_table_path.strip()))
        return path, "explicit"

    run_dir = _pick_raw_ingest_run_dir(raw_ingest_root, raw_ingest_run_id)
    path = run_dir / "delta" / "raw_moex_history.delta"
    return path, f"raw-ingest:{run_dir.name}"


def _resolve_raw_ingest_report_path(
    *,
    raw_ingest_report_path: str,
    raw_ingest_root: Path,
    raw_ingest_run_id: str,
) -> tuple[Path, str]:
    if raw_ingest_report_path.strip():
        path = _resolve(Path(raw_ingest_report_path.strip()))
        return path, "explicit"

    run_dir = _pick_raw_ingest_run_dir(raw_ingest_root, raw_ingest_run_id)
    candidates = (
        run_dir / "raw-ingest-report.pass1.json",
        run_dir / "raw-ingest-report.json",
        run_dir / "raw-ingest-report.pass2.json",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate, f"raw-ingest:{run_dir.name}:{candidate.name}"
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
        help="Path to legacy raw-ingest raw_moex_history.delta. If omitted, resolved from --raw-ingest-root.",
    )
    parser.add_argument(
        "--raw-ingest-root",
        default=DEFAULT_RAW_INGEST_ROOT.as_posix(),
        help="Legacy raw-ingest artifact root used when --raw-table-path is omitted.",
    )
    parser.add_argument(
        "--raw-ingest-run-id",
        default="",
        help="Specific raw-ingest run folder name (YYYYMMDDTHHMMSSZ). If omitted, latest run is used.",
    )
    parser.add_argument(
        "--raw-ingest-report-path",
        default="",
        help="Path to raw-ingest report JSON. If omitted, resolved from --raw-ingest-root run folder.",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT.as_posix(),
        help="Root folder for canonicalization run artifacts.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; defaults to current UTC timestamp.",
    )
    args = parser.parse_args()

    print(
        "route-note: scripts/run_moex_canonicalization.py is a legacy Spark canonicalization rebuild/repair aid. "
        "It is not the canonical operator-facing scheduled route after Dagster cutover.",
        flush=True,
    )

    run_id = args.run_id.strip() or _default_run_id()
    raw_ingest_root = _resolve(Path(args.raw_ingest_root))
    raw_table_path, raw_source = _resolve_raw_table_path(
        raw_table_path=args.raw_table_path,
        raw_ingest_root=raw_ingest_root,
        raw_ingest_run_id=args.raw_ingest_run_id,
    )
    raw_ingest_report_path, raw_ingest_source = _resolve_raw_ingest_report_path(
        raw_ingest_report_path=args.raw_ingest_report_path,
        raw_ingest_root=raw_ingest_root,
        raw_ingest_run_id=args.raw_ingest_run_id,
    )
    raw_ingest_report_payload = json.loads(raw_ingest_report_path.read_text(encoding="utf-8"))
    if not isinstance(raw_ingest_report_payload, dict):
        raise ValueError(
            f"canonicalization raw-ingest report must be JSON object: {raw_ingest_report_path.as_posix()}"
        )
    output_root = _resolve(Path(args.output_root))
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    report = run_moex_canonicalization(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id=run_id,
        raw_ingest_run_report=raw_ingest_report_payload,
        repo_root=ROOT,
    )
    report["raw_source_resolution"] = raw_source
    report["raw_ingest_report_source_resolution"] = raw_ingest_source
    report["raw_ingest_report_path"] = raw_ingest_report_path.as_posix()

    report_path = output_dir / "canonicalization-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if report.get("publish_decision") != "publish":
        raise SystemExit("canonicalization contour blocked by fail-closed checks")


if __name__ == "__main__":
    main()
