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


DEFAULT_PHASE01_ROOT = Path("artifacts/codex/moex-phase01")
DEFAULT_OUTPUT_ROOT = Path("artifacts/codex/moex-phase02")
RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _pick_phase01_run_dir(phase01_root: Path, requested_run_id: str) -> Path:
    if requested_run_id.strip():
        run_dir = phase01_root / requested_run_id.strip()
        if not run_dir.exists():
            raise FileNotFoundError(f"phase-01 run directory not found: {run_dir.as_posix()}")
        return run_dir

    candidates = [
        item
        for item in phase01_root.iterdir()
        if item.is_dir() and RUN_ID_PATTERN.match(item.name)
    ]
    if not candidates:
        raise FileNotFoundError(
            "cannot auto-resolve phase-01 run directory: no run folders matching "
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
        path = _resolve(Path(raw_table_path.strip()))
        return path, "explicit"

    run_dir = _pick_phase01_run_dir(phase01_root, phase01_run_id)
    path = run_dir / "delta" / "raw_moex_history.delta"
    return path, f"phase01:{run_dir.name}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run MOEX Phase-02 Canonical contour: deterministic resampling "
            "(5m/15m/1h/4h/1d/1w), fail-closed QC, contract compatibility check, and runtime decoupling proof."
        )
    )
    parser.add_argument(
        "--raw-table-path",
        default="",
        help="Path to phase-01 raw_moex_history.delta. If omitted, resolved from --phase01-root.",
    )
    parser.add_argument(
        "--phase01-root",
        default=DEFAULT_PHASE01_ROOT.as_posix(),
        help="Phase-01 artifact root used when --raw-table-path is omitted.",
    )
    parser.add_argument(
        "--phase01-run-id",
        default="",
        help="Specific phase-01 run folder name (YYYYMMDDTHHMMSSZ). If omitted, latest run is used.",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT.as_posix(),
        help="Root folder for phase-02 run artifacts.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; defaults to current UTC timestamp.",
    )
    args = parser.parse_args()

    run_id = args.run_id.strip() or _default_run_id()
    phase01_root = _resolve(Path(args.phase01_root))
    raw_table_path, raw_source = _resolve_raw_table_path(
        raw_table_path=args.raw_table_path,
        phase01_root=phase01_root,
        phase01_run_id=args.phase01_run_id,
    )
    output_root = _resolve(Path(args.output_root))
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    report = run_phase02_canonical(
        raw_table_path=raw_table_path,
        output_dir=output_dir,
        run_id=run_id,
        repo_root=ROOT,
    )
    report["raw_source_resolution"] = raw_source

    report_path = output_dir / "phase02-canonical-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if report.get("publish_decision") != "publish":
        raise SystemExit("phase-02 canonical contour blocked by fail-closed checks")


if __name__ == "__main__":
    main()
