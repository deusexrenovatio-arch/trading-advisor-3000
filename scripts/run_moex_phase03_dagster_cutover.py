from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex import run_phase03_dagster_cutover


DEFAULT_OUTPUT_ROOT = Path("artifacts/codex/moex-phase03")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _parse_backoff(raw: str) -> list[int]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        raise ValueError("retry backoff list must not be empty")
    parsed: list[int] = []
    for item in values:
        value = int(item)
        if value <= 0:
            raise ValueError("retry backoff values must be positive integers")
        parsed.append(value)
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run MOEX Phase-03 Dagster Cutover contour: canonical route graph materialization, "
            "single-writer lease enforcement, repair/backfill reuse, recovery drill, and two nightly readiness cycles."
        )
    )
    parser.add_argument("--raw-table-path", default="", help="Path to authoritative raw_moex_history.delta.")
    parser.add_argument("--raw-ingest-report-path", default="", help="Path to raw_ingest_run_report.v2 json.")
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT.as_posix(), help="Root folder for phase-03 artifacts.")
    parser.add_argument("--run-id", default="", help="Optional run id; defaults to current UTC timestamp.")
    parser.add_argument(
        "--nightly-readiness-observed-at-utc",
        action="append",
        default=[],
        help="Observed readiness UTC timestamp for nightly cycle. Repeat exactly twice.",
    )
    parser.add_argument("--schedule-cron", default="0 2 * * *")
    parser.add_argument("--retry-max-attempts", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", default="60,300,900")
    parser.add_argument("--lease-timeout-sec", type=int, default=900)
    parser.add_argument(
        "--staging-binding-report-path",
        default="",
        help=(
            "Path to external staging Dagster binding evidence JSON. "
            "Required for governed Phase-03 staging-real proof."
        ),
    )
    parser.add_argument("--route-signal", default="worker:phase-only")
    args = parser.parse_args()

    raw_table_raw = args.raw_table_path.strip()
    raw_report_raw = args.raw_ingest_report_path.strip()
    if not raw_table_raw:
        raise SystemExit("phase-03 requires --raw-table-path")
    if not raw_report_raw:
        raise SystemExit("phase-03 requires --raw-ingest-report-path")
    if len(args.nightly_readiness_observed_at_utc) != 2:
        raise SystemExit(
            "phase-03 requires exactly two --nightly-readiness-observed-at-utc values "
            "to prove consecutive governed nightly cycles"
        )
    staging_binding_raw = str(args.staging_binding_report_path).strip()
    if not staging_binding_raw:
        raise SystemExit(
            "phase-03 governed cutover requires --staging-binding-report-path "
            "to keep proof_class at staging-real"
        )

    run_id = args.run_id.strip() or _default_run_id()
    output_root = _resolve(Path(args.output_root))
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    report = run_phase03_dagster_cutover(
        raw_table_path=_resolve(Path(raw_table_raw)),
        raw_ingest_report_path=_resolve(Path(raw_report_raw)),
        output_dir=output_dir,
        run_id=run_id,
        nightly_readiness_observed_at_utc=list(args.nightly_readiness_observed_at_utc),
        route_signal=args.route_signal,
        schedule_cron=args.schedule_cron,
        retry_max_attempts=int(args.retry_max_attempts),
        retry_backoff_seconds=_parse_backoff(args.retry_backoff_seconds),
        lease_timeout_sec=int(args.lease_timeout_sec),
        staging_binding_report_path=_resolve(Path(staging_binding_raw)),
        require_staging_real=True,
    )

    report_path = output_dir / "phase03-dagster-cutover-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report.get("status") != "PASS":
        raise SystemExit("phase-03 dagster cutover contour blocked by fail-closed checks")


if __name__ == "__main__":
    main()
