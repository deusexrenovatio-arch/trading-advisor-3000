from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex import run_phase01_foundation
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    RAW_INGEST_STORAGE_DIRNAME,
    RAW_INGEST_SUMMARY_REPORT_FILENAME,
    resolve_external_root,
)


DEFAULT_MAPPING_REGISTRY = Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml")
DEFAULT_UNIVERSE = Path("configs/moex_phase01/universe/moex-futures-priority.v1.yaml")
def _repo_root() -> Path:
    return ROOT


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (_repo_root() / path).resolve()


def _parse_timeframes(raw: str) -> set[str]:
    values = {item.strip() for item in raw.split(",") if item.strip()}
    if not values:
        raise ValueError("timeframes must not be empty")
    return values


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _default_ingest_till_utc() -> str:
    now = datetime.now(tz=UTC).replace(second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the MOEX raw-ingest tool for bootstrap, repair, and idempotent evidence capture: "
            "discovery + deterministic bootstrap ingest + idempotent rerun proof."
        )
    )
    parser.add_argument("--mapping-registry", default=DEFAULT_MAPPING_REGISTRY.as_posix())
    parser.add_argument("--universe", default=DEFAULT_UNIVERSE.as_posix())
    parser.add_argument(
        "--output-root",
        default="",
        help=(
            "Absolute external root folder for raw-ingest artifacts. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument("--run-id", default="")
    parser.add_argument("--timeframes", default="5m,15m,1h,4h,1d,1w")
    parser.add_argument("--bootstrap-window-days", type=int, default=1461)
    parser.add_argument("--stability-lag-minutes", type=int, default=20)
    parser.add_argument(
        "--expand-contract-chain",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable multi-contract discovery per instrument using MOEX history snapshots.",
    )
    parser.add_argument(
        "--contract-discovery-step-days",
        type=int,
        default=14,
        help="Step between MOEX history snapshot dates for contract chain discovery.",
    )
    parser.add_argument(
        "--refresh-overlap-minutes",
        type=int,
        default=180,
        help="Refetch overlap window near watermark to handle late corrections while avoiding full-window reloads.",
    )
    parser.add_argument("--ingest-till-utc", default="")
    args = parser.parse_args()

    print(
        "route-note: scripts/run_moex_raw_ingest.py is the manual raw-ingest tool. "
        "Dagster owns scheduled route ordering; use this command for bootstrap, repair, or evidence capture.",
        flush=True,
    )

    run_id = args.run_id.strip() or _default_run_id()
    ingest_till_utc = args.ingest_till_utc.strip() or _default_ingest_till_utc()
    output_root = resolve_external_root(
        args.output_root,
        repo_root=ROOT,
        field_name="--output-root",
        default_subdir=RAW_INGEST_STORAGE_DIRNAME,
    )
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    mapping_registry_path = _resolve(Path(args.mapping_registry))
    universe_path = _resolve(Path(args.universe))
    timeframes = _parse_timeframes(args.timeframes)

    pass1 = run_phase01_foundation(
        mapping_registry_path=mapping_registry_path,
        universe_path=universe_path,
        output_dir=output_dir,
        run_id=f"{run_id}-pass1",
        timeframes=timeframes,
        bootstrap_window_days=args.bootstrap_window_days,
        ingest_till_utc=ingest_till_utc,
        stability_lag_minutes=args.stability_lag_minutes,
        expand_contract_chain=bool(args.expand_contract_chain),
        contract_discovery_step_days=args.contract_discovery_step_days,
        refresh_overlap_minutes=args.refresh_overlap_minutes,
    )
    coverage_pass1 = output_dir / "coverage-report.pass1.json"
    coverage_csv_pass1 = output_dir / "coverage-report.pass1.csv"
    ingest_pass1 = output_dir / "raw-ingest-report.pass1.json"
    shutil.copyfile(_resolve(Path(pass1.coverage_report_path)), coverage_pass1)
    shutil.copyfile(_resolve(Path(pass1.coverage_table_path)), coverage_csv_pass1)
    shutil.copyfile(_resolve(Path(pass1.raw_ingest_report_path)), ingest_pass1)

    pass2 = run_phase01_foundation(
        mapping_registry_path=mapping_registry_path,
        universe_path=universe_path,
        output_dir=output_dir,
        run_id=f"{run_id}-pass2",
        timeframes=timeframes,
        bootstrap_window_days=args.bootstrap_window_days,
        ingest_till_utc=ingest_till_utc,
        stability_lag_minutes=args.stability_lag_minutes,
        expand_contract_chain=bool(args.expand_contract_chain),
        contract_discovery_step_days=args.contract_discovery_step_days,
        refresh_overlap_minutes=args.refresh_overlap_minutes,
    )
    coverage_pass2 = output_dir / "coverage-report.pass2.json"
    coverage_csv_pass2 = output_dir / "coverage-report.pass2.csv"
    ingest_pass2 = output_dir / "raw-ingest-report.pass2.json"
    shutil.copyfile(_resolve(Path(pass2.coverage_report_path)), coverage_pass2)
    shutil.copyfile(_resolve(Path(pass2.coverage_table_path)), coverage_csv_pass2)
    shutil.copyfile(_resolve(Path(pass2.raw_ingest_report_path)), ingest_pass2)

    idempotent = pass2.incremental_rows == 0
    summary = {
        "run_id": run_id,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "ingest_till_utc": ingest_till_utc,
        "timeframes": sorted(timeframes),
        "source_intervals": pass2.source_interval_set,
        "source_timeframes": pass2.source_timeframe_set,
        "bootstrap_window_days": args.bootstrap_window_days,
        "stability_lag_minutes": args.stability_lag_minutes,
        "expand_contract_chain": bool(args.expand_contract_chain),
        "contract_discovery_step_days": args.contract_discovery_step_days,
        "refresh_overlap_minutes": args.refresh_overlap_minutes,
        "idempotent_rerun": idempotent,
        "pass_1": pass1.to_dict(),
        "pass_2": pass2.to_dict(),
        "artifacts": {
            "coverage_report_pass1": coverage_pass1.as_posix(),
            "coverage_report_pass2": coverage_pass2.as_posix(),
            "coverage_table_pass1": coverage_csv_pass1.as_posix(),
            "coverage_table_pass2": coverage_csv_pass2.as_posix(),
            "raw_ingest_report_pass1": ingest_pass1.as_posix(),
            "raw_ingest_report_pass2": ingest_pass2.as_posix(),
            "raw_table": pass2.raw_table_path,
        },
        "real_bindings": pass2.real_bindings,
    }
    summary_path = output_dir / RAW_INGEST_SUMMARY_REPORT_FILENAME
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if not idempotent:
        raise SystemExit("raw-ingest idempotency proof failed: second run added new rows")


if __name__ == "__main__":
    main()
