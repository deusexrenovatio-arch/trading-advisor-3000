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

from trading_advisor_3000.product_plane.data_plane.moex.baseline_update import run_moex_baseline_update
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    BASELINE_UPDATE_STORAGE_DIRNAME,
    CANONICAL_BASELINE_BARS_FILENAME,
    CANONICAL_BASELINE_PROVENANCE_FILENAME,
    CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
    RAW_BASELINE_TABLE_RELATIVE_PATH,
    configured_moex_historical_data_root,
    resolve_external_file_path,
    resolve_external_root,
)


DEFAULT_MAPPING_REGISTRY = Path("configs/moex_foundation/instrument_mapping_registry.v1.yaml")
DEFAULT_UNIVERSE = Path("configs/moex_foundation/universe/moex-futures-priority.v1.yaml")
DEFAULT_TIMEFRAMES = "5m,15m,1h,4h,1d,1w"


def _resolve_repo_path(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _default_ingest_till_utc() -> str:
    return datetime.now(tz=UTC).replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")


def _split_timeframes(raw: str) -> set[str]:
    values = {item.strip() for item in raw.split(",") if item.strip()}
    if not values:
        raise SystemExit("timeframes must not be empty")
    return values


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the MOEX baseline updater: append/merge fresh raw bars into the stable baseline "
            "and Spark-refresh only the changed canonical windows."
        )
    )
    parser.add_argument("--mapping-registry", default=DEFAULT_MAPPING_REGISTRY.as_posix())
    parser.add_argument("--universe", default=DEFAULT_UNIVERSE.as_posix())
    parser.add_argument("--raw-table-path", default="")
    parser.add_argument("--canonical-bars-path", default="")
    parser.add_argument("--canonical-provenance-path", default="")
    parser.add_argument("--evidence-root", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--ingest-till-utc", default="")
    parser.add_argument("--timeframes", default=DEFAULT_TIMEFRAMES)
    parser.add_argument("--refresh-window-days", type=int, default=7)
    parser.add_argument("--contract-discovery-lookback-days", type=int, default=45)
    parser.add_argument("--contract-discovery-step-days", type=int, default=14)
    parser.add_argument("--refresh-overlap-minutes", type=int, default=180)
    parser.add_argument("--max-changed-window-days", type=int, default=10)
    parser.add_argument("--stability-lag-minutes", type=int, default=20)
    parser.add_argument(
        "--expand-contract-chain",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    args = parser.parse_args()

    data_root = configured_moex_historical_data_root(repo_root=ROOT)
    canonical_root = data_root / CANONICAL_BASELINE_ROOT_RELATIVE_PATH
    raw_table_path = (
        resolve_external_file_path(args.raw_table_path, repo_root=ROOT, field_name="--raw-table-path")
        if args.raw_table_path.strip()
        else data_root / RAW_BASELINE_TABLE_RELATIVE_PATH
    )
    canonical_bars_path = (
        resolve_external_file_path(args.canonical_bars_path, repo_root=ROOT, field_name="--canonical-bars-path")
        if args.canonical_bars_path.strip()
        else canonical_root / CANONICAL_BASELINE_BARS_FILENAME
    )
    canonical_provenance_path = (
        resolve_external_file_path(
            args.canonical_provenance_path,
            repo_root=ROOT,
            field_name="--canonical-provenance-path",
        )
        if args.canonical_provenance_path.strip()
        else canonical_root / CANONICAL_BASELINE_PROVENANCE_FILENAME
    )
    evidence_root = resolve_external_root(
        args.evidence_root,
        repo_root=ROOT,
        field_name="--evidence-root",
        default_subdir=BASELINE_UPDATE_STORAGE_DIRNAME,
    )

    report = run_moex_baseline_update(
        mapping_registry_path=_resolve_repo_path(Path(args.mapping_registry)),
        universe_path=_resolve_repo_path(Path(args.universe)),
        raw_table_path=raw_table_path,
        canonical_bars_path=canonical_bars_path,
        canonical_provenance_path=canonical_provenance_path,
        evidence_dir=evidence_root,
        run_id=args.run_id.strip() or _default_run_id(),
        timeframes=_split_timeframes(args.timeframes),
        ingest_till_utc=args.ingest_till_utc.strip() or _default_ingest_till_utc(),
        refresh_window_days=int(args.refresh_window_days),
        contract_discovery_lookback_days=int(args.contract_discovery_lookback_days),
        max_changed_window_days=int(args.max_changed_window_days),
        stability_lag_minutes=int(args.stability_lag_minutes),
        refresh_overlap_minutes=int(args.refresh_overlap_minutes),
        contract_discovery_step_days=int(args.contract_discovery_step_days),
        expand_contract_chain=bool(args.expand_contract_chain),
        repo_root=ROOT,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report.get("publish_decision") != "publish":
        raise SystemExit("MOEX baseline update blocked")


if __name__ == "__main__":
    main()
