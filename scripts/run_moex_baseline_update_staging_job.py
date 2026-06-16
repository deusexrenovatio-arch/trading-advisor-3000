from __future__ import annotations

# ruff: noqa: E501
import argparse
import json
import os
import re
import shutil
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dagster import DagsterInstance

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.dagster_defs import execute_moex_baseline_update_job  # noqa: E402
from trading_advisor_3000.product_plane.data_plane.moex.foundation import (  # noqa: E402
    load_mapping_registry,
    validate_mapping_registry,
)
from trading_advisor_3000.product_plane.data_plane.moex.session_schedule import (  # noqa: E402
    DEFAULT_PUBLIC_RULE_CATALOG_PATH,
    materialize_reconstructed_session_schedule_for_changed_windows,
)
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (  # noqa: E402
    CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
    CANONICAL_BASELINE_SESSION_INTERVALS_FILENAME,
    MOEX_HISTORICAL_DATA_ROOT_ENV,
    RAW_BASELINE_TABLE_RELATIVE_PATH,
    configured_moex_runtime_staging_roots,
    resolve_external_root,
)

PROFILE_PRODUCT_RUNTIME = "product-runtime"
PROFILE_VERIFICATION = "verification"
DEFAULT_MAPPING_REGISTRY = (
    ROOT / "configs" / "moex_foundation" / "instrument_mapping_registry.v1.yaml"
)
RAW_SESSION_SCHEDULE_FILENAME = "raw_moex_session_schedule.delta"


def _default_run_id() -> str:
    return "moex-baseline-staging-" + datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _safe_run_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "-", value.strip()).strip(".-")
    if not cleaned:
        raise ValueError("run id must contain at least one safe character")
    return cleaned


def _resolve_explicit_root(raw_root: str) -> Path | None:
    if not raw_root.strip():
        return None
    return resolve_external_root(
        raw_root,
        repo_root=ROOT,
        field_name="--baseline-root",
        default_subdir="",
    )


def _resolve_baseline_root(*, profile: str, run_id: str, explicit_root: Path | None) -> Path:
    if explicit_root is not None:
        return explicit_root
    roots = configured_moex_runtime_staging_roots(repo_root=ROOT)
    if profile == PROFILE_PRODUCT_RUNTIME:
        return roots.product_runtime_root
    return roots.verification_root / run_id


def _copy_tree(*, source: Path, target: Path, label: str, overwrite: bool) -> dict[str, object]:
    if not source.exists():
        raise FileNotFoundError(f"{label} seed source does not exist: {source.as_posix()}")
    if target.exists() and any(target.iterdir()) and not overwrite:
        raise FileExistsError(
            f"{label} seed target already exists and is not empty: {target.as_posix()}; "
            "use --overwrite-seed to refresh it"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and overwrite:
        shutil.rmtree(target)
    shutil.copytree(source, target, dirs_exist_ok=target.exists())
    return {
        "label": label,
        "source": source.as_posix(),
        "target": target.as_posix(),
        "copied": True,
    }


def _seed_baseline_root(
    *, source_root: Path, target_root: Path, overwrite: bool
) -> dict[str, object]:
    if source_root.resolve() == target_root.resolve():
        raise ValueError("seed source root and target root must be different")
    copied = [
        _copy_tree(
            source=source_root / RAW_BASELINE_TABLE_RELATIVE_PATH,
            target=target_root / RAW_BASELINE_TABLE_RELATIVE_PATH,
            label="raw_baseline_table",
            overwrite=overwrite,
        ),
        _copy_tree(
            source=source_root / CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
            target=target_root / CANONICAL_BASELINE_ROOT_RELATIVE_PATH,
            label="canonical_baseline_root",
            overwrite=overwrite,
        ),
    ]
    return {
        "source_root": source_root.as_posix(),
        "target_root": target_root.as_posix(),
        "copied": copied,
    }


def _parse_ingest_till_date(value: str) -> date:
    text = str(value).strip()
    if not text:
        return datetime.now(tz=UTC).astimezone(ZoneInfo("Europe/Moscow")).date()
    normalized = text.replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(ZoneInfo("Europe/Moscow")).date()


def _session_preflight_changed_windows(
    *,
    date_from: date,
    date_till: date,
    mappings,
) -> list[dict[str, object]]:
    moscow_tz = ZoneInfo("Europe/Moscow")
    window_start_utc = datetime.combine(date_from, time.min, tzinfo=moscow_tz).astimezone(UTC)
    window_end_utc = datetime.combine(
        date_till + timedelta(days=1),
        time.min,
        tzinfo=moscow_tz,
    ).astimezone(UTC)
    window_start_text = window_start_utc.isoformat().replace("+00:00", "Z")
    window_end_text = window_end_utc.isoformat().replace("+00:00", "Z")
    windows: list[dict[str, object]] = []
    for mapping in mappings:
        if not getattr(mapping, "is_active", True):
            continue
        windows.append(
            {
                "internal_id": mapping.internal_id,
                "source_timeframe": "1d",
                "source_interval": 24,
                "moex_engine": mapping.moex_engine,
                "moex_market": mapping.moex_market,
                "moex_board": mapping.moex_board,
                "moex_secid": mapping.moex_secid,
                "window_start_utc": window_start_text,
                "window_end_utc": window_end_text,
                "incremental_rows": 0,
            }
        )
    return windows


def _materialize_session_schedule_preflight(
    *,
    baseline_root: Path,
    run_id: str,
    ingest_till_utc: str,
    refresh_window_days: int,
    contract_discovery_lookback_days: int,
    max_changed_window_days: int,
) -> dict[str, object]:
    date_till = _parse_ingest_till_date(ingest_till_utc)
    lookback_days = (
        max(
            int(refresh_window_days),
            int(contract_discovery_lookback_days),
            int(max_changed_window_days),
        )
        + 2
    )
    date_from = date_till - timedelta(days=lookback_days)
    mappings = load_mapping_registry(DEFAULT_MAPPING_REGISTRY)
    validate_mapping_registry(mappings)
    changed_windows = _session_preflight_changed_windows(
        date_from=date_from,
        date_till=date_till,
        mappings=mappings,
    )
    canonical_root = baseline_root / CANONICAL_BASELINE_ROOT_RELATIVE_PATH
    raw_schedule_path = (
        baseline_root / RAW_BASELINE_TABLE_RELATIVE_PATH.parent / RAW_SESSION_SCHEDULE_FILENAME
    )
    intervals_path = canonical_root / CANONICAL_BASELINE_SESSION_INTERVALS_FILENAME
    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=changed_windows,
        mappings=mappings,
        raw_table_path=baseline_root / RAW_BASELINE_TABLE_RELATIVE_PATH,
        raw_schedule_path=raw_schedule_path,
        canonical_session_intervals_path=intervals_path,
        rule_catalog_path=DEFAULT_PUBLIC_RULE_CATALOG_PATH,
        allow_candle_inference=True,
    )
    report["preflight"] = "moex_baseline_staging_session_schedule"
    report["run_id"] = run_id
    report["date_from"] = date_from.isoformat()
    report["date_till"] = date_till.isoformat()
    report_path = (
        baseline_root / "moex-baseline-update" / run_id / "session-schedule-preflight-report.json"
    )
    report["report_path"] = report_path.as_posix()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the real MOEX baseline Dagster job against an explicit staging root. "
            "Verification profile writes into an isolated run directory; product-runtime profile writes into the stable staging root."
        )
    )
    parser.add_argument(
        "--profile",
        choices=[PROFILE_VERIFICATION, PROFILE_PRODUCT_RUNTIME],
        default=PROFILE_VERIFICATION,
    )
    parser.add_argument(
        "--run-id", default="", help="Logical run id. Defaults to a timestamped staging run id."
    )
    parser.add_argument(
        "--baseline-root",
        default="",
        help="Optional absolute external root. Defaults to configured product-runtime or verification staging roots.",
    )
    parser.add_argument(
        "--seed-from-root",
        default="",
        help="Optional absolute root to copy baseline raw/canonical Delta tables from before running the job.",
    )
    parser.add_argument(
        "--seed-from-product-runtime",
        action="store_true",
        help="Seed verification profile from the configured product-runtime staging root before running the job.",
    )
    parser.add_argument("--overwrite-seed", action="store_true")
    parser.add_argument(
        "--ingest-till-utc", default="", help="Optional ISO UTC upper bound for source ingestion."
    )
    parser.add_argument(
        "--timeframes", default="1d", help="Comma-separated timeframes for this staging run."
    )
    parser.add_argument("--refresh-window-days", type=int, default=1)
    parser.add_argument("--contract-discovery-lookback-days", type=int, default=7)
    parser.add_argument("--contract-discovery-step-days", type=int, default=7)
    parser.add_argument("--refresh-overlap-minutes", type=int, default=180)
    parser.add_argument("--max-changed-window-days", type=int, default=2)
    parser.add_argument("--stability-lag-minutes", type=int, default=20)
    parser.add_argument(
        "--coverage-mode",
        choices=["local_tail", "live_discovery"],
        default="local_tail",
        help=(
            "Use local_tail for normal verification/product-runtime tail updates without "
            "MOEX metadata discovery; live_discovery is an explicit coverage refresh."
        ),
    )
    parser.add_argument("--no-expand-contract-chain", action="store_true")
    parser.add_argument(
        "--allow-product-runtime-write",
        action="store_true",
        help="Required for --profile product-runtime so verification runs cannot accidentally mutate the stable product staging root.",
    )
    parser.add_argument("--raise-on-error", action="store_true")
    args = parser.parse_args()

    profile = str(args.profile)
    if profile == PROFILE_PRODUCT_RUNTIME and not args.allow_product_runtime_write:
        raise SystemExit("product-runtime staging writes require --allow-product-runtime-write")

    run_id = _safe_run_id(args.run_id or _default_run_id())
    baseline_root = _resolve_baseline_root(
        profile=profile,
        run_id=run_id,
        explicit_root=_resolve_explicit_root(str(args.baseline_root)),
    )
    product_runtime_seed_root = (
        configured_moex_runtime_staging_roots(repo_root=ROOT).product_runtime_root
        if bool(args.seed_from_product_runtime)
        else None
    )
    os.environ[MOEX_HISTORICAL_DATA_ROOT_ENV] = baseline_root.as_posix()
    seed_report: dict[str, object] | None = None
    seed_from_root = _resolve_explicit_root(str(args.seed_from_root))
    if bool(args.seed_from_product_runtime):
        if seed_from_root is not None:
            raise SystemExit("use either --seed-from-root or --seed-from-product-runtime, not both")
        if profile != PROFILE_VERIFICATION:
            raise SystemExit("--seed-from-product-runtime is only valid for verification profile")
        seed_from_root = product_runtime_seed_root
    if seed_from_root is not None:
        seed_report = _seed_baseline_root(
            source_root=seed_from_root,
            target_root=baseline_root,
            overwrite=bool(args.overwrite_seed),
        )

    session_schedule_preflight = _materialize_session_schedule_preflight(
        baseline_root=baseline_root,
        run_id=run_id,
        ingest_till_utc=str(args.ingest_till_utc).strip(),
        refresh_window_days=int(args.refresh_window_days),
        contract_discovery_lookback_days=int(args.contract_discovery_lookback_days),
        max_changed_window_days=int(args.max_changed_window_days),
    )

    report = execute_moex_baseline_update_job(
        baseline_root=baseline_root,
        instance=DagsterInstance.ephemeral(),
        run_id=run_id,
        ingest_till_utc=str(args.ingest_till_utc).strip() or None,
        staging_profile=profile,
        timeframes=str(args.timeframes),
        refresh_window_days=int(args.refresh_window_days),
        contract_discovery_lookback_days=int(args.contract_discovery_lookback_days),
        contract_discovery_step_days=int(args.contract_discovery_step_days),
        refresh_overlap_minutes=int(args.refresh_overlap_minutes),
        max_changed_window_days=int(args.max_changed_window_days),
        stability_lag_minutes=int(args.stability_lag_minutes),
        expand_contract_chain=not bool(args.no_expand_contract_chain),
        coverage_mode=str(args.coverage_mode),
        raise_on_error=bool(args.raise_on_error),
    )
    if seed_report is not None:
        report["seed"] = seed_report
    report["session_schedule_preflight"] = session_schedule_preflight
    output_paths = dict(report.get("output_paths", {}) or {})
    evidence_root = Path(
        str(output_paths.get("evidence_root", baseline_root / "moex-baseline-update"))
    )
    report_path = evidence_root / run_id / "dagster-staging-job-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report.get("success"):
        raise SystemExit("MOEX baseline staging Dagster job failed")


if __name__ == "__main__":
    main()
