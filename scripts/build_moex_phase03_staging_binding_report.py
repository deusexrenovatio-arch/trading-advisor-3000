from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex.phase03_staging_binding import (
    build_phase03_staging_binding_report,
)
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    PHASE03_STAGING_STORAGE_DIRNAME,
    resolve_external_root,
)


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _read_token(env_var_name: str) -> str | None:
    normalized = env_var_name.strip()
    if not normalized:
        return None
    value = os.environ.get(normalized, "").strip()
    if not value:
        raise SystemExit(f"token env var `{normalized}` is empty or missing")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build the external staging Dagster binding report for MOEX Phase-03 cutover "
            "from real Dagster run ids and phase-scoped evidence artifacts."
        )
    )
    parser.add_argument("--dagster-url", required=True, help="Dagster base URL or GraphQL endpoint.")
    parser.add_argument(
        "--output-root",
        default="",
        help=(
            "Absolute external output root for staging binding bundles. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument("--run-id", default="", help="Optional bundle id; defaults to current UTC timestamp.")
    parser.add_argument("--job-name", default="moex_historical_cutover_job")
    parser.add_argument("--dagster-binding", default="dagster://staging/moex-historical-cutover")
    parser.add_argument("--orchestrator", default="dagster-daemon")
    parser.add_argument("--token-env-var", default="", help="Optional env var carrying Dagster bearer token.")
    parser.add_argument("--request-timeout-sec", type=float, default=30.0)
    parser.add_argument("--real-binding", action="append", default=[])

    parser.add_argument("--nightly-1-run-id", required=True)
    parser.add_argument("--nightly-2-run-id", required=True)
    parser.add_argument("--repair-run-id", required=True)
    parser.add_argument("--backfill-run-id", required=True)
    parser.add_argument("--recovery-run-id", required=True)

    parser.add_argument("--nightly-1-artifact-path", required=True)
    parser.add_argument("--nightly-2-artifact-path", required=True)
    parser.add_argument("--repair-artifact-path", required=True)
    parser.add_argument("--backfill-artifact-path", required=True)
    parser.add_argument("--recovery-artifact-path", required=True)
    args = parser.parse_args()

    bundle_id = args.run_id.strip() or _default_run_id()
    output_root = resolve_external_root(
        args.output_root,
        repo_root=ROOT,
        field_name="--output-root",
        default_subdir=PHASE03_STAGING_STORAGE_DIRNAME,
    )
    output_dir = output_root / bundle_id

    result = build_phase03_staging_binding_report(
        dagster_url=args.dagster_url,
        output_dir=output_dir,
        run_ids={
            "nightly_1": args.nightly_1_run_id,
            "nightly_2": args.nightly_2_run_id,
            "repair": args.repair_run_id,
            "backfill": args.backfill_run_id,
            "recovery": args.recovery_run_id,
        },
        artifact_paths_by_mode={
            "nightly_1": _resolve(Path(args.nightly_1_artifact_path)),
            "nightly_2": _resolve(Path(args.nightly_2_artifact_path)),
            "repair": _resolve(Path(args.repair_artifact_path)),
            "backfill": _resolve(Path(args.backfill_artifact_path)),
            "recovery": _resolve(Path(args.recovery_artifact_path)),
        },
        expected_job_name=args.job_name,
        dagster_binding=args.dagster_binding,
        extra_real_bindings=list(args.real_binding),
        orchestrator=args.orchestrator,
        request_timeout_sec=float(args.request_timeout_sec),
        token=_read_token(args.token_env_var),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
