from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from trading_advisor_3000.dagster_defs import materialize_phase2b_bootstrap_assets

from ._common import print_summary, runtime_profile, validate_phase2b_contracts, write_json


def run_bootstrap_job(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: tuple[str, ...],
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    report_json: Path | None = None,
) -> dict[str, object]:
    started = perf_counter()
    report = materialize_phase2b_bootstrap_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        dataset_version=dataset_version,
        timeframes=timeframes,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
    )
    contract_validation = validate_phase2b_contracts(
        output_paths=dict(report["output_paths"]),
        materialized_assets=list(report["materialized_assets"]),
        rows_by_table=dict(report.get("rows_by_table", {})),
    )
    success = bool(report["success"]) and contract_validation["status"] == "passed"
    payload = {
        "job_name": "phase2b_bootstrap_cli",
        "success": success,
        "duration_seconds": round(perf_counter() - started, 6),
        "contract_validation": contract_validation,
        "input_versions": {
            "dataset_version": dataset_version,
            "timeframes": timeframes,
            "indicator_set_version": indicator_set_version,
            "indicator_profile_version": indicator_profile_version,
            "feature_set_version": feature_set_version,
            "feature_profile_version": feature_profile_version,
        },
        "selected_assets": report["selected_assets"],
        "materialized_assets": report["materialized_assets"],
        "rows_by_table": report.get("rows_by_table", {}),
        "output_paths": report["output_paths"],
        "runtime_profile": runtime_profile(),
    }
    if report_json is not None:
        write_json(report_json, payload)
        payload["report_json"] = report_json.as_posix()
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize the phase2b bootstrap research layer.")
    parser.add_argument("--canonical-output-dir", required=True)
    parser.add_argument("--research-output-dir", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--timeframes", nargs="+", required=True)
    parser.add_argument("--indicator-set-version", default="indicators-v1")
    parser.add_argument("--indicator-profile-version", default="core_v1")
    parser.add_argument("--feature-set-version", default="features-v1")
    parser.add_argument("--feature-profile-version", default="core_v1")
    parser.add_argument("--report-json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = run_bootstrap_job(
        canonical_output_dir=Path(args.canonical_output_dir).resolve(),
        research_output_dir=Path(args.research_output_dir).resolve(),
        dataset_version=str(args.dataset_version),
        timeframes=tuple(str(item) for item in args.timeframes),
        indicator_set_version=str(args.indicator_set_version),
        indicator_profile_version=str(args.indicator_profile_version),
        feature_set_version=str(args.feature_set_version),
        feature_profile_version=str(args.feature_profile_version),
        report_json=Path(args.report_json).resolve() if args.report_json else None,
    )
    print_summary(payload)
    return 0 if payload["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
