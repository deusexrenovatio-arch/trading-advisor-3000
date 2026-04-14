from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from trading_advisor_3000.dagster_defs import materialize_phase2b_projection_assets

from ._common import print_summary, runtime_profile, write_json


def run_projection_job(
    *,
    canonical_output_dir: Path,
    research_output_dir: Path,
    dataset_version: str,
    timeframes: tuple[str, ...],
    strategy_versions: tuple[str, ...],
    combination_count: int,
    indicator_set_version: str = "indicators-v1",
    indicator_profile_version: str = "core_v1",
    feature_set_version: str = "features-v1",
    feature_profile_version: str = "core_v1",
    param_batch_size: int = 25,
    series_batch_size: int = 4,
    backtest_timeframe: str = "",
    selection_policy: str = "top_robust_per_series",
    max_candidates_per_partition: int = 1,
    min_robust_score: float = 0.55,
    decision_lag_bars_max: int = 1,
    report_json: Path | None = None,
) -> dict[str, object]:
    started = perf_counter()
    report = materialize_phase2b_projection_assets(
        canonical_output_dir=canonical_output_dir,
        research_output_dir=research_output_dir,
        dataset_version=dataset_version,
        timeframes=timeframes,
        indicator_set_version=indicator_set_version,
        indicator_profile_version=indicator_profile_version,
        feature_set_version=feature_set_version,
        feature_profile_version=feature_profile_version,
        strategy_versions=strategy_versions,
        combination_count=combination_count,
        param_batch_size=param_batch_size,
        series_batch_size=series_batch_size,
        backtest_timeframe=backtest_timeframe,
        selection_policy=selection_policy,
        max_candidates_per_partition=max_candidates_per_partition,
        min_robust_score=min_robust_score,
        decision_lag_bars_max=decision_lag_bars_max,
    )
    payload = {
        "job_name": "phase2b_projection_cli",
        "success": bool(report["success"]),
        "duration_seconds": round(perf_counter() - started, 6),
        "contract_validation": {"status": "passed" if report["success"] else "failed"},
        "input_versions": {
            "dataset_version": dataset_version,
            "timeframes": timeframes,
            "indicator_set_version": indicator_set_version,
            "indicator_profile_version": indicator_profile_version,
            "feature_set_version": feature_set_version,
            "feature_profile_version": feature_profile_version,
            "strategy_versions": strategy_versions,
            "combination_count": combination_count,
            "param_batch_size": param_batch_size,
            "series_batch_size": series_batch_size,
            "backtest_timeframe": backtest_timeframe,
            "selection_policy": selection_policy,
            "max_candidates_per_partition": max_candidates_per_partition,
            "min_robust_score": min_robust_score,
            "decision_lag_bars_max": decision_lag_bars_max,
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
    parser = argparse.ArgumentParser(description="Project runtime candidates from the phase2b research plane.")
    parser.add_argument("--canonical-output-dir", required=True)
    parser.add_argument("--research-output-dir", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--timeframes", nargs="+", required=True)
    parser.add_argument("--strategy-versions", nargs="+", required=True)
    parser.add_argument("--combination-count", type=int, default=1)
    parser.add_argument("--indicator-set-version", default="indicators-v1")
    parser.add_argument("--indicator-profile-version", default="core_v1")
    parser.add_argument("--feature-set-version", default="features-v1")
    parser.add_argument("--feature-profile-version", default="core_v1")
    parser.add_argument("--param-batch-size", type=int, default=25)
    parser.add_argument("--series-batch-size", type=int, default=4)
    parser.add_argument("--backtest-timeframe", default="")
    parser.add_argument("--selection-policy", default="top_robust_per_series")
    parser.add_argument("--max-candidates-per-partition", type=int, default=1)
    parser.add_argument("--min-robust-score", type=float, default=0.55)
    parser.add_argument("--decision-lag-bars-max", type=int, default=1)
    parser.add_argument("--report-json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = run_projection_job(
        canonical_output_dir=Path(args.canonical_output_dir).resolve(),
        research_output_dir=Path(args.research_output_dir).resolve(),
        dataset_version=str(args.dataset_version),
        timeframes=tuple(str(item) for item in args.timeframes),
        strategy_versions=tuple(str(item) for item in args.strategy_versions),
        combination_count=int(args.combination_count),
        indicator_set_version=str(args.indicator_set_version),
        indicator_profile_version=str(args.indicator_profile_version),
        feature_set_version=str(args.feature_set_version),
        feature_profile_version=str(args.feature_profile_version),
        param_batch_size=int(args.param_batch_size),
        series_batch_size=int(args.series_batch_size),
        backtest_timeframe=str(args.backtest_timeframe),
        selection_policy=str(args.selection_policy),
        max_candidates_per_partition=int(args.max_candidates_per_partition),
        min_robust_score=float(args.min_robust_score),
        decision_lag_bars_max=int(args.decision_lag_bars_max),
        report_json=Path(args.report_json).resolve() if args.report_json else None,
    )
    print_summary(payload)
    return 0 if payload["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
