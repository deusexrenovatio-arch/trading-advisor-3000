from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from trading_advisor_3000.dagster_defs import materialize_phase2b_backtest_assets

from ._common import print_summary, runtime_profile, validate_phase2b_contracts, write_json


def run_backtest_job(
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
    require_out_of_sample_pass: bool = True,
    min_trade_count: int = 4,
    max_drawdown_cap: float = 0.35,
    min_positive_fold_ratio: float = 0.5,
    stress_slippage_bps: float = 7.5,
    min_parameter_stability: float = 0.35,
    min_slippage_score: float = 0.45,
    report_json: Path | None = None,
) -> dict[str, object]:
    started = perf_counter()
    report = materialize_phase2b_backtest_assets(
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
        require_out_of_sample_pass=require_out_of_sample_pass,
        min_trade_count=min_trade_count,
        max_drawdown_cap=max_drawdown_cap,
        min_positive_fold_ratio=min_positive_fold_ratio,
        stress_slippage_bps=stress_slippage_bps,
        min_parameter_stability=min_parameter_stability,
        min_slippage_score=min_slippage_score,
    )
    contract_validation = validate_phase2b_contracts(
        output_paths=dict(report["output_paths"]),
        materialized_assets=list(report["materialized_assets"]),
        rows_by_table=dict(report.get("rows_by_table", {})),
    )
    success = bool(report["success"]) and contract_validation["status"] == "passed"
    payload = {
        "job_name": "phase2b_backtest_cli",
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
            "strategy_versions": strategy_versions,
            "combination_count": combination_count,
            "param_batch_size": param_batch_size,
            "series_batch_size": series_batch_size,
            "backtest_timeframe": backtest_timeframe,
            "require_out_of_sample_pass": require_out_of_sample_pass,
            "min_trade_count": min_trade_count,
            "max_drawdown_cap": max_drawdown_cap,
            "min_positive_fold_ratio": min_positive_fold_ratio,
            "stress_slippage_bps": stress_slippage_bps,
            "min_parameter_stability": min_parameter_stability,
            "min_slippage_score": min_slippage_score,
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
    parser = argparse.ArgumentParser(description="Run the phase2b vectorbt backtest layer.")
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
    parser.add_argument("--require-out-of-sample-pass", choices=("true", "false"), default="true")
    parser.add_argument("--min-trade-count", type=int, default=4)
    parser.add_argument("--max-drawdown-cap", type=float, default=0.35)
    parser.add_argument("--min-positive-fold-ratio", type=float, default=0.5)
    parser.add_argument("--stress-slippage-bps", type=float, default=7.5)
    parser.add_argument("--min-parameter-stability", type=float, default=0.35)
    parser.add_argument("--min-slippage-score", type=float, default=0.45)
    parser.add_argument("--report-json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = run_backtest_job(
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
        require_out_of_sample_pass=str(args.require_out_of_sample_pass).lower() == "true",
        min_trade_count=int(args.min_trade_count),
        max_drawdown_cap=float(args.max_drawdown_cap),
        min_positive_fold_ratio=float(args.min_positive_fold_ratio),
        stress_slippage_bps=float(args.stress_slippage_bps),
        min_parameter_stability=float(args.min_parameter_stability),
        min_slippage_score=float(args.min_slippage_score),
        report_json=Path(args.report_json).resolve() if args.report_json else None,
    )
    print_summary(payload)
    return 0 if payload["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
