from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry
from trading_advisor_3000.product_plane.research.backtests import (
    BacktestBatchRequest,
    BacktestEngineConfig,
    build_ephemeral_strategy_space,
    run_backtest_batch,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchDatasetManifest, materialize_research_dataset
from trading_advisor_3000.product_plane.research.derived_indicators import materialize_derived_indicator_frames
from trading_advisor_3000.product_plane.research.indicators import materialize_indicator_frames
from trading_advisor_3000.product_plane.research.io import ResearchFrameCache
from trading_advisor_3000.product_plane.research.strategies import (
    StrategyCatalog,
    StrategyParameter,
    StrategyRankingMetadata,
    StrategyRegistry,
    StrategyRiskPolicy,
    StrategySpec,
)

from ._common import delta_row_count, delta_version_count, print_summary, runtime_profile, write_json, write_text


def _benchmark_registry() -> StrategyRegistry:
    spec = StrategySpec(
        version="breakout-benchmark-v1",
        family="breakout",
        description="Benchmark-only breakout sweep over the materialized research plane.",
        required_columns=(
            "close",
            "high",
            "low",
            "adx_14",
            "atr_14",
        ),
        parameter_grid=(
            StrategyParameter("breakout_window", (3, 4, 5, 6, 7)),
            StrategyParameter("min_adx", (10, 12, 14, 16, 18, 20, 22, 24, 26, 28)),
            StrategyParameter("entry_buffer_atr", (0.0, 0.25, 0.5, 0.75, 1.0)),
        ),
        signal_builder_key="breakout",
        risk_policy=StrategyRiskPolicy(stop_atr_multiple=1.0, target_atr_multiple=2.0),
        ranking_metadata=StrategyRankingMetadata(tags=("benchmark",)),
    )
    return StrategyRegistry(catalog=StrategyCatalog(version="research-benchmark-catalog-v1", strategies=(spec,)))


def _benchmark_context(
    *,
    instruments: int,
    bars_per_instrument: int,
) -> tuple[list[CanonicalBar], list[SessionCalendarEntry], list[RollMapEntry]]:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    bars: list[CanonicalBar] = []
    session_calendar: list[SessionCalendarEntry] = []
    roll_map: list[RollMapEntry] = []

    for instrument_index in range(instruments):
        instrument_id = f"INST{instrument_index + 1:02d}"
        contract_id = f"{instrument_id}-6.26"
        base_close = 80.0 + (instrument_index * 7.5)
        step = 0.18 + (instrument_index * 0.015)
        for index in range(bars_per_instrument):
            ts = (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")
            if index < bars_per_instrument // 3:
                close = base_close + (index * step)
            elif index < (2 * bars_per_instrument) // 3:
                close = base_close + ((bars_per_instrument // 3) * step) - ((index - (bars_per_instrument // 3)) * step * 1.1)
            else:
                close = (
                    base_close
                    + ((bars_per_instrument // 3) * step)
                    - ((bars_per_instrument // 3) * step * 1.1)
                    + ((index - ((2 * bars_per_instrument) // 3)) * step * 1.3)
                )
            open_price = close - (0.4 * step)
            high = max(open_price, close) + (0.8 * step)
            low = min(open_price, close) - (0.9 * step)
            bars.append(
                CanonicalBar.from_dict(
                    {
                        "contract_id": contract_id,
                        "instrument_id": instrument_id,
                        "timeframe": "15m",
                        "ts": ts,
                        "open": round(open_price, 6),
                        "high": round(high, 6),
                        "low": round(low, 6),
                        "close": round(close, 6),
                        "volume": 1_200 + (index * 12) + (instrument_index * 35),
                        "open_interest": 15_000 + (instrument_index * 500) + index,
                    }
                )
            )
        session_dates = sorted(
            {
                (start + timedelta(minutes=15 * index)).date().isoformat()
                for index in range(bars_per_instrument)
            }
        )
        for session_date in session_dates:
            session_calendar.append(
                SessionCalendarEntry(
                    instrument_id=instrument_id,
                    timeframe="15m",
                    session_date=session_date,
                    session_open_ts=f"{session_date}T00:00:00Z",
                    session_close_ts=f"{session_date}T23:45:00Z",
                )
            )
            roll_map.append(
                RollMapEntry(
                    instrument_id=instrument_id,
                    session_date=session_date,
                    active_contract_id=contract_id,
                    reason="benchmark_single_contract",
                )
            )
    return bars, session_calendar, roll_map


def _markdown_report(report: dict[str, object]) -> str:
    lines = [
        "# Research Benchmark Report",
        "",
        "## Dataset",
        f"- scenario: {report['dataset']['scenario']}",
        f"- instruments: {report['dataset']['instrument_count']}",
        f"- bars_per_instrument: {report['dataset']['bars_per_instrument']}",
        f"- materialized_bar_rows: {report['dataset']['materialized_bar_rows']}",
        "",
        "## Timings",
        f"- cold_bootstrap_seconds: {report['cold_bootstrap']['duration_seconds']}",
        f"- cold_backtest_seconds: {report['cold_backtest']['duration_seconds']}",
        f"- hot_backtest_seconds: {report['hot_backtest']['duration_seconds']}",
        f"- hot_speedup_vs_cold_total: {report['thresholds']['hot_speedup_vs_cold_total']}",
        "",
        "## Thresholds",
        f"- no_recompute_indicators_derived: {report['thresholds']['no_recompute_indicators_derived']}",
        f"- hot_path_threshold_pass: {report['thresholds']['hot_path_threshold_pass']}",
        f"- param_100_completed: {report['thresholds']['param_100_completed']}",
        "",
        "## Scalability",
        "| combinations | duration_seconds | cache_hit | run_count |",
        "| --- | --- | --- | --- |",
    ]
    for row in report["scalability_runs"]:
        lines.append(
            f"| {row['combination_count']} | {row['duration_seconds']} | {row['cache_hit']} | {row['run_count']} |"
        )
    lines.extend(["", "## Cache Markers", *[f"- {line}" for line in report["cache_markers"]]])
    return "\n".join(lines) + "\n"


def run_benchmark_job(
    *,
    output_dir: Path,
    dataset_version: str = "benchmark_small_v1",
    instruments: int = 6,
    bars_per_instrument: int = 96,
    combination_sizes: tuple[int, ...] = (10, 50, 100, 250),
    param_batch_size: int = 25,
    report_json: Path | None = None,
    report_md: Path | None = None,
) -> dict[str, object]:
    materialized_dir = output_dir / "materialized"
    backtests_dir = output_dir / "backtests"
    cache_log_path = output_dir / "cache-markers.log"
    bars, session_calendar, roll_map = _benchmark_context(
        instruments=instruments,
        bars_per_instrument=bars_per_instrument,
    )
    registry = _benchmark_registry()

    bootstrap_started = perf_counter()
    materialize_research_dataset(
        manifest_seed=ResearchDatasetManifest(
            dataset_version=dataset_version,
            dataset_name="research benchmark",
            source_table="benchmark_synthetic_bars",
            universe_id="benchmark-synthetic",
            timeframes=("15m",),
            base_timeframe="15m",
            split_method="full",
            warmup_bars=0,
            code_version="research-benchmark-job",
        ),
        bars=bars,
        session_calendar=session_calendar,
        roll_map=roll_map,
        output_dir=materialized_dir,
    )
    materialize_indicator_frames(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
    )
    materialize_derived_indicator_frames(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        profile_version="core_v1",
    )
    cold_bootstrap_duration = round(perf_counter() - bootstrap_started, 6)

    benchmark_count = min(combination_sizes)
    cache = ResearchFrameCache()
    benchmark_strategy_space = build_ephemeral_strategy_space(
        strategy_registry=registry,
        strategy_version_labels=("breakout-benchmark-v1",),
        instances_per_strategy=benchmark_count,
    )
    request = BacktestBatchRequest(
        campaign_run_id="crun_benchmark",
        strategy_space_id=benchmark_strategy_space.strategy_space_id,
        dataset_version=dataset_version,
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        search_specs=benchmark_strategy_space.search_specs,
        combination_count=sum(len(spec.parameter_space.get("rows", ())) or 1 for spec in benchmark_strategy_space.search_specs),
        param_batch_size=param_batch_size,
        series_batch_size=2,
        timeframe="15m",
    )
    engine_config = BacktestEngineConfig(window_count=2)

    cold_backtest_started = perf_counter()
    cold_backtest = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=backtests_dir / "cold",
        request=request,
        engine_config=engine_config,
        strategy_registry=registry,
        cache=cache,
    )
    cold_backtest_duration = round(perf_counter() - cold_backtest_started, 6)

    indicator_versions_before_hot = delta_version_count(materialized_dir / "research_indicator_frames.delta")
    derived_versions_before_hot = delta_version_count(materialized_dir / "research_derived_indicator_frames.delta")
    hot_backtest_started = perf_counter()
    hot_backtest = run_backtest_batch(
        dataset_output_dir=materialized_dir,
        indicator_output_dir=materialized_dir,
        derived_indicator_output_dir=materialized_dir,
        output_dir=backtests_dir / "hot",
        request=request,
        engine_config=engine_config,
        strategy_registry=registry,
        cache=cache,
    )
    hot_backtest_duration = round(perf_counter() - hot_backtest_started, 6)
    indicator_versions_after_hot = delta_version_count(materialized_dir / "research_indicator_frames.delta")
    derived_versions_after_hot = delta_version_count(materialized_dir / "research_derived_indicator_frames.delta")

    scalability_runs: list[dict[str, object]] = []
    for count in combination_sizes:
        scale_strategy_space = build_ephemeral_strategy_space(
            strategy_registry=registry,
            strategy_version_labels=("breakout-benchmark-v1",),
            instances_per_strategy=count,
        )
        scale_started = perf_counter()
        scale_report = run_backtest_batch(
            dataset_output_dir=materialized_dir,
            indicator_output_dir=materialized_dir,
            derived_indicator_output_dir=materialized_dir,
            output_dir=backtests_dir / f"scale-{count}",
            request=BacktestBatchRequest(
                campaign_run_id="crun_benchmark",
                strategy_space_id=scale_strategy_space.strategy_space_id,
                dataset_version=dataset_version,
                indicator_set_version="indicators-v1",
                derived_indicator_set_version="derived-v1",
                search_specs=scale_strategy_space.search_specs,
                combination_count=sum(len(spec.parameter_space.get("rows", ())) or 1 for spec in scale_strategy_space.search_specs),
                param_batch_size=param_batch_size,
                series_batch_size=2,
                timeframe="15m",
            ),
            engine_config=engine_config,
            strategy_registry=registry,
            cache=cache,
        )
        scalability_runs.append(
            {
                "combination_count": count,
                "duration_seconds": round(perf_counter() - scale_started, 6),
                "cache_hit": bool(scale_report["cache_hit"]),
                "run_count": len(scale_report["run_rows"]),
            }
        )

    cold_total_duration = cold_bootstrap_duration + cold_backtest_duration
    hot_speedup = round(cold_total_duration / max(hot_backtest_duration, 1e-9), 6)
    hot_ratio = hot_backtest_duration / max(cold_total_duration, 1e-9)
    thresholds = {
        "no_recompute_indicators_derived": (
            indicator_versions_before_hot == indicator_versions_after_hot
            and derived_versions_before_hot == derived_versions_after_hot
        ),
        "hot_speedup_vs_cold_total": hot_speedup,
        "hot_ratio_vs_cold_total": round(hot_ratio, 6),
        "hot_path_threshold_pass": (
            indicator_versions_before_hot == indicator_versions_after_hot
            and derived_versions_before_hot == derived_versions_after_hot
            and (hot_speedup >= 3.0 or hot_ratio <= 0.30)
        ),
        "param_100_completed": any(
            int(row["combination_count"]) >= 100 and int(row["run_count"]) >= 100 for row in scalability_runs
        ),
    }
    cache_markers = [
        f"cold_backtest.cache_hit={str(cold_backtest['cache_hit']).lower()}",
        f"hot_backtest.cache_hit={str(hot_backtest['cache_hit']).lower()}",
        *[f"scale.{row['combination_count']}.cache_hit={str(row['cache_hit']).lower()}" for row in scalability_runs],
    ]
    write_text(cache_log_path, "\n".join(cache_markers) + "\n")

    payload = {
        "job_name": "research_benchmark_cli",
        "dataset": {
            "scenario": "benchmark_small",
            "dataset_version": dataset_version,
            "instrument_count": instruments,
            "bars_per_instrument": bars_per_instrument,
            "materialized_bar_rows": delta_row_count(materialized_dir / "research_bar_views.delta"),
            "indicator_rows": delta_row_count(materialized_dir / "research_indicator_frames.delta"),
            "derived_indicator_rows": delta_row_count(materialized_dir / "research_derived_indicator_frames.delta"),
        },
        "versions": {
            "indicator_set_version": "indicators-v1",
            "derived_indicator_set_version": "derived-v1",
            "strategy_catalog_version": registry.catalog.version,
        },
        "cold_bootstrap": {"duration_seconds": cold_bootstrap_duration},
        "cold_backtest": {
            "duration_seconds": cold_backtest_duration,
            "cache_hit": bool(cold_backtest["cache_hit"]),
            "run_count": len(cold_backtest["run_rows"]),
        },
        "hot_backtest": {
            "duration_seconds": hot_backtest_duration,
            "cache_hit": bool(hot_backtest["cache_hit"]),
            "run_count": len(hot_backtest["run_rows"]),
        },
        "scalability_runs": scalability_runs,
        "thresholds": thresholds,
        "cache_markers": cache_markers,
        "artifacts": {
            "cache_log": cache_log_path.name,
            "report_json": "research-benchmark-report.json",
            "report_md": "research-benchmark-report.md",
        },
        "runtime_profile": runtime_profile(),
    }

    json_path = report_json or (output_dir / "research-benchmark-report.json")
    md_path = report_md or (output_dir / "research-benchmark-report.md")
    write_json(json_path, payload)
    write_text(md_path, _markdown_report(payload))
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run cold-vs-hot and param-scalability benchmark for the materialized research route.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--dataset-version", default="benchmark_small_v1")
    parser.add_argument("--instruments", type=int, default=6)
    parser.add_argument("--bars-per-instrument", type=int, default=96)
    parser.add_argument("--combination-sizes", nargs="+", type=int, default=[10, 50, 100, 250])
    parser.add_argument("--param-batch-size", type=int, default=25)
    parser.add_argument("--report-json")
    parser.add_argument("--report-md")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = run_benchmark_job(
        output_dir=Path(args.output_dir).resolve(),
        dataset_version=str(args.dataset_version),
        instruments=int(args.instruments),
        bars_per_instrument=int(args.bars_per_instrument),
        combination_sizes=tuple(int(item) for item in args.combination_sizes),
        param_batch_size=int(args.param_batch_size),
        report_json=Path(args.report_json).resolve() if args.report_json else None,
        report_md=Path(args.report_md).resolve() if args.report_md else None,
    )
    print_summary(payload)
    return 0 if payload["thresholds"]["hot_path_threshold_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
