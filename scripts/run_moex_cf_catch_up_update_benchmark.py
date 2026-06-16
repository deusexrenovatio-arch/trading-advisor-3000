from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median

from deltalake import DeltaTable, write_deltalake

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.dagster_defs import research_assets  # noqa: E402
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (  # noqa: E402
    count_delta_table_rows,
    has_delta_log,
)
from trading_advisor_3000.product_plane.research.continuous_front import (  # noqa: E402
    continuous_front_store_contract,
)

SCENARIO = "ta3000-moex-cf-catch-up-update-v1"
DEFAULT_DATASET_VERSION = "perf_baseline_moex_research_data_prep_v1"
DEFAULT_SOURCE_MATERIALIZED_ROOT = Path(
    "D:/TA3000-data/staging/verification/performance-baselines/"
    "ta3000-moex-no-network-research-data-prep-v1/"
    "resource-local4-12g-20260616T072412Z/materialized"
)
DEFAULT_CANONICAL_OUTPUT_DIR = Path(
    "D:/TA3000-data/staging/verification/pipeline-speed-current-20260615T125911Z/"
    "branch/canonical/moex/baseline-4y-current"
)
DEFAULT_BENCHMARK_DATA_ROOT = (
    Path("D:/TA3000-data/staging/verification/performance-baselines") / SCENARIO
)
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts" / "benchmarks" / SCENARIO
DEFAULT_TIMEFRAMES = ("15m", "1h", "4h", "1d")
DEFAULT_WINDOW_START = "2026-06-07T18:00:00Z"
DEFAULT_WINDOW_END = "2026-06-08T20:49:00Z"
CONTINUOUS_FRONT_TABLES = (
    "continuous_front_bars",
    "continuous_front_roll_events",
    "continuous_front_adjustment_ladder",
    "continuous_front_qc_report",
)


def _utc_stamp() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(parent.resolve(strict=False))
    except ValueError:
        return False
    return True


def _safe_remove_tree(path: Path, *, allowed_parent: Path) -> None:
    resolved = path.resolve(strict=False)
    if not _is_relative_to(resolved, allowed_parent.resolve(strict=False)):
        raise ValueError(f"refusing to remove path outside benchmark root: {resolved.as_posix()}")
    if resolved.exists():
        shutil.rmtree(resolved, ignore_errors=True)
    if resolved.exists():
        raise OSError(f"failed to remove benchmark path: {resolved.as_posix()}")


def _copy_seed_tables(*, source_root: Path, materialized_root: Path, overwrite: bool) -> None:
    contract = continuous_front_store_contract()
    missing = [
        table_name
        for table_name in CONTINUOUS_FRONT_TABLES
        if not has_delta_log(source_root / f"{table_name}.delta")
    ]
    if missing:
        raise FileNotFoundError(
            "seed materialized root is missing Delta tables: " + ", ".join(missing)
        )
    materialized_root.mkdir(parents=True, exist_ok=True)
    for table_name in CONTINUOUS_FRONT_TABLES:
        source = source_root / f"{table_name}.delta"
        target = materialized_root / f"{table_name}.delta"
        if target.exists() and not overwrite:
            raise FileExistsError(
                f"target table already exists: {target.as_posix()}; use --overwrite"
            )
        if target.exists():
            _safe_remove_tree(target, allowed_parent=materialized_root)
        arrow_table = DeltaTable(str(source)).to_pyarrow_table()
        write_deltalake(
            str(target),
            arrow_table,
            mode="overwrite",
            partition_by=list(contract[table_name].get("partition_by") or []),
        )


def _configure_runtime_env(
    *,
    data_root: Path,
    driver_memory: str,
    executor_memory: str,
    driver_max_result_size: str,
    shuffle_partitions: str,
) -> dict[str, str]:
    hadoop_home = Path("D:/TA3000-data/runtime/hadoop-3.3.6")
    if hadoop_home.exists():
        os.environ["HADOOP_HOME"] = hadoop_home.as_posix()
        os.environ["PATH"] = f"{(hadoop_home / 'bin').as_posix()};{os.environ.get('PATH', '')}"
    runtime_root = data_root / "spark-runtime"
    os.environ["TA3000_SPARK_RUNTIME_ROOT"] = runtime_root.as_posix()
    os.environ["TA3000_SPARK_DRIVER_MEMORY"] = driver_memory
    os.environ["TA3000_SPARK_EXECUTOR_MEMORY"] = executor_memory
    os.environ["TA3000_SPARK_DRIVER_MAX_RESULT_SIZE"] = driver_max_result_size
    os.environ["TA3000_SPARK_SQL_SHUFFLE_PARTITIONS"] = shuffle_partitions
    return {
        "hadoop_home": os.environ.get("HADOOP_HOME", ""),
        "spark_runtime_root": runtime_root.as_posix(),
        "driver_memory": driver_memory,
        "executor_memory": executor_memory,
        "driver_max_result_size": driver_max_result_size,
        "shuffle_partitions": shuffle_partitions,
    }


def _discover_instruments(*, materialized_root: Path, dataset_version: str) -> tuple[str, ...]:
    import pyarrow.compute as pc

    table = DeltaTable(str(materialized_root / "continuous_front_bars.delta")).to_pyarrow_table(
        columns=["dataset_version", "instrument_id"]
    )
    mask = pc.equal(table["dataset_version"], dataset_version)
    scoped = table.filter(mask)
    return tuple(sorted(str(item) for item in pc.unique(scoped["instrument_id"]).to_pylist()))


def _build_windows(
    *,
    instruments: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str,
    end_ts: str,
    limit: int,
) -> list[dict[str, object]]:
    windows = [
        {
            "instrument_id": instrument_id,
            "timeframe": timeframe,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "overlap_minutes": 180,
            "source_window_start_utc": start_ts,
            "source_window_end_utc": end_ts,
            "source_changed_windows": [],
            "source_changed_window_count": 1,
            "window_hash_sha256": f"manual-tail-replay-{instrument_id}-{timeframe}",
        }
        for instrument_id in instruments
        for timeframe in timeframes
    ]
    return windows[:limit] if limit > 0 else windows


def _batch_windows(
    windows: list[dict[str, object]], *, batch_size: int
) -> list[list[dict[str, object]]]:
    if not windows:
        return []
    if batch_size <= 0:
        return [windows]
    return [windows[index : index + batch_size] for index in range(0, len(windows), batch_size)]


def _table_counts(*, materialized_root: Path, dataset_version: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in CONTINUOUS_FRONT_TABLES:
        counts[table_name] = count_delta_table_rows(
            materialized_root / f"{table_name}.delta",
            filters=[("dataset_version", "=", dataset_version)],
        )
    return counts


def _window_bar_count(
    *,
    materialized_root: Path,
    dataset_version: str,
    instruments: tuple[str, ...],
    timeframes: tuple[str, ...],
    start_ts: str,
    end_ts: str,
) -> int:
    return count_delta_table_rows(
        materialized_root / "continuous_front_bars.delta",
        filters=[
            ("dataset_version", "=", dataset_version),
            ("instrument_id", "in", list(instruments)),
            ("timeframe", "in", list(timeframes)),
            ("ts", ">=", start_ts),
            ("ts", "<=", end_ts),
        ],
    )


def _summarize_stage_timings(windows: list[dict[str, object]]) -> dict[str, dict[str, float]]:
    by_stage: dict[str, list[float]] = {}
    for window in windows:
        stage_timings = window.get("stage_timings")
        if not isinstance(stage_timings, dict):
            continue
        for stage, payload in stage_timings.items():
            if not isinstance(payload, dict):
                continue
            elapsed = payload.get("elapsed_seconds")
            if isinstance(elapsed, (int, float)):
                by_stage.setdefault(str(stage), []).append(float(elapsed))
    return {
        stage: {
            "total_seconds": round(sum(values), 6),
            "mean_seconds": round(mean(values), 6),
            "median_seconds": round(median(values), 6),
            "count": float(len(values)),
        }
        for stage, values in sorted(by_stage.items(), key=lambda item: sum(item[1]), reverse=True)
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(path: Path, summary: dict[str, object]) -> None:
    duration = dict(summary["duration"])
    row_counts = dict(summary["row_counts"])
    scenario = dict(summary["scenario"])
    lines = [
        f"# {scenario['scenario_id']}",
        "",
        f"- run: `{scenario['run_name']}`",
        f"- status: `{summary['status']}`",
        f"- mode: `{scenario['mode']}`",
        f"- job: `{scenario['dagster_job']}`",
        f"- windows: `{scenario['window_count']}`",
        f"- batches: `{scenario['batch_count']}`",
        f"- batch size: `{scenario['batch_size']}`",
        f"- canonical root: `{scenario['canonical_output_dir']}`",
        f"- materialized root: `{scenario['materialized_root']}`",
        f"- total seconds: `{duration['total_seconds']}`",
        f"- mean batch seconds: `{duration['mean_batch_seconds']}`",
        f"- median batch seconds: `{duration['median_batch_seconds']}`",
        f"- mean window seconds: `{duration['mean_window_seconds']}`",
        f"- window bars before: `{row_counts['window_bars_before']}`",
        f"- window bars after: `{row_counts['window_bars_after']}`",
        "",
        "## Stage Timings",
        "",
    ]
    stage_timings = dict(summary.get("stage_timings", {}))
    if stage_timings:
        lines.append("| stage | total seconds | mean seconds | count |")
        lines.append("| --- | ---: | ---: | ---: |")
        for stage, payload in stage_timings.items():
            stage_payload = dict(payload)
            lines.append(
                f"| `{stage}` | {stage_payload['total_seconds']} | "
                f"{stage_payload['mean_seconds']} | {int(stage_payload['count'])} |"
            )
    else:
        lines.append("Stage timings were not available from Dagster asset output.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark the MOEX continuous-front windowed catch-up update route."
    )
    parser.add_argument("--run-name", default=f"local4-12g-tail-replay-{_utc_stamp()}")
    parser.add_argument("--dataset-version", default=DEFAULT_DATASET_VERSION)
    parser.add_argument("--canonical-output-dir", type=Path, default=DEFAULT_CANONICAL_OUTPUT_DIR)
    parser.add_argument(
        "--source-materialized-root", type=Path, default=DEFAULT_SOURCE_MATERIALIZED_ROOT
    )
    parser.add_argument("--benchmark-data-root", type=Path, default=DEFAULT_BENCHMARK_DATA_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--window-start", default=DEFAULT_WINDOW_START)
    parser.add_argument("--window-end", default=DEFAULT_WINDOW_END)
    parser.add_argument("--timeframes", default=",".join(DEFAULT_TIMEFRAMES))
    parser.add_argument("--instruments", default="")
    parser.add_argument("--limit-windows", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=0)
    parser.add_argument("--spark-master", default="local[4]")
    parser.add_argument("--driver-memory", default="12g")
    parser.add_argument("--executor-memory", default="12g")
    parser.add_argument("--driver-max-result-size", default="4g")
    parser.add_argument("--shuffle-partitions", default="8")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    run_name = str(args.run_name)
    data_root = args.benchmark_data_root / run_name
    materialized_root = data_root / "materialized"
    results_root = data_root / "results"
    artifact_dir = args.artifact_root / run_name
    artifact_dir.mkdir(parents=True, exist_ok=True)
    results_root.mkdir(parents=True, exist_ok=True)

    if materialized_root.exists() and any(materialized_root.iterdir()) and not args.overwrite:
        raise FileExistsError(
            f"benchmark materialized root already exists: {materialized_root.as_posix()}; "
            "use --overwrite or a new --run-name"
        )
    if materialized_root.exists() and args.overwrite:
        _safe_remove_tree(materialized_root, allowed_parent=data_root)

    _copy_seed_tables(
        source_root=args.source_materialized_root,
        materialized_root=materialized_root,
        overwrite=args.overwrite,
    )

    instruments = _split_csv(args.instruments) or _discover_instruments(
        materialized_root=materialized_root,
        dataset_version=args.dataset_version,
    )
    timeframes = _split_csv(args.timeframes)
    windows = _build_windows(
        instruments=instruments,
        timeframes=timeframes,
        start_ts=args.window_start,
        end_ts=args.window_end,
        limit=max(int(args.limit_windows), 0),
    )
    batches = _batch_windows(windows, batch_size=int(args.batch_size))
    scenario = {
        "scenario_id": SCENARIO,
        "run_name": run_name,
        "mode": "batched_continuous_front_catch_up_tail_replay",
        "dagster_job": research_assets.MOEX_CF_CATCH_UP_JOB_NAME,
        "dataset_version": args.dataset_version,
        "canonical_output_dir": args.canonical_output_dir.as_posix(),
        "source_materialized_root": args.source_materialized_root.as_posix(),
        "materialized_root": materialized_root.as_posix(),
        "results_root": results_root.as_posix(),
        "artifact_dir": artifact_dir.as_posix(),
        "window_start": args.window_start,
        "window_end": args.window_end,
        "window_count": len(windows),
        "batch_count": len(batches),
        "batch_size": int(args.batch_size),
        "instruments": list(instruments),
        "timeframes": list(timeframes),
        "spark_master": args.spark_master,
    }
    _write_json(artifact_dir / "scenario.json", scenario)
    _write_json(artifact_dir / "windows.json", windows)
    print(json.dumps({"event": "scenario_prepared", **scenario}, ensure_ascii=False))
    if args.dry_run:
        return 0

    runtime_profile = _configure_runtime_env(
        data_root=data_root,
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
        driver_max_result_size=args.driver_max_result_size,
        shuffle_partitions=args.shuffle_partitions,
    )
    os.environ[research_assets.RESEARCH_DATA_PREP_MATERIALIZED_OUTPUT_DIR_ENV] = (
        materialized_root.as_posix()
    )
    os.environ[research_assets.RESEARCH_DATA_PREP_RESULTS_OUTPUT_DIR_ENV] = results_root.as_posix()

    counts_before = _table_counts(
        materialized_root=materialized_root,
        dataset_version=args.dataset_version,
    )
    window_bars_before = _window_bar_count(
        materialized_root=materialized_root,
        dataset_version=args.dataset_version,
        instruments=instruments,
        timeframes=timeframes,
        start_ts=args.window_start,
        end_ts=args.window_end,
    )

    repository = research_assets.build_research_definitions().get_repository_def()
    job = repository.get_job(research_assets.MOEX_CF_CATCH_UP_JOB_NAME)
    batch_results: list[dict[str, object]] = []
    started = time.perf_counter()
    status = "PASS"
    error: str | None = None
    for index, batch_windows in enumerate(batches, start=1):
        run_id = f"{run_name}-batch-{index:03d}-{len(batch_windows)}w"
        run_config = research_assets._build_moex_cf_catch_up_run_config(  # noqa: SLF001
            canonical_output_dir=args.canonical_output_dir,
            dataset_version=args.dataset_version,
            campaign_run_id=run_id,
            windows=batch_windows,
        )
        op_config = run_config["ops"]["continuous_front_bars"]["config"]
        op_config["spark_master"] = args.spark_master
        batch_started = time.perf_counter()
        print(
            json.dumps(
                {
                    "event": "batch_start",
                    "index": index,
                    "batch_count": len(batches),
                    "batch_window_count": len(batch_windows),
                    "window_hashes": [
                        str(window.get("window_hash_sha256") or "") for window in batch_windows
                    ],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        try:
            result = job.execute_in_process(run_config=run_config, raise_on_error=True)
            output = result.output_for_node("continuous_front_bars")
            elapsed = round(time.perf_counter() - batch_started, 6)
            batch_payload = {
                "index": index,
                "status": "PASS" if result.success else "FAILED",
                "elapsed_seconds": elapsed,
                "run_id": run_id,
                "window_count": len(batch_windows),
                "windows": batch_windows,
                "rows_by_table": output.get("rows_by_table", {})
                if isinstance(output, dict)
                else {},
                "stage_timings": output.get("stage_timings", {})
                if isinstance(output, dict)
                else {},
            }
            batch_results.append(batch_payload)
            print(
                json.dumps(
                    {
                        "event": "batch_done",
                        "index": index,
                        "elapsed_seconds": elapsed,
                        "status": batch_payload["status"],
                        "batch_window_count": len(batch_windows),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            status = "FAILED"
            error = repr(exc)
            batch_results.append(
                {
                    "index": index,
                    "status": "FAILED",
                    "elapsed_seconds": round(time.perf_counter() - batch_started, 6),
                    "run_id": run_id,
                    "window_count": len(batch_windows),
                    "windows": batch_windows,
                    "error": error,
                }
            )
            break

    total_seconds = round(time.perf_counter() - started, 6)
    counts_after = _table_counts(
        materialized_root=materialized_root,
        dataset_version=args.dataset_version,
    )
    window_bars_after = _window_bar_count(
        materialized_root=materialized_root,
        dataset_version=args.dataset_version,
        instruments=instruments,
        timeframes=timeframes,
        start_ts=args.window_start,
        end_ts=args.window_end,
    )
    elapsed_values = [
        float(item["elapsed_seconds"])
        for item in batch_results
        if isinstance(item.get("elapsed_seconds"), (int, float))
    ]
    completed_window_count = sum(
        int(item.get("window_count", 0) or 0)
        for item in batch_results
        if item.get("status") == "PASS"
    )
    summary: dict[str, object] = {
        "status": status,
        "error": error,
        "collected_at_utc": _utc_now_iso(),
        "scenario": scenario,
        "runtime_profile": runtime_profile,
        "duration": {
            "total_seconds": total_seconds,
            "batch_count_completed": len(batch_results),
            "window_count_completed": completed_window_count,
            "mean_batch_seconds": round(mean(elapsed_values), 6) if elapsed_values else 0.0,
            "median_batch_seconds": round(median(elapsed_values), 6) if elapsed_values else 0.0,
            "min_batch_seconds": round(min(elapsed_values), 6) if elapsed_values else 0.0,
            "max_batch_seconds": round(max(elapsed_values), 6) if elapsed_values else 0.0,
            "mean_window_seconds": (
                round(total_seconds / completed_window_count, 6) if completed_window_count else 0.0
            ),
        },
        "row_counts": {
            "before": counts_before,
            "after": counts_after,
            "delta": {
                table_name: counts_after.get(table_name, 0) - counts_before.get(table_name, 0)
                for table_name in CONTINUOUS_FRONT_TABLES
            },
            "window_bars_before": window_bars_before,
            "window_bars_after": window_bars_after,
        },
        "stage_timings": _summarize_stage_timings(batch_results),
        "batches": batch_results,
        "windows": windows,
    }
    _write_json(artifact_dir / "summary.json", summary)
    _write_markdown(artifact_dir / "summary.md", summary)
    print(
        json.dumps(
            {
                "event": "summary_written",
                "summary": (artifact_dir / "summary.json").as_posix(),
                "status": status,
            },
            ensure_ascii=False,
        )
    )
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
