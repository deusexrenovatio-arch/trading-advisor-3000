from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (  # noqa: E402
    count_delta_table_rows,
    has_delta_log,
)
from trading_advisor_3000.product_plane.research.derived_indicators.materialize import (  # noqa: E402
    materialize_derived_indicator_frames,
)

SCENARIO = "ta3000-moex-derived-catch-up-update-v1"
DEFAULT_SOURCE_MATERIALIZED_ROOT = Path(
    "D:/TA3000-data/trading-advisor-3000-nightly/research/gold/current"
)
DEFAULT_BENCHMARK_DATA_ROOT = (
    Path("D:/TA3000-data/staging/verification/performance-baselines") / SCENARIO
)
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts" / "benchmarks" / SCENARIO
DEFAULT_WINDOWS_JSON = (
    ROOT
    / "artifacts/benchmarks/ta3000-moex-cf-catch-up-update-v1/"
    / "local4-12g-batched-tail-replay-20260616T093514Z/windows.json"
)
DEFAULT_DATASET_VERSION = "moex_approved_universe_current_v1"
DEFAULT_CONTOUR_ID = "pit_active_front"
DEFAULT_INDICATOR_SET_VERSION = "indicators-v1"
DEFAULT_DERIVED_INDICATOR_SET_VERSION = "derived-v1"
DEFAULT_PROFILE_VERSION = "core_v1"
SEED_TABLES = (
    "research_derived_source_frames.delta",
    "research_derived_indicator_frames.delta",
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


def _windows_long_path(path: Path) -> Path:
    if os.name != "nt":
        return path
    resolved = str(path.resolve(strict=False)).replace("/", "\\")
    if resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved.lstrip("\\"))
    return Path("\\\\?\\" + resolved)


def _safe_remove_tree(path: Path, *, allowed_parent: Path) -> None:
    resolved = path.resolve(strict=False)
    if not _is_relative_to(resolved, allowed_parent.resolve(strict=False)):
        raise ValueError(f"refusing to remove path outside benchmark root: {resolved.as_posix()}")
    long_resolved = _windows_long_path(resolved)
    if long_resolved.exists():
        shutil.rmtree(long_resolved, ignore_errors=True)
    if resolved.exists():
        raise OSError(f"failed to remove benchmark path: {resolved.as_posix()}")


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


def _load_windows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("windows JSON must contain a list")
    windows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("windows JSON must contain only objects")
        windows.append(dict(item))
    return windows


def _scope_from_windows(
    windows: list[dict[str, Any]],
    *,
    fallback_instruments: tuple[str, ...],
    fallback_timeframes: tuple[str, ...],
) -> dict[str, Any]:
    instruments = tuple(
        sorted(
            {
                str(item.get("instrument_id") or "").strip()
                for item in windows
                if str(item.get("instrument_id") or "").strip()
            }
        )
    )
    timeframes = tuple(
        sorted(
            {
                str(item.get("timeframe") or "").strip()
                for item in windows
                if str(item.get("timeframe") or "").strip()
            }
        )
    )
    starts = [
        str(item.get("start_ts") or "").strip()
        for item in windows
        if str(item.get("start_ts") or "").strip()
    ]
    ends = [
        str(item.get("end_ts") or "").strip()
        for item in windows
        if str(item.get("end_ts") or "").strip()
    ]
    return {
        "window_count": len(windows),
        "instrument_ids": instruments or fallback_instruments,
        "timeframes": timeframes or fallback_timeframes,
        "start_ts": min(starts) if starts else "",
        "end_ts": max(ends) if ends else "",
    }


def _copy_seed_tables(*, source_root: Path, materialized_root: Path, overwrite: bool) -> None:
    missing = [
        table_name for table_name in SEED_TABLES if not has_delta_log(source_root / table_name)
    ]
    if missing:
        raise FileNotFoundError("source root is missing seed Delta tables: " + ", ".join(missing))
    materialized_root.mkdir(parents=True, exist_ok=True)
    for table_name in SEED_TABLES:
        source = source_root / table_name
        target = materialized_root / table_name
        if target.exists() and not overwrite:
            raise FileExistsError(
                f"target table already exists: {target.as_posix()}; use --overwrite"
            )
        if target.exists():
            _safe_remove_tree(target, allowed_parent=materialized_root)
        shutil.copytree(_windows_long_path(source), _windows_long_path(target))


def _scoped_count(
    *,
    root: Path,
    table_name: str,
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str,
    instruments: tuple[str, ...],
    timeframes: tuple[str, ...],
    derived_indicator_set_version: str | None = None,
) -> int:
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("contour_id", "=", contour_id),
        ("indicator_set_version", "=", indicator_set_version),
    ]
    if derived_indicator_set_version is not None:
        filters.append(("derived_indicator_set_version", "=", derived_indicator_set_version))
    if instruments:
        filters.append(("instrument_id", "in", list(instruments)))
    if timeframes:
        filters.append(("timeframe", "in", list(timeframes)))
    return count_delta_table_rows(root / table_name, filters=filters)


def _table_counts(
    *,
    root: Path,
    dataset_version: str,
    contour_id: str,
    indicator_set_version: str,
    derived_indicator_set_version: str,
    instruments: tuple[str, ...],
    timeframes: tuple[str, ...],
) -> dict[str, int]:
    return {
        "research_derived_source_frames": _scoped_count(
            root=root,
            table_name="research_derived_source_frames.delta",
            dataset_version=dataset_version,
            contour_id=contour_id,
            indicator_set_version=indicator_set_version,
            instruments=instruments,
            timeframes=timeframes,
        ),
        "research_derived_indicator_frames": _scoped_count(
            root=root,
            table_name="research_derived_indicator_frames.delta",
            dataset_version=dataset_version,
            contour_id=contour_id,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            instruments=instruments,
            timeframes=timeframes,
        ),
    }


def _summarize_stage_timings(report: dict[str, Any]) -> list[dict[str, Any]]:
    timings = report.get("stage_timings", {})
    if not isinstance(timings, dict):
        return []
    rows: list[dict[str, Any]] = []
    for stage, payload in timings.items():
        if not isinstance(payload, dict):
            continue
        elapsed_seconds = float(
            payload.get("duration_seconds", payload.get("elapsed_seconds", 0.0)) or 0.0
        )
        rows.append(
            {
                "stage": stage,
                "duration_seconds": elapsed_seconds,
                **{
                    key: value
                    for key, value in payload.items()
                    if key not in {"duration_seconds", "elapsed_seconds", "started_at", "ended_at"}
                },
            }
        )
    rows.sort(key=lambda item: item["duration_seconds"], reverse=True)
    return rows


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    stage_rows = summary.get("stage_timings_ranked", [])
    source_stage_rows = summary.get("source_frame_stage_timings_ranked", [])
    lines = [
        f"# {SCENARIO}",
        "",
        f"- run_id: `{summary['run_id']}`",
        f"- status: `{summary['status']}`",
        f"- duration_seconds: `{summary['duration_seconds']:.3f}`",
        f"- source_materialized_root: `{summary['source_materialized_root']}`",
        f"- benchmark_materialized_root: `{summary['benchmark_materialized_root']}`",
        f"- window_count: `{summary['scope']['window_count']}`",
        f"- instruments: `{len(summary['scope']['instrument_ids'])}`",
        f"- timeframes: `{', '.join(summary['scope']['timeframes'])}`",
        "",
        "## Row Counts",
        "",
        "| table | before | after |",
        "|---|---:|---:|",
    ]
    before = summary.get("counts_before", {})
    after = summary.get("counts_after", {})
    for table_name in sorted(set(before) | set(after)):
        lines.append(
            f"| {table_name} | {before.get(table_name, '')} | {after.get(table_name, '')} |"
        )
    lines.extend(["", "## Stage Timings", "", "| stage | seconds | details |", "|---|---:|---|"])
    for row in stage_rows:
        details = {
            key: value for key, value in row.items() if key not in {"stage", "duration_seconds"}
        }
        lines.append(
            f"| {row['stage']} | {float(row['duration_seconds']):.3f} | "
            f"`{json.dumps(details, ensure_ascii=False, sort_keys=True)}` |"
        )
    if source_stage_rows:
        lines.extend(
            [
                "",
                "## Source Frame Spark Timings",
                "",
                "| stage | seconds | details |",
                "|---|---:|---|",
            ]
        )
        for row in source_stage_rows:
            details = {
                key: value for key, value in row.items() if key not in {"stage", "duration_seconds"}
            }
            lines.append(
                f"| {row['stage']} | {float(row['duration_seconds']):.3f} | "
                f"`{json.dumps(details, ensure_ascii=False, sort_keys=True)}` |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=f"local4-12g-derived-tail-replay-{_utc_stamp()}")
    parser.add_argument(
        "--source-materialized-root",
        type=Path,
        default=DEFAULT_SOURCE_MATERIALIZED_ROOT,
    )
    parser.add_argument("--benchmark-data-root", type=Path, default=DEFAULT_BENCHMARK_DATA_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--windows-json", type=Path, default=DEFAULT_WINDOWS_JSON)
    parser.add_argument("--dataset-version", default=DEFAULT_DATASET_VERSION)
    parser.add_argument("--contour-id", default=DEFAULT_CONTOUR_ID)
    parser.add_argument("--indicator-set-version", default=DEFAULT_INDICATOR_SET_VERSION)
    parser.add_argument(
        "--derived-indicator-set-version",
        default=DEFAULT_DERIVED_INDICATOR_SET_VERSION,
    )
    parser.add_argument("--profile-version", default=DEFAULT_PROFILE_VERSION)
    parser.add_argument("--instruments", default="")
    parser.add_argument("--timeframes", default="")
    parser.add_argument("--spark-master", default="local[4]")
    parser.add_argument("--driver-memory", default="12g")
    parser.add_argument("--executor-memory", default="12g")
    parser.add_argument("--driver-max-result-size", default="4g")
    parser.add_argument("--shuffle-partitions", default="64")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    data_root = args.benchmark_data_root.resolve()
    run_data_root = data_root / args.run_id
    materialized_root = run_data_root / "materialized"
    artifact_root = args.artifact_root.resolve() / args.run_id
    source_root = args.source_materialized_root.resolve()

    process_payload = {
        "scenario": SCENARIO,
        "run_id": args.run_id,
        "started_at": _utc_now_iso(),
        "status": "running",
        "pid": os.getpid(),
        "source_materialized_root": source_root.as_posix(),
        "benchmark_materialized_root": materialized_root.as_posix(),
    }
    _write_json(artifact_root / "process.json", process_payload)

    if materialized_root.exists() and args.overwrite:
        _safe_remove_tree(materialized_root, allowed_parent=data_root)

    windows = _load_windows(args.windows_json) if args.windows_json.exists() else []
    scope = _scope_from_windows(
        windows,
        fallback_instruments=_split_csv(args.instruments),
        fallback_timeframes=_split_csv(args.timeframes),
    )
    if not scope["instrument_ids"] or not scope["timeframes"]:
        raise ValueError("benchmark scope requires windows JSON or explicit instruments/timeframes")

    runtime_env = _configure_runtime_env(
        data_root=run_data_root,
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
        driver_max_result_size=args.driver_max_result_size,
        shuffle_partitions=args.shuffle_partitions,
    )

    _copy_seed_tables(
        source_root=source_root,
        materialized_root=materialized_root,
        overwrite=args.overwrite,
    )

    scenario = {
        "scenario": SCENARIO,
        "run_id": args.run_id,
        "dataset_version": args.dataset_version,
        "contour_id": args.contour_id,
        "indicator_set_version": args.indicator_set_version,
        "derived_indicator_set_version": args.derived_indicator_set_version,
        "profile_version": args.profile_version,
        "source_materialized_root": source_root.as_posix(),
        "benchmark_materialized_root": materialized_root.as_posix(),
        "artifact_root": artifact_root.as_posix(),
        "runtime_env": runtime_env,
        "scope": scope,
    }
    _write_json(artifact_root / "scenario.json", scenario)
    _write_json(artifact_root / "windows.json", windows)

    counts_before = _table_counts(
        root=materialized_root,
        dataset_version=args.dataset_version,
        contour_id=args.contour_id,
        indicator_set_version=args.indicator_set_version,
        derived_indicator_set_version=args.derived_indicator_set_version,
        instruments=tuple(scope["instrument_ids"]),
        timeframes=tuple(scope["timeframes"]),
    )

    started = time.perf_counter()
    try:
        report = materialize_derived_indicator_frames(
            dataset_output_dir=source_root,
            indicator_output_dir=source_root,
            derived_indicator_output_dir=materialized_root,
            dataset_version=args.dataset_version,
            contour_id=args.contour_id,
            indicator_set_version=args.indicator_set_version,
            derived_indicator_set_version=args.derived_indicator_set_version,
            profile_version=args.profile_version,
            spark_master=args.spark_master,
            timeframes=tuple(scope["timeframes"]),
            dataset_instrument_ids=tuple(scope["instrument_ids"]),
        )
        status = "PASS"
        error: str | None = None
    except Exception as exc:  # pragma: no cover - benchmark diagnostic path
        report = {}
        status = "FAIL"
        error = f"{type(exc).__name__}: {exc}"
    duration_seconds = time.perf_counter() - started

    counts_after = (
        _table_counts(
            root=materialized_root,
            dataset_version=args.dataset_version,
            contour_id=args.contour_id,
            indicator_set_version=args.indicator_set_version,
            derived_indicator_set_version=args.derived_indicator_set_version,
            instruments=tuple(scope["instrument_ids"]),
            timeframes=tuple(scope["timeframes"]),
        )
        if status == "PASS"
        else {}
    )
    summary = {
        **scenario,
        "status": status,
        "error": error,
        "started_at": process_payload["started_at"],
        "ended_at": _utc_now_iso(),
        "duration_seconds": duration_seconds,
        "counts_before": counts_before,
        "counts_after": counts_after,
        "materialize_report": report,
        "stage_timings_ranked": _summarize_stage_timings(report),
        "source_frame_stage_timings_ranked": _summarize_stage_timings(
            report.get("source_frame_report", {}) if isinstance(report, dict) else {}
        ),
    }
    _write_json(artifact_root / "summary.json", summary)
    _write_markdown(artifact_root / "summary.md", summary)
    process_payload.update(
        {
            "status": status,
            "ended_at": summary["ended_at"],
            "duration_seconds": duration_seconds,
            "summary_path": (artifact_root / "summary.json").as_posix(),
        }
    )
    if error:
        process_payload["error"] = error
    _write_json(artifact_root / "process.json", process_payload)

    print(json.dumps(process_payload, ensure_ascii=False, indent=2))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
