from __future__ import annotations

import argparse
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS, load_universe, run_phase01_foundation
from trading_advisor_3000.product_plane.data_plane.moex.phase02_canonical import run_phase02_canonical


DEFAULT_MAPPING_REGISTRY = Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml")
DEFAULT_UNIVERSE = Path("configs/moex_phase01/universe/moex-futures-priority.v1.yaml")
DEFAULT_PHASE01_ROOT = Path("artifacts/codex/moex-phase01")
DEFAULT_PHASE02_ROOT = Path("artifacts/codex/moex-phase02")
DEFAULT_OUTPUT_ROOT = Path("artifacts/codex/moex-nightly")
DEFAULT_TIMEFRAMES = "5m,15m,1h,4h,1d,1w"
DEFAULT_BATCH_SIZE = 250_000


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _default_ingest_till_utc() -> str:
    now = datetime.now(tz=UTC).replace(second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _parse_timeframes(raw: str) -> set[str]:
    values = {item.strip() for item in raw.split(",") if item.strip()}
    if not values:
        raise ValueError("timeframes must not be empty")
    return values


def _split_symbols(symbols: list[Any], workers: int) -> list[list[Any]]:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    if not symbols:
        return []
    worker_count = min(workers, len(symbols))
    shards: list[list[Any]] = [[] for _ in range(worker_count)]
    for idx, symbol in enumerate(sorted(symbols, key=lambda item: item.internal_id)):
        shards[idx % worker_count].append(symbol)
    return [item for item in shards if item]


def _symbol_to_payload(symbol: Any) -> dict[str, object]:
    moex_payload: dict[str, object] = {
        "engine": symbol.moex_engine,
        "market": symbol.moex_market,
        "board": symbol.moex_board,
        "secid": symbol.moex_secid,
    }
    if symbol.moex_asset_codes:
        moex_payload["asset_codes"] = list(symbol.moex_asset_codes)
    return {
        "internal_id": symbol.internal_id,
        "asset_class": symbol.asset_class,
        "asset_group": symbol.asset_group,
        "status": symbol.status,
        "finam_symbol": symbol.finam_symbol,
        "moex": moex_payload,
    }


def _write_shard_universe(path: Path, symbols: list[Any], shard_id: str) -> Path:
    payload = {
        "version": 1,
        "source": "moex-nightly-sharded",
        "shard_id": shard_id,
        "symbols": [_symbol_to_payload(item) for item in symbols],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _run_shard_job(job: dict[str, object]) -> dict[str, object]:
    shard_id = str(job["shard_id"])
    try:
        report = run_phase01_foundation(
            mapping_registry_path=Path(str(job["mapping_registry_path"])),
            universe_path=Path(str(job["universe_path"])),
            output_dir=Path(str(job["output_dir"])),
            run_id=str(job["run_id"]),
            timeframes=set(job["timeframes"]),
            bootstrap_window_days=int(job["bootstrap_window_days"]),
            ingest_till_utc=str(job["ingest_till_utc"]),
            stability_lag_minutes=int(job["stability_lag_minutes"]),
            expand_contract_chain=bool(job["expand_contract_chain"]),
            contract_discovery_step_days=int(job["contract_discovery_step_days"]),
            refresh_overlap_minutes=int(job["refresh_overlap_minutes"]),
        )
        return {
            "status": "PASS",
            "shard_id": shard_id,
            "internal_ids": list(job["internal_ids"]),
            "report": report.to_dict(),
        }
    except Exception as exc:  # noqa: BLE001 - keep worker failure visible in nightly report
        return {
            "status": "FAIL",
            "shard_id": shard_id,
            "internal_ids": list(job.get("internal_ids", [])),
            "error": f"{type(exc).__name__}: {exc}",
        }


def _append_delta_table(
    *,
    source_path: Path,
    target_path: Path,
    batch_size: int,
    has_written_target: bool,
) -> bool:
    source_table = DeltaTable(str(source_path))
    scanner = source_table.to_pyarrow_dataset().scanner(batch_size=batch_size)

    wrote_any = has_written_target
    for batch in scanner.to_batches():
        table = pa.Table.from_batches([batch], schema=batch.schema)
        mode = "append" if wrote_any else "overwrite"
        write_deltalake(str(target_path), table, mode=mode)
        wrote_any = True
    return wrote_any


def _merge_raw_tables(
    *,
    shard_reports: list[dict[str, object]],
    target_raw_path: Path,
    batch_size: int,
) -> int:
    if target_raw_path.exists():
        import shutil

        shutil.rmtree(target_raw_path)
    target_raw_path.parent.mkdir(parents=True, exist_ok=True)

    wrote_any = False
    written_shards = 0
    for item in sorted(shard_reports, key=lambda row: str(row["shard_id"])):
        report = item["report"]
        if not isinstance(report, dict):
            continue
        source_raw = Path(str(report.get("raw_table_path", "")))
        if not source_raw.exists() or not has_delta_log(source_raw):
            continue
        wrote_any = _append_delta_table(
            source_path=source_raw,
            target_path=target_raw_path,
            batch_size=batch_size,
            has_written_target=wrote_any,
        )
        written_shards += 1

    if not wrote_any:
        write_delta_table_rows(table_path=target_raw_path, rows=[], columns=RAW_COLUMNS)
    return written_shards


def _merge_coverage_reports(
    *,
    shard_reports: list[dict[str, object]],
    output_dir: Path,
) -> tuple[Path, Path, int]:
    coverage_rows: list[dict[str, object]] = []
    for item in shard_reports:
        report = item["report"]
        if not isinstance(report, dict):
            continue
        coverage_path = Path(str(report.get("coverage_report_path", "")))
        if not coverage_path.exists():
            continue
        try:
            payload = json.loads(coverage_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, dict):
                    coverage_rows.append(row)

    coverage_rows = sorted(
        coverage_rows,
        key=lambda row: (
            str(row.get("internal_id", "")),
            str(row.get("source_interval", "")),
            str(row.get("moex_secid", "")),
            str(row.get("coverage_begin_utc", "")),
        ),
    )
    json_path = output_dir / "coverage-report.json"
    csv_path = output_dir / "coverage-report.csv"
    json_path.write_text(json.dumps(coverage_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    columns: list[str] = []
    for row in coverage_rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)
    if not columns:
        columns = [
            "internal_id",
            "finam_symbol",
            "moex_engine",
            "moex_market",
            "moex_board",
            "moex_secid",
            "asset_group",
            "requested_target_timeframes",
            "source_interval",
            "source_timeframe",
            "coverage_begin_utc",
            "coverage_end_utc",
            "discovered_at_utc",
            "discovery_url",
        ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in coverage_rows:
            writer.writerow({field: row.get(field) for field in columns})
    return json_path, csv_path, len(coverage_rows)


def _merge_watermarks(shard_reports: list[dict[str, object]]) -> dict[str, str]:
    merged: dict[str, str] = {}
    for item in shard_reports:
        report = item["report"]
        if not isinstance(report, dict):
            continue
        raw = report.get("watermark_by_key")
        if not isinstance(raw, dict):
            continue
        for key, value in raw.items():
            watermark_key = str(key)
            watermark_value = str(value)
            current = merged.get(watermark_key)
            if current is None or watermark_value > current:
                merged[watermark_key] = watermark_value
    return dict(sorted(merged.items()))


def _build_jobs(
    *,
    mapping_registry_path: Path,
    run_id: str,
    phase01_run_dir: Path,
    shards_dir: Path,
    shards: list[list[Any]],
    timeframes: set[str],
    bootstrap_window_days: int,
    ingest_till_utc: str,
    stability_lag_minutes: int,
    expand_contract_chain: bool,
    contract_discovery_step_days: int,
    refresh_overlap_minutes: int,
) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []
    for index, symbols in enumerate(shards, start=1):
        shard_id = f"shard-{index:02d}"
        universe_path = _write_shard_universe(
            shards_dir / f"{shard_id}.universe.yaml",
            symbols=symbols,
            shard_id=shard_id,
        )
        shard_output_dir = phase01_run_dir / "shards" / shard_id
        jobs.append(
            {
                "shard_id": shard_id,
                "internal_ids": [item.internal_id for item in symbols],
                "mapping_registry_path": mapping_registry_path.as_posix(),
                "universe_path": universe_path.as_posix(),
                "output_dir": shard_output_dir.as_posix(),
                "run_id": f"{run_id}-{shard_id}",
                "timeframes": sorted(timeframes),
                "bootstrap_window_days": bootstrap_window_days,
                "ingest_till_utc": ingest_till_utc,
                "stability_lag_minutes": stability_lag_minutes,
                "expand_contract_chain": expand_contract_chain,
                "contract_discovery_step_days": contract_discovery_step_days,
                "refresh_overlap_minutes": refresh_overlap_minutes,
            }
        )
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run sharded MOEX nightly backfill with parallel workers, consolidate raw Delta output, "
            "and build canonical bars in one end-to-end contour."
        )
    )
    parser.add_argument("--mapping-registry", default=DEFAULT_MAPPING_REGISTRY.as_posix())
    parser.add_argument("--universe", default=DEFAULT_UNIVERSE.as_posix())
    parser.add_argument("--phase01-root", default=DEFAULT_PHASE01_ROOT.as_posix())
    parser.add_argument("--phase02-root", default=DEFAULT_PHASE02_ROOT.as_posix())
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT.as_posix())
    parser.add_argument("--run-id", default="")
    parser.add_argument("--timeframes", default=DEFAULT_TIMEFRAMES)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--bootstrap-window-days", type=int, default=1461)
    parser.add_argument("--stability-lag-minutes", type=int, default=20)
    parser.add_argument(
        "--expand-contract-chain",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--contract-discovery-step-days", type=int, default=14)
    parser.add_argument("--refresh-overlap-minutes", type=int, default=180)
    parser.add_argument("--ingest-till-utc", default="")
    args = parser.parse_args()

    if args.workers <= 0:
        raise SystemExit("workers must be > 0")
    if args.batch_size <= 0:
        raise SystemExit("batch-size must be > 0")

    run_id = args.run_id.strip() or _default_run_id()
    ingest_till_utc = args.ingest_till_utc.strip() or _default_ingest_till_utc()
    timeframes = _parse_timeframes(args.timeframes)

    mapping_registry_path = _resolve(Path(args.mapping_registry))
    universe_path = _resolve(Path(args.universe))
    phase01_root = _resolve(Path(args.phase01_root))
    phase02_root = _resolve(Path(args.phase02_root))
    output_root = _resolve(Path(args.output_root))

    phase01_run_dir = phase01_root / run_id
    phase01_run_dir.mkdir(parents=True, exist_ok=True)
    phase02_run_dir = phase02_root / run_id
    phase02_run_dir.mkdir(parents=True, exist_ok=True)
    nightly_run_dir = output_root / run_id
    nightly_run_dir.mkdir(parents=True, exist_ok=True)
    shards_dir = nightly_run_dir / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)

    universe = load_universe(universe_path)
    active_symbols = [item for item in universe if item.is_active]
    if not active_symbols:
        raise SystemExit("nightly backfill cannot start: no active symbols in universe")

    shards = _split_symbols(active_symbols, args.workers)
    jobs = _build_jobs(
        mapping_registry_path=mapping_registry_path,
        run_id=run_id,
        phase01_run_dir=phase01_run_dir,
        shards_dir=shards_dir,
        shards=shards,
        timeframes=timeframes,
        bootstrap_window_days=args.bootstrap_window_days,
        ingest_till_utc=ingest_till_utc,
        stability_lag_minutes=args.stability_lag_minutes,
        expand_contract_chain=bool(args.expand_contract_chain),
        contract_discovery_step_days=args.contract_discovery_step_days,
        refresh_overlap_minutes=args.refresh_overlap_minutes,
    )

    shard_results: list[dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=len(jobs)) as executor:
        future_by_shard = {executor.submit(_run_shard_job, job): job["shard_id"] for job in jobs}
        for future in as_completed(future_by_shard):
            result = future.result()
            shard_results.append(result)

    shard_results = sorted(shard_results, key=lambda row: str(row.get("shard_id", "")))
    failed_shards = [item for item in shard_results if item.get("status") != "PASS"]
    if failed_shards:
        report = {
            "run_id": run_id,
            "status": "FAILED",
            "reason": "one or more shard workers failed",
            "failed_shards": failed_shards,
        }
        report_path = nightly_run_dir / "nightly-backfill-report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        raise SystemExit("nightly backfill failed: at least one shard worker returned FAIL")

    coverage_json, coverage_csv, coverage_rows = _merge_coverage_reports(
        shard_reports=shard_results,
        output_dir=phase01_run_dir,
    )
    consolidated_raw_path = phase01_run_dir / "delta" / "raw_moex_history.delta"
    merged_shards = _merge_raw_tables(
        shard_reports=shard_results,
        target_raw_path=consolidated_raw_path,
        batch_size=args.batch_size,
    )

    source_rows = 0
    incremental_rows = 0
    deduplicated_rows = 0
    stale_rows = 0
    source_intervals: set[int] = set()
    source_timeframes: set[str] = set()
    real_bindings: set[str] = set()
    for item in shard_results:
        report = item["report"]
        if not isinstance(report, dict):
            continue
        source_rows += int(report.get("source_rows", 0))
        incremental_rows += int(report.get("incremental_rows", 0))
        deduplicated_rows += int(report.get("deduplicated_rows", 0))
        stale_rows += int(report.get("stale_rows", 0))
        for value in report.get("source_interval_set", []):
            source_intervals.add(int(value))
        for value in report.get("source_timeframe_set", []):
            source_timeframes.add(str(value))
        for value in report.get("real_bindings", []):
            real_bindings.add(str(value))

    phase01_report = {
        "run_id": run_id,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "mode": "parallel-sharded-nightly",
        "ingest_till_utc": ingest_till_utc,
        "timeframes": sorted(timeframes),
        "workers_requested": int(args.workers),
        "workers_started": len(jobs),
        "merged_shards": merged_shards,
        "bootstrap_window_days": int(args.bootstrap_window_days),
        "stability_lag_minutes": int(args.stability_lag_minutes),
        "expand_contract_chain": bool(args.expand_contract_chain),
        "contract_discovery_step_days": int(args.contract_discovery_step_days),
        "refresh_overlap_minutes": int(args.refresh_overlap_minutes),
        "source_intervals": sorted(source_intervals),
        "source_timeframes": sorted(source_timeframes),
        "coverage_rows": coverage_rows,
        "source_rows": source_rows,
        "incremental_rows": incremental_rows,
        "deduplicated_rows": deduplicated_rows,
        "stale_rows": stale_rows,
        "watermark_by_key": _merge_watermarks(shard_results),
        "artifacts": {
            "coverage_report": coverage_json.as_posix(),
            "coverage_table": coverage_csv.as_posix(),
            "raw_table": consolidated_raw_path.as_posix(),
        },
        "shards": shard_results,
        "real_bindings": sorted(real_bindings),
    }
    phase01_report_path = phase01_run_dir / "phase01-foundation-report.json"
    phase01_report_path.write_text(json.dumps(phase01_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    canonical_report = run_phase02_canonical(
        raw_table_path=consolidated_raw_path,
        output_dir=phase02_run_dir,
        run_id=run_id,
        repo_root=ROOT,
    )

    nightly_status = "PASS" if str(canonical_report.get("publish_decision")) == "publish" else "BLOCKED"
    report = {
        "run_id": run_id,
        "status": nightly_status,
        "phase01_run_id": run_id,
        "phase02_run_id": run_id,
        "workers_requested": int(args.workers),
        "workers_started": len(jobs),
        "phase01_report_path": phase01_report_path.as_posix(),
        "phase02_report_path": (phase02_run_dir / "phase02-canonical-report.json").as_posix(),
        "phase01_summary": {
            "coverage_rows": coverage_rows,
            "source_rows": source_rows,
            "incremental_rows": incremental_rows,
            "deduplicated_rows": deduplicated_rows,
            "stale_rows": stale_rows,
            "source_intervals": sorted(source_intervals),
            "source_timeframes": sorted(source_timeframes),
        },
        "canonical_summary": {
            "publish_decision": canonical_report.get("publish_decision"),
            "source_rows": canonical_report.get("source_rows"),
            "canonical_rows": canonical_report.get("canonical_rows"),
            "target_timeframes": canonical_report.get("target_timeframes"),
        },
        "artifacts": {
            "phase01_root": phase01_run_dir.as_posix(),
            "phase02_root": phase02_run_dir.as_posix(),
            "nightly_root": nightly_run_dir.as_posix(),
        },
    }
    report_path = nightly_run_dir / "nightly-backfill-report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if nightly_status != "PASS":
        raise SystemExit("nightly backfill blocked: phase-02 canonical publish_decision is not publish")


if __name__ == "__main__":
    main()
