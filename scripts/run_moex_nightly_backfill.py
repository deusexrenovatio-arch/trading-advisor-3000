from __future__ import annotations

import argparse
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import traceback
from typing import Any

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import build_raw_ingest_run_report_v2
from trading_advisor_3000.product_plane.data_plane.moex.foundation import (
    RAW_COLUMNS,
    DiscoveryRecord,
    FoundationRunReport,
    _append_progress_event,
    _write_coverage_artifacts,
    discover_coverage,
    ingest_moex_bootstrap_window,
    load_mapping_registry,
    load_universe,
    validate_mapping_registry,
    validate_universe_mapping_alignment,
)
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexISSClient
from trading_advisor_3000.product_plane.data_plane.moex.phase02_canonical import run_phase02_canonical
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    NIGHTLY_STORAGE_DIRNAME,
    PHASE01_STORAGE_DIRNAME,
    PHASE02_STORAGE_DIRNAME,
    resolve_external_root,
)


DEFAULT_MAPPING_REGISTRY = Path("configs/moex_phase01/instrument_mapping_registry.v1.yaml")
DEFAULT_UNIVERSE = Path("configs/moex_phase01/universe/moex-futures-priority.v1.yaml")
DEFAULT_TIMEFRAMES = "5m,15m,1h,4h,1d,1w"
DEFAULT_BATCH_SIZE = 250_000
DEFAULT_EXECUTION_MODE = "sequential"
DEFAULT_CONTRACT_DISCOVERY_LOOKBACK_DAYS = 180
LEGACY_ROUTE_ACK_ENV = "TA3000_ALLOW_LEGACY_MOEX_NIGHTLY"
LEGACY_ROUTE_DECISION_DOC = "docs/architecture/product-plane/moex-historical-route-decision.md"


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else (ROOT / path).resolve()


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _default_ingest_till_utc() -> str:
    now = datetime.now(tz=UTC).replace(second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _assert_legacy_route_enabled(*, allow_legacy_route: bool) -> None:
    if allow_legacy_route or os.environ.get(LEGACY_ROUTE_ACK_ENV, "").strip() == "1":
        return
    raise SystemExit(
        "scripts/run_moex_nightly_backfill.py is a retired legacy full-snapshot route and is blocked by default. "
        "Use the sanctioned historical route decision in "
        f"{LEGACY_ROUTE_DECISION_DOC} and the manual raw-ingest/canonicalization runbooks for bootstrap or repair. "
        f"If you intentionally need a forensic legacy rerun, pass --allow-legacy-route or set {LEGACY_ROUTE_ACK_ENV}=1."
    )


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


def _load_cached_shard_success(output_dir: Path) -> dict[str, object] | None:
    success_path = output_dir / "shard-success.json"
    if not success_path.exists():
        return None
    try:
        payload = json.loads(success_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("status", "")).strip() != "PASS":
        return None
    report = payload.get("report")
    if not isinstance(report, dict):
        return None
    raw_table_path = Path(str(report.get("raw_table_path", "")).strip())
    raw_ingest_report_path = Path(str(report.get("raw_ingest_report_path", "")).strip())
    coverage_report_path = Path(str(report.get("coverage_report_path", "")).strip())
    coverage_table_path = Path(str(report.get("coverage_table_path", "")).strip())
    if not raw_table_path.as_posix() or not has_delta_log(raw_table_path):
        return None
    if not raw_ingest_report_path.exists():
        return None
    if not coverage_report_path.exists() or not coverage_table_path.exists():
        return None
    result = dict(payload)
    result["resume_mode"] = "reused_success"
    return result


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


def _discovery_record_from_payload(payload: dict[str, object]) -> DiscoveryRecord:
    return DiscoveryRecord(
        internal_id=str(payload["internal_id"]),
        finam_symbol=str(payload["finam_symbol"]),
        moex_engine=str(payload["moex_engine"]),
        moex_market=str(payload["moex_market"]),
        moex_board=str(payload["moex_board"]),
        moex_secid=str(payload["moex_secid"]),
        asset_group=str(payload["asset_group"]),
        requested_target_timeframes=str(payload["requested_target_timeframes"]),
        source_interval=int(payload["source_interval"]),
        source_timeframe=str(payload["source_timeframe"]),
        coverage_begin_utc=str(payload["coverage_begin_utc"]),
        coverage_end_utc=str(payload["coverage_end_utc"]),
        discovered_at_utc=str(payload["discovered_at_utc"]),
        discovery_url=str(payload["discovery_url"]),
    )


def _event_sort_key(payload: dict[str, object]) -> tuple[str, str]:
    for key in ("emitted_at_utc", "processed_at_utc", "reported_at_utc"):
        value = str(payload.get(key, "")).strip()
        if value:
            return value, json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return "", json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _merge_jsonl_artifacts(
    *,
    source_paths: list[Path],
    target_path: Path,
    latest_path: Path,
    empty_latest_payload: dict[str, object] | None = None,
) -> int:
    events: list[dict[str, object]] = []
    for source_path in source_paths:
        if not source_path.exists() or source_path.is_dir():
            continue
        for raw_line in source_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                events.append(payload)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as handle:
        for payload in sorted(events, key=_event_sort_key):
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    if events:
        latest_payload = max(events, key=_event_sort_key)
        latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    elif empty_latest_payload is not None:
        latest_path.write_text(json.dumps(empty_latest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    else:
        latest_path.unlink(missing_ok=True)
    return len(events)


def _run_shard_job(job: dict[str, object]) -> dict[str, object]:
    shard_id = str(job["shard_id"])
    output_dir = Path(str(job["output_dir"]))
    cached_success = _load_cached_shard_success(output_dir)
    if cached_success is not None:
        return cached_success
    try:
        coverage = [
            _discovery_record_from_payload(item)
            for item in list(job.get("coverage", []))
            if isinstance(item, dict)
        ]
        if not coverage:
            raise ValueError(f"shared discovery produced no coverage rows for shard `{shard_id}`")

        coverage_json, coverage_csv = _write_coverage_artifacts(coverage, output_dir=output_dir)
        request_log_path = output_dir / "moex-request-log.jsonl"
        request_latest_path = output_dir / "moex-request.latest.json"
        client = MoexISSClient(
            request_event_hook=lambda payload: _append_progress_event(
                jsonl_path=request_log_path,
                latest_path=request_latest_path,
                payload=payload,
            )
        )
        raw_table_path = output_dir / "delta" / "raw_moex_history.delta"
        ingest_report = ingest_moex_bootstrap_window(
            client=client,
            coverage=coverage,
            table_path=raw_table_path,
            run_id=str(job["run_id"]),
            ingest_till_utc=str(job["ingest_till_utc"]),
            bootstrap_window_days=int(job["bootstrap_window_days"]),
            stability_lag_minutes=int(job["stability_lag_minutes"]),
            refresh_overlap_minutes=int(job["refresh_overlap_minutes"]),
            progress_path=output_dir / "raw-ingest-progress.jsonl",
            progress_latest_path=output_dir / "raw-ingest-progress.latest.json",
            error_path=output_dir / "raw-ingest-errors.jsonl",
            error_latest_path=output_dir / "raw-ingest-error.latest.json",
        )
        raw_ingest_report_path = output_dir / "raw-ingest-report.json"
        raw_ingest_report_path.write_text(
            json.dumps(ingest_report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        report = FoundationRunReport(
            run_id=str(job["run_id"]),
            route_signal="worker:phase-only",
            timeframe_set=sorted({value for item in coverage for value in item.requested_target_timeframes.split(",")}),
            source_interval_set=sorted({item.source_interval for item in coverage}),
            source_timeframe_set=sorted({item.source_timeframe for item in coverage}),
            expand_contract_chain=bool(job["expand_contract_chain"]),
            contract_discovery_step_days=int(job["contract_discovery_step_days"]),
            contract_discovery_lookback_days=int(job["contract_discovery_lookback_days"]),
            refresh_overlap_minutes=int(job["refresh_overlap_minutes"]),
            universe_size=len(list(job["internal_ids"])),
            coverage_rows=len(coverage),
            coverage_report_path=coverage_json.as_posix(),
            coverage_table_path=coverage_csv.as_posix(),
            moex_request_log_path=request_log_path.as_posix(),
            moex_request_latest_path=request_latest_path.as_posix(),
            raw_ingest_report_path=raw_ingest_report_path.as_posix(),
            raw_ingest_progress_path=str(ingest_report["raw_ingest_progress_path"]),
            raw_ingest_error_path=str(ingest_report["raw_ingest_error_path"]),
            raw_ingest_error_latest_path=str(ingest_report["raw_ingest_error_latest_path"]),
            raw_table_path=raw_table_path.as_posix(),
            source_rows=int(ingest_report["source_rows"]),
            incremental_rows=int(ingest_report["incremental_rows"]),
            deduplicated_rows=int(ingest_report["deduplicated_rows"]),
            stale_rows=int(ingest_report["stale_rows"]),
            watermark_by_key=dict(ingest_report["watermark_by_key"]),
            real_bindings=[
                "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/<SECID>/candles.json",
            ],
        )
        result = {
            "status": "PASS",
            "shard_id": shard_id,
            "internal_ids": list(job["internal_ids"]),
            "output_dir": output_dir.as_posix(),
            "report": report.to_dict(),
        }
        (output_dir / "shard-success.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return result
    except Exception as exc:  # noqa: BLE001 - keep worker failure visible in nightly report
        (output_dir / "shard-success.json").unlink(missing_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        progress_latest = output_dir / "raw-ingest-progress.latest.json"
        ingest_error_latest = output_dir / "raw-ingest-error.latest.json"
        request_latest = output_dir / "moex-request.latest.json"
        traceback_text = traceback.format_exc()
        error_payload = {
            "status": "FAIL",
            "shard_id": shard_id,
            "run_id": str(job.get("run_id", "")),
            "internal_ids": list(job.get("internal_ids", [])),
            "output_dir": output_dir.as_posix(),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback_text,
            "progress_latest_path": progress_latest.as_posix() if progress_latest.exists() else None,
            "ingest_error_latest_path": ingest_error_latest.as_posix() if ingest_error_latest.exists() else None,
            "request_latest_path": request_latest.as_posix() if request_latest.exists() else None,
            "reported_at_utc": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        }
        (output_dir / "shard-error.json").write_text(
            json.dumps(error_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "status": "FAIL",
            "shard_id": shard_id,
            "internal_ids": list(job.get("internal_ids", [])),
            "output_dir": output_dir.as_posix(),
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback_text,
            "shard_error_path": (output_dir / "shard-error.json").as_posix(),
            "progress_latest_path": progress_latest.as_posix() if progress_latest.exists() else None,
            "ingest_error_latest_path": ingest_error_latest.as_posix() if ingest_error_latest.exists() else None,
            "request_latest_path": request_latest.as_posix() if request_latest.exists() else None,
        }


def _append_jsonl_event(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _safe_progress_print(message: str) -> None:
    try:
        print(message, flush=True)
    except OSError:
        # The durable report files are authoritative; a broken stdout pipe must not fail the run.
        return


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


def _merge_changed_windows(shard_reports: list[dict[str, object]]) -> list[dict[str, object]]:
    changed_windows: list[dict[str, object]] = []
    for item in shard_reports:
        report = item.get("report")
        if not isinstance(report, dict):
            continue
        raw_ingest_report_path = Path(str(report.get("raw_ingest_report_path", "")))
        if not raw_ingest_report_path.exists():
            continue
        try:
            payload = json.loads(raw_ingest_report_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        candidate_windows = payload.get("changed_windows", [])
        if not isinstance(candidate_windows, list):
            continue
        for row in candidate_windows:
            if isinstance(row, dict):
                changed_windows.append(dict(row))
    return changed_windows


def _build_jobs(
    *,
    run_id: str,
    phase01_run_dir: Path,
    shards_dir: Path,
    shards: list[list[Any]],
    coverage: list[DiscoveryRecord],
    timeframes: set[str],
    bootstrap_window_days: int,
    ingest_till_utc: str,
    stability_lag_minutes: int,
    expand_contract_chain: bool,
    contract_discovery_step_days: int,
    contract_discovery_lookback_days: int,
    refresh_overlap_minutes: int,
) -> list[dict[str, object]]:
    coverage_by_internal_id: dict[str, list[DiscoveryRecord]] = {}
    for item in coverage:
        coverage_by_internal_id.setdefault(item.internal_id, []).append(item)

    jobs: list[dict[str, object]] = []
    for index, symbols in enumerate(shards, start=1):
        shard_id = f"shard-{index:02d}"
        internal_ids = [item.internal_id for item in symbols]
        shard_coverage = [
            row
            for internal_id in internal_ids
            for row in coverage_by_internal_id.get(internal_id, [])
        ]
        if not shard_coverage:
            missing_text = ", ".join(sorted(internal_ids))
            raise RuntimeError(f"shared discovery produced no coverage rows for shard `{shard_id}`: {missing_text}")

        _write_shard_universe(
            shards_dir / f"{shard_id}.universe.yaml",
            symbols=symbols,
            shard_id=shard_id,
        )
        shard_output_dir = phase01_run_dir / "shards" / shard_id
        _write_coverage_artifacts(shard_coverage, output_dir=shard_output_dir)
        jobs.append(
            {
                "shard_id": shard_id,
                "internal_ids": internal_ids,
                "coverage": [item.to_dict() for item in shard_coverage],
                "output_dir": shard_output_dir.as_posix(),
                "run_id": f"{run_id}-{shard_id}",
                "timeframes": sorted(timeframes),
                "bootstrap_window_days": bootstrap_window_days,
                "ingest_till_utc": ingest_till_utc,
                "stability_lag_minutes": stability_lag_minutes,
                "expand_contract_chain": expand_contract_chain,
                "contract_discovery_step_days": contract_discovery_step_days,
                "contract_discovery_lookback_days": contract_discovery_lookback_days,
                "refresh_overlap_minutes": refresh_overlap_minutes,
            }
        )
    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Legacy full-snapshot MOEX nightly contour. This script is retained only for explicit forensic reruns "
            "and is not the canonical historical refresh route."
        )
    )
    parser.add_argument("--mapping-registry", default=DEFAULT_MAPPING_REGISTRY.as_posix())
    parser.add_argument("--universe", default=DEFAULT_UNIVERSE.as_posix())
    parser.add_argument(
        "--phase01-root",
        default="",
        help=(
            "Absolute external phase-01 artifact root. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument(
        "--phase02-root",
        default="",
        help=(
            "Absolute external phase-02 artifact root. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument(
        "--output-root",
        default="",
        help=(
            "Absolute external nightly route root. "
            "Required unless TA3000_MOEX_HISTORICAL_DATA_ROOT is set."
        ),
    )
    parser.add_argument("--run-id", default="")
    parser.add_argument("--timeframes", default=DEFAULT_TIMEFRAMES)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--execution-mode", choices=("sequential", "parallel"), default=DEFAULT_EXECUTION_MODE)
    parser.add_argument("--bootstrap-window-days", type=int, default=1461)
    parser.add_argument("--stability-lag-minutes", type=int, default=20)
    parser.add_argument(
        "--stop-after-raw-ingest",
        action="store_true",
        help=(
            "Execute only the sharded raw ingest refresh slice and stop before canonicalization. "
            "Use this when Dagster owns step ordering and canonicalization runs as a separate route step."
        ),
    )
    parser.add_argument(
        "--allow-legacy-route",
        action="store_true",
        help=(
            "Explicitly acknowledge execution of the retired full-snapshot route. "
            "Use only for diagnostics or historical forensic reruns."
        ),
    )
    parser.add_argument(
        "--expand-contract-chain",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--contract-discovery-step-days", type=int, default=14)
    parser.add_argument(
        "--contract-discovery-lookback-days",
        type=int,
        default=0,
        help=(
            "Optional shared discovery lookback window for scheduled refresh. "
            "When <= 0, the route falls back to bootstrap-window-days."
        ),
    )
    parser.add_argument("--refresh-overlap-minutes", type=int, default=180)
    parser.add_argument("--ingest-till-utc", default="")
    args = parser.parse_args()

    if args.workers <= 0:
        raise SystemExit("workers must be > 0")
    if args.batch_size <= 0:
        raise SystemExit("batch-size must be > 0")
    _assert_legacy_route_enabled(allow_legacy_route=bool(args.allow_legacy_route))

    run_id = args.run_id.strip() or _default_run_id()
    ingest_till_utc = args.ingest_till_utc.strip() or _default_ingest_till_utc()
    timeframes = _parse_timeframes(args.timeframes)

    mapping_registry_path = _resolve(Path(args.mapping_registry))
    universe_path = _resolve(Path(args.universe))
    phase01_root = resolve_external_root(
        args.phase01_root,
        repo_root=ROOT,
        field_name="--phase01-root",
        default_subdir=PHASE01_STORAGE_DIRNAME,
    )
    phase02_root = resolve_external_root(
        args.phase02_root,
        repo_root=ROOT,
        field_name="--phase02-root",
        default_subdir=PHASE02_STORAGE_DIRNAME,
    )
    output_root = resolve_external_root(
        args.output_root,
        repo_root=ROOT,
        field_name="--output-root",
        default_subdir=NIGHTLY_STORAGE_DIRNAME,
    )

    phase01_run_dir = phase01_root / run_id
    phase01_run_dir.mkdir(parents=True, exist_ok=True)
    phase02_run_dir = phase02_root / run_id
    phase02_run_dir.mkdir(parents=True, exist_ok=True)
    nightly_run_dir = output_root / run_id
    nightly_run_dir.mkdir(parents=True, exist_ok=True)
    shards_dir = nightly_run_dir / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)

    universe = load_universe(universe_path)
    mappings = load_mapping_registry(mapping_registry_path)
    validate_mapping_registry(mappings)
    validate_universe_mapping_alignment(universe, mappings)
    active_symbols = [item for item in universe if item.is_active]
    if not active_symbols:
        raise SystemExit("nightly backfill cannot start: no active symbols in universe")

    contract_discovery_lookback_days = (
        int(args.contract_discovery_lookback_days)
        if int(args.contract_discovery_lookback_days) > 0
        else int(args.bootstrap_window_days)
    )
    shared_request_log_path = phase01_run_dir / "moex-request-log.jsonl"
    shared_request_latest_path = phase01_run_dir / "moex-request.latest.json"
    shared_client = MoexISSClient(
        request_event_hook=lambda payload: _append_progress_event(
            jsonl_path=shared_request_log_path,
            latest_path=shared_request_latest_path,
            payload=payload,
        )
    )
    shared_discovered_at_utc = datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    try:
        coverage = discover_coverage(
            client=shared_client,
            universe=universe,
            mappings=mappings,
            timeframes=timeframes,
            discovered_at_utc=shared_discovered_at_utc,
            ingest_till_utc=ingest_till_utc,
            bootstrap_window_days=int(args.bootstrap_window_days),
            expand_contract_chain=bool(args.expand_contract_chain),
            contract_discovery_step_days=int(args.contract_discovery_step_days),
            contract_discovery_lookback_days=contract_discovery_lookback_days,
        )
        coverage_json, coverage_csv = _write_coverage_artifacts(coverage, output_dir=phase01_run_dir)
    except Exception as exc:  # noqa: BLE001 - discovery must emit a durable route failure report
        report = {
            "run_id": run_id,
            "status": "FAILED",
            "stage": "shared_contract_discovery",
            "reason": "shared contract discovery failed before shard ingest",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "moex_request_log_path": shared_request_log_path.as_posix(),
            "moex_request_latest_path": shared_request_latest_path.as_posix(),
        }
        report_path = nightly_run_dir / "nightly-backfill-report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        _safe_progress_print(json.dumps(report, ensure_ascii=False, indent=2))
        raise SystemExit("nightly backfill failed during shared contract discovery") from exc

    shards = _split_symbols(active_symbols, args.workers)
    jobs = _build_jobs(
        run_id=run_id,
        phase01_run_dir=phase01_run_dir,
        shards_dir=shards_dir,
        shards=shards,
        coverage=coverage,
        timeframes=timeframes,
        bootstrap_window_days=args.bootstrap_window_days,
        ingest_till_utc=ingest_till_utc,
        stability_lag_minutes=args.stability_lag_minutes,
        expand_contract_chain=bool(args.expand_contract_chain),
        contract_discovery_step_days=args.contract_discovery_step_days,
        contract_discovery_lookback_days=contract_discovery_lookback_days,
        refresh_overlap_minutes=args.refresh_overlap_minutes,
    )

    shard_results: list[dict[str, object]] = []
    progress_path = nightly_run_dir / "nightly-progress.jsonl"
    if args.execution_mode == "parallel":
        with ProcessPoolExecutor(max_workers=len(jobs)) as executor:
            future_by_shard = {executor.submit(_run_shard_job, job): job["shard_id"] for job in jobs}
            for future in as_completed(future_by_shard):
                result = future.result()
                shard_results.append(result)
                progress_payload = {
                    "run_id": run_id,
                    "execution_mode": args.execution_mode,
                    "shard_id": str(result.get("shard_id", "")),
                    "status": str(result.get("status", "")),
                    "internal_ids": list(result.get("internal_ids", [])),
                    "reported_at_utc": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                }
                _append_jsonl_event(progress_path, progress_payload)
                _safe_progress_print(
                    "[moex-nightly] "
                    f"{run_id} shard={progress_payload['shard_id']} "
                    f"status={progress_payload['status']} mode={args.execution_mode}"
                )
    else:
        for index, job in enumerate(jobs, start=1):
            result = _run_shard_job(job)
            shard_results.append(result)
            progress_payload = {
                "run_id": run_id,
                "execution_mode": args.execution_mode,
                "shard_index": index,
                "shard_total": len(jobs),
                "shard_id": str(result.get("shard_id", "")),
                "status": str(result.get("status", "")),
                "internal_ids": list(result.get("internal_ids", [])),
                "reported_at_utc": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            }
            _append_jsonl_event(progress_path, progress_payload)
            _safe_progress_print(
                "[moex-nightly] "
                f"{run_id} shard={progress_payload['shard_id']} "
                f"status={progress_payload['status']} "
                f"step={index}/{len(jobs)} mode={args.execution_mode}"
            )

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
        _safe_progress_print(json.dumps(report, ensure_ascii=False, indent=2))
        raise SystemExit("nightly backfill failed: at least one shard worker returned FAIL")

    coverage_rows = len(coverage)
    consolidated_raw_path = phase01_run_dir / "delta" / "raw_moex_history.delta"
    merged_shards = _merge_raw_tables(
        shard_reports=shard_results,
        target_raw_path=consolidated_raw_path,
        batch_size=args.batch_size,
    )
    merged_request_event_count = _merge_jsonl_artifacts(
        source_paths=[
            shared_request_log_path,
            *[
                Path(str(item["report"].get("moex_request_log_path", "")))
                for item in shard_results
                if isinstance(item.get("report"), dict)
            ],
        ],
        target_path=shared_request_log_path,
        latest_path=shared_request_latest_path,
    )
    raw_ingest_progress_path = phase01_run_dir / "raw-ingest-progress.jsonl"
    raw_ingest_progress_latest_path = phase01_run_dir / "raw-ingest-progress.latest.json"
    merged_progress_event_count = _merge_jsonl_artifacts(
        source_paths=[
            Path(str(item["report"].get("raw_ingest_progress_path", "")))
            for item in shard_results
            if isinstance(item.get("report"), dict)
        ],
        target_path=raw_ingest_progress_path,
        latest_path=raw_ingest_progress_latest_path,
        empty_latest_payload={
            "run_id": run_id,
            "status": "NO-DATA",
            "reported_at_utc": datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
    )
    raw_ingest_error_path = phase01_run_dir / "raw-ingest-errors.jsonl"
    raw_ingest_error_latest_path = phase01_run_dir / "raw-ingest-error.latest.json"
    merged_error_event_count = _merge_jsonl_artifacts(
        source_paths=[
            Path(str(item["report"].get("raw_ingest_error_path", "")))
            for item in shard_results
            if isinstance(item.get("report"), dict)
        ],
        target_path=raw_ingest_error_path,
        latest_path=raw_ingest_error_latest_path,
        empty_latest_payload={
            "run_id": run_id,
            "status": "PASS",
            "reported_at_utc": datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "message": "no raw ingest errors recorded",
        },
    )

    source_rows = 0
    incremental_rows = 0
    deduplicated_rows = 0
    stale_rows = 0
    source_intervals: set[int] = set()
    source_timeframes: set[str] = set()
    real_bindings: set[str] = set()
    merged_watermarks = _merge_watermarks(shard_results)
    changed_windows = _merge_changed_windows(shard_results)
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
    real_bindings.update(
        {
            "https://iss.moex.com/iss/history/engines/futures/markets/forts/boards/RFUD/securities.json",
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/<SECID>/candleborders.json",
        }
    )

    raw_ingest_report_payload = build_raw_ingest_run_report_v2(
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
        source_rows=source_rows,
        incremental_rows=incremental_rows,
        deduplicated_rows=deduplicated_rows,
        stale_rows=stale_rows,
        watermark_by_key=merged_watermarks,
        raw_table_path=consolidated_raw_path.as_posix(),
        raw_ingest_progress_path=raw_ingest_progress_path.as_posix(),
        raw_ingest_error_path=raw_ingest_error_path.as_posix(),
        raw_ingest_error_latest_path=raw_ingest_error_latest_path.as_posix(),
        changed_windows=changed_windows,
    )
    raw_ingest_report_path = phase01_run_dir / "raw-ingest-report.json"
    raw_ingest_report_path.write_text(
        json.dumps(raw_ingest_report_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    phase01_report = {
        "run_id": run_id,
        "route_signal": "worker:phase-only",
        "proof_class": "staging-real",
        "mode": f"{args.execution_mode}-sharded-nightly",
        "ingest_till_utc": ingest_till_utc,
        "timeframes": sorted(timeframes),
        "execution_mode": args.execution_mode,
        "workers_requested": int(args.workers),
        "workers_started": len(jobs),
        "merged_shards": merged_shards,
        "bootstrap_window_days": int(args.bootstrap_window_days),
        "stability_lag_minutes": int(args.stability_lag_minutes),
        "expand_contract_chain": bool(args.expand_contract_chain),
        "contract_discovery_step_days": int(args.contract_discovery_step_days),
        "contract_discovery_lookback_days": contract_discovery_lookback_days,
        "refresh_overlap_minutes": int(args.refresh_overlap_minutes),
        "source_intervals": sorted(source_intervals),
        "source_timeframes": sorted(source_timeframes),
        "coverage_rows": coverage_rows,
        "source_rows": source_rows,
        "incremental_rows": incremental_rows,
        "deduplicated_rows": deduplicated_rows,
        "stale_rows": stale_rows,
        "watermark_by_key": merged_watermarks,
        "merged_request_event_count": merged_request_event_count,
        "merged_progress_event_count": merged_progress_event_count,
        "merged_error_event_count": merged_error_event_count,
        "artifacts": {
            "coverage_report": coverage_json.as_posix(),
            "coverage_table": coverage_csv.as_posix(),
            "raw_table": consolidated_raw_path.as_posix(),
            "raw_ingest_report": raw_ingest_report_path.as_posix(),
            "moex_request_log": shared_request_log_path.as_posix(),
            "moex_request_latest": shared_request_latest_path.as_posix(),
            "raw_ingest_progress": raw_ingest_progress_path.as_posix(),
            "raw_ingest_progress_latest": raw_ingest_progress_latest_path.as_posix(),
            "raw_ingest_errors": raw_ingest_error_path.as_posix(),
            "raw_ingest_error_latest": raw_ingest_error_latest_path.as_posix(),
        },
        "shards": shard_results,
        "real_bindings": sorted(real_bindings),
    }
    phase01_report_path = phase01_run_dir / "phase01-foundation-report.json"
    phase01_report_path.write_text(json.dumps(phase01_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.stop_after_raw_ingest:
        report = {
            "run_id": run_id,
            "status": "PASS",
            "route_signal": "worker:phase-only",
            "step": "raw_ingest",
            "workers_requested": int(args.workers),
            "workers_started": len(jobs),
            "execution_mode": args.execution_mode,
            "raw_ingest_report_path": raw_ingest_report_path.as_posix(),
            "raw_table_path": consolidated_raw_path.as_posix(),
            "raw_ingest_root": phase01_run_dir.as_posix(),
            "nightly_progress_path": progress_path.as_posix(),
            "raw_ingest_summary": {
                "coverage_rows": coverage_rows,
                "source_rows": source_rows,
                "incremental_rows": incremental_rows,
                "deduplicated_rows": deduplicated_rows,
                "stale_rows": stale_rows,
                "source_intervals": sorted(source_intervals),
                "source_timeframes": sorted(source_timeframes),
                "contract_discovery_lookback_days": contract_discovery_lookback_days,
            },
            "artifacts": {
                "raw_ingest_root": phase01_run_dir.as_posix(),
                "raw_ingest_report": raw_ingest_report_path.as_posix(),
                "raw_ingest_summary_report": phase01_report_path.as_posix(),
                "raw_table": consolidated_raw_path.as_posix(),
                "nightly_root": nightly_run_dir.as_posix(),
            },
        }
        report_path = nightly_run_dir / "nightly-backfill-report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        _safe_progress_print(json.dumps(report, ensure_ascii=False, indent=2))
        return

    canonical_report = run_phase02_canonical(
        raw_table_path=consolidated_raw_path,
        output_dir=phase02_run_dir,
        run_id=run_id,
        raw_ingest_run_report=raw_ingest_report_payload,
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
        "execution_mode": args.execution_mode,
        "phase01_report_path": phase01_report_path.as_posix(),
        "phase02_report_path": (phase02_run_dir / "phase02-canonical-report.json").as_posix(),
        "nightly_progress_path": progress_path.as_posix(),
        "phase01_summary": {
            "coverage_rows": coverage_rows,
            "source_rows": source_rows,
            "incremental_rows": incremental_rows,
            "deduplicated_rows": deduplicated_rows,
            "stale_rows": stale_rows,
            "source_intervals": sorted(source_intervals),
            "source_timeframes": sorted(source_timeframes),
            "contract_discovery_lookback_days": contract_discovery_lookback_days,
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
    _safe_progress_print(json.dumps(report, ensure_ascii=False, indent=2))

    if nightly_status != "PASS":
        raise SystemExit("nightly backfill blocked: canonicalization publish_decision is not publish")


if __name__ == "__main__":
    main()
