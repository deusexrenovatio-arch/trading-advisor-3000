from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from .foundation import (
    DiscoveryRecord,
    _parse_iso_utc,
    _parse_moex_datetime,
    _to_iso_utc,
    _utc_now_iso,
    _write_raw_source_row,
)
from .iss_client import MoexISSClient


def _write_json(path: Path, payload: Any) -> None:
    temporary = path.with_name(path.name + "." + str(uuid4()) + ".partial")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _digest(path: Path) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    size = lines = 0
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
            size += len(block)
            lines += block.count(b"\n")
    return digest.hexdigest(), size, lines


def plan_scopes(
    coverage: list[DiscoveryRecord], ingest_till_utc: str, bootstrap_window_days: int
) -> list[dict[str, Any]]:
    if bootstrap_window_days <= 0:
        raise ValueError("bootstrap_window_days must be positive")
    cutoff = _parse_iso_utc(ingest_till_utc)
    scopes = []
    keys = set()
    for item in coverage:
        key = (item.internal_id, item.source_timeframe, item.moex_secid)
        if key in keys:
            raise ValueError(f"duplicate source scope: {key}")
        keys.add(key)
        end = min(_parse_iso_utc(item.coverage_end_utc), cutoff)
        start = max(
            _parse_iso_utc(item.coverage_begin_utc), end - timedelta(days=bootstrap_window_days)
        )
        if start > end:
            continue
        scope = {
            "coverage": item.to_dict(),
            "window_start_utc": _to_iso_utc(start),
            "window_end_utc": _to_iso_utc(end),
        }
        scope["id"] = hashlib.sha256(json.dumps(scope, sort_keys=True).encode()).hexdigest()
        scopes.append(scope)
    return scopes


class RequestLimiter:
    def __init__(self, requests_per_second: float):
        if not 0 < requests_per_second <= 32:
            raise ValueError("requests_per_second must be in (0, 32]")
        self.spacing = 1.0 / requests_per_second
        self.next_request = 0.0
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                delay = self.next_request - time.monotonic()
                if delay <= 0:
                    self.next_request = time.monotonic() + self.spacing
                    return
            time.sleep(delay)

    def cool_down(self) -> None:
        with self.lock:
            self.spacing = min(1.0, self.spacing * 1.25)
            self.next_request = max(self.next_request, time.monotonic() + 5.0)


class LimitedMoexClient(MoexISSClient):
    def __init__(self, *, limiter: RequestLimiter, **kwargs: Any):
        super().__init__(**kwargs)
        self.limiter = limiter

    def _get_json(self, path: str, *, params: dict[str, str], event_context=None):
        self.limiter.acquire()
        return super()._get_json(path, params=params, event_context=event_context)


def read_checkpoint(root: Path, scope: dict[str, Any]) -> dict[str, Any] | None:
    directory = root / "completed" / scope["id"]
    if not directory.exists():
        return None
    receipt = json.loads((directory / "receipt.json").read_text(encoding="utf-8"))
    if receipt["scope"] != scope:
        raise ValueError("checkpoint scope mismatch")
    digest, size, rows = _digest(directory / "source.jsonl")
    if (digest, size, rows) != (receipt["sha256"], receipt["bytes"], receipt["rows"]):
        raise ValueError("checkpoint checksum, length or row count mismatch")
    return receipt


def commit_part(root: Path, scope: dict[str, Any], pending: Path, run_id: str) -> dict[str, Any]:
    digest, size, rows = _digest(pending / "source.jsonl")
    receipt = {
        "scope": scope,
        "sha256": digest,
        "bytes": size,
        "rows": rows,
        "run_id": run_id,
        "completed_at_utc": _utc_now_iso(),
    }
    _write_json(pending / "receipt.json", receipt)
    target = root / "completed" / scope["id"]
    if target.exists():
        raise FileExistsError("refusing to replace a committed source part")
    pending.rename(target)
    return receipt


def _download_one(scope, root, run_id, ingested_at_utc, client_factory, limiter):
    cached = read_checkpoint(root, scope)
    if cached is not None:
        return cached
    item = DiscoveryRecord(**scope["coverage"])
    start = _parse_iso_utc(scope["window_start_utc"])
    end = _parse_iso_utc(scope["window_end_utc"])
    pending = root / "inflight" / str(uuid4())
    pending.mkdir(parents=True)

    def request_event(payload):
        with (pending / "requests.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
        if "429" in str(payload.get("error", "")) or "503" in str(payload.get("error", "")):
            limiter.cool_down()
        if payload.get("status") == "retry":
            limiter.acquire()

    client = client_factory(request_event_hook=request_event)
    try:
        with (pending / "source.jsonl").open("x", encoding="utf-8", newline="\n") as handle:
            order = 0
            for candle in client.iter_candles(
                engine=item.moex_engine,
                market=item.moex_market,
                board=item.moex_board,
                secid=item.moex_secid,
                interval=item.source_interval,
                date_from=start.date(),
                date_till=end.date(),
            ):
                opened = _parse_moex_datetime(candle.begin)
                closed = _parse_moex_datetime(candle.end)
                if closed < start or closed > end:
                    continue
                order += 1
                row = {
                    "internal_id": item.internal_id,
                    "finam_symbol": item.finam_symbol,
                    "moex_engine": item.moex_engine,
                    "moex_market": item.moex_market,
                    "moex_board": item.moex_board,
                    "moex_secid": item.moex_secid,
                    "asset_group": item.asset_group,
                    "timeframe": item.source_timeframe,
                    "source_interval": item.source_interval,
                    "ts_open": _to_iso_utc(opened),
                    "ts_close": _to_iso_utc(closed),
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                    "open_interest": None,
                    "ingest_run_id": run_id,
                    "ingested_at_utc": ingested_at_utc,
                    "provenance_json": {
                        "source_provider": "moex_iss",
                        "source_interval": item.source_interval,
                        "source_timeframe": item.source_timeframe,
                        "requested_target_timeframes": item.requested_target_timeframes,
                        "run_id": run_id,
                        "window_start_utc": scope["window_start_utc"],
                        "window_end_utc": scope["window_end_utc"],
                        "stability_lag_minutes": 0,
                        "refresh_overlap_minutes": 0,
                        "discovery_url": item.discovery_url,
                    },
                }
                _write_raw_source_row(handle, row, source_order=order)
            handle.flush()
            os.fsync(handle.fileno())
        return commit_part(root, scope, pending, run_id)
    except Exception as exc:
        _write_json(pending / "error.json", {"error": str(exc), "scope_id": scope["id"]})
        raise


def _download_scopes(
    scopes,
    root: Path,
    run_id: str,
    *,
    workers=12,
    requests_per_second=16.0,
    client_factory=None,
    ingested_at_utc=None,
):
    if not 1 <= workers <= 16:
        raise ValueError("workers must be between 1 and 16")
    if len({scope["id"] for scope in scopes}) != len(scopes):
        raise ValueError("duplicate scope ids")
    root.mkdir(parents=True, exist_ok=True)
    (root / "completed").mkdir(exist_ok=True)
    limiter = RequestLimiter(requests_per_second)
    if client_factory is None:

        def client_factory(**kwargs):
            return LimitedMoexClient(limiter=limiter, **kwargs)

    report = {
        "status": "RUNNING",
        "run_id": run_id,
        "workers": workers,
        "requests_per_second": requests_per_second,
        "total_scopes": len(scopes),
        "completed_scopes": 0,
        "source_rows": 0,
        "source_bytes": 0,
        "failures": [],
        "started_at_utc": _utc_now_iso(),
    }
    _write_json(root / "download-progress.json", report)
    ingested_at_utc = ingested_at_utc or _utc_now_iso()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _download_one, scope, root, run_id, ingested_at_utc, client_factory, limiter
            ): scope
            for scope in scopes
        }
        for future in as_completed(futures):
            scope = futures[future]
            try:
                receipt = future.result()
                report["completed_scopes"] += 1
                report["source_rows"] += receipt["rows"]
                report["source_bytes"] += receipt["bytes"]
            except Exception as exc:
                report["failures"].append({"scope_id": scope["id"], "error": str(exc)})
            report["updated_at_utc"] = _utc_now_iso()
            report["effective_requests_per_second"] = round(1.0 / limiter.spacing, 3)
            _write_json(root / "download-progress.json", report)
    report["status"] = "FAIL" if report["failures"] else "PASS"
    report["finished_at_utc"] = _utc_now_iso()
    _write_json(root / "download-progress.json", report)
    return report


def download_scopes(scopes, root: Path, run_id: str, **kwargs):
    root.mkdir(parents=True, exist_ok=True)
    lock = root / "download.lock"
    with lock.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps({"pid": os.getpid(), "run_id": run_id}))
    try:
        return _download_scopes(scopes, root, run_id, **kwargs)
    finally:
        lock.unlink()


def combine_sources(scopes, root: Path) -> Path:
    target = root / "combined-source.jsonl"
    temporary = root / ("combined-" + str(uuid4()) + ".partial")
    with temporary.open("xb") as output:
        for scope in scopes:
            if read_checkpoint(root, scope) is None:
                raise ValueError("cannot combine an incomplete acquisition")
            with (root / "completed" / scope["id"] / "source.jsonl").open("rb") as source:
                shutil.copyfileobj(source, output, length=1024 * 1024)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(target)
    return target


def materialize_sources(scopes, root: Path, run_id: str, ingest_till_utc: str):
    from trading_advisor_3000.spark_jobs.moex_raw_ingest_job import (
        run_moex_raw_ingest_spark_delta_job,
    )

    source = combine_sources(scopes, root)
    attempt = root / "materializations" / str(uuid4())
    attempt.mkdir(parents=True)
    windows = [
        {
            "internal_id": scope["coverage"]["internal_id"],
            "timeframe": scope["coverage"]["source_timeframe"],
            "source_interval": scope["coverage"]["source_interval"],
            "moex_secid": scope["coverage"]["moex_secid"],
            "window_start_utc": scope["window_start_utc"],
            "window_end_utc": scope["window_end_utc"],
        }
        for scope in scopes
    ]
    report = run_moex_raw_ingest_spark_delta_job(
        table_path=attempt / "raw.delta",
        source_rows_path=source,
        window_scopes=windows,
        initial_watermarks={},
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
        refresh_overlap_minutes=0,
        progress_path=attempt / "progress.jsonl",
        progress_latest_path=attempt / "progress.latest.json",
        error_path=attempt / "errors.jsonl",
        error_latest_path=attempt / "errors.latest.json",
    )
    _write_json(attempt / "report.json", report)
    _write_json(root / "materialization-report.json", report)
    return report
