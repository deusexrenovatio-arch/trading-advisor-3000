from __future__ import annotations

# ruff: noqa: E501
import hashlib
import json
import threading
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_filtered_delta_table_rows,
)

from .foundation import (
    DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    DEFAULT_REFRESH_OVERLAP_MINUTES,
    SOURCE_INTERVAL_BY_TARGET_TIMEFRAME,
    DiscoveryRecord,
    _append_progress_event,
    _select_active_mappings_for_universe,
    _sorted_target_timeframes,
    _source_timeframe_label,
    _write_coverage_artifacts,
    discover_coverage,
    ingest_moex_baseline_window,
    load_mapping_registry,
    load_universe,
    validate_mapping_registry,
    validate_universe_mapping_alignment,
)
from .historical_canonical_route import (
    CANONICAL_MERGE_SCOPED_DELETE_INSERT,
    run_historical_canonical_route,
)
from .historical_route_contracts import (
    STATUS_PASS,
    STATUS_PASS_NOOP,
    changed_windows_hash_sha256,
    normalize_changed_windows,
)
from .iss_client import MoexISSClient

BASELINE_UPDATE_REPORT_FILENAME = "baseline-update-report.json"
PENDING_CHANGED_WINDOWS_FILENAME = "pending-changed-windows.json"
CANONICAL_REFRESH_HEARTBEAT_FILENAME = "canonical-refresh-heartbeat.json"
CANONICAL_REFRESH_HEARTBEAT_INTERVAL_SECONDS = 30.0
SESSION_SCHEDULE_MODE_MANUAL_OPTIONAL = "manual_backfill_optional"
SESSION_SCHEDULE_MODE_MANUAL_REQUIRED = "manual_backfill_required"
BASELINE_COVERAGE_MODE_LOCAL_TAIL = "local_tail"
BASELINE_COVERAGE_MODE_LIVE_DISCOVERY = "live_discovery"
BASELINE_COVERAGE_MODES = {
    BASELINE_COVERAGE_MODE_LOCAL_TAIL,
    BASELINE_COVERAGE_MODE_LIVE_DISCOVERY,
}
BASELINE_UPDATE_HOT_TABLE_RUNTIME = "delta_rs_raw_tail+spark_delta_canonical"
LOCAL_TAIL_ROLL_MAP_LOOKBACK_DAYS = 45
BASELINE_TAIL_REQUEST_TIMEOUT_SECONDS = 6.0
BASELINE_TAIL_REQUEST_MAX_RETRIES = 1
BASELINE_TAIL_REQUEST_RETRY_BACKOFF_SECONDS = 0.5
BASELINE_TAIL_REQUEST_RETRY_JITTER_RATIO = 0.0
DEFAULT_CF_CATCH_UP_TIMEFRAMES = ("15m", "1h", "4h", "1d")
DEFAULT_CF_CATCH_UP_OVERLAP_MINUTES = DEFAULT_REFRESH_OVERLAP_MINUTES


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path.as_posix()}")
    return payload


def _write_json(path: Path, payload: dict[str, object] | list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _start_canonical_refresh_heartbeat(
    *,
    heartbeat_path: Path,
    run_id: str,
    interval_seconds: float = CANONICAL_REFRESH_HEARTBEAT_INTERVAL_SECONDS,
) -> Callable[[str, Mapping[str, object] | None], None]:
    started_at_utc = _utc_now_iso()
    stop_event = threading.Event()
    lock = threading.Lock()
    heartbeat_count = 0

    def _write(status: str, extra: Mapping[str, object] | None = None) -> None:
        nonlocal heartbeat_count
        with lock:
            heartbeat_count += 1
            payload: dict[str, object] = {
                "step": "canonical_refresh",
                "run_id": run_id,
                "status": status,
                "started_at_utc": started_at_utc,
                "heartbeat_at_utc": _utc_now_iso(),
                "heartbeat_count": heartbeat_count,
            }
            payload.update(dict(extra or {}))
            _write_json(heartbeat_path, payload)

    _write("RUNNING")

    def _loop() -> None:
        while not stop_event.wait(interval_seconds):
            _write("RUNNING")

    thread = threading.Thread(
        target=_loop,
        name=f"ta3000-canonical-refresh-heartbeat-{run_id}",
        daemon=True,
    )
    thread.start()

    def _stop(status: str, extra: Mapping[str, object] | None = None) -> None:
        stop_event.set()
        thread.join(timeout=max(1.0, interval_seconds))
        final_payload = dict(extra or {})
        final_payload["completed_at_utc"] = _utc_now_iso()
        _write(status, final_payload)

    return _stop


def _block_completed_run_id_unless_retry(
    *, run_dir: Path, run_id: str, allow_run_id_retry: bool
) -> None:
    if allow_run_id_retry:
        return
    completed_report_path = run_dir / BASELINE_UPDATE_REPORT_FILENAME
    if completed_report_path.exists():
        raise RuntimeError(
            f"run_id `{run_id}` already has a completed baseline update report: "
            f"{completed_report_path.as_posix()}; use an explicit retry run_id"
        )


def _load_pending_changed_windows(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    payload = _read_json_object(path)
    raw = payload.get("changed_windows", [])
    if not isinstance(raw, list):
        raise ValueError(
            f"pending changed windows payload must contain list `changed_windows`: {path.as_posix()}"
        )
    return normalize_changed_windows(raw)


def _write_pending_changed_windows(
    *,
    path: Path,
    run_id: str,
    changed_windows: list[dict[str, object]],
    reason: str,
) -> None:
    normalized = normalize_changed_windows(changed_windows)
    if not normalized:
        path.unlink(missing_ok=True)
        return
    _write_json(
        path,
        {
            "status": "PENDING",
            "run_id": run_id,
            "reason": reason,
            "changed_windows": normalized,
            "updated_at_utc": _utc_now_iso(),
        },
    )


def _merge_changed_windows(
    *,
    pending: list[dict[str, object]],
    current: list[dict[str, object]],
) -> list[dict[str, object]]:
    return normalize_changed_windows([*pending, *current])


def _parse_utc_timestamp(value: object, *, field_name: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_json(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _cf_catch_up_timeframes(timeframes: Sequence[str] | None) -> tuple[str, ...]:
    normalized = tuple(
        dict.fromkeys(str(item).strip() for item in (timeframes or ()) if str(item).strip())
    )
    return normalized or DEFAULT_CF_CATCH_UP_TIMEFRAMES


def _baseline_update_timeframes(timeframes: Sequence[str] | set[str] | str) -> set[str]:
    raw_items = timeframes.split(",") if isinstance(timeframes, str) else timeframes
    normalized = {str(item).strip() for item in raw_items if str(item).strip()}
    if not normalized:
        raise ValueError("timeframes must not be empty")
    unknown = sorted(item for item in normalized if item not in SOURCE_INTERVAL_BY_TARGET_TIMEFRAME)
    if unknown:
        joined = ", ".join(unknown)
        raise ValueError(f"unsupported baseline update timeframes: {joined}")
    return normalized


def _cf_catch_up_window_hash(window: dict[str, object]) -> str:
    return _sha256_json(
        {
            "instrument_id": window["instrument_id"],
            "timeframe": window["timeframe"],
            "start_ts": window["start_ts"],
            "end_ts": window["end_ts"],
            "overlap_minutes": window["overlap_minutes"],
            "source_changed_windows": window["source_changed_windows"],
        }
    )


def _build_cf_catch_up_report(
    *,
    changed_windows: list[dict[str, object]],
    refresh_overlap_minutes: int,
    cf_catch_up_overlap_minutes: int | None,
    cf_catch_up_timeframes: Sequence[str] | None,
) -> dict[str, object]:
    if refresh_overlap_minutes < 0:
        return {
            "status": "BLOCKED",
            "blocked_reason": "refresh_overlap_minutes_must_be_non_negative",
            "overlap_minutes": refresh_overlap_minutes,
            "target_timeframes": list(_cf_catch_up_timeframes(cf_catch_up_timeframes)),
            "windows": [],
            "windows_hash_sha256": _sha256_json([]),
        }

    requested_overlap = (
        DEFAULT_CF_CATCH_UP_OVERLAP_MINUTES
        if cf_catch_up_overlap_minutes is None
        else int(cf_catch_up_overlap_minutes)
    )
    if requested_overlap < 0:
        return {
            "status": "BLOCKED",
            "blocked_reason": "cf_catch_up_overlap_minutes_must_be_non_negative",
            "overlap_minutes": requested_overlap,
            "target_timeframes": list(_cf_catch_up_timeframes(cf_catch_up_timeframes)),
            "windows": [],
            "windows_hash_sha256": _sha256_json([]),
        }

    target_timeframes = _cf_catch_up_timeframes(cf_catch_up_timeframes)
    overlap_minutes = max(refresh_overlap_minutes, requested_overlap)
    normalized_source_windows = normalize_changed_windows(changed_windows)
    if not normalized_source_windows:
        return {
            "status": "NOOP",
            "overlap_minutes": overlap_minutes,
            "target_timeframes": list(target_timeframes),
            "windows": [],
            "source_changed_windows": [],
            "source_changed_windows_hash_sha256": changed_windows_hash_sha256([]),
            "windows_hash_sha256": _sha256_json([]),
        }

    candidates: list[dict[str, object]] = []
    for source_window in normalized_source_windows:
        source_start = _parse_utc_timestamp(
            source_window["window_start_utc"],
            field_name="changed_window.window_start_utc",
        )
        source_end = _parse_utc_timestamp(
            source_window["window_end_utc"],
            field_name="changed_window.window_end_utc",
        )
        start_ts = _utc_iso(source_start - timedelta(minutes=overlap_minutes))
        end_ts = _utc_iso(source_end)
        for timeframe in target_timeframes:
            candidates.append(
                {
                    "instrument_id": str(source_window["internal_id"]),
                    "timeframe": timeframe,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "overlap_minutes": overlap_minutes,
                    "source_window_start_utc": source_window["window_start_utc"],
                    "source_window_end_utc": source_window["window_end_utc"],
                    "source_changed_windows": [dict(source_window)],
                }
            )

    merged: list[dict[str, object]] = []
    for candidate in sorted(
        candidates,
        key=lambda item: (
            str(item["instrument_id"]),
            str(item["timeframe"]),
            str(item["start_ts"]),
            str(item["end_ts"]),
        ),
    ):
        current = merged[-1] if merged else None
        same_scope = (
            current is not None
            and current["instrument_id"] == candidate["instrument_id"]
            and current["timeframe"] == candidate["timeframe"]
        )
        overlaps = bool(
            same_scope
            and _parse_utc_timestamp(candidate["start_ts"], field_name="cf_catch_up.start_ts")
            <= _parse_utc_timestamp(current["end_ts"], field_name="cf_catch_up.end_ts")
        )
        if not overlaps:
            merged.append(dict(candidate))
            continue

        if str(candidate["end_ts"]) > str(current["end_ts"]):
            current["end_ts"] = candidate["end_ts"]
        if str(candidate["source_window_start_utc"]) < str(current["source_window_start_utc"]):
            current["source_window_start_utc"] = candidate["source_window_start_utc"]
        if str(candidate["source_window_end_utc"]) > str(current["source_window_end_utc"]):
            current["source_window_end_utc"] = candidate["source_window_end_utc"]
        current["source_changed_windows"] = normalize_changed_windows(
            [
                *list(current["source_changed_windows"]),
                *list(candidate["source_changed_windows"]),
            ]
        )

    for window in merged:
        window["source_changed_window_count"] = len(list(window["source_changed_windows"]))
        window["window_hash_sha256"] = _cf_catch_up_window_hash(window)

    return {
        "status": "READY",
        "overlap_minutes": overlap_minutes,
        "target_timeframes": list(target_timeframes),
        "windows": merged,
        "source_changed_windows": normalized_source_windows,
        "source_changed_windows_hash_sha256": changed_windows_hash_sha256(
            normalized_source_windows
        ),
        "windows_hash_sha256": _sha256_json(merged),
    }


def _baseline_raw_report_for_canonical(
    *,
    raw_report: dict[str, object],
    merged_changed_windows: list[dict[str, object]],
) -> dict[str, object]:
    payload = dict(raw_report)
    normalized_windows = normalize_changed_windows(merged_changed_windows)
    payload["changed_windows"] = normalized_windows
    payload["changed_windows_hash_sha256"] = changed_windows_hash_sha256(normalized_windows)
    if normalized_windows and str(payload.get("status", "")).strip() == STATUS_PASS_NOOP:
        payload["status"] = STATUS_PASS
    return payload


def _moex_secid_from_contract_id(contract_id: object) -> str:
    text = str(contract_id or "").strip()
    if not text:
        return ""
    return text.split("@", 1)[0].strip()


def _latest_roll_map_contracts(
    *,
    canonical_roll_map_path: Path,
    ingest_till_utc: str,
    lookback_days: int,
) -> dict[str, dict[str, str]]:
    if lookback_days <= 0 or not has_delta_log(canonical_roll_map_path):
        return {}
    ingest_till = datetime.fromisoformat(ingest_till_utc.replace("Z", "+00:00")).astimezone(UTC)
    session_date_from = (ingest_till - timedelta(days=lookback_days)).date().isoformat()
    rows = read_filtered_delta_table_rows(
        canonical_roll_map_path,
        filters=[("session_date", ">=", session_date_from)],
        columns=["instrument_id", "session_date", "active_contract_id"],
    )
    latest: dict[str, dict[str, str]] = {}
    for row in rows:
        instrument_id = str(row.get("instrument_id", "")).strip()
        session_date = str(row.get("session_date", "")).strip()
        secid = _moex_secid_from_contract_id(row.get("active_contract_id"))
        if not instrument_id or not session_date or not secid:
            continue
        current = latest.get(instrument_id)
        if current is None or session_date > current["session_date"]:
            latest[instrument_id] = {"session_date": session_date, "moex_secid": secid}
    return latest


def _local_tail_coverage(
    *,
    universe: list[object],
    mappings: list[object],
    timeframes: set[str],
    discovered_at_utc: str,
    ingest_till_utc: str,
    refresh_window_days: int,
    canonical_roll_map_path: Path | None = None,
    roll_map_lookback_days: int = LOCAL_TAIL_ROLL_MAP_LOOKBACK_DAYS,
) -> list[DiscoveryRecord]:
    ordered_timeframes = _sorted_target_timeframes(timeframes)
    target_timeframes_by_interval: dict[int, list[str]] = {}
    for target_timeframe in ordered_timeframes:
        source_interval = SOURCE_INTERVAL_BY_TARGET_TIMEFRAME[target_timeframe]
        target_timeframes_by_interval.setdefault(source_interval, []).append(target_timeframe)

    active_mappings = _select_active_mappings_for_universe(universe, mappings)
    if not active_mappings:
        raise ValueError("local tail coverage requires at least one active mapping")
    latest_roll_contracts = (
        _latest_roll_map_contracts(
            canonical_roll_map_path=canonical_roll_map_path,
            ingest_till_utc=ingest_till_utc,
            lookback_days=max(roll_map_lookback_days, refresh_window_days),
        )
        if canonical_roll_map_path is not None
        else {}
    )

    ingest_till = datetime.fromisoformat(ingest_till_utc.replace("Z", "+00:00"))
    coverage_begin = ingest_till - timedelta(days=refresh_window_days)
    coverage_begin_utc = (
        coverage_begin.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    coverage_end_utc = (
        ingest_till.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    records: list[DiscoveryRecord] = []
    for mapping in active_mappings:
        roll_contract = latest_roll_contracts.get(mapping.internal_id)
        moex_secid = (
            str(roll_contract["moex_secid"]) if roll_contract is not None else mapping.moex_secid
        )
        finam_symbol = f"{moex_secid}@MOEX" if roll_contract is not None else mapping.finam_symbol
        discovery_source = (
            f"canonical-roll-map/{mapping.internal_id}/{{source_interval}}/{moex_secid}/"
            f"{roll_contract['session_date']}"
            if roll_contract is not None
            else f"mapping-registry/{mapping.internal_id}/{{source_interval}}/{moex_secid}"
        )
        for source_interval, target_timeframes in sorted(target_timeframes_by_interval.items()):
            records.append(
                DiscoveryRecord(
                    internal_id=mapping.internal_id,
                    finam_symbol=finam_symbol,
                    moex_engine=mapping.moex_engine,
                    moex_market=mapping.moex_market,
                    moex_board=mapping.moex_board,
                    moex_secid=moex_secid,
                    asset_group=mapping.asset_group,
                    requested_target_timeframes=",".join(target_timeframes),
                    source_interval=source_interval,
                    source_timeframe=_source_timeframe_label(source_interval),
                    coverage_begin_utc=coverage_begin_utc,
                    coverage_end_utc=coverage_end_utc,
                    discovered_at_utc=discovered_at_utc,
                    discovery_url=(
                        "local-tail://" + discovery_source.format(source_interval=source_interval)
                    ),
                )
            )
    return sorted(
        records,
        key=lambda row: (
            row.internal_id,
            row.source_interval,
            row.coverage_begin_utc,
            row.coverage_end_utc,
            row.moex_secid,
        ),
    )


def run_moex_baseline_update(
    *,
    mapping_registry_path: Path,
    universe_path: Path,
    raw_table_path: Path,
    canonical_bars_path: Path,
    canonical_provenance_path: Path,
    canonical_session_intervals_path: Path | None = None,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    evidence_dir: Path,
    run_id: str,
    timeframes: Sequence[str] | set[str] | str,
    ingest_till_utc: str,
    refresh_window_days: int,
    contract_discovery_lookback_days: int,
    max_changed_window_days: int,
    stability_lag_minutes: int = 20,
    refresh_overlap_minutes: int = DEFAULT_REFRESH_OVERLAP_MINUTES,
    cf_catch_up_timeframes: Sequence[str] | None = None,
    cf_catch_up_overlap_minutes: int | None = None,
    contract_discovery_step_days: int = DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    expand_contract_chain: bool = True,
    coverage_mode: str = BASELINE_COVERAGE_MODE_LOCAL_TAIL,
    repo_root: Path | None = None,
    allow_run_id_retry: bool = False,
) -> dict[str, object]:
    if refresh_window_days <= 0:
        raise ValueError("refresh_window_days must be > 0")
    if contract_discovery_lookback_days <= 0:
        raise ValueError("contract_discovery_lookback_days must be > 0")
    if max_changed_window_days <= 0:
        raise ValueError("max_changed_window_days must be > 0")
    resolved_timeframes = _baseline_update_timeframes(timeframes)
    if coverage_mode not in BASELINE_COVERAGE_MODES:
        joined = ", ".join(sorted(BASELINE_COVERAGE_MODES))
        raise ValueError(f"coverage_mode must be one of: {joined}")

    raw_table_path = raw_table_path.resolve()
    canonical_bars_path = canonical_bars_path.resolve()
    canonical_provenance_path = canonical_provenance_path.resolve()
    canonical_session_calendar_path = (
        canonical_session_calendar_path
        or (canonical_bars_path.parent / "canonical_session_calendar.delta")
    ).resolve()
    canonical_session_intervals_path = (
        canonical_session_intervals_path.resolve()
        if canonical_session_intervals_path is not None
        else None
    )
    canonical_roll_map_path = (
        canonical_roll_map_path or (canonical_bars_path.parent / "canonical_roll_map.delta")
    ).resolve()
    if not has_delta_log(raw_table_path):
        raise FileNotFoundError(
            f"baseline raw table is missing `_delta_log`: {raw_table_path.as_posix()}"
        )
    if not has_delta_log(canonical_bars_path):
        raise FileNotFoundError(
            f"baseline canonical bars table is missing `_delta_log`: {canonical_bars_path.as_posix()}"
        )
    if not has_delta_log(canonical_provenance_path):
        raise FileNotFoundError(
            "baseline canonical provenance table is missing `_delta_log`: "
            f"{canonical_provenance_path.as_posix()}"
        )

    run_dir = evidence_dir.resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _block_completed_run_id_unless_retry(
        run_dir=run_dir,
        run_id=run_id,
        allow_run_id_retry=allow_run_id_retry,
    )
    pending_changed_windows_path = evidence_dir.resolve() / PENDING_CHANGED_WINDOWS_FILENAME

    universe = load_universe(universe_path)
    mappings = load_mapping_registry(mapping_registry_path)
    validate_mapping_registry(mappings)
    validate_universe_mapping_alignment(universe, mappings)

    request_log_path = run_dir / "moex-request-log.jsonl"
    request_latest_path = run_dir / "moex-request.latest.json"
    client_kwargs: dict[str, object] = {}
    if coverage_mode == BASELINE_COVERAGE_MODE_LOCAL_TAIL:
        client_kwargs.update(
            {
                "timeout_seconds": BASELINE_TAIL_REQUEST_TIMEOUT_SECONDS,
                "max_retries": BASELINE_TAIL_REQUEST_MAX_RETRIES,
                "retry_backoff_seconds": BASELINE_TAIL_REQUEST_RETRY_BACKOFF_SECONDS,
                "retry_jitter_ratio": BASELINE_TAIL_REQUEST_RETRY_JITTER_RATIO,
            }
        )
    client = MoexISSClient(
        **client_kwargs,
        request_event_hook=lambda payload: _append_progress_event(
            jsonl_path=request_log_path,
            latest_path=request_latest_path,
            payload=payload,
        ),
    )
    discovered_at_utc = _utc_now_iso()
    if coverage_mode == BASELINE_COVERAGE_MODE_LIVE_DISCOVERY:
        coverage = discover_coverage(
            client=client,
            universe=universe,
            mappings=mappings,
            timeframes=resolved_timeframes,
            discovered_at_utc=discovered_at_utc,
            ingest_till_utc=ingest_till_utc,
            bootstrap_window_days=refresh_window_days,
            expand_contract_chain=expand_contract_chain,
            contract_discovery_step_days=contract_discovery_step_days,
            contract_discovery_lookback_days=contract_discovery_lookback_days,
        )
    else:
        coverage = _local_tail_coverage(
            universe=universe,
            mappings=mappings,
            timeframes=resolved_timeframes,
            discovered_at_utc=discovered_at_utc,
            ingest_till_utc=ingest_till_utc,
            refresh_window_days=refresh_window_days,
            canonical_roll_map_path=canonical_roll_map_path,
            roll_map_lookback_days=max(
                LOCAL_TAIL_ROLL_MAP_LOOKBACK_DAYS,
                contract_discovery_lookback_days,
            ),
        )
    coverage_json, coverage_csv = _write_coverage_artifacts(coverage, output_dir=run_dir)

    raw_report = ingest_moex_baseline_window(
        client=client,
        coverage=coverage,
        table_path=raw_table_path,
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
        refresh_window_days=refresh_window_days,
        stability_lag_minutes=stability_lag_minutes,
        refresh_overlap_minutes=refresh_overlap_minutes,
        progress_path=run_dir / "raw-ingest-progress.jsonl",
        progress_latest_path=run_dir / "raw-ingest-progress.latest.json",
        error_path=run_dir / "raw-ingest-errors.jsonl",
        error_latest_path=run_dir / "raw-ingest-error.latest.json",
    )
    raw_report_path = run_dir / "raw-ingest-report.json"
    _write_json(raw_report_path, raw_report)

    pending_changed_windows = _load_pending_changed_windows(pending_changed_windows_path)
    current_changed_windows_raw = raw_report.get("changed_windows", [])
    if not isinstance(current_changed_windows_raw, list):
        raise ValueError("raw ingest report must contain list `changed_windows`")
    current_changed_windows = normalize_changed_windows(current_changed_windows_raw)
    merged_changed_windows = _merge_changed_windows(
        pending=pending_changed_windows,
        current=current_changed_windows,
    )
    canonical_raw_report = _baseline_raw_report_for_canonical(
        raw_report=raw_report,
        merged_changed_windows=merged_changed_windows,
    )
    canonical_input_report_path = run_dir / "canonical-input-raw-ingest-report.json"
    _write_json(canonical_input_report_path, canonical_raw_report)

    canonical_refresh_report_path: Path | None = (
        run_dir / "canonical-refresh" / ("canonical-refresh-report.json")
    )
    if not merged_changed_windows:
        canonical_refresh_report_path = None
        canonical_report = {
            "status": "PASS-NOOP",
            "publish_decision": "publish",
            "scoped_source_rows": 0,
            "scoped_canonical_rows": 0,
            "canonical_rows": 0,
            "sidecar_refresh": "skipped",
            "mutation_applied": False,
            "skipped_reason": "no_raw_or_pending_changed_windows",
        }
        pending_changed_windows_path.unlink(missing_ok=True)
    else:
        stop_canonical_heartbeat = _start_canonical_refresh_heartbeat(
            heartbeat_path=run_dir / CANONICAL_REFRESH_HEARTBEAT_FILENAME,
            run_id=run_id,
        )
        try:
            canonical_report = run_historical_canonical_route(
                raw_table_path=raw_table_path,
                output_dir=run_dir / "canonical-refresh",
                run_id=run_id,
                raw_ingest_run_report=canonical_raw_report,
                repo_root=repo_root,
                canonical_bars_path=canonical_bars_path,
                canonical_provenance_path=canonical_provenance_path,
                canonical_session_intervals_path=canonical_session_intervals_path,
                canonical_session_calendar_path=canonical_session_calendar_path,
                canonical_roll_map_path=canonical_roll_map_path,
                canonical_merge_strategy=CANONICAL_MERGE_SCOPED_DELETE_INSERT,
                max_changed_window_days=max_changed_window_days,
                target_timeframes=timeframes,
            )
        except Exception as exc:
            stop_canonical_heartbeat(
                "FAILED",
                {
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            _write_pending_changed_windows(
                path=pending_changed_windows_path,
                run_id=run_id,
                changed_windows=merged_changed_windows,
                reason="canonical_refresh_failed",
            )
            raise
        else:
            stop_canonical_heartbeat(
                "COMPLETED",
                {
                    "publish_decision": str(canonical_report.get("publish_decision", "")).strip(),
                    "canonical_status": str(canonical_report.get("status", "")).strip(),
                },
            )

        if str(canonical_report.get("publish_decision", "")).strip() == "publish":
            pending_changed_windows_path.unlink(missing_ok=True)
        else:
            _write_pending_changed_windows(
                path=pending_changed_windows_path,
                run_id=run_id,
                changed_windows=merged_changed_windows,
                reason="canonical_refresh_blocked",
            )

    status = (
        "PASS"
        if str(canonical_report.get("publish_decision", "")).strip() == "publish"
        else "BLOCKED"
    )
    if status == "PASS":
        cf_catch_up_report = _build_cf_catch_up_report(
            changed_windows=merged_changed_windows,
            refresh_overlap_minutes=refresh_overlap_minutes,
            cf_catch_up_overlap_minutes=cf_catch_up_overlap_minutes,
            cf_catch_up_timeframes=cf_catch_up_timeframes,
        )
    else:
        target_timeframes = _cf_catch_up_timeframes(cf_catch_up_timeframes)
        overlap_minutes = max(
            refresh_overlap_minutes,
            DEFAULT_CF_CATCH_UP_OVERLAP_MINUTES
            if cf_catch_up_overlap_minutes is None
            else int(cf_catch_up_overlap_minutes),
        )
        cf_catch_up_report = {
            "status": "BLOCKED",
            "blocked_reason": "canonical_refresh_not_published",
            "overlap_minutes": overlap_minutes,
            "target_timeframes": list(target_timeframes),
            "windows": [],
            "source_changed_windows": normalize_changed_windows(merged_changed_windows),
            "source_changed_windows_hash_sha256": changed_windows_hash_sha256(
                merged_changed_windows
            ),
            "windows_hash_sha256": _sha256_json([]),
        }
    report = {
        "run_id": run_id,
        "status": status,
        "publish_decision": "publish" if status == "PASS" else "blocked",
        "mode": "baseline_update",
        "coverage_mode": coverage_mode,
        "runtime_boundary": {
            "orchestrator": "dagster",
            "hot_table_runtime": BASELINE_UPDATE_HOT_TABLE_RUNTIME,
            "python_role": "source_adapter_config_and_evidence",
        },
        "refresh_window_days": refresh_window_days,
        "contract_discovery_lookback_days": contract_discovery_lookback_days,
        "max_changed_window_days": max_changed_window_days,
        "refresh_overlap_minutes": refresh_overlap_minutes,
        "raw_table_path": raw_table_path.as_posix(),
        "canonical_bars_path": canonical_bars_path.as_posix(),
        "canonical_provenance_path": canonical_provenance_path.as_posix(),
        "session_schedule_mode": SESSION_SCHEDULE_MODE_MANUAL_REQUIRED,
        "session_schedule_required": True,
        "raw_session_schedule_path": "",
        "canonical_session_intervals_path": (
            canonical_session_intervals_path.as_posix()
            if canonical_session_intervals_path is not None
            else ""
        ),
        "canonical_session_calendar_path": canonical_session_calendar_path.as_posix(),
        "canonical_roll_map_path": canonical_roll_map_path.as_posix(),
        "source_rows": raw_report.get("source_rows", 0),
        "incremental_rows": raw_report.get("incremental_rows", 0),
        "deduplicated_rows": raw_report.get("deduplicated_rows", 0),
        "stale_rows": raw_report.get("stale_rows", 0),
        "pending_changed_windows_in": len(pending_changed_windows),
        "current_changed_windows": len(current_changed_windows),
        "effective_changed_windows": len(merged_changed_windows),
        "cf_catch_up": cf_catch_up_report,
        "canonical_report": {
            "status": canonical_report.get("status"),
            "publish_decision": canonical_report.get("publish_decision"),
            "scoped_source_rows": canonical_report.get("scoped_source_rows"),
            "scoped_canonical_rows": canonical_report.get("scoped_canonical_rows"),
            "canonical_rows": canonical_report.get("canonical_rows"),
            "sidecar_refresh": canonical_report.get("sidecar_refresh"),
            "mutation_applied": canonical_report.get("mutation_applied"),
            "skipped_reason": canonical_report.get("skipped_reason"),
        },
        "artifact_paths": {
            "coverage_report": coverage_json.as_posix(),
            "coverage_table": coverage_csv.as_posix(),
            "raw_ingest_report": raw_report_path.as_posix(),
            "canonical_input_raw_ingest_report": canonical_input_report_path.as_posix(),
            "session_schedule_report": "",
            "official_session_schedule_report": "",
            "canonical_refresh_report": (
                canonical_refresh_report_path.as_posix()
                if canonical_refresh_report_path is not None
                else ""
            ),
            "pending_changed_windows": (
                pending_changed_windows_path.as_posix()
                if pending_changed_windows_path.exists()
                else ""
            ),
        },
        "generated_at_utc": _utc_now_iso(),
    }
    _write_json(run_dir / BASELINE_UPDATE_REPORT_FILENAME, report)
    return report
