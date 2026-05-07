from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log

from .foundation import (
    DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    DEFAULT_REFRESH_OVERLAP_MINUTES,
    _append_progress_event,
    _write_coverage_artifacts,
    discover_coverage,
    ingest_moex_baseline_window,
    load_mapping_registry,
    load_universe,
    validate_mapping_registry,
    validate_universe_mapping_alignment,
)
from .historical_route_contracts import (
    STATUS_PASS,
    STATUS_PASS_NOOP,
    _SORTED_CHANGED_WINDOW_KEYS,
    _sha256_json,
    normalize_changed_windows,
)
from .iss_client import MoexISSClient
from .historical_canonical_route import CANONICAL_MERGE_SCOPED_DELETE_INSERT, run_historical_canonical_route


BASELINE_UPDATE_REPORT_FILENAME = "baseline-update-report.json"
PENDING_CHANGED_WINDOWS_FILENAME = "pending-changed-windows.json"


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


def _load_pending_changed_windows(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    payload = _read_json_object(path)
    raw = payload.get("changed_windows", [])
    if not isinstance(raw, list):
        raise ValueError(f"pending changed windows payload must contain list `changed_windows`: {path.as_posix()}")
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


def _baseline_raw_report_for_canonical(
    *,
    raw_report: dict[str, object],
    merged_changed_windows: list[dict[str, object]],
) -> dict[str, object]:
    payload = dict(raw_report)
    normalized_windows = normalize_changed_windows(merged_changed_windows)
    payload["changed_windows"] = normalized_windows
    payload["changed_windows_hash_sha256"] = _sha256_json(
        [{key: row[key] for key in _SORTED_CHANGED_WINDOW_KEYS} for row in normalized_windows]
    )
    if merged_changed_windows and str(payload.get("status", "")).strip() == STATUS_PASS_NOOP:
        payload["status"] = STATUS_PASS
    return payload


def run_moex_baseline_update(
    *,
    mapping_registry_path: Path,
    universe_path: Path,
    raw_table_path: Path,
    canonical_bars_path: Path,
    canonical_provenance_path: Path,
    canonical_session_calendar_path: Path | None = None,
    canonical_roll_map_path: Path | None = None,
    evidence_dir: Path,
    run_id: str,
    timeframes: set[str],
    ingest_till_utc: str,
    refresh_window_days: int,
    contract_discovery_lookback_days: int,
    max_changed_window_days: int,
    stability_lag_minutes: int = 20,
    refresh_overlap_minutes: int = DEFAULT_REFRESH_OVERLAP_MINUTES,
    contract_discovery_step_days: int = DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    expand_contract_chain: bool = True,
    repo_root: Path | None = None,
) -> dict[str, object]:
    if refresh_window_days <= 0:
        raise ValueError("refresh_window_days must be > 0")
    if contract_discovery_lookback_days <= 0:
        raise ValueError("contract_discovery_lookback_days must be > 0")
    if max_changed_window_days <= 0:
        raise ValueError("max_changed_window_days must be > 0")
    if not timeframes:
        raise ValueError("timeframes must not be empty")

    raw_table_path = raw_table_path.resolve()
    canonical_bars_path = canonical_bars_path.resolve()
    canonical_provenance_path = canonical_provenance_path.resolve()
    canonical_session_calendar_path = (
        canonical_session_calendar_path or (canonical_bars_path.parent / "canonical_session_calendar.delta")
    ).resolve()
    canonical_roll_map_path = (
        canonical_roll_map_path or (canonical_bars_path.parent / "canonical_roll_map.delta")
    ).resolve()
    if not has_delta_log(raw_table_path):
        raise FileNotFoundError(f"baseline raw table is missing `_delta_log`: {raw_table_path.as_posix()}")
    if not has_delta_log(canonical_bars_path):
        raise FileNotFoundError(f"baseline canonical bars table is missing `_delta_log`: {canonical_bars_path.as_posix()}")
    if not has_delta_log(canonical_provenance_path):
        raise FileNotFoundError(
            "baseline canonical provenance table is missing `_delta_log`: "
            f"{canonical_provenance_path.as_posix()}"
        )

    run_dir = evidence_dir.resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    pending_changed_windows_path = evidence_dir.resolve() / PENDING_CHANGED_WINDOWS_FILENAME

    universe = load_universe(universe_path)
    mappings = load_mapping_registry(mapping_registry_path)
    validate_mapping_registry(mappings)
    validate_universe_mapping_alignment(universe, mappings)

    request_log_path = run_dir / "moex-request-log.jsonl"
    request_latest_path = run_dir / "moex-request.latest.json"
    client = MoexISSClient(
        request_event_hook=lambda payload: _append_progress_event(
            jsonl_path=request_log_path,
            latest_path=request_latest_path,
            payload=payload,
        )
    )
    discovered_at_utc = _utc_now_iso()
    coverage = discover_coverage(
        client=client,
        universe=universe,
        mappings=mappings,
        timeframes=timeframes,
        discovered_at_utc=discovered_at_utc,
        ingest_till_utc=ingest_till_utc,
        bootstrap_window_days=refresh_window_days,
        expand_contract_chain=expand_contract_chain,
        contract_discovery_step_days=contract_discovery_step_days,
        contract_discovery_lookback_days=contract_discovery_lookback_days,
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

    try:
        canonical_report = run_historical_canonical_route(
            raw_table_path=raw_table_path,
            output_dir=run_dir / "canonical-refresh",
            run_id=run_id,
            raw_ingest_run_report=canonical_raw_report,
            repo_root=repo_root,
            canonical_bars_path=canonical_bars_path,
            canonical_provenance_path=canonical_provenance_path,
            canonical_session_calendar_path=canonical_session_calendar_path,
            canonical_roll_map_path=canonical_roll_map_path,
            canonical_merge_strategy=CANONICAL_MERGE_SCOPED_DELETE_INSERT,
            max_changed_window_days=max_changed_window_days,
        )
    except Exception:
        _write_pending_changed_windows(
            path=pending_changed_windows_path,
            run_id=run_id,
            changed_windows=merged_changed_windows,
            reason="canonical_refresh_failed",
        )
        raise

    if str(canonical_report.get("publish_decision", "")).strip() == "publish":
        pending_changed_windows_path.unlink(missing_ok=True)
    else:
        _write_pending_changed_windows(
            path=pending_changed_windows_path,
            run_id=run_id,
            changed_windows=merged_changed_windows,
            reason="canonical_refresh_blocked",
        )

    status = "PASS" if str(canonical_report.get("publish_decision", "")).strip() == "publish" else "BLOCKED"
    report = {
        "run_id": run_id,
        "status": status,
        "publish_decision": "publish" if status == "PASS" else "blocked",
        "mode": "baseline_update",
        "refresh_window_days": refresh_window_days,
        "contract_discovery_lookback_days": contract_discovery_lookback_days,
        "max_changed_window_days": max_changed_window_days,
        "refresh_overlap_minutes": refresh_overlap_minutes,
        "raw_table_path": raw_table_path.as_posix(),
        "canonical_bars_path": canonical_bars_path.as_posix(),
        "canonical_provenance_path": canonical_provenance_path.as_posix(),
        "canonical_session_calendar_path": canonical_session_calendar_path.as_posix(),
        "canonical_roll_map_path": canonical_roll_map_path.as_posix(),
        "source_rows": raw_report.get("source_rows", 0),
        "incremental_rows": raw_report.get("incremental_rows", 0),
        "deduplicated_rows": raw_report.get("deduplicated_rows", 0),
        "stale_rows": raw_report.get("stale_rows", 0),
        "pending_changed_windows_in": len(pending_changed_windows),
        "current_changed_windows": len(current_changed_windows),
        "effective_changed_windows": len(merged_changed_windows),
        "canonical_report": {
            "status": canonical_report.get("status"),
            "publish_decision": canonical_report.get("publish_decision"),
            "scoped_source_rows": canonical_report.get("scoped_source_rows"),
            "scoped_canonical_rows": canonical_report.get("scoped_canonical_rows"),
            "canonical_rows": canonical_report.get("canonical_rows"),
            "sidecar_refresh": canonical_report.get("sidecar_refresh"),
            "mutation_applied": canonical_report.get("mutation_applied"),
        },
        "artifact_paths": {
            "coverage_report": coverage_json.as_posix(),
            "coverage_table": coverage_csv.as_posix(),
            "raw_ingest_report": raw_report_path.as_posix(),
            "canonical_input_raw_ingest_report": canonical_input_report_path.as_posix(),
            "canonical_refresh_report": (run_dir / "canonical-refresh" / "canonical-refresh-report.json").as_posix(),
            "pending_changed_windows": (
                pending_changed_windows_path.as_posix() if pending_changed_windows_path.exists() else ""
            ),
        },
        "generated_at_utc": _utc_now_iso(),
    }
    _write_json(run_dir / BASELINE_UPDATE_REPORT_FILENAME, report)
    return report
