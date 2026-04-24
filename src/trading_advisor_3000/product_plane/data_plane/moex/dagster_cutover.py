from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from dagster import DagsterInstance

from trading_advisor_3000.dagster_defs.moex_historical_assets import (
    MOEX_HISTORICAL_NIGHTLY_CRON,
    MOEX_HISTORICAL_RETRY_POLICY,
    build_moex_historical_dagster_binding_artifact,
    assert_moex_historical_definitions_executable,
    execute_moex_historical_cutover_job,
    moex_historical_asset_specs,
)
from trading_advisor_3000.product_plane.data_plane.moex.staging_binding import (
    validate_external_dagster_url,
)
from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import (
    LEASE_BACKEND_DELTA_LEDGER_CAS,
    LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE,
    STATUS_BLOCKED,
    STATUS_FAILED,
    STATUS_PASS,
    STATUS_PASS_NOOP,
    acquire_technical_route_lease,
    heartbeat_technical_route_lease,
    read_technical_route_run_ledger,
    record_technical_route_blocked_conflict,
    release_technical_route_lease,
    takeover_technical_route_lease,
)


CANONICAL_ROUTE_ID = "moex_historical_canonical_dagster_route.v1"
READINESS_TIMEZONE = "Europe/Moscow"
READINESS_TARGET_HOUR = 6
PASS_LIKE_STATUSES = {STATUS_PASS, STATUS_PASS_NOOP}
STAGING_RUN_ID_KEYS = ("nightly_1", "nightly_2", "repair", "backfill", "recovery")
LOCAL_DAGSTER_BINDING = "dagster://local-temp/moex-historical-cutover"
CANONICAL_RETRY_BACKOFF_SECONDS = (60, 300, 900)


@dataclass(frozen=True)
class CutoverCycle:
    mode: str
    run_id: str
    requested_at_utc: str
    readiness_observed_at_utc: str
    retry_of_run_id: str | None = None


def _normalize_bindings(values: object) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: set[str] = set()
    for item in values:
        text = str(item).strip()
        if text:
            normalized.add(text)
    return sorted(normalized)


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str, *, field_name: str) -> datetime:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"`{field_name}` must be non-empty")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError(f"`{field_name}` must include timezone")
    return parsed.astimezone(UTC)


def _json_load(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path.as_posix()}")
    return payload


def _json_write(path: Path, payload: dict[str, object] | list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_changed_windows_hash(raw_ingest_report_path: Path) -> str | None:
    payload = _json_load(raw_ingest_report_path)
    raw = str(payload.get("changed_windows_hash_sha256", "")).strip().lower()
    return raw or None


def _read_raw_status(raw_ingest_report_path: Path) -> str:
    payload = _json_load(raw_ingest_report_path)
    return str(payload.get("status", "")).strip()


def _load_staging_binding_report(path: Path) -> dict[str, object]:
    payload = _json_load(path)

    proof_class = str(payload.get("proof_class", "")).strip().lower()
    if proof_class != "staging-real":
        raise ValueError(
            f"staging binding report must declare `proof_class=staging-real`: {path.as_posix()}"
        )

    environment = str(payload.get("environment", "")).strip().lower()
    if environment != "staging-real":
        raise ValueError(
            f"staging binding report must declare `environment=staging-real`: {path.as_posix()}"
        )

    dagster_url = str(payload.get("dagster_url", "")).strip()
    if not dagster_url:
        raise ValueError(f"staging binding report is missing `dagster_url`: {path.as_posix()}")
    dagster_url = validate_external_dagster_url(dagster_url)

    orchestrator = str(payload.get("orchestrator", "")).strip().lower()
    if not orchestrator.startswith("dagster"):
        raise ValueError(
            "staging binding report must confirm Dagster-owned orchestration "
            f"(got `{orchestrator or 'EMPTY'}`): {path.as_posix()}"
        )

    run_ids_raw = payload.get("run_ids")
    if not isinstance(run_ids_raw, dict):
        raise ValueError(f"staging binding report must provide `run_ids` object: {path.as_posix()}")
    run_ids: dict[str, str] = {}
    for key in STAGING_RUN_ID_KEYS:
        value = str(run_ids_raw.get(key, "")).strip()
        if not value:
            raise ValueError(
                f"staging binding report is missing non-empty run id `{key}`: {path.as_posix()}"
            )
        run_ids[key] = value

    artifact_paths = _normalize_bindings(payload.get("artifact_paths", []))
    if not artifact_paths:
        raise ValueError(
            f"staging binding report must provide non-empty `artifact_paths`: {path.as_posix()}"
        )

    real_bindings = _normalize_bindings(payload.get("real_bindings", []))
    if not real_bindings:
        raise ValueError(
            f"staging binding report must provide non-empty `real_bindings`: {path.as_posix()}"
        )
    if not any(binding.startswith("dagster://") for binding in real_bindings):
        raise ValueError(
            "staging binding report must include at least one `dagster://...` binding: "
            f"{path.as_posix()}"
        )

    return {
        "proof_class": "staging-real",
        "environment": "staging-real",
        "dagster_url": dagster_url,
        "orchestrator": orchestrator,
        "run_ids": run_ids,
        "artifact_paths": artifact_paths,
        "real_bindings": real_bindings,
    }


def _readiness_gate(*, observed_at_utc: str) -> dict[str, object]:
    parsed_utc = _parse_iso_utc(observed_at_utc, field_name="readiness_observed_at_utc")
    local = parsed_utc.astimezone(ZoneInfo(READINESS_TIMEZONE))
    deadline = local.replace(hour=READINESS_TARGET_HOUR, minute=0, second=0, microsecond=0)
    met = local <= deadline
    return {
        "status": "PASS" if met else "FAIL",
        "observed_at_utc": parsed_utc.isoformat().replace("+00:00", "Z"),
        "observed_at_local": local.isoformat(),
        "timezone": READINESS_TIMEZONE,
        "target_local": f"{READINESS_TARGET_HOUR:02d}:00",
        "target_met": met,
    }


def _validate_nightly_cycle_sequence(*, first_observed_utc: datetime, second_observed_utc: datetime) -> None:
    if second_observed_utc <= first_observed_utc:
        raise ValueError(
            "dagster cutover requires two nightly readiness timestamps in strictly increasing order"
        )

    local_zone = ZoneInfo(READINESS_TIMEZONE)
    first_local_date = first_observed_utc.astimezone(local_zone).date()
    second_local_date = second_observed_utc.astimezone(local_zone).date()
    if second_local_date != first_local_date + timedelta(days=1):
        raise ValueError(
            "dagster cutover requires two consecutive nightly cycles; "
            "nightly readiness timestamps must map to adjacent local dates in Europe/Moscow"
        )


def _build_graph_definition_artifact() -> dict[str, object]:
    specs = moex_historical_asset_specs()
    return {
        "route_id": CANONICAL_ROUTE_ID,
        "job_name": "moex_historical_cutover_job",
        "graph_edges": [
            {"from": "moex_raw_ingest", "to": "moex_canonical_refresh"},
        ],
        "assets": [
            {
                "key": spec.key,
                "description": spec.description,
                "inputs": list(spec.inputs),
                "outputs": list(spec.outputs),
            }
            for spec in specs
        ],
    }


def _build_schedule_retry_lock_artifact(
    *,
    schedule_cron: str,
    retry_max_attempts: int,
    retry_backoff_seconds: list[int],
    lease_timeout_sec: int,
) -> dict[str, object]:
    if not schedule_cron.strip():
        raise ValueError("`schedule_cron` must be non-empty")
    if retry_max_attempts <= 0:
        raise ValueError("`retry_max_attempts` must be > 0")
    if not retry_backoff_seconds or any(item <= 0 for item in retry_backoff_seconds):
        raise ValueError("`retry_backoff_seconds` must contain positive integers")
    if lease_timeout_sec <= 0:
        raise ValueError("`lease_timeout_sec` must be > 0")

    return {
        "route_id": CANONICAL_ROUTE_ID,
        "schedule": {
            "cron": schedule_cron,
            "timezone": READINESS_TIMEZONE,
            "readiness_target_local": f"{READINESS_TARGET_HOUR:02d}:00",
        },
        "retry": {
            "max_attempts": retry_max_attempts,
            "backoff_seconds": retry_backoff_seconds,
        },
        "lock": {
            "lease_scope": LEASE_SCOPE_AUTHORITATIVE_STORE_ROUTE,
            "lease_backend": LEASE_BACKEND_DELTA_LEDGER_CAS,
            "lease_timeout_sec": lease_timeout_sec,
            "single_writer": True,
        },
    }


def _validate_cutover_runtime_contract(
    *,
    schedule_cron: str,
    retry_max_attempts: int,
    retry_backoff_seconds: list[int],
) -> None:
    normalized_schedule_cron = schedule_cron.strip()
    if normalized_schedule_cron != MOEX_HISTORICAL_NIGHTLY_CRON:
        raise ValueError(
            "dagster cutover canonical route requires Dagster nightly cron "
            f"`{MOEX_HISTORICAL_NIGHTLY_CRON}`, got `{normalized_schedule_cron or 'EMPTY'}`"
        )
    if retry_max_attempts != int(MOEX_HISTORICAL_RETRY_POLICY.max_retries or 0):
        raise ValueError(
            "dagster cutover canonical route requires "
            f"`retry_max_attempts={MOEX_HISTORICAL_RETRY_POLICY.max_retries}`, "
            f"got `{retry_max_attempts}`"
        )
    canonical_backoff = list(CANONICAL_RETRY_BACKOFF_SECONDS)
    if retry_backoff_seconds != canonical_backoff:
        raise ValueError(
            "dagster cutover canonical route requires "
            f"`retry_backoff_seconds={canonical_backoff}`, got `{retry_backoff_seconds}`"
        )


def _run_single_writer_probe(
    *,
    ledger_table_path: Path,
    requested_at_utc: str,
    lease_timeout_sec: int,
    changed_windows_hash: str | None,
) -> dict[str, object]:
    holder_a = "dagster-writer-A"
    holder_b = "dagster-writer-B"

    acquire_a = acquire_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id=holder_a,
        owner_job="dagster-moex-nightly",
        requested_at_utc=requested_at_utc,
        ttl_seconds=lease_timeout_sec,
        run_id="dagster-cutover-single-writer-probe-A",
        changed_windows_hash=changed_windows_hash,
    )

    acquire_b = acquire_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id=holder_b,
        owner_job="dagster-moex-nightly",
        requested_at_utc=requested_at_utc,
        ttl_seconds=lease_timeout_sec,
        run_id="dagster-cutover-single-writer-probe-B",
        changed_windows_hash=changed_windows_hash,
    )

    release_a = release_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id=holder_a,
        lease_token=str(acquire_a.get("lease_token", "")).strip(),
        requested_at_utc=(
            _parse_iso_utc(requested_at_utc, field_name="requested_at_utc") + timedelta(minutes=1)
        ).isoformat().replace("+00:00", "Z"),
        run_id="dagster-cutover-single-writer-probe-A-release",
        changed_windows_hash=changed_windows_hash,
    )

    blocked = str(acquire_b.get("status", "")).strip() == STATUS_BLOCKED
    released = str(release_a.get("status", "")).strip() in PASS_LIKE_STATUSES
    return {
        "status": "PASS" if blocked and released else "FAIL",
        "route_id": CANONICAL_ROUTE_ID,
        "probe_requested_at_utc": requested_at_utc,
        "writer_a_acquire": acquire_a,
        "writer_b_acquire": acquire_b,
        "writer_a_release": release_a,
        "blocked_conflict_proven": blocked,
        "release_after_probe": released,
    }


def _run_route_cycle(
    *,
    cycle: CutoverCycle,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    output_dir: Path,
    dagster_instance: DagsterInstance,
    ledger_table_path: Path,
    holder_id: str,
    lease_timeout_sec: int,
    changed_windows_hash: str | None,
    enforce_readiness_target: bool,
) -> dict[str, object]:
    request_utc = _parse_iso_utc(cycle.requested_at_utc, field_name=f"{cycle.mode}.requested_at_utc")
    request_iso = request_utc.isoformat().replace("+00:00", "Z")

    acquire = acquire_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id=holder_id,
        owner_job=f"dagster-moex-{cycle.mode}",
        requested_at_utc=request_iso,
        ttl_seconds=lease_timeout_sec,
        run_id=cycle.run_id,
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": cycle.mode},
    )
    acquire_status = str(acquire.get("status", "")).strip()

    cycle_report: dict[str, object] = {
        "mode": cycle.mode,
        "run_id": cycle.run_id,
        "route_id": CANONICAL_ROUTE_ID,
        "requested_at_utc": request_iso,
        "readiness_observed_at_utc": cycle.readiness_observed_at_utc,
        "acquire": acquire,
        "heartbeat": None,
        "materialization": None,
        "release": None,
        "readiness_gate": {},
        "enforce_readiness_target": enforce_readiness_target,
    }
    if acquire_status == STATUS_BLOCKED:
        conflict = record_technical_route_blocked_conflict(
            ledger_table_path=ledger_table_path,
            route_id=CANONICAL_ROUTE_ID,
            holder_id=holder_id,
            owner_job=f"dagster-moex-{cycle.mode}",
            requested_at_utc=request_iso,
            reason_code="lease_conflict",
            blocking_holder_id=str(acquire.get("blocking_holder_id", "")).strip() or None,
            blocking_lease_token=str(acquire.get("blocking_lease_token", "")).strip() or None,
            run_id=cycle.run_id,
            changed_windows_hash=changed_windows_hash,
            metadata={"mode": cycle.mode},
        )
        cycle_report["conflict_record"] = conflict
        cycle_report["status"] = STATUS_BLOCKED
        cycle_report["publish_decision"] = "blocked"
        cycle_report["reasons"] = ["single-writer lease conflict"]
        cycle_report["readiness_gate"] = _readiness_gate(observed_at_utc=cycle.readiness_observed_at_utc)
        return cycle_report
    if acquire_status not in PASS_LIKE_STATUSES:
        cycle_report["status"] = STATUS_FAILED
        cycle_report["publish_decision"] = "blocked"
        cycle_report["reasons"] = [f"unexpected lease acquire status={acquire_status or 'EMPTY'}"]
        cycle_report["readiness_gate"] = _readiness_gate(observed_at_utc=cycle.readiness_observed_at_utc)
        return cycle_report

    lease_token = str(acquire.get("lease_token", "")).strip()
    lease_version = int(acquire.get("lease_version", 1) or 1)
    heartbeat = heartbeat_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id=holder_id,
        owner_job=f"dagster-moex-{cycle.mode}",
        lease_token=lease_token,
        requested_at_utc=(request_utc + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        ttl_seconds=lease_timeout_sec,
        expected_lease_version=lease_version,
        run_id=f"{cycle.run_id}:heartbeat",
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": cycle.mode},
    )
    cycle_report["heartbeat"] = heartbeat
    if str(heartbeat.get("status", "")).strip() not in PASS_LIKE_STATUSES:
        cycle_report["status"] = STATUS_BLOCKED
        cycle_report["publish_decision"] = "blocked"
        cycle_report["reasons"] = ["lease heartbeat failed before materialization"]
        cycle_report["readiness_gate"] = _readiness_gate(observed_at_utc=cycle.readiness_observed_at_utc)
        return cycle_report

    cycle_output_dir = output_dir / cycle.mode / cycle.run_id
    materialization = execute_moex_historical_cutover_job(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=cycle_output_dir,
        canonical_run_id=cycle.run_id,
        instance=dagster_instance,
        run_id=cycle.run_id,
        extra_tags={
            "dagster/mode": cycle.mode,
            "dagster/retry_of_run_id": cycle.retry_of_run_id or "",
        },
        scheduled_execution_time=request_utc if cycle.mode == "nightly" else None,
        raise_on_error=False,
    )
    cycle_report["materialization"] = materialization

    release = release_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id=holder_id,
        owner_job=f"dagster-moex-{cycle.mode}",
        lease_token=lease_token,
        requested_at_utc=(request_utc + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
        run_id=f"{cycle.run_id}:release",
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": cycle.mode},
    )
    cycle_report["release"] = release
    readiness_gate = _readiness_gate(observed_at_utc=cycle.readiness_observed_at_utc)
    cycle_report["readiness_gate"] = readiness_gate

    reasons: list[str] = []
    if not bool(materialization.get("success")):
        reasons.append("dagster materialization failed")
    if str(release.get("status", "")).strip() not in PASS_LIKE_STATUSES:
        reasons.append("lease release failed")
    if enforce_readiness_target and not bool(readiness_gate.get("target_met")):
        reasons.append("morning readiness target missed")

    if reasons:
        cycle_report["status"] = STATUS_BLOCKED
        cycle_report["publish_decision"] = "blocked"
        cycle_report["reasons"] = reasons
    else:
        cycle_report["status"] = STATUS_PASS
        cycle_report["publish_decision"] = "publish"
        cycle_report["reasons"] = []
    return cycle_report


def _run_recovery_drill(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    output_dir: Path,
    dagster_instance: DagsterInstance,
    ledger_table_path: Path,
    run_id: str,
    requested_at_utc: str,
    lease_timeout_sec: int,
    changed_windows_hash: str | None,
    recovery_environment: str,
    recovery_binding: str,
) -> dict[str, object]:
    request_time = _parse_iso_utc(requested_at_utc, field_name="recovery.requested_at_utc")
    stale_takeover_time = request_time + timedelta(seconds=lease_timeout_sec + 5)
    replay_run_id = f"{run_id}-recovery"

    acquire = acquire_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id="dagster-nightly-owner",
        owner_job="dagster-moex-nightly",
        requested_at_utc=request_time.isoformat().replace("+00:00", "Z"),
        ttl_seconds=lease_timeout_sec,
        run_id=f"{run_id}-recovery-initial",
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": "recovery-initial"},
    )

    conflict = acquire_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id="dagster-recovery-owner",
        owner_job="dagster-moex-recovery",
        requested_at_utc=(request_time + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
        ttl_seconds=lease_timeout_sec,
        run_id=f"{run_id}-recovery-conflict",
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": "recovery-conflict"},
    )

    conflict_record = record_technical_route_blocked_conflict(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id="dagster-recovery-owner",
        owner_job="dagster-moex-recovery",
        requested_at_utc=(request_time + timedelta(seconds=35)).isoformat().replace("+00:00", "Z"),
        reason_code="lease_conflict",
        blocking_holder_id=str(conflict.get("blocking_holder_id", "")).strip() or None,
        blocking_lease_token=str(conflict.get("blocking_lease_token", "")).strip() or None,
        expected_lease_version=int(conflict.get("lease_version", 1) or 1),
        run_id=f"{run_id}-recovery-conflict-record",
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": "recovery-conflict-record"},
    )

    takeover = takeover_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id="dagster-recovery-owner",
        owner_job="dagster-moex-recovery",
        requested_at_utc=stale_takeover_time.isoformat().replace("+00:00", "Z"),
        ttl_seconds=lease_timeout_sec,
        expected_lease_version=int(acquire.get("lease_version", 1) or 1),
        previous_lease_token=str(acquire.get("lease_token", "")).strip() or None,
        run_id=f"{run_id}-recovery-takeover",
        retry_of_run_id=f"{run_id}-recovery-initial",
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": "recovery-takeover"},
    )

    materialization = execute_moex_historical_cutover_job(
        raw_table_path=raw_table_path,
        raw_ingest_report_path=raw_ingest_report_path,
        canonical_output_dir=output_dir / "recovery" / replay_run_id,
        canonical_run_id=replay_run_id,
        instance=dagster_instance,
        run_id=replay_run_id,
        extra_tags={
            "dagster/mode": "recovery",
            "dagster/retry_of_run_id": f"{run_id}-recovery-initial",
        },
        raise_on_error=False,
    )

    release = release_technical_route_lease(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
        holder_id="dagster-recovery-owner",
        owner_job="dagster-moex-recovery",
        lease_token=str(takeover.get("lease_token", "")).strip(),
        requested_at_utc=(stale_takeover_time + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
        run_id=f"{run_id}-recovery-release",
        changed_windows_hash=changed_windows_hash,
        metadata={"mode": "recovery-release"},
    )

    fail_stage_ok = str(conflict.get("status", "")).strip() == STATUS_BLOCKED
    retry_stage_ok = str(takeover.get("status", "")).strip() in PASS_LIKE_STATUSES
    replay_stage_ok = bool(materialization.get("success"))
    recover_stage_ok = str(release.get("status", "")).strip() in PASS_LIKE_STATUSES

    sequence = [
        {
            "stage": "fail",
            "status": "SUCCESS" if fail_stage_ok else "FAILED",
            "at_utc": (request_time + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
        },
        {
            "stage": "retry",
            "status": "SUCCESS" if retry_stage_ok else "FAILED",
            "at_utc": stale_takeover_time.isoformat().replace("+00:00", "Z"),
        },
        {
            "stage": "replay",
            "status": "SUCCESS" if replay_stage_ok else "FAILED",
            "at_utc": (stale_takeover_time + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
        },
        {
            "stage": "recover",
            "status": "SUCCESS" if recover_stage_ok else "FAILED",
            "at_utc": (stale_takeover_time + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
        },
    ]

    status = "PASS" if all(item["status"] == "SUCCESS" for item in sequence) else "FAIL"
    return {
        "status": status,
        "environment": recovery_environment,
        "recovery_binding": recovery_binding,
        "scenario": "transient_lease_conflict_recovery",
        "manual_cleanup_performed": False,
        "deterministic_replay": replay_stage_ok,
        "sequence": sequence,
        "publish_pointer": {
            "last_healthy_snapshot": f"{run_id}-nightly-2",
            "rolled_back_to": f"{run_id}-nightly-2",
            "restored_after_recovery": replay_run_id,
            "restored_healthy": recover_stage_ok and replay_stage_ok,
        },
        "acquire": acquire,
        "conflict": conflict,
        "conflict_record": conflict_record,
        "takeover": takeover,
        "materialization": materialization,
        "release": release,
    }


def run_moex_dagster_cutover(
    *,
    raw_table_path: Path,
    raw_ingest_report_path: Path,
    output_dir: Path,
    run_id: str,
    nightly_readiness_observed_at_utc: list[str],
    route_signal: str = "worker:capability-only",
    schedule_cron: str = "0 2 * * *",
    retry_max_attempts: int = 3,
    retry_backoff_seconds: list[int] | None = None,
    lease_timeout_sec: int = 900,
    staging_binding_report_path: Path | None = None,
    require_staging_real: bool = False,
) -> dict[str, object]:
    if len(nightly_readiness_observed_at_utc) != 2:
        raise ValueError("dagster cutover requires exactly two nightly readiness timestamps")
    if not run_id.strip():
        raise ValueError("`run_id` must be non-empty")
    normalized_route_signal = route_signal.strip()
    if not normalized_route_signal:
        raise ValueError("`route_signal` must be non-empty")

    raw_table_resolved = raw_table_path.resolve()
    raw_ingest_report_resolved = raw_ingest_report_path.resolve()
    if not raw_table_resolved.exists():
        raise FileNotFoundError(f"raw table path does not exist: {raw_table_resolved.as_posix()}")
    if not raw_ingest_report_resolved.exists():
        raise FileNotFoundError(f"raw ingest report path does not exist: {raw_ingest_report_resolved.as_posix()}")

    raw_status = _read_raw_status(raw_ingest_report_resolved)
    if raw_status not in PASS_LIKE_STATUSES:
        raise ValueError(
            "dagster cutover requires raw-ingest PASS/PASS-NOOP before canonical route launch; "
            f"got `{raw_status or 'EMPTY'}`"
        )

    assert_moex_historical_definitions_executable()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    staging_binding: dict[str, object] | None = None
    staging_binding_path_resolved: Path | None = None
    if staging_binding_report_path is not None:
        staging_binding_path_resolved = staging_binding_report_path.resolve()
        if not staging_binding_path_resolved.exists():
            raise FileNotFoundError(
                "staging binding report path does not exist: "
                f"{staging_binding_path_resolved.as_posix()}"
            )
        staging_binding = _load_staging_binding_report(staging_binding_path_resolved)

    normalized_schedule_cron = schedule_cron.strip()
    retry_backoff = list(retry_backoff_seconds or CANONICAL_RETRY_BACKOFF_SECONDS)
    _validate_cutover_runtime_contract(
        schedule_cron=normalized_schedule_cron,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_seconds=retry_backoff,
    )
    changed_windows_hash = _read_changed_windows_hash(raw_ingest_report_resolved)
    ledger_table_path = output_dir / "delta" / "technical_route_ledger.delta"
    dagster_instance_root = output_dir / "dagster-instance"
    dagster_instance_root.mkdir(parents=True, exist_ok=True)
    dagster_instance = DagsterInstance.local_temp(tempdir=dagster_instance_root.as_posix())

    graph_artifact = _build_graph_definition_artifact()
    dagster_binding_artifact = build_moex_historical_dagster_binding_artifact()
    schedule_lock_artifact = _build_schedule_retry_lock_artifact(
        schedule_cron=normalized_schedule_cron,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_seconds=retry_backoff,
        lease_timeout_sec=lease_timeout_sec,
    )

    graph_artifact_path = output_dir / "dagster-graph-definition.json"
    dagster_binding_artifact_path = output_dir / "dagster-runtime-binding.json"
    schedule_lock_artifact_path = output_dir / "schedule-retry-lock-contract.json"
    _json_write(graph_artifact_path, graph_artifact)
    _json_write(dagster_binding_artifact_path, dagster_binding_artifact)
    _json_write(schedule_lock_artifact_path, schedule_lock_artifact)

    cycle_1_time = _parse_iso_utc(nightly_readiness_observed_at_utc[0], field_name="nightly[0]")
    cycle_2_time = _parse_iso_utc(nightly_readiness_observed_at_utc[1], field_name="nightly[1]")
    _validate_nightly_cycle_sequence(
        first_observed_utc=cycle_1_time,
        second_observed_utc=cycle_2_time,
    )

    nightly_cycles = [
        CutoverCycle(
            mode="nightly",
            run_id=f"{run_id}-nightly-1",
            requested_at_utc=(cycle_1_time - timedelta(minutes=20)).isoformat().replace("+00:00", "Z"),
            readiness_observed_at_utc=cycle_1_time.isoformat().replace("+00:00", "Z"),
        ),
        CutoverCycle(
            mode="nightly",
            run_id=f"{run_id}-nightly-2",
            requested_at_utc=(cycle_2_time - timedelta(minutes=20)).isoformat().replace("+00:00", "Z"),
            readiness_observed_at_utc=cycle_2_time.isoformat().replace("+00:00", "Z"),
            retry_of_run_id=f"{run_id}-nightly-1",
        ),
    ]
    repair_cycle = CutoverCycle(
        mode="repair",
        run_id=f"{run_id}-repair-1",
        requested_at_utc=(cycle_2_time + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        readiness_observed_at_utc=(cycle_2_time + timedelta(hours=1, minutes=10)).isoformat().replace("+00:00", "Z"),
        retry_of_run_id=f"{run_id}-nightly-2",
    )
    backfill_cycle = CutoverCycle(
        mode="backfill",
        run_id=f"{run_id}-backfill-1",
        requested_at_utc=(cycle_2_time + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
        readiness_observed_at_utc=(cycle_2_time + timedelta(hours=2, minutes=10)).isoformat().replace("+00:00", "Z"),
        retry_of_run_id=f"{run_id}-repair-1",
    )

    single_writer_probe = _run_single_writer_probe(
        ledger_table_path=ledger_table_path,
        requested_at_utc=(cycle_1_time - timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
        lease_timeout_sec=lease_timeout_sec,
        changed_windows_hash=changed_windows_hash,
    )

    cycle_reports = [
        _run_route_cycle(
            cycle=nightly_cycles[0],
            raw_table_path=raw_table_resolved,
            raw_ingest_report_path=raw_ingest_report_resolved,
            output_dir=output_dir,
            dagster_instance=dagster_instance,
            ledger_table_path=ledger_table_path,
            holder_id="dagster-nightly-owner",
            lease_timeout_sec=lease_timeout_sec,
            changed_windows_hash=changed_windows_hash,
            enforce_readiness_target=True,
        ),
        _run_route_cycle(
            cycle=nightly_cycles[1],
            raw_table_path=raw_table_resolved,
            raw_ingest_report_path=raw_ingest_report_resolved,
            output_dir=output_dir,
            dagster_instance=dagster_instance,
            ledger_table_path=ledger_table_path,
            holder_id="dagster-nightly-owner",
            lease_timeout_sec=lease_timeout_sec,
            changed_windows_hash=changed_windows_hash,
            enforce_readiness_target=True,
        ),
        _run_route_cycle(
            cycle=repair_cycle,
            raw_table_path=raw_table_resolved,
            raw_ingest_report_path=raw_ingest_report_resolved,
            output_dir=output_dir,
            dagster_instance=dagster_instance,
            ledger_table_path=ledger_table_path,
            holder_id="dagster-repair-owner",
            lease_timeout_sec=lease_timeout_sec,
            changed_windows_hash=changed_windows_hash,
            enforce_readiness_target=False,
        ),
        _run_route_cycle(
            cycle=backfill_cycle,
            raw_table_path=raw_table_resolved,
            raw_ingest_report_path=raw_ingest_report_resolved,
            output_dir=output_dir,
            dagster_instance=dagster_instance,
            ledger_table_path=ledger_table_path,
            holder_id="dagster-backfill-owner",
            lease_timeout_sec=lease_timeout_sec,
            changed_windows_hash=changed_windows_hash,
            enforce_readiness_target=False,
        ),
    ]

    recovery_environment = "integration-local"
    recovery_binding = LOCAL_DAGSTER_BINDING
    if staging_binding is not None:
        recovery_environment = str(staging_binding.get("environment", "staging-real"))
        bindings = _normalize_bindings(staging_binding.get("real_bindings", []))
        preferred = next((item for item in bindings if item.startswith("dagster://")), "")
        if preferred:
            recovery_binding = preferred

    recovery_drill = _run_recovery_drill(
        raw_table_path=raw_table_resolved,
        raw_ingest_report_path=raw_ingest_report_resolved,
        output_dir=output_dir,
        dagster_instance=dagster_instance,
        ledger_table_path=ledger_table_path,
        run_id=run_id,
        requested_at_utc=(cycle_2_time + timedelta(hours=3)).isoformat().replace("+00:00", "Z"),
        lease_timeout_sec=lease_timeout_sec,
        changed_windows_hash=changed_windows_hash,
        recovery_environment=recovery_environment,
        recovery_binding=recovery_binding,
    )
    recovery_drill_path = output_dir / "recovery-drill-artifact.json"
    _json_write(recovery_drill_path, recovery_drill)

    nightly_cycle_artifacts: list[str] = []
    for index, cycle_report in enumerate(cycle_reports[:2], start=1):
        path = output_dir / f"nightly-cycle-{index:02d}-report.json"
        _json_write(path, cycle_report)
        nightly_cycle_artifacts.append(path.as_posix())

    status = "PASS"
    reasons: list[str] = []
    if single_writer_probe["status"] != "PASS":
        status = "BLOCKED"
        reasons.append("single-writer probe did not confirm blocked conflict behavior")

    for cycle_report in cycle_reports:
        if cycle_report["status"] != STATUS_PASS:
            status = "BLOCKED"
            reasons.append(
                f"{cycle_report['mode']} cycle `{cycle_report['run_id']}` failed: "
                + ", ".join(cycle_report.get("reasons", []))
            )
    if recovery_drill["status"] != "PASS":
        status = "BLOCKED"
        reasons.append("recovery drill failed")

    proof_class = "staging-real" if staging_binding is not None else "integration"
    if require_staging_real and proof_class != "staging-real":
        status = "BLOCKED"
        reasons.append(
            "staging-real proof requires --staging-binding-report-path from a real Dagster environment"
        )

    ledger_rows = read_technical_route_run_ledger(
        ledger_table_path=ledger_table_path,
        route_id=CANONICAL_ROUTE_ID,
    )
    real_bindings: set[str] = set(_normalize_bindings((staging_binding or {}).get("real_bindings", [])))
    if not real_bindings:
        real_bindings.add(LOCAL_DAGSTER_BINDING)
    real_bindings.add("delta-ledger-cas://technical-route-run-ledger")
    for cycle_report in cycle_reports:
        materialization = cycle_report.get("materialization")
        if not isinstance(materialization, dict):
            continue
        canonicalization_report_path = Path(str(materialization.get("output_paths", {}).get("canonicalization_report", "")))
        if not canonicalization_report_path.exists():
            continue
        canonicalization_report = _json_load(canonicalization_report_path)
        for item in canonicalization_report.get("real_bindings", []):
            if isinstance(item, str) and item.strip():
                real_bindings.add(item.strip())

    report = {
        "run_id": run_id,
        "route_signal": normalized_route_signal,
        "proof_class": proof_class,
        "status": status,
        "publish_decision": "publish" if status == "PASS" else "blocked",
        "route_id": CANONICAL_ROUTE_ID,
        "raw_status": raw_status,
        "staging_binding": staging_binding,
        "single_writer_probe": single_writer_probe,
        "cycles": cycle_reports,
        "recovery_drill": recovery_drill,
        "reasons": reasons,
        "artifact_paths": {
            "dagster_graph_definition": graph_artifact_path.as_posix(),
            "dagster_runtime_binding": dagster_binding_artifact_path.as_posix(),
            "schedule_retry_lock_contract": schedule_lock_artifact_path.as_posix(),
            "recovery_drill": recovery_drill_path.as_posix(),
            "nightly_cycle_reports": nightly_cycle_artifacts,
            "staging_binding_report": (
                staging_binding_path_resolved.as_posix()
                if staging_binding_path_resolved is not None
                else ""
            ),
        },
        "output_paths": {
            "technical_route_ledger": ledger_table_path.as_posix(),
            "dagster_instance_root": dagster_instance_root.as_posix(),
        },
        "ledger_event_count": len(ledger_rows),
        "readiness_target": {
            "timezone": READINESS_TIMEZONE,
            "target_local": f"{READINESS_TARGET_HOUR:02d}:00",
            "required_consecutive_nightly_cycles": 2,
        },
        "real_bindings": sorted(real_bindings),
        "generated_at_utc": _utc_now_iso(),
    }
    _json_write(output_dir / "dagster-cutover-report.json", report)
    return report
