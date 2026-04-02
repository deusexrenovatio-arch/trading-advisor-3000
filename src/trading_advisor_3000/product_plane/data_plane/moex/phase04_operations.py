from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re

import yaml


MANDATORY_PHASE04_JOB_IDS: tuple[str, ...] = (
    "moex_backfill_bootstrap",
    "moex_incremental_ingest",
    "cross_source_reconciliation",
    "qc_gate_and_publish",
)

MANDATORY_MONITORING_METRICS: tuple[str, ...] = (
    "pipeline_run_latency_seconds",
    "pipeline_run_failures_total",
    "qc_gate_fail_total",
    "reconciliation_drift_bps_p95",
    "publish_freshness_lag_seconds",
)

MANDATORY_RECOVERY_STAGES: tuple[str, ...] = (
    "fail",
    "retry",
    "replay",
    "recover",
)

P1_P2_LEVELS: set[str] = {"P1", "P2"}
RESOLVED_DEFECT_STATUSES: set[str] = {"resolved", "closed", "done"}
SHA256_HEX_RE = re.compile(r"^[a-f0-9]{64}$")
TEMPLATE_BINDING_TOKEN_RE = re.compile(r"<[^>]+>")
ENV_TEMPLATE_BINDING_TOKEN_RE = re.compile(r"\$\{[^}]+\}")
ACCEPTANCE_ROUTE_SIGNAL = "acceptance:governed-phase-route"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str) -> datetime:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("timestamp must be non-empty")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _sample_errors(values: list[str], *, limit: int = 10) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
        if len(unique) >= limit:
            break
    return unique


def _is_template_binding(value: str) -> bool:
    normalized = value.strip()
    if not normalized:
        return True
    if TEMPLATE_BINDING_TOKEN_RE.search(normalized):
        return True
    if ENV_TEMPLATE_BINDING_TOKEN_RE.search(normalized):
        return True
    return False


def _collect_real_bindings(values: object) -> set[str]:
    bindings: set[str] = set()
    if not isinstance(values, list):
        return bindings
    for item in values:
        if not isinstance(item, str):
            continue
        normalized = item.strip()
        if not normalized or _is_template_binding(normalized):
            continue
        bindings.add(normalized)
    return bindings


def _json_load(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path.as_posix()}")
    return payload


def _json_write(path: Path, payload: dict[str, object] | list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 64)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_supporting_path(*, source_path: Path, candidate: str, label: str) -> Path:
    raw = str(candidate).strip()
    if not raw:
        raise ValueError(f"{label} must be non-empty")
    provided = Path(raw)
    resolved = provided if provided.is_absolute() else (source_path.parent / provided).resolve()
    if not resolved.exists():
        raise ValueError(f"{label} does not exist: {resolved.as_posix()}")
    return resolved


def _validate_source_provenance(
    *,
    payload: dict[str, object],
    source_path: Path,
    evidence_name: str,
    errors: list[str],
) -> dict[str, object]:
    provenance_raw = payload.get("source_provenance")
    if not isinstance(provenance_raw, dict):
        msg = f"{evidence_name} requires object `source_provenance`"
        errors.append(msg)
        return {
            "status": "FAIL",
            "evidence_name": evidence_name,
            "source_path": source_path.resolve().as_posix(),
            "errors": [msg],
        }

    source_system = str(provenance_raw.get("source_system", "")).strip()
    source_channel = str(provenance_raw.get("source_channel", "")).strip()
    collector = str(provenance_raw.get("collector", "")).strip()
    collection_id = str(provenance_raw.get("collection_id", "")).strip()
    exported_at_utc = str(provenance_raw.get("exported_at_utc", "")).strip()
    collected_at_utc = str(provenance_raw.get("collected_at_utc", "")).strip()
    immutable_export_path_raw = str(provenance_raw.get("immutable_export_path", "")).strip()
    immutable_export_sha256 = str(provenance_raw.get("immutable_export_sha256", "")).strip().lower()

    provenance_errors: list[str] = []
    if not source_system:
        provenance_errors.append("source_provenance.source_system is missing")
    if not source_channel:
        provenance_errors.append("source_provenance.source_channel is missing")
    if not collector:
        provenance_errors.append("source_provenance.collector is missing")
    if not collection_id:
        provenance_errors.append("source_provenance.collection_id is missing")

    exported_at = None
    if not exported_at_utc:
        provenance_errors.append("source_provenance.exported_at_utc is missing")
    else:
        try:
            exported_at = _parse_iso_utc(exported_at_utc)
        except ValueError:
            provenance_errors.append("source_provenance.exported_at_utc is invalid ISO timestamp")

    collected_at = None
    if not collected_at_utc:
        provenance_errors.append("source_provenance.collected_at_utc is missing")
    else:
        try:
            collected_at = _parse_iso_utc(collected_at_utc)
        except ValueError:
            provenance_errors.append("source_provenance.collected_at_utc is invalid ISO timestamp")

    if exported_at is not None and collected_at is not None and exported_at > collected_at:
        provenance_errors.append("source_provenance.exported_at_utc is later than collected_at_utc")

    immutable_export_path = None
    if not immutable_export_path_raw:
        provenance_errors.append("source_provenance.immutable_export_path is missing")
    else:
        try:
            immutable_export_path = _resolve_supporting_path(
                source_path=source_path,
                candidate=immutable_export_path_raw,
                label="source_provenance.immutable_export_path",
            )
        except ValueError as exc:
            provenance_errors.append(str(exc))

    if not immutable_export_sha256:
        provenance_errors.append("source_provenance.immutable_export_sha256 is missing")
    elif not SHA256_HEX_RE.match(immutable_export_sha256):
        provenance_errors.append("source_provenance.immutable_export_sha256 must be lowercase sha256 hex")

    resolved_export_sha256 = ""
    if immutable_export_path is not None:
        resolved_export_sha256 = _sha256_path(immutable_export_path)
        if immutable_export_sha256 and immutable_export_sha256 != resolved_export_sha256:
            provenance_errors.append(
                "source_provenance.immutable_export_sha256 does not match immutable export content hash"
            )

    for item in provenance_errors:
        errors.append(f"{evidence_name}: {item}")

    return {
        "status": "PASS" if not provenance_errors else "FAIL",
        "evidence_name": evidence_name,
        "source_system": source_system,
        "source_channel": source_channel,
        "collector": collector,
        "collection_id": collection_id,
        "exported_at_utc": exported_at_utc,
        "collected_at_utc": collected_at_utc,
        "immutable_export_path": (
            immutable_export_path.as_posix() if immutable_export_path is not None else immutable_export_path_raw
        ),
        "immutable_export_sha256": immutable_export_sha256,
        "resolved_export_sha256": resolved_export_sha256,
        "errors": _sample_errors(provenance_errors, limit=20),
    }


@dataclass(frozen=True)
class SchedulerJobPolicy:
    job_id: str
    required: bool
    cron: str
    retry_max_attempts: int
    retry_backoff_seconds: tuple[int, ...]


@dataclass(frozen=True)
class SchedulerPolicy:
    version: int
    scheduler_system: str
    max_tick_age_minutes: int
    max_queued_runs_per_job: int
    jobs: tuple[SchedulerJobPolicy, ...]

    @property
    def required_job_ids(self) -> tuple[str, ...]:
        return tuple(sorted(item.job_id for item in self.jobs if item.required))


@dataclass(frozen=True)
class MonitoringMetricRule:
    metric_id: str
    min_value: float | None
    max_value: float | None


@dataclass(frozen=True)
class MonitoringPolicy:
    version: int
    freshness_sla_seconds: float
    required_metrics: tuple[MonitoringMetricRule, ...]
    required_dashboard_ids: tuple[str, ...]
    required_query_ids: tuple[str, ...]

    @property
    def required_metric_ids(self) -> tuple[str, ...]:
        return tuple(rule.metric_id for rule in self.required_metrics)


def load_phase04_scheduler_policy(path: Path) -> SchedulerPolicy:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("scheduler policy must be yaml object")

    version = payload.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise ValueError("scheduler policy `version` must be positive integer")

    scheduler_system = str(payload.get("scheduler_system", "")).strip()
    if not scheduler_system:
        raise ValueError("scheduler policy `scheduler_system` must be non-empty string")

    max_tick_age_minutes = payload.get("max_tick_age_minutes")
    if isinstance(max_tick_age_minutes, bool) or not isinstance(max_tick_age_minutes, int) or max_tick_age_minutes <= 0:
        raise ValueError("scheduler policy `max_tick_age_minutes` must be positive integer")

    max_queued_runs_per_job = payload.get("max_queued_runs_per_job")
    if isinstance(max_queued_runs_per_job, bool) or not isinstance(max_queued_runs_per_job, int) or max_queued_runs_per_job < 0:
        raise ValueError("scheduler policy `max_queued_runs_per_job` must be integer >= 0")

    jobs_raw = payload.get("jobs")
    if not isinstance(jobs_raw, list) or not jobs_raw:
        raise ValueError("scheduler policy `jobs` must be non-empty list")

    jobs: list[SchedulerJobPolicy] = []
    seen_job_ids: set[str] = set()
    for index, row in enumerate(jobs_raw):
        if not isinstance(row, dict):
            raise ValueError(f"scheduler policy jobs[{index}] must be object")

        job_id = str(row.get("job_id", "")).strip()
        if not job_id:
            raise ValueError(f"scheduler policy jobs[{index}].job_id must be non-empty string")
        if job_id in seen_job_ids:
            raise ValueError(f"duplicate scheduler policy job_id: {job_id}")
        seen_job_ids.add(job_id)

        required = row.get("required")
        if not isinstance(required, bool):
            raise ValueError(f"scheduler policy jobs[{index}].required must be boolean")

        cron = str(row.get("cron", "")).strip()
        if not cron:
            raise ValueError(f"scheduler policy jobs[{index}].cron must be non-empty string")

        retry_raw = row.get("retry")
        if not isinstance(retry_raw, dict):
            raise ValueError(f"scheduler policy jobs[{index}].retry must be object")
        retry_max_attempts = retry_raw.get("max_attempts")
        if isinstance(retry_max_attempts, bool) or not isinstance(retry_max_attempts, int) or retry_max_attempts <= 0:
            raise ValueError(f"scheduler policy jobs[{index}].retry.max_attempts must be positive integer")

        backoff_raw = retry_raw.get("backoff_seconds")
        if not isinstance(backoff_raw, list) or not backoff_raw:
            raise ValueError(f"scheduler policy jobs[{index}].retry.backoff_seconds must be non-empty list")
        backoff: list[int] = []
        for item in backoff_raw:
            if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
                raise ValueError(
                    f"scheduler policy jobs[{index}].retry.backoff_seconds values must be positive integers"
                )
            backoff.append(int(item))

        jobs.append(
            SchedulerJobPolicy(
                job_id=job_id,
                required=required,
                cron=cron,
                retry_max_attempts=retry_max_attempts,
                retry_backoff_seconds=tuple(backoff),
            )
        )

    required_job_ids = {item.job_id for item in jobs if item.required}
    missing_mandatory = sorted(set(MANDATORY_PHASE04_JOB_IDS) - required_job_ids)
    if missing_mandatory:
        missing_text = ", ".join(missing_mandatory)
        raise ValueError(f"scheduler policy missing mandatory required jobs: {missing_text}")

    return SchedulerPolicy(
        version=version,
        scheduler_system=scheduler_system,
        max_tick_age_minutes=max_tick_age_minutes,
        max_queued_runs_per_job=max_queued_runs_per_job,
        jobs=tuple(jobs),
    )


def load_phase04_monitoring_policy(path: Path) -> MonitoringPolicy:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("monitoring policy must be yaml object")

    version = payload.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise ValueError("monitoring policy `version` must be positive integer")

    freshness_sla_seconds = payload.get("freshness_sla_seconds")
    if isinstance(freshness_sla_seconds, bool) or not isinstance(freshness_sla_seconds, (int, float)):
        raise ValueError("monitoring policy `freshness_sla_seconds` must be number")
    if float(freshness_sla_seconds) <= 0:
        raise ValueError("monitoring policy `freshness_sla_seconds` must be > 0")

    metrics_raw = payload.get("required_metrics")
    if not isinstance(metrics_raw, dict) or not metrics_raw:
        raise ValueError("monitoring policy `required_metrics` must be non-empty object")

    metric_rules: list[MonitoringMetricRule] = []
    for key, value in metrics_raw.items():
        metric_id = str(key).strip()
        if not metric_id:
            raise ValueError("monitoring policy contains empty metric id")
        if not isinstance(value, dict):
            raise ValueError(f"monitoring policy metric `{metric_id}` must be object")
        min_raw = value.get("min")
        max_raw = value.get("max")
        min_value = None
        max_value = None
        if min_raw is not None:
            if isinstance(min_raw, bool) or not isinstance(min_raw, (int, float)):
                raise ValueError(f"monitoring policy metric `{metric_id}` min must be number")
            min_value = float(min_raw)
        if max_raw is not None:
            if isinstance(max_raw, bool) or not isinstance(max_raw, (int, float)):
                raise ValueError(f"monitoring policy metric `{metric_id}` max must be number")
            max_value = float(max_raw)
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError(f"monitoring policy metric `{metric_id}` has min > max")
        metric_rules.append(
            MonitoringMetricRule(
                metric_id=metric_id,
                min_value=min_value,
                max_value=max_value,
            )
        )

    missing_mandatory = sorted(set(MANDATORY_MONITORING_METRICS) - {item.metric_id for item in metric_rules})
    if missing_mandatory:
        missing_text = ", ".join(missing_mandatory)
        raise ValueError(f"monitoring policy missing mandatory metrics: {missing_text}")

    dashboards_raw = payload.get("required_dashboards")
    if not isinstance(dashboards_raw, list) or not dashboards_raw:
        raise ValueError("monitoring policy `required_dashboards` must be non-empty list")
    required_dashboards = tuple(sorted({str(item).strip() for item in dashboards_raw if str(item).strip()}))
    if not required_dashboards:
        raise ValueError("monitoring policy `required_dashboards` must contain non-empty values")

    queries_raw = payload.get("required_queries")
    if not isinstance(queries_raw, list) or not queries_raw:
        raise ValueError("monitoring policy `required_queries` must be non-empty list")
    required_queries = tuple(sorted({str(item).strip() for item in queries_raw if str(item).strip()}))
    if not required_queries:
        raise ValueError("monitoring policy `required_queries` must contain non-empty values")

    return MonitoringPolicy(
        version=version,
        freshness_sla_seconds=float(freshness_sla_seconds),
        required_metrics=tuple(sorted(metric_rules, key=lambda item: item.metric_id)),
        required_dashboard_ids=required_dashboards,
        required_query_ids=required_queries,
    )


def _validate_acceptance_closure(
    *,
    acceptance_report: dict[str, object],
    acceptance_path: Path,
) -> tuple[bool, list[str], dict[str, object]]:
    verdict = str(acceptance_report.get("verdict", "")).upper()
    route_signal = str(acceptance_report.get("route_signal", "")).strip()

    reasons: list[str] = []
    if verdict != "PASS":
        reasons.append(f"verdict={verdict or 'missing'}")
    if route_signal != ACCEPTANCE_ROUTE_SIGNAL:
        reasons.append(f"route_signal={route_signal or 'missing'}")

    counters: dict[str, int] = {}
    for field in ("blockers", "policy_blockers", "evidence_gaps", "prohibited_findings"):
        raw = acceptance_report.get(field, [])
        if raw is None:
            count = 0
        elif isinstance(raw, list):
            count = len(raw)
        else:
            count = 0
            reasons.append(f"{field}=invalid-type")
        counters[field] = count
        if count > 0:
            reasons.append(f"{field}={count}")

    details = {
        "status": "PASS" if not reasons else "FAIL",
        "acceptance_path": acceptance_path.resolve().as_posix(),
        "verdict": verdict,
        "route_signal": route_signal,
        "blockers_count": counters["blockers"],
        "policy_blockers_count": counters["policy_blockers"],
        "evidence_gaps_count": counters["evidence_gaps"],
        "prohibited_findings_count": counters["prohibited_findings"],
        "reasons": reasons,
    }
    return not reasons, reasons, details


def _validate_phase01_gate(
    *,
    report: dict[str, object],
    path: Path,
    acceptance_report: dict[str, object],
    acceptance_path: Path,
) -> dict[str, object]:
    idempotent = bool(report.get("idempotent_rerun"))
    pass2 = report.get("pass_2")
    pass2_incremental = None
    if isinstance(pass2, dict):
        raw = pass2.get("incremental_rows")
        if isinstance(raw, (int, float)):
            pass2_incremental = int(raw)
    acceptance_pass, acceptance_reasons, acceptance_details = _validate_acceptance_closure(
        acceptance_report=acceptance_report,
        acceptance_path=acceptance_path,
    )
    pass_condition = idempotent and pass2_incremental == 0 and acceptance_pass
    reasons: list[str] = []
    if not idempotent:
        reasons.append("idempotent_rerun is false")
    if pass2_incremental != 0:
        reasons.append("pass_2.incremental_rows is not 0")
    reasons.extend(f"acceptance_closure:{item}" for item in acceptance_reasons)
    return {
        "gate": "A",
        "name": "Foundation",
        "status": "PASS" if pass_condition else "FAIL",
        "report_path": path.as_posix(),
        "acceptance_path": acceptance_path.resolve().as_posix(),
        "acceptance_closure": acceptance_details,
        "reasons": reasons,
    }


def _validate_phase_status_gate(
    *,
    gate: str,
    name: str,
    report: dict[str, object],
    path: Path,
    acceptance_report: dict[str, object],
    acceptance_path: Path,
) -> dict[str, object]:
    status = str(report.get("status", "")).upper()
    publish_decision = str(report.get("publish_decision", "")).lower()
    acceptance_pass, acceptance_reasons, acceptance_details = _validate_acceptance_closure(
        acceptance_report=acceptance_report,
        acceptance_path=acceptance_path,
    )
    pass_condition = status == "PASS" and publish_decision == "publish" and acceptance_pass
    reasons: list[str] = []
    if status != "PASS":
        reasons.append(f"status={status or 'missing'}")
    if publish_decision != "publish":
        reasons.append(f"publish_decision={publish_decision or 'missing'}")
    reasons.extend(f"acceptance_closure:{item}" for item in acceptance_reasons)
    return {
        "gate": gate,
        "name": name,
        "status": "PASS" if pass_condition else "FAIL",
        "report_path": path.as_posix(),
        "acceptance_path": acceptance_path.resolve().as_posix(),
        "acceptance_closure": acceptance_details,
        "reasons": reasons,
    }


def _validate_scheduler_evidence(
    *,
    policy: SchedulerPolicy,
    source_path: Path,
) -> tuple[dict[str, object], list[str]]:
    payload = _json_load(source_path)
    errors: list[str] = []
    provenance_report = _validate_source_provenance(
        payload=payload,
        source_path=source_path,
        evidence_name="scheduler evidence",
        errors=errors,
    )

    environment = str(payload.get("environment", "")).strip()
    if environment != "production":
        errors.append("scheduler evidence environment must be `production`")

    binding = str(payload.get("scheduler_binding", "")).strip()
    if not binding:
        errors.append("scheduler evidence requires non-empty `scheduler_binding`")

    collected_at_raw = str(payload.get("collected_at_utc", "")).strip()
    if not collected_at_raw:
        errors.append("scheduler evidence requires non-empty `collected_at_utc`")
        collected_at = datetime.now(tz=UTC)
    else:
        collected_at = _parse_iso_utc(collected_at_raw)

    jobs_raw = payload.get("jobs")
    if not isinstance(jobs_raw, list):
        errors.append("scheduler evidence `jobs` must be list")
        jobs_raw = []

    jobs_by_id: dict[str, dict[str, object]] = {}
    for row in jobs_raw:
        if not isinstance(row, dict):
            continue
        job_id = str(row.get("job_id", "")).strip()
        if job_id:
            jobs_by_id[job_id] = row

    job_results: list[dict[str, object]] = []
    for job_id in policy.required_job_ids:
        row = jobs_by_id.get(job_id)
        if row is None:
            job_results.append(
                {
                    "job_id": job_id,
                    "status": "FAIL",
                    "reasons": ["missing scheduler status row"],
                }
            )
            continue

        row_reasons: list[str] = []
        tick_status = str(row.get("last_tick_status", "")).upper()
        run_status = str(row.get("last_run_status", "")).upper()
        tick_at_raw = str(row.get("last_tick_utc", "")).strip()
        queued_runs_raw = row.get("queued_runs")
        run_id = str(row.get("last_run_id", "")).strip()

        if tick_status != "SUCCESS":
            row_reasons.append(f"last_tick_status={tick_status or 'missing'}")
        if run_status != "SUCCESS":
            row_reasons.append(f"last_run_status={run_status or 'missing'}")
        if not run_id:
            row_reasons.append("last_run_id is missing")

        if not tick_at_raw:
            row_reasons.append("last_tick_utc is missing")
            tick_age_minutes = None
        else:
            tick_at = _parse_iso_utc(tick_at_raw)
            tick_age_minutes = round((collected_at - tick_at).total_seconds() / 60.0, 3)
            if tick_age_minutes < 0:
                row_reasons.append("last_tick_utc is in the future relative to collected_at_utc")
            if tick_age_minutes > float(policy.max_tick_age_minutes):
                row_reasons.append(
                    f"tick age {tick_age_minutes}m exceeds max_tick_age_minutes={policy.max_tick_age_minutes}"
                )

        if isinstance(queued_runs_raw, bool) or not isinstance(queued_runs_raw, int):
            row_reasons.append("queued_runs must be integer")
            queued_runs = None
        else:
            queued_runs = int(queued_runs_raw)
            if queued_runs > policy.max_queued_runs_per_job:
                row_reasons.append(
                    f"queued_runs={queued_runs} exceeds max_queued_runs_per_job={policy.max_queued_runs_per_job}"
                )

        job_results.append(
            {
                "job_id": job_id,
                "status": "PASS" if not row_reasons else "FAIL",
                "last_tick_status": tick_status,
                "last_run_status": run_status,
                "last_run_id": run_id,
                "tick_age_minutes": tick_age_minutes,
                "queued_runs": queued_runs,
                "reasons": row_reasons,
            }
        )

    failed_jobs = sorted(item["job_id"] for item in job_results if item["status"] == "FAIL")
    if failed_jobs:
        errors.append(f"required scheduler jobs failed: {', '.join(failed_jobs)}")

    report = {
        "status": "PASS" if not errors else "FAIL",
        "source_path": source_path.resolve().as_posix(),
        "scheduler_system": policy.scheduler_system,
        "environment": environment,
        "scheduler_binding": binding,
        "max_tick_age_minutes": policy.max_tick_age_minutes,
        "max_queued_runs_per_job": policy.max_queued_runs_per_job,
        "required_jobs": list(policy.required_job_ids),
        "job_results": job_results,
        "source_provenance": provenance_report,
        "errors": _sample_errors(errors, limit=20),
    }
    real_bindings = [binding] if binding else []
    provenance_channel = str(provenance_report.get("source_channel", "")).strip()
    if provenance_channel:
        real_bindings.append(provenance_channel)
    return report, real_bindings


def _extract_id_set(values: object) -> set[str]:
    if not isinstance(values, list):
        return set()
    identifiers: set[str] = set()
    for item in values:
        if isinstance(item, str) and item.strip():
            identifiers.add(item.strip())
            continue
        if isinstance(item, dict):
            raw = item.get("id")
            if isinstance(raw, str) and raw.strip():
                identifiers.add(raw.strip())
    return identifiers


def _validate_monitoring_evidence(
    *,
    policy: MonitoringPolicy,
    source_path: Path,
) -> tuple[dict[str, object], list[str]]:
    payload = _json_load(source_path)
    errors: list[str] = []
    provenance_report = _validate_source_provenance(
        payload=payload,
        source_path=source_path,
        evidence_name="monitoring evidence",
        errors=errors,
    )

    environment = str(payload.get("environment", "")).strip()
    if environment != "production":
        errors.append("monitoring evidence environment must be `production`")

    bindings_raw = payload.get("monitoring_bindings")
    bindings: list[str] = []
    if isinstance(bindings_raw, list):
        for item in bindings_raw:
            if isinstance(item, str) and item.strip():
                bindings.append(item.strip())
    if not bindings:
        errors.append("monitoring evidence requires non-empty `monitoring_bindings`")

    metrics_raw = payload.get("metrics")
    if not isinstance(metrics_raw, dict):
        errors.append("monitoring evidence `metrics` must be object")
        metrics_raw = {}

    metric_results: list[dict[str, object]] = []
    for rule in policy.required_metrics:
        raw_value = metrics_raw.get(rule.metric_id)
        reasons: list[str] = []
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            reasons.append("metric value missing or non-numeric")
            value = None
        else:
            value = float(raw_value)
            if rule.min_value is not None and value < rule.min_value:
                reasons.append(f"value {value} < min {rule.min_value}")
            if rule.max_value is not None and value > rule.max_value:
                reasons.append(f"value {value} > max {rule.max_value}")

        metric_results.append(
            {
                "metric_id": rule.metric_id,
                "status": "PASS" if not reasons else "FAIL",
                "value": value,
                "min": rule.min_value,
                "max": rule.max_value,
                "reasons": reasons,
            }
        )

    failed_metrics = sorted(item["metric_id"] for item in metric_results if item["status"] == "FAIL")
    if failed_metrics:
        errors.append(f"required monitoring metrics failed: {', '.join(failed_metrics)}")

    dashboards_raw = payload.get("dashboards")
    if not isinstance(dashboards_raw, list):
        errors.append("monitoring evidence `dashboards` must be list")
        dashboards_raw = []
    for index, row in enumerate(dashboards_raw):
        if not isinstance(row, dict):
            continue
        dashboard_id = str(row.get("id", "")).strip() or f"dashboard-{index}"
        url = str(row.get("url", "")).strip()
        if not url:
            errors.append(f"monitoring dashboard `{dashboard_id}` requires non-empty url")
        elif "example" in url.lower():
            errors.append(f"monitoring dashboard `{dashboard_id}` url must not use example placeholder")

    dashboard_ids = _extract_id_set(dashboards_raw)
    missing_dashboards = sorted(set(policy.required_dashboard_ids) - dashboard_ids)
    if missing_dashboards:
        errors.append(f"missing required dashboards: {', '.join(missing_dashboards)}")

    query_ids = _extract_id_set(payload.get("queries"))
    missing_queries = sorted(set(policy.required_query_ids) - query_ids)
    if missing_queries:
        errors.append(f"missing required queries: {', '.join(missing_queries)}")

    report = {
        "status": "PASS" if not errors else "FAIL",
        "source_path": source_path.resolve().as_posix(),
        "environment": environment,
        "monitoring_bindings": sorted(bindings),
        "required_metrics": list(policy.required_metric_ids),
        "metric_results": metric_results,
        "required_dashboards": list(policy.required_dashboard_ids),
        "present_dashboards": sorted(dashboard_ids),
        "required_queries": list(policy.required_query_ids),
        "present_queries": sorted(query_ids),
        "freshness_sla_seconds": policy.freshness_sla_seconds,
        "source_provenance": provenance_report,
        "errors": _sample_errors(errors, limit=20),
    }
    real_bindings = sorted(bindings)
    provenance_channel = str(provenance_report.get("source_channel", "")).strip()
    if provenance_channel:
        real_bindings = sorted(set(real_bindings) | {provenance_channel})
    return report, real_bindings


def _validate_recovery_drill(
    *,
    source_path: Path,
) -> tuple[dict[str, object], list[str]]:
    payload = _json_load(source_path)
    errors: list[str] = []
    provenance_report = _validate_source_provenance(
        payload=payload,
        source_path=source_path,
        evidence_name="recovery evidence",
        errors=errors,
    )

    environment = str(payload.get("environment", "")).strip()
    if environment != "production":
        errors.append("recovery evidence environment must be `production`")

    recovery_binding = str(payload.get("recovery_binding", "")).strip()
    if not recovery_binding:
        errors.append("recovery evidence requires non-empty `recovery_binding`")

    scenario = str(payload.get("scenario", "")).strip()
    if "transient" not in scenario.lower():
        errors.append("recovery scenario must explicitly represent transient failure")

    manual_cleanup_performed = payload.get("manual_cleanup_performed")
    if not isinstance(manual_cleanup_performed, bool):
        errors.append("recovery evidence `manual_cleanup_performed` must be boolean")
        manual_cleanup_performed = True
    if manual_cleanup_performed:
        errors.append("recovery drill required manual cleanup")

    deterministic_replay = payload.get("deterministic_replay")
    if not isinstance(deterministic_replay, bool):
        errors.append("recovery evidence `deterministic_replay` must be boolean")
        deterministic_replay = False
    if not deterministic_replay:
        errors.append("recovery drill did not prove deterministic replay")

    sequence_raw = payload.get("sequence")
    if not isinstance(sequence_raw, list):
        errors.append("recovery evidence `sequence` must be list")
        sequence_raw = []

    stage_results: dict[str, str] = {}
    normalized_sequence: list[dict[str, object]] = []
    for row in sequence_raw:
        if not isinstance(row, dict):
            continue
        stage = str(row.get("stage", "")).strip().lower()
        status = str(row.get("status", "")).strip().upper()
        at_utc = str(row.get("at_utc", "")).strip()
        if stage:
            stage_results[stage] = status
        if at_utc:
            _parse_iso_utc(at_utc)
        normalized_sequence.append(
            {
                "stage": stage,
                "status": status,
                "at_utc": at_utc,
            }
        )

    missing_stages = sorted(stage for stage in MANDATORY_RECOVERY_STAGES if stage not in stage_results)
    if missing_stages:
        errors.append(f"recovery sequence missing mandatory stages: {', '.join(missing_stages)}")

    failed_stages = sorted(stage for stage, status in stage_results.items() if status != "SUCCESS")
    if failed_stages:
        errors.append(f"recovery sequence has non-success stages: {', '.join(failed_stages)}")

    publish_pointer_raw = payload.get("publish_pointer")
    if not isinstance(publish_pointer_raw, dict):
        errors.append("recovery evidence `publish_pointer` must be object")
        publish_pointer_raw = {}

    last_healthy_snapshot = str(publish_pointer_raw.get("last_healthy_snapshot", "")).strip()
    rolled_back_to = str(publish_pointer_raw.get("rolled_back_to", "")).strip()
    restored_after_recovery = str(publish_pointer_raw.get("restored_after_recovery", "")).strip()
    restored_healthy = publish_pointer_raw.get("restored_healthy")

    if not last_healthy_snapshot:
        errors.append("publish_pointer.last_healthy_snapshot is missing")
    if not rolled_back_to:
        errors.append("publish_pointer.rolled_back_to is missing")
    if last_healthy_snapshot and rolled_back_to and rolled_back_to != last_healthy_snapshot:
        errors.append("publish pointer rollback is not aligned with last healthy snapshot")
    if not restored_after_recovery:
        errors.append("publish_pointer.restored_after_recovery is missing")
    if not isinstance(restored_healthy, bool):
        errors.append("publish_pointer.restored_healthy must be boolean")
    elif not restored_healthy:
        errors.append("publish pointer was not restored to healthy state")

    report = {
        "status": "PASS" if not errors else "FAIL",
        "source_path": source_path.resolve().as_posix(),
        "environment": environment,
        "recovery_binding": recovery_binding,
        "scenario": scenario,
        "manual_cleanup_performed": manual_cleanup_performed,
        "deterministic_replay": deterministic_replay,
        "mandatory_stages": list(MANDATORY_RECOVERY_STAGES),
        "sequence": normalized_sequence,
        "publish_pointer": {
            "last_healthy_snapshot": last_healthy_snapshot,
            "rolled_back_to": rolled_back_to,
            "restored_after_recovery": restored_after_recovery,
            "restored_healthy": restored_healthy,
        },
        "source_provenance": provenance_report,
        "errors": _sample_errors(errors, limit=20),
    }
    real_bindings = [recovery_binding] if recovery_binding else []
    provenance_channel = str(provenance_report.get("source_channel", "")).strip()
    if provenance_channel:
        real_bindings.append(provenance_channel)
    return report, real_bindings


def _load_p1_p2_defects(path: Path | None) -> dict[str, object]:
    if path is None:
        return {
            "status": "PASS",
            "source_path": "",
            "unresolved_p1_p2": [],
            "checked_defects": 0,
            "errors": [],
        }

    payload = _json_load(path)
    defects_raw = payload.get("defects")
    if not isinstance(defects_raw, list):
        raise ValueError("defects payload must contain list field `defects`")

    unresolved: list[dict[str, str]] = []
    for index, row in enumerate(defects_raw):
        if not isinstance(row, dict):
            continue
        defect_id = str(row.get("id", "")).strip() or f"defect-{index}"
        severity = str(row.get("severity", "")).strip().upper()
        status = str(row.get("status", "")).strip().lower()
        if severity in P1_P2_LEVELS and status not in RESOLVED_DEFECT_STATUSES:
            unresolved.append({"id": defect_id, "severity": severity, "status": status})

    return {
        "status": "PASS" if not unresolved else "FAIL",
        "source_path": path.resolve().as_posix(),
        "unresolved_p1_p2": unresolved,
        "checked_defects": len(defects_raw),
        "errors": [],
    }


def run_phase04_production_hardening(
    *,
    phase01_report_path: Path,
    phase02_report_path: Path,
    phase03_report_path: Path,
    phase01_acceptance_path: Path,
    phase02_acceptance_path: Path,
    phase03_acceptance_path: Path,
    scheduler_policy_path: Path,
    scheduler_status_source_path: Path,
    monitoring_policy_path: Path,
    monitoring_evidence_source_path: Path,
    recovery_drill_source_path: Path,
    output_dir: Path,
    run_id: str,
    defects_source_path: Path | None = None,
    route_signal: str = "worker:phase-only",
) -> dict[str, object]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    normalized_route_signal = route_signal.strip()
    if not normalized_route_signal:
        raise ValueError("phase-04 route_signal must be non-empty")

    phase01_report = _json_load(phase01_report_path)
    phase02_report = _json_load(phase02_report_path)
    phase03_report = _json_load(phase03_report_path)
    phase01_acceptance = _json_load(phase01_acceptance_path)
    phase02_acceptance = _json_load(phase02_acceptance_path)
    phase03_acceptance = _json_load(phase03_acceptance_path)

    scheduler_policy = load_phase04_scheduler_policy(scheduler_policy_path)
    monitoring_policy = load_phase04_monitoring_policy(monitoring_policy_path)

    gate_a = _validate_phase01_gate(
        report=phase01_report,
        path=phase01_report_path,
        acceptance_report=phase01_acceptance,
        acceptance_path=phase01_acceptance_path,
    )
    gate_b = _validate_phase_status_gate(
        gate="B",
        name="Canonical + Resampling",
        report=phase02_report,
        path=phase02_report_path,
        acceptance_report=phase02_acceptance,
        acceptance_path=phase02_acceptance_path,
    )
    gate_c = _validate_phase_status_gate(
        gate="C",
        name="Reconciliation",
        report=phase03_report,
        path=phase03_report_path,
        acceptance_report=phase03_acceptance,
        acceptance_path=phase03_acceptance_path,
    )

    scheduler_report, scheduler_bindings = _validate_scheduler_evidence(
        policy=scheduler_policy,
        source_path=scheduler_status_source_path,
    )
    monitoring_report, monitoring_bindings = _validate_monitoring_evidence(
        policy=monitoring_policy,
        source_path=monitoring_evidence_source_path,
    )
    recovery_report, recovery_bindings = _validate_recovery_drill(
        source_path=recovery_drill_source_path,
    )
    defects_report = _load_p1_p2_defects(defects_source_path)

    gate_d_reasons: list[str] = []
    if scheduler_report["status"] != "PASS":
        gate_d_reasons.append("scheduler evidence failed")
    if monitoring_report["status"] != "PASS":
        gate_d_reasons.append("monitoring evidence failed")
    if recovery_report["status"] != "PASS":
        gate_d_reasons.append("recovery drill evidence failed")

    gate_d = {
        "gate": "D",
        "name": "Operations Hardening",
        "status": "PASS" if not gate_d_reasons else "FAIL",
        "reasons": gate_d_reasons,
    }

    gate_results = [gate_a, gate_b, gate_c, gate_d]
    failed_gates = [item["gate"] for item in gate_results if item["status"] != "PASS"]
    unresolved_p1_p2 = list(defects_report["unresolved_p1_p2"])

    checklist = [
        {
            "item": "scheduled_runs_configured_and_observable",
            "status": "PASS" if scheduler_report["status"] == "PASS" else "FAIL",
            "evidence_path": scheduler_status_source_path.resolve().as_posix(),
        },
        {
            "item": "recovery_replay_without_manual_cleanup",
            "status": "PASS" if recovery_report["status"] == "PASS" else "FAIL",
            "evidence_path": recovery_drill_source_path.resolve().as_posix(),
        },
        {
            "item": "monitoring_metrics_dashboards_queries_available",
            "status": "PASS" if monitoring_report["status"] == "PASS" else "FAIL",
            "evidence_path": monitoring_evidence_source_path.resolve().as_posix(),
        },
        {
            "item": "all_acceptance_gates_a_to_d_pass",
            "status": "PASS" if not failed_gates else "FAIL",
            "evidence_path": "phase-gate-results",
        },
        {
            "item": "no_unresolved_p1_p2_data_integrity_defects",
            "status": "PASS" if not unresolved_p1_p2 else "FAIL",
            "evidence_path": defects_report["source_path"] or "not-provided",
        },
    ]

    checklist_status = "PASS" if all(item["status"] == "PASS" for item in checklist) else "FAIL"

    allow_release = not failed_gates and not unresolved_p1_p2
    final_decision = (
        "ALLOW_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR"
        if allow_release
        else "DENY_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR"
    )

    scheduler_snapshot_path = output_dir / "scheduler-config-snapshot.json"
    monitoring_snapshot_path = output_dir / "monitoring-dashboard-query-evidence.json"
    recovery_snapshot_path = output_dir / "recovery-drill-artifact.json"
    provenance_path = output_dir / "source-authenticated-provenance.json"
    checklist_path = output_dir / "operations-runbook-checklist.json"
    release_bundle_path = output_dir / "release-decision-bundle.json"

    scheduler_snapshot = {
        "run_id": run_id,
        "generated_at_utc": _utc_now_iso(),
        "scheduler_policy_path": scheduler_policy_path.resolve().as_posix(),
        "scheduler_policy": {
            "version": scheduler_policy.version,
            "scheduler_system": scheduler_policy.scheduler_system,
            "max_tick_age_minutes": scheduler_policy.max_tick_age_minutes,
            "max_queued_runs_per_job": scheduler_policy.max_queued_runs_per_job,
            "required_jobs": list(scheduler_policy.required_job_ids),
        },
        "scheduler_status_validation": scheduler_report,
    }

    monitoring_snapshot = {
        "run_id": run_id,
        "generated_at_utc": _utc_now_iso(),
        "monitoring_policy_path": monitoring_policy_path.resolve().as_posix(),
        "monitoring_policy": {
            "version": monitoring_policy.version,
            "freshness_sla_seconds": monitoring_policy.freshness_sla_seconds,
            "required_metric_ids": list(monitoring_policy.required_metric_ids),
            "required_dashboards": list(monitoring_policy.required_dashboard_ids),
            "required_queries": list(monitoring_policy.required_query_ids),
        },
        "monitoring_validation": monitoring_report,
    }

    recovery_snapshot = {
        "run_id": run_id,
        "generated_at_utc": _utc_now_iso(),
        "recovery_validation": recovery_report,
    }

    provenance_payload = {
        "run_id": run_id,
        "generated_at_utc": _utc_now_iso(),
        "sources": {
            "scheduler": scheduler_report.get("source_provenance", {}),
            "monitoring": monitoring_report.get("source_provenance", {}),
            "recovery": recovery_report.get("source_provenance", {}),
        },
    }

    checklist_payload = {
        "run_id": run_id,
        "status": checklist_status,
        "items": checklist,
    }

    release_bundle = {
        "run_id": run_id,
        "route_signal": normalized_route_signal,
        "proof_class": "live-real",
        "generated_at_utc": _utc_now_iso(),
        "target_release_decision": final_decision,
        "gate_results": gate_results,
        "failed_gates": failed_gates,
        "unresolved_p1_p2_defects": unresolved_p1_p2,
        "checklist_status": checklist_status,
        "phase_reports": {
            "phase01": phase01_report_path.resolve().as_posix(),
            "phase02": phase02_report_path.resolve().as_posix(),
            "phase03": phase03_report_path.resolve().as_posix(),
        },
        "phase_acceptance_artifacts": {
            "phase01": phase01_acceptance_path.resolve().as_posix(),
            "phase02": phase02_acceptance_path.resolve().as_posix(),
            "phase03": phase03_acceptance_path.resolve().as_posix(),
        },
        "operations_evidence": {
            "scheduler_snapshot": scheduler_snapshot_path.as_posix(),
            "monitoring_snapshot": monitoring_snapshot_path.as_posix(),
            "recovery_snapshot": recovery_snapshot_path.as_posix(),
            "source_provenance": provenance_path.as_posix(),
            "checklist": checklist_path.as_posix(),
        },
    }

    _json_write(scheduler_snapshot_path, scheduler_snapshot)
    _json_write(monitoring_snapshot_path, monitoring_snapshot)
    _json_write(recovery_snapshot_path, recovery_snapshot)
    _json_write(provenance_path, provenance_payload)
    _json_write(checklist_path, checklist_payload)
    _json_write(release_bundle_path, release_bundle)

    prior_bindings: set[str] = set()
    for report in (phase01_report, phase02_report, phase03_report):
        prior_bindings |= _collect_real_bindings(report.get("real_bindings"))

    real_bindings = sorted(
        prior_bindings
        | _collect_real_bindings(scheduler_bindings)
        | _collect_real_bindings(monitoring_bindings)
        | _collect_real_bindings(recovery_bindings)
    )

    report = {
        "run_id": run_id,
        "route_signal": normalized_route_signal,
        "proof_class": "live-real",
        "status": "PASS" if allow_release else "BLOCKED",
        "publish_decision": "release_decision_allow" if allow_release else "release_decision_blocked",
        "target_release_decision": final_decision,
        "accepted_state_label": "release_decision" if allow_release else "planned",
        "release_surface_transition": {
            "surface": "operations_recovery_contour",
            "from": "planned",
            "to": "release_decision" if allow_release else "planned",
        },
        "gate_results": gate_results,
        "failed_gates": failed_gates,
        "checklist_status": checklist_status,
        "unresolved_p1_p2_defects": unresolved_p1_p2,
        "defects_report": defects_report,
        "input_paths": {
            "phase01_report": phase01_report_path.resolve().as_posix(),
            "phase02_report": phase02_report_path.resolve().as_posix(),
            "phase03_report": phase03_report_path.resolve().as_posix(),
            "phase01_acceptance": phase01_acceptance_path.resolve().as_posix(),
            "phase02_acceptance": phase02_acceptance_path.resolve().as_posix(),
            "phase03_acceptance": phase03_acceptance_path.resolve().as_posix(),
            "scheduler_policy": scheduler_policy_path.resolve().as_posix(),
            "scheduler_status_source": scheduler_status_source_path.resolve().as_posix(),
            "monitoring_policy": monitoring_policy_path.resolve().as_posix(),
            "monitoring_evidence_source": monitoring_evidence_source_path.resolve().as_posix(),
            "recovery_drill_source": recovery_drill_source_path.resolve().as_posix(),
            "defects_source": defects_source_path.resolve().as_posix() if defects_source_path else "",
        },
        "artifact_paths": {
            "scheduler_snapshot": scheduler_snapshot_path.as_posix(),
            "monitoring_snapshot": monitoring_snapshot_path.as_posix(),
            "recovery_snapshot": recovery_snapshot_path.as_posix(),
            "source_provenance": provenance_path.as_posix(),
            "operations_checklist": checklist_path.as_posix(),
            "release_decision_bundle": release_bundle_path.as_posix(),
        },
        "real_bindings": real_bindings,
    }

    _json_write(output_dir / "phase04-production-hardening-report.json", report)
    return report
