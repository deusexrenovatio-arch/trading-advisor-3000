from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

from trading_advisor_3000.product_plane.data_plane.moex import run_moex_operational_hardening


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _with_source_provenance(
    *,
    evidence_path: Path,
    raw_payload: dict[str, object],
    source_system: str,
    source_channel: str,
    collection_id: str,
    exported_at_utc: str,
    collected_at_utc: str,
) -> dict[str, object]:
    immutable_export_path = evidence_path.with_name(f"{evidence_path.stem}.immutable-export.json")
    _write_json(immutable_export_path, raw_payload)
    payload = dict(raw_payload)
    payload["source_provenance"] = {
        "source_system": source_system,
        "source_channel": source_channel,
        "collector": "operational-hardening-remediation-worker",
        "collection_id": collection_id,
        "exported_at_utc": exported_at_utc,
        "collected_at_utc": collected_at_utc,
        "immutable_export_path": immutable_export_path.name,
        "immutable_export_sha256": _sha256_path(immutable_export_path),
    }
    return payload


def _write_acceptance_report(path: Path, *, verdict: str = "PASS") -> Path:
    return _write_json(
        path,
        {
            "verdict": verdict,
            "route_signal": "acceptance:governed-capability-route",
            "blockers": [],
            "policy_blockers": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
        },
    )


def _phase_reports(base: Path) -> tuple[Path, Path, Path, Path, Path, Path]:
    phase01 = _write_json(
        base / "raw-ingest-summary-report.json",
        {
            "run_id": "raw_ingest",
            "idempotent_rerun": True,
            "pass_2": {"incremental_rows": 0},
            "real_bindings": ["moex_iss"],
        },
    )
    phase02 = _write_json(
        base / "canonical-refresh-report.json",
        {
            "run_id": "canonicalization",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["moex_iss"],
        },
    )
    phase03 = _write_json(
        base / "reconciliation-report.json",
        {
            "run_id": "reconciliation",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["finam_archive", "finam://archive/moex"],
        },
    )
    raw_ingest_acceptance = _write_acceptance_report(base / "raw-ingest-acceptance.json")
    canonicalization_acceptance = _write_acceptance_report(base / "canonicalization-acceptance.json")
    reconciliation_acceptance = _write_acceptance_report(base / "reconciliation-acceptance.json")
    return raw_ingest, canonicalization, reconciliation, raw_ingest_acceptance, canonicalization_acceptance, reconciliation_acceptance


def _scheduler_policy() -> dict[str, object]:
    return {
        "version": 1,
        "scheduler_system": "dagster",
        "max_tick_age_minutes": 180,
        "max_queued_runs_per_job": 0,
        "jobs": [
            {
                "job_id": "moex_backfill_bootstrap",
                "required": True,
                "cron": "0 21 * * *",
                "retry": {"max_attempts": 3, "backoff_seconds": [30, 120, 300]},
            },
            {
                "job_id": "moex_incremental_ingest",
                "required": True,
                "cron": "*/15 * * * *",
                "retry": {"max_attempts": 3, "backoff_seconds": [15, 60, 180]},
            },
            {
                "job_id": "cross_source_reconciliation",
                "required": True,
                "cron": "10-59/15 * * * *",
                "retry": {"max_attempts": 3, "backoff_seconds": [30, 120, 300]},
            },
            {
                "job_id": "qc_gate_and_publish",
                "required": True,
                "cron": "12-59/15 * * * *",
                "retry": {"max_attempts": 2, "backoff_seconds": [30, 90]},
            },
        ],
    }


def _monitoring_policy() -> dict[str, object]:
    return {
        "version": 1,
        "freshness_sla_seconds": 900,
        "required_metrics": {
            "pipeline_run_latency_seconds": {"min": 0, "max": 900},
            "pipeline_run_failures_total": {"min": 0, "max": 0},
            "qc_gate_fail_total": {"min": 0, "max": 0},
            "reconciliation_drift_bps_p95": {"min": 0, "max": 40},
            "publish_freshness_lag_seconds": {"min": 0, "max": 900},
        },
        "required_dashboards": [
            "moex-operations-overview",
            "moex-qc-reconciliation",
            "moex-freshness-sla",
        ],
        "required_queries": [
            "pipeline_run_latency_seconds",
            "pipeline_run_failures_total",
            "qc_gate_fail_total",
            "reconciliation_drift_bps_p95",
            "publish_freshness_lag_seconds",
        ],
    }


def _scheduler_evidence(path: Path) -> Path:
    raw_payload = {
        "environment": "production",
        "collected_at_utc": "2026-04-02T12:00:00Z",
        "scheduler_binding": "dagster://prod/moex",
        "jobs": [
            {
                "job_id": "moex_backfill_bootstrap",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T11:30:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "bootstrap-1",
                "queued_runs": 0,
            },
            {
                "job_id": "moex_incremental_ingest",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T11:55:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "incremental-1",
                "queued_runs": 0,
            },
            {
                "job_id": "cross_source_reconciliation",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T11:50:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "recon-1",
                "queued_runs": 0,
            },
            {
                "job_id": "qc_gate_and_publish",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T11:52:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "qc-1",
                "queued_runs": 0,
            },
        ],
    }
    return _write_json(
        path,
        _with_source_provenance(
            evidence_path=path,
            raw_payload=raw_payload,
            source_system="dagster",
            source_channel="dagster://prod/moex/graphql",
            collection_id="operational-hardening-scheduler-collection-1",
            exported_at_utc="2026-04-02T12:00:30Z",
            collected_at_utc="2026-04-02T12:01:00Z",
        ),
    )


def _monitoring_evidence(path: Path) -> Path:
    raw_payload = {
        "environment": "production",
        "monitoring_bindings": ["grafana://prod/moex", "prometheus://prod/moex"],
        "metrics": {
            "pipeline_run_latency_seconds": 180.0,
            "pipeline_run_failures_total": 0,
            "qc_gate_fail_total": 0,
            "reconciliation_drift_bps_p95": 0.8,
            "publish_freshness_lag_seconds": 95.0,
        },
        "dashboards": [
            {"id": "moex-operations-overview", "url": "https://grafana.prod.trading-advisor.local/d/moex-ops"},
            {"id": "moex-qc-reconciliation", "url": "https://grafana.prod.trading-advisor.local/d/moex-qc"},
            {"id": "moex-freshness-sla", "url": "https://grafana.prod.trading-advisor.local/d/moex-freshness"},
        ],
        "queries": [
            {"id": "pipeline_run_latency_seconds", "expression": "histogram_quantile(...)"},
            {"id": "pipeline_run_failures_total", "expression": "sum(...)"},
            {"id": "qc_gate_fail_total", "expression": "sum(...)"},
            {"id": "reconciliation_drift_bps_p95", "expression": "quantile(...)"},
            {"id": "publish_freshness_lag_seconds", "expression": "max(...)"},
        ],
    }
    return _write_json(
        path,
        _with_source_provenance(
            evidence_path=path,
            raw_payload=raw_payload,
            source_system="grafana-prometheus",
            source_channel="grafana://prod/moex/export-api",
            collection_id="operational-hardening-monitoring-collection-1",
            exported_at_utc="2026-04-02T12:02:00Z",
            collected_at_utc="2026-04-02T12:02:30Z",
        ),
    )


def _recovery_evidence(path: Path) -> Path:
    raw_payload = {
        "environment": "production",
        "recovery_binding": "orchestrator://prod/moex",
        "scenario": "transient_source_api_failure",
        "manual_cleanup_performed": False,
        "deterministic_replay": True,
        "sequence": [
            {"stage": "fail", "status": "SUCCESS", "at_utc": "2026-04-02T11:00:00Z"},
            {"stage": "retry", "status": "SUCCESS", "at_utc": "2026-04-02T11:01:00Z"},
            {"stage": "replay", "status": "SUCCESS", "at_utc": "2026-04-02T11:02:00Z"},
            {"stage": "recover", "status": "SUCCESS", "at_utc": "2026-04-02T11:03:00Z"},
        ],
        "publish_pointer": {
            "last_healthy_snapshot": "snapshot-20260402T105500Z",
            "rolled_back_to": "snapshot-20260402T105500Z",
            "restored_after_recovery": "snapshot-20260402T110300Z",
            "restored_healthy": True,
        },
    }
    return _write_json(
        path,
        _with_source_provenance(
            evidence_path=path,
            raw_payload=raw_payload,
            source_system="orchestrator",
            source_channel="orchestrator://prod/moex/recovery-audit",
            collection_id="operational-hardening-recovery-collection-1",
            exported_at_utc="2026-04-02T12:03:00Z",
            collected_at_utc="2026-04-02T12:03:30Z",
        ),
    )


def test_operational_hardening_generates_release_bundle_and_checklist(tmp_path: Path) -> None:
    raw_ingest, canonicalization, reconciliation, raw_ingest_acceptance, canonicalization_acceptance, reconciliation_acceptance = _phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "config" / "scheduler.yaml", _scheduler_policy())
    monitoring_policy = _write_yaml(tmp_path / "config" / "monitoring.yaml", _monitoring_policy())
    scheduler_evidence = _scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _monitoring_evidence(tmp_path / "inputs" / "monitoring.json")
    recovery_evidence = _recovery_evidence(tmp_path / "inputs" / "recovery.json")

    report = run_moex_operational_hardening(
        raw_ingest_report_path=raw_ingest,
        canonicalization_report_path=canonicalization,
        reconciliation_report_path=reconciliation,
        raw_ingest_acceptance_path=raw_ingest_acceptance,
        canonicalization_acceptance_path=canonicalization_acceptance,
        reconciliation_acceptance_path=reconciliation_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        defects_source_path=None,
        output_dir=tmp_path / "operational_hardening",
        run_id="operational-hardening-int-pass",
    )

    assert report["status"] == "PASS"
    assert report["target_release_decision"] == "ALLOW_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR"
    assert report["checklist_status"] == "PASS"
    assert report["failed_gates"] == []
    assert report["real_bindings"]

    release_bundle = Path(str(report["artifact_paths"]["release_decision_bundle"]))
    checklist = Path(str(report["artifact_paths"]["operations_checklist"]))
    scheduler_snapshot = Path(str(report["artifact_paths"]["scheduler_snapshot"]))
    monitoring_snapshot = Path(str(report["artifact_paths"]["monitoring_snapshot"]))
    recovery_snapshot = Path(str(report["artifact_paths"]["recovery_snapshot"]))
    source_provenance = Path(str(report["artifact_paths"]["source_provenance"]))
    assert release_bundle.exists()
    assert checklist.exists()
    assert scheduler_snapshot.exists()
    assert monitoring_snapshot.exists()
    assert recovery_snapshot.exists()
    assert source_provenance.exists()

    bundle_payload = json.loads(release_bundle.read_text(encoding="utf-8"))
    assert bundle_payload["proof_class"] == "live-real"
    assert bundle_payload["failed_gates"] == []


def test_operational_hardening_denies_release_when_open_p1_exists(tmp_path: Path) -> None:
    raw_ingest, canonicalization, reconciliation, raw_ingest_acceptance, canonicalization_acceptance, reconciliation_acceptance = _phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "config" / "scheduler.yaml", _scheduler_policy())
    monitoring_policy = _write_yaml(tmp_path / "config" / "monitoring.yaml", _monitoring_policy())
    scheduler_evidence = _scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _monitoring_evidence(tmp_path / "inputs" / "monitoring.json")
    recovery_evidence = _recovery_evidence(tmp_path / "inputs" / "recovery.json")
    defects = _write_json(
        tmp_path / "inputs" / "defects.json",
        {
            "defects": [
                {"id": "D-100", "severity": "P1", "status": "open"},
                {"id": "D-101", "severity": "P3", "status": "open"},
            ]
        },
    )

    report = run_moex_operational_hardening(
        raw_ingest_report_path=raw_ingest,
        canonicalization_report_path=canonicalization,
        reconciliation_report_path=reconciliation,
        raw_ingest_acceptance_path=raw_ingest_acceptance,
        canonicalization_acceptance_path=canonicalization_acceptance,
        reconciliation_acceptance_path=reconciliation_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        defects_source_path=defects,
        output_dir=tmp_path / "operational_hardening",
        run_id="operational-hardening-int-defect-block",
    )

    assert report["status"] == "BLOCKED"
    assert report["target_release_decision"] == "DENY_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR"
    assert report["unresolved_p1_p2_defects"] == [{"id": "D-100", "severity": "P1", "status": "open"}]


def test_operational_hardening_integration_blocks_when_monitoring_provenance_hash_mismatch(tmp_path: Path) -> None:
    raw_ingest, canonicalization, reconciliation, raw_ingest_acceptance, canonicalization_acceptance, reconciliation_acceptance = _phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "config" / "scheduler.yaml", _scheduler_policy())
    monitoring_policy = _write_yaml(tmp_path / "config" / "monitoring.yaml", _monitoring_policy())
    scheduler_evidence = _scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _monitoring_evidence(tmp_path / "inputs" / "monitoring.json")
    recovery_evidence = _recovery_evidence(tmp_path / "inputs" / "recovery.json")

    monitoring_payload = json.loads(monitoring_evidence.read_text(encoding="utf-8"))
    source_provenance = monitoring_payload.get("source_provenance")
    assert isinstance(source_provenance, dict)
    source_provenance["immutable_export_sha256"] = "f" * 64
    monitoring_evidence.write_text(json.dumps(monitoring_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = run_moex_operational_hardening(
        raw_ingest_report_path=raw_ingest,
        canonicalization_report_path=canonicalization,
        reconciliation_report_path=reconciliation,
        raw_ingest_acceptance_path=raw_ingest_acceptance,
        canonicalization_acceptance_path=canonicalization_acceptance,
        reconciliation_acceptance_path=reconciliation_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        defects_source_path=None,
        output_dir=tmp_path / "operational_hardening",
        run_id="operational-hardening-int-monitoring-hash-mismatch",
    )

    assert report["status"] == "BLOCKED"
    assert "D" in report["failed_gates"]
    monitoring_snapshot = Path(str(report["artifact_paths"]["monitoring_snapshot"]))
    snapshot_payload = json.loads(monitoring_snapshot.read_text(encoding="utf-8"))
    errors = snapshot_payload["monitoring_validation"]["errors"]
    assert any("does not match immutable export content hash" in str(item) for item in errors)


def test_operational_hardening_integration_filters_template_real_bindings(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    phase01 = _write_json(
        reports_dir / "raw-ingest-summary-report.json",
        {
            "run_id": "raw_ingest",
            "idempotent_rerun": True,
            "pass_2": {"incremental_rows": 0},
            "real_bindings": [
                "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/<SECID>/candleborders.json",
                "moex_iss",
            ],
        },
    )
    phase02 = _write_json(
        reports_dir / "canonical-refresh-report.json",
        {
            "run_id": "canonicalization",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["moex_iss"],
        },
    )
    phase03 = _write_json(
        reports_dir / "reconciliation-report.json",
        {
            "run_id": "reconciliation",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["finam_archive", "finam://archive/moex"],
        },
    )
    raw_ingest_acceptance = _write_acceptance_report(reports_dir / "raw-ingest-acceptance.json")
    canonicalization_acceptance = _write_acceptance_report(reports_dir / "canonicalization-acceptance.json")
    reconciliation_acceptance = _write_acceptance_report(reports_dir / "reconciliation-acceptance.json")
    scheduler_policy = _write_yaml(tmp_path / "config" / "scheduler.yaml", _scheduler_policy())
    monitoring_policy = _write_yaml(tmp_path / "config" / "monitoring.yaml", _monitoring_policy())
    scheduler_evidence = _scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _monitoring_evidence(tmp_path / "inputs" / "monitoring.json")
    recovery_evidence = _recovery_evidence(tmp_path / "inputs" / "recovery.json")

    report = run_moex_operational_hardening(
        raw_ingest_report_path=raw_ingest,
        canonicalization_report_path=canonicalization,
        reconciliation_report_path=reconciliation,
        raw_ingest_acceptance_path=raw_ingest_acceptance,
        canonicalization_acceptance_path=canonicalization_acceptance,
        reconciliation_acceptance_path=reconciliation_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        defects_source_path=None,
        output_dir=tmp_path / "operational_hardening",
        run_id="operational-hardening-int-template-binding-filter",
    )

    assert report["status"] == "PASS"
    assert not any("<SECID>" in binding for binding in report["real_bindings"])


def test_operational_hardening_integration_blocks_without_prior_acceptance_closure(tmp_path: Path) -> None:
    raw_ingest, canonicalization, reconciliation, raw_ingest_acceptance, canonicalization_acceptance, reconciliation_acceptance = _phase_reports(
        tmp_path / "reports"
    )
    _write_acceptance_report(reconciliation_acceptance, verdict="BLOCKED")
    scheduler_policy = _write_yaml(tmp_path / "config" / "scheduler.yaml", _scheduler_policy())
    monitoring_policy = _write_yaml(tmp_path / "config" / "monitoring.yaml", _monitoring_policy())
    scheduler_evidence = _scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _monitoring_evidence(tmp_path / "inputs" / "monitoring.json")
    recovery_evidence = _recovery_evidence(tmp_path / "inputs" / "recovery.json")

    report = run_moex_operational_hardening(
        raw_ingest_report_path=raw_ingest,
        canonicalization_report_path=canonicalization,
        reconciliation_report_path=reconciliation,
        raw_ingest_acceptance_path=raw_ingest_acceptance,
        canonicalization_acceptance_path=canonicalization_acceptance,
        reconciliation_acceptance_path=reconciliation_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        defects_source_path=None,
        output_dir=tmp_path / "operational_hardening",
        run_id="operational-hardening-int-prior-acceptance-blocked",
    )

    assert report["status"] == "BLOCKED"
    assert "C" in report["failed_gates"]
    gate_c = next(item for item in report["gate_results"] if item["gate"] == "C")
    reasons = gate_c["reasons"]
    assert "acceptance_closure:verdict=BLOCKED" in reasons

