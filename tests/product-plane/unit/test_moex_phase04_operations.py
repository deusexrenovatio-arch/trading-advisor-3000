from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml

from trading_advisor_3000.product_plane.data_plane.moex.phase04_operations import (
    load_phase04_scheduler_policy,
    run_phase04_production_hardening,
)


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
        "collector": "phase04-remediation-worker",
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
            "route_signal": "acceptance:governed-phase-route",
            "blockers": [],
            "policy_blockers": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
        },
    )


def _write_phase_reports(base: Path) -> tuple[Path, Path, Path, Path, Path, Path]:
    phase01 = _write_json(
        base / "raw-ingest-summary-report.json",
        {
            "run_id": "phase01",
            "idempotent_rerun": True,
            "pass_2": {"incremental_rows": 0},
            "real_bindings": ["moex_iss"],
        },
    )
    phase02 = _write_json(
        base / "canonical-refresh-report.json",
        {
            "run_id": "phase02",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["moex_iss"],
        },
    )
    phase03 = _write_json(
        base / "reconciliation-report.json",
        {
            "run_id": "phase03",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["finam_archive"],
        },
    )
    phase01_acceptance = _write_acceptance_report(base / "phase01-acceptance.json")
    phase02_acceptance = _write_acceptance_report(base / "phase02-acceptance.json")
    phase03_acceptance = _write_acceptance_report(base / "phase03-acceptance.json")
    return phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance


def _scheduler_policy_payload(*, include_qc: bool = True) -> dict[str, object]:
    jobs: list[dict[str, object]] = [
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
    ]
    if include_qc:
        jobs.append(
            {
                "job_id": "qc_gate_and_publish",
                "required": True,
                "cron": "12-59/15 * * * *",
                "retry": {"max_attempts": 2, "backoff_seconds": [30, 90]},
            }
        )
    return {
        "version": 1,
        "scheduler_system": "dagster",
        "max_tick_age_minutes": 180,
        "max_queued_runs_per_job": 0,
        "jobs": jobs,
    }


def _monitoring_policy_payload() -> dict[str, object]:
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


def _write_scheduler_evidence(path: Path) -> Path:
    raw_payload = {
        "environment": "production",
        "collected_at_utc": "2026-04-02T10:00:00Z",
        "scheduler_binding": "dagster://prod/moex",
        "jobs": [
            {
                "job_id": "moex_backfill_bootstrap",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T09:40:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "run-bootstrap-1",
                "queued_runs": 0,
            },
            {
                "job_id": "moex_incremental_ingest",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T09:55:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "run-incremental-1",
                "queued_runs": 0,
            },
            {
                "job_id": "cross_source_reconciliation",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T09:50:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "run-recon-1",
                "queued_runs": 0,
            },
            {
                "job_id": "qc_gate_and_publish",
                "last_tick_status": "SUCCESS",
                "last_tick_utc": "2026-04-02T09:52:00Z",
                "last_run_status": "SUCCESS",
                "last_run_id": "run-qc-1",
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
            collection_id="phase04-scheduler-collection-1",
            exported_at_utc="2026-04-02T10:00:30Z",
            collected_at_utc="2026-04-02T10:01:00Z",
        ),
    )


def _write_monitoring_evidence(path: Path, *, omit_freshness: bool = False) -> Path:
    metrics = {
        "pipeline_run_latency_seconds": 220.0,
        "pipeline_run_failures_total": 0,
        "qc_gate_fail_total": 0,
        "reconciliation_drift_bps_p95": 0.7,
        "publish_freshness_lag_seconds": 120.0,
    }
    if omit_freshness:
        metrics.pop("publish_freshness_lag_seconds", None)
    raw_payload = {
        "environment": "production",
        "monitoring_bindings": [
            "grafana://prod/moex",
            "prometheus://prod/moex",
        ],
        "metrics": metrics,
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
            collection_id="phase04-monitoring-collection-1",
            exported_at_utc="2026-04-02T10:02:00Z",
            collected_at_utc="2026-04-02T10:02:30Z",
        ),
    )


def _write_recovery_evidence(path: Path, *, manual_cleanup: bool = False) -> Path:
    raw_payload = {
        "environment": "production",
        "recovery_binding": "orchestrator://prod/moex",
        "scenario": "transient_source_api_failure",
        "manual_cleanup_performed": manual_cleanup,
        "deterministic_replay": True,
        "sequence": [
            {"stage": "fail", "status": "SUCCESS", "at_utc": "2026-04-02T09:30:00Z"},
            {"stage": "retry", "status": "SUCCESS", "at_utc": "2026-04-02T09:31:00Z"},
            {"stage": "replay", "status": "SUCCESS", "at_utc": "2026-04-02T09:32:00Z"},
            {"stage": "recover", "status": "SUCCESS", "at_utc": "2026-04-02T09:33:00Z"},
        ],
        "publish_pointer": {
            "last_healthy_snapshot": "snapshot-20260402T092500Z",
            "rolled_back_to": "snapshot-20260402T092500Z",
            "restored_after_recovery": "snapshot-20260402T093300Z",
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
            collection_id="phase04-recovery-collection-1",
            exported_at_utc="2026-04-02T10:03:00Z",
            collected_at_utc="2026-04-02T10:03:30Z",
        ),
    )


def test_phase04_scheduler_policy_rejects_missing_mandatory_qc_job(tmp_path: Path) -> None:
    policy_path = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=False))

    with pytest.raises(ValueError, match="missing mandatory required jobs"):
        load_phase04_scheduler_policy(policy_path)


def test_phase04_blocks_when_monitoring_freshness_metric_missing(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=True)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-missing-freshness",
    )

    assert report["status"] == "BLOCKED"
    assert report["target_release_decision"] == "DENY_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR"
    assert "D" in report["failed_gates"]


def test_phase04_blocks_when_recovery_drill_requires_manual_cleanup(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=True)

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-manual-cleanup",
    )

    assert report["status"] == "BLOCKED"
    assert report["target_release_decision"] == "DENY_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR"
    assert "D" in report["failed_gates"]


def test_phase04_blocks_when_monitoring_dashboards_use_example_urls(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    payload = json.loads(monitoring_evidence.read_text(encoding="utf-8"))
    payload["dashboards"][0]["url"] = "https://grafana.example/moex-overview"
    monitoring_evidence.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-placeholder-dashboard",
    )

    assert report["status"] == "BLOCKED"
    assert "D" in report["failed_gates"]


def test_phase04_blocks_when_scheduler_source_provenance_missing(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    scheduler_payload = json.loads(scheduler_evidence.read_text(encoding="utf-8"))
    scheduler_payload.pop("source_provenance", None)
    scheduler_evidence.write_text(json.dumps(scheduler_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-missing-source-provenance",
    )

    assert report["status"] == "BLOCKED"
    assert "D" in report["failed_gates"]
    scheduler_snapshot = json.loads(Path(str(report["artifact_paths"]["scheduler_snapshot"])).read_text(encoding="utf-8"))
    scheduler_errors = scheduler_snapshot["scheduler_status_validation"]["errors"]
    assert any("source_provenance" in str(item) for item in scheduler_errors)


def test_phase04_blocks_when_monitoring_immutable_export_path_missing(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    monitoring_payload = json.loads(monitoring_evidence.read_text(encoding="utf-8"))
    source_provenance = monitoring_payload.get("source_provenance")
    assert isinstance(source_provenance, dict)
    source_provenance.pop("immutable_export_path", None)
    monitoring_evidence.write_text(json.dumps(monitoring_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-missing-immutable-export-path",
    )

    assert report["status"] == "BLOCKED"
    assert "D" in report["failed_gates"]
    monitoring_snapshot = json.loads(Path(str(report["artifact_paths"]["monitoring_snapshot"])).read_text(encoding="utf-8"))
    monitoring_errors = monitoring_snapshot["monitoring_validation"]["errors"]
    assert any("immutable_export_path is missing" in str(item) for item in monitoring_errors)


def test_phase04_blocks_when_recovery_immutable_export_hash_mismatch(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    recovery_payload = json.loads(recovery_evidence.read_text(encoding="utf-8"))
    source_provenance = recovery_payload.get("source_provenance")
    assert isinstance(source_provenance, dict)
    source_provenance["immutable_export_sha256"] = "0" * 64
    recovery_evidence.write_text(json.dumps(recovery_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-immutable-export-sha-mismatch",
    )

    assert report["status"] == "BLOCKED"
    assert "D" in report["failed_gates"]
    recovery_snapshot = json.loads(Path(str(report["artifact_paths"]["recovery_snapshot"])).read_text(encoding="utf-8"))
    recovery_errors = recovery_snapshot["recovery_validation"]["errors"]
    assert any("does not match immutable export content hash" in str(item) for item in recovery_errors)


def test_phase04_filters_template_real_bindings_from_prior_phases(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    phase01 = _write_json(
        reports_dir / "raw-ingest-summary-report.json",
        {
            "run_id": "phase01",
            "idempotent_rerun": True,
            "pass_2": {"incremental_rows": 0},
            "real_bindings": [
                "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/<SECID>/candles.json",
                "moex_iss",
            ],
        },
    )
    phase02 = _write_json(
        reports_dir / "canonical-refresh-report.json",
        {
            "run_id": "phase02",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["moex_iss"],
        },
    )
    phase03 = _write_json(
        reports_dir / "reconciliation-report.json",
        {
            "run_id": "phase03",
            "status": "PASS",
            "publish_decision": "publish",
            "real_bindings": ["finam_archive"],
        },
    )
    phase01_acceptance = _write_acceptance_report(reports_dir / "phase01-acceptance.json")
    phase02_acceptance = _write_acceptance_report(reports_dir / "phase02-acceptance.json")
    phase03_acceptance = _write_acceptance_report(reports_dir / "phase03-acceptance.json")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-template-bindings-filter",
    )

    assert report["status"] == "PASS"
    assert "dagster://prod/moex" in report["real_bindings"]
    assert not any("<SECID>" in binding for binding in report["real_bindings"])


def test_phase04_blocks_when_prior_phase_acceptance_not_closed(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(
        tmp_path / "reports"
    )
    _write_acceptance_report(phase02_acceptance, verdict="BLOCKED")

    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-prior-acceptance-blocked",
    )

    assert report["status"] == "BLOCKED"
    assert "B" in report["failed_gates"]
    gate_b = next(item for item in report["gate_results"] if item["gate"] == "B")
    reasons = gate_b["reasons"]
    assert "acceptance_closure:verdict=BLOCKED" in reasons


def test_phase04_allows_release_decision_on_full_live_real_bundle(tmp_path: Path) -> None:
    phase01, phase02, phase03, phase01_acceptance, phase02_acceptance, phase03_acceptance = _write_phase_reports(tmp_path / "reports")
    scheduler_policy = _write_yaml(tmp_path / "scheduler.yaml", _scheduler_policy_payload(include_qc=True))
    monitoring_policy = _write_yaml(tmp_path / "monitoring.yaml", _monitoring_policy_payload())
    scheduler_evidence = _write_scheduler_evidence(tmp_path / "inputs" / "scheduler.json")
    monitoring_evidence = _write_monitoring_evidence(tmp_path / "inputs" / "monitoring.json", omit_freshness=False)
    recovery_evidence = _write_recovery_evidence(tmp_path / "inputs" / "recovery.json", manual_cleanup=False)

    report = run_phase04_production_hardening(
        phase01_report_path=phase01,
        phase02_report_path=phase02,
        phase03_report_path=phase03,
        phase01_acceptance_path=phase01_acceptance,
        phase02_acceptance_path=phase02_acceptance,
        phase03_acceptance_path=phase03_acceptance,
        scheduler_policy_path=scheduler_policy,
        scheduler_status_source_path=scheduler_evidence,
        monitoring_policy_path=monitoring_policy,
        monitoring_evidence_source_path=monitoring_evidence,
        recovery_drill_source_path=recovery_evidence,
        output_dir=tmp_path / "phase04",
        run_id="phase04-pass",
    )

    assert report["status"] == "PASS"
    assert report["target_release_decision"] == "ALLOW_MOEX_SPARK_DELTA_PRODUCTION_CONTOUR"
    assert report["proof_class"] == "live-real"
    assert report["failed_gates"] == []

    artifact_paths = report["artifact_paths"]
    assert Path(str(artifact_paths["scheduler_snapshot"])).exists()
    assert Path(str(artifact_paths["monitoring_snapshot"])).exists()
    assert Path(str(artifact_paths["recovery_snapshot"])).exists()
    assert Path(str(artifact_paths["source_provenance"])).exists()
    assert Path(str(artifact_paths["operations_checklist"])).exists()
    assert Path(str(artifact_paths["release_decision_bundle"])).exists()


