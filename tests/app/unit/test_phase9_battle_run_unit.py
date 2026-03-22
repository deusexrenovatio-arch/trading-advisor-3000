from __future__ import annotations

from trading_advisor_3000.app.phase9 import build_phase9_battle_run_report, render_phase9_evidence_markdown


def test_phase9_battle_run_report_is_ready_for_review_when_ws_a_to_ws_c_are_green() -> None:
    report = build_phase9_battle_run_report(
        bootstrap_report={
            "dataset_version": "phase9-dataset-v1",
            "materialization_mode": "manifest_only_jsonl_samples",
        },
        strategy_report={
            "strategy_spec": {"strategy_version_id": "phase9-moex-breakout-v1"},
            "pilot_readiness": {"status": "ready_for_shadow_pilot"},
            "live_smoke": {"status": "ok"},
            "replay_summary": {"runtime_signal_ids": ["SIG-1", "SIG-2"]},
        },
        runtime_report={
            "publisher_channel": "@ta3000_advisory",
            "publication_transport": "bot-api",
            "source_signal_ids": ["SIG-1", "SIG-2"],
            "ready_for_battle_run": True,
            "publication_audit": {"lifecycle_total": 10},
            "output_paths": {
                "observability_prometheus_metrics": "artifacts/metrics.txt",
                "observability_loki_events": "artifacts/events.jsonl",
            },
        },
        output_paths={
            "bootstrap_report": "artifacts/bootstrap.json",
            "strategy_report": "artifacts/strategy.json",
            "runtime_report": "artifacts/runtime.json",
        },
        git_ref="HEAD",
        publication_posture="advisory",
        phase8_proving=None,
        sidecar_preflight={"status": "ok"},
    )
    markdown = render_phase9_evidence_markdown(report)

    assert report["phase9a_status"] == "ready_for_review"
    assert any("Phase 8 proving is not attached" in item for item in report["warnings"])
    assert any("advisory publication posture" in item for item in report["warnings"])
    assert report["signal_continuity"]["status"] == "matched"
    assert "phase9a integration status: `ready_for_review`" in markdown
    assert "publisher channel: `@ta3000_advisory`" in markdown
    assert "signal continuity: `matched`" in markdown


def test_phase9_battle_run_report_blocks_when_live_smoke_or_runtime_are_not_ready() -> None:
    report = build_phase9_battle_run_report(
        bootstrap_report={
            "dataset_version": "phase9-dataset-v1",
            "materialization_mode": "manifest_only_jsonl_samples",
        },
        strategy_report={
            "strategy_spec": {"strategy_version_id": "phase9-moex-breakout-v1"},
            "pilot_readiness": {"status": "blocked"},
            "live_smoke": {"status": "degraded"},
            "replay_summary": {"runtime_signal_ids": ["SIG-1", "SIG-2"]},
        },
        runtime_report={
            "ready_for_battle_run": False,
            "source_signal_ids": ["SIG-9"],
            "publication_audit": {"lifecycle_total": 4},
            "output_paths": {},
        },
        output_paths={},
        git_ref="HEAD",
        publication_posture="shadow",
        phase8_proving={"status": "ok", "report_path": "artifacts/phase8.json"},
        sidecar_preflight=None,
    )

    assert report["phase9a_status"] == "blocked"
    assert any("QUIK live smoke is not green" in item for item in report["warnings"])
    assert any("Telegram/PostgreSQL battle-run smoke is not ready" in item for item in report["warnings"])
    assert any("diverged from strategy-produced signal ids" in item for item in report["warnings"])
