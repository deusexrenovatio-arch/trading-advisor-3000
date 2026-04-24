from __future__ import annotations

import json

from trading_advisor_3000.product_plane.runtime.analytics.review import (
    build_loki_event_lines,
    build_review_observability_report,
    export_prometheus_metrics,
)


def test_observability_export_prometheus_and_loki_exports_are_built() -> None:
    outcomes = [
        {
            "signal_id": "SIG-OBS-1",
            "strategy_version_id": "ma-cross-v1",
            "contract_id": "BR-6.26",
            "mode": "shadow",
            "opened_at": "2026-03-17T09:30:00Z",
            "closed_at": "2026-03-17T10:00:00Z",
            "pnl_r": 1.5,
            "mfe_r": 2.0,
            "mae_r": -0.4,
            "close_reason": "closed_profit",
        }
    ]
    signal_events = [
        {
            "event_id": "SEVT-1",
            "signal_id": "SIG-OBS-1",
            "event_ts": "2026-03-17T09:30:00Z",
            "event_type": "signal_opened",
            "reason_code": "candidate_created",
            "payload_json": {
                "strategy_version_id": "ma-cross-v1",
                "contract_id": "BR-6.26",
                "mode": "shadow",
            },
        },
        {
            "event_id": "SEVT-2",
            "signal_id": "SIG-OBS-1",
            "event_ts": "2026-03-17T09:30:01Z",
            "event_type": "signal_activated",
            "reason_code": "published",
            "payload_json": {},
        },
        {
            "event_id": "SEVT-3",
            "signal_id": "SIG-OBS-1",
            "event_ts": "2026-03-17T10:00:00Z",
            "event_type": "signal_closed",
            "reason_code": "closed_profit",
            "payload_json": {},
        },
    ]

    report = build_review_observability_report(outcomes=outcomes, signal_events=signal_events)
    prometheus = export_prometheus_metrics(report)
    loki_lines = build_loki_event_lines(report)

    assert "ta3000_strategy_signals_total" in prometheus
    assert "ta3000_latency_status_total" in prometheus
    assert loki_lines
    parsed = [json.loads(item) for item in loki_lines]
    assert {row["stream"] for row in parsed} >= {"latency", "strategy_dashboard"}

