from __future__ import annotations

from trading_advisor_3000.product_plane.runtime.analytics.review import build_latency_metrics


def _opened_event(signal_id: str, ts: str) -> dict[str, object]:
    return {
        "event_id": f"SEVT-{signal_id}-OPEN",
        "signal_id": signal_id,
        "event_ts": ts,
        "event_type": "signal_opened",
        "reason_code": "candidate_created",
        "payload_json": {
            "strategy_version_id": "ma-cross-v1",
            "contract_id": "BR-6.26",
            "mode": "shadow",
            "side": "long",
            "entry_ref": 100.0,
            "stop_ref": 99.0,
        },
    }


def test_phase5_latency_metrics_happy_path() -> None:
    events = [
        _opened_event("SIG-LAT-1", "2026-03-17T09:30:00Z"),
        {
            "event_id": "SEVT-SIG-LAT-1-ACT",
            "signal_id": "SIG-LAT-1",
            "event_ts": "2026-03-17T09:30:01Z",
            "event_type": "signal_activated",
            "reason_code": "published",
            "payload_json": {},
        },
        {
            "event_id": "SEVT-SIG-LAT-1-FILL-OPEN",
            "signal_id": "SIG-LAT-1",
            "event_ts": "2026-03-17T09:30:02Z",
            "event_type": "execution_fill",
            "reason_code": "open",
            "payload_json": {"fill_id": "FILL-1", "role": "open"},
        },
        {
            "event_id": "SEVT-SIG-LAT-1-CLOSE",
            "signal_id": "SIG-LAT-1",
            "event_ts": "2026-03-17T10:00:00Z",
            "event_type": "signal_closed",
            "reason_code": "closed_profit",
            "payload_json": {},
        },
        {
            "event_id": "SEVT-SIG-LAT-1-FILL-CLOSE",
            "signal_id": "SIG-LAT-1",
            "event_ts": "2026-03-17T10:00:01Z",
            "event_type": "execution_fill",
            "reason_code": "close",
            "payload_json": {"fill_id": "FILL-2", "role": "close"},
        },
    ]

    rows = build_latency_metrics(events)
    assert len(rows) == 1
    row = rows[0]
    assert row.latency_status == "ok"
    assert row.decision_to_activation_ms == 1000.0
    assert row.activation_to_open_fill_ms == 1000.0
    assert row.close_to_fill_ms == 1000.0


def test_phase5_latency_metrics_non_happy_path_statuses() -> None:
    missing_activation_events = [
        _opened_event("SIG-LAT-2", "2026-03-17T09:30:00Z"),
        {
            "event_id": "SEVT-SIG-LAT-2-CLOSE",
            "signal_id": "SIG-LAT-2",
            "event_ts": "2026-03-17T09:31:00Z",
            "event_type": "signal_closed",
            "reason_code": "closed_timeout",
            "payload_json": {},
        },
    ]
    clock_skew_events = [
        _opened_event("SIG-LAT-3", "2026-03-17T09:30:00Z"),
        {
            "event_id": "SEVT-SIG-LAT-3-ACT",
            "signal_id": "SIG-LAT-3",
            "event_ts": "2026-03-17T09:29:59Z",
            "event_type": "signal_activated",
            "reason_code": "published",
            "payload_json": {},
        },
    ]

    rows = build_latency_metrics([*missing_activation_events, *clock_skew_events])
    status_by_signal = {item.signal_id: item.latency_status for item in rows}
    assert status_by_signal["SIG-LAT-2"] == "missing_activation"
    assert status_by_signal["SIG-LAT-3"] == "clock_skew"

