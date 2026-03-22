from __future__ import annotations

import json

from trading_advisor_3000.app.contracts import DecisionPublication, RuntimeSignal, SignalEvent


def _publication_key(publication: DecisionPublication) -> tuple[str, str, str, str, str]:
    return (
        publication.signal_id,
        publication.publication_type.value,
        publication.message_id,
        publication.status.value,
        publication.published_at,
    )


def build_phase9_battle_run_audit(
    *,
    publication_events: list[DecisionPublication],
    signal_events: list[SignalEvent],
    active_signals: list[RuntimeSignal],
    restart_published_delta: int,
    preflight_ready: bool,
    warnings: list[str] | None = None,
    observability_targets: dict[str, str | None] | None = None,
) -> dict[str, object]:
    publication_type_counts: dict[str, int] = {}
    publication_status_counts: dict[str, int] = {}
    publication_keys = [_publication_key(item) for item in publication_events]
    signal_event_type_counts: dict[str, int] = {}
    channels = sorted({publication.channel for publication in publication_events})
    for publication in publication_events:
        publication_type = publication.publication_type.value
        publication_status = publication.status.value
        publication_type_counts[publication_type] = publication_type_counts.get(publication_type, 0) + 1
        publication_status_counts[publication_status] = publication_status_counts.get(publication_status, 0) + 1
    for event in signal_events:
        signal_event_type_counts[event.event_type] = signal_event_type_counts.get(event.event_type, 0) + 1

    duplicate_publication_events = len(publication_keys) - len(set(publication_keys))
    lifecycle_total = sum(publication_type_counts.values())
    return {
        "status": (
            "ok"
            if preflight_ready
            and duplicate_publication_events == 0
            and restart_published_delta == 0
            and not active_signals
            else "degraded"
        ),
        "publication_total": len(publication_events),
        "publication_type_counts": publication_type_counts,
        "publication_status_counts": publication_status_counts,
        "signal_event_type_counts": signal_event_type_counts,
        "duplicate_publication_events": duplicate_publication_events,
        "restart_published_delta": restart_published_delta,
        "active_signal_total": len(active_signals),
        "channels": channels,
        "unique_signal_total": len({publication.signal_id for publication in publication_events}),
        "unique_message_total": len({publication.message_id for publication in publication_events}),
        "lifecycle_total": lifecycle_total,
        "warnings": warnings or [],
        "observability_targets": observability_targets or {},
    }


def export_phase9_battle_run_prometheus(audit: dict[str, object]) -> str:
    publication_type_counts = audit.get("publication_type_counts", {})
    signal_event_type_counts = audit.get("signal_event_type_counts", {})
    lines = [
        "# HELP ta3000_phase9_battle_run_ready Battle-run closure readiness as binary gauge.",
        "# TYPE ta3000_phase9_battle_run_ready gauge",
        f"ta3000_phase9_battle_run_ready {1 if audit.get('status') == 'ok' else 0}",
        "# HELP ta3000_phase9_telegram_publications_total Telegram publication events by type.",
        "# TYPE ta3000_phase9_telegram_publications_total gauge",
    ]
    if isinstance(publication_type_counts, dict):
        for publication_type, total in sorted(publication_type_counts.items()):
            lines.append(f'ta3000_phase9_telegram_publications_total{{publication_type="{publication_type}"}} {int(total)}')
    lines.extend(
        [
            "# HELP ta3000_phase9_signal_events_total Runtime signal events by type.",
            "# TYPE ta3000_phase9_signal_events_total gauge",
        ]
    )
    if isinstance(signal_event_type_counts, dict):
        for event_type, total in sorted(signal_event_type_counts.items()):
            lines.append(f'ta3000_phase9_signal_events_total{{event_type="{event_type}"}} {int(total)}')
    lines.extend(
        [
            "# HELP ta3000_phase9_duplicate_publications_total Duplicate publication events detected in the evidence window.",
            "# TYPE ta3000_phase9_duplicate_publications_total gauge",
            f"ta3000_phase9_duplicate_publications_total {int(audit.get('duplicate_publication_events', 0))}",
            "# HELP ta3000_phase9_restart_published_delta Publications created by replay after restart.",
            "# TYPE ta3000_phase9_restart_published_delta gauge",
            f"ta3000_phase9_restart_published_delta {int(audit.get('restart_published_delta', 0))}",
            "# HELP ta3000_phase9_active_signals_total Active signals left after lifecycle smoke.",
            "# TYPE ta3000_phase9_active_signals_total gauge",
            f"ta3000_phase9_active_signals_total {int(audit.get('active_signal_total', 0))}",
        ]
    )
    return "\n".join(lines) + "\n"


def build_phase9_battle_run_loki_lines(
    *,
    publication_events: list[DecisionPublication],
    signal_events: list[SignalEvent],
    audit: dict[str, object],
) -> list[str]:
    rows: list[str] = []
    for publication in publication_events:
        rows.append(
            json.dumps(
                {
                    "stream": "telegram_publication",
                    "signal_id": publication.signal_id,
                    "channel": publication.channel,
                    "message_id": publication.message_id,
                    "publication_type": publication.publication_type.value,
                    "status": publication.status.value,
                    "published_at": publication.published_at,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    for event in signal_events:
        rows.append(
            json.dumps(
                {
                    "stream": "runtime_signal_event",
                    "signal_id": event.signal_id,
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "reason_code": event.reason_code,
                    "event_ts": event.event_ts,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    rows.append(
        json.dumps(
            {
                "stream": "phase9_battle_run_summary",
                **audit,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return rows
