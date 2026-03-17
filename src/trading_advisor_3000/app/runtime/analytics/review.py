from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from .outcomes import SignalOutcome


def _parse_utc(ts: str) -> datetime | None:
    if not isinstance(ts, str) or not ts.strip():
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _latency_ms(start_ts: str, end_ts: str) -> float | None:
    start = _parse_utc(start_ts)
    end = _parse_utc(end_ts)
    if start is None or end is None:
        return None
    return (end - start).total_seconds() * 1000.0


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    sorted_values = sorted(float(item) for item in values)
    rank = max(0.0, min(1.0, p)) * (len(sorted_values) - 1)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def _max_drawdown_r(pnl_series: list[float]) -> float:
    if not pnl_series:
        return 0.0
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnl_series:
        running += pnl
        peak = max(peak, running)
        max_drawdown = max(max_drawdown, peak - running)
    return max_drawdown


def _instrument_id(contract_id: str) -> str:
    if not contract_id:
        return ""
    return contract_id.split("-", maxsplit=1)[0]


def _as_outcomes(rows: list[SignalOutcome | dict[str, object]]) -> list[SignalOutcome]:
    outcomes: list[SignalOutcome] = []
    for row in rows:
        if isinstance(row, SignalOutcome):
            outcomes.append(row)
            continue
        outcomes.append(
            SignalOutcome(
                signal_id=str(row.get("signal_id", "")),
                strategy_version_id=str(row.get("strategy_version_id", "")),
                contract_id=str(row.get("contract_id", "")),
                mode=str(row.get("mode", "")),
                opened_at=str(row.get("opened_at", "")),
                closed_at=str(row.get("closed_at", "")),
                pnl_r=float(row.get("pnl_r", 0.0)),
                mfe_r=float(row.get("mfe_r", 0.0)),
                mae_r=float(row.get("mae_r", 0.0)),
                close_reason=str(row.get("close_reason", "")),
            )
        )
    return sorted(outcomes, key=lambda item: (item.closed_at, item.signal_id))


@dataclass(frozen=True)
class StrategyMetricsRow:
    trade_date: str
    strategy_version_id: str
    mode: str
    signals_count: int
    wins_count: int
    losses_count: int
    win_rate: float
    avg_r: float
    sum_r: float
    mfe_avg_r: float
    mae_avg_r: float
    best_r: float
    worst_r: float
    max_dd_r: float

    def to_dict(self) -> dict[str, object]:
        return {
            "trade_date": self.trade_date,
            "strategy_version_id": self.strategy_version_id,
            "mode": self.mode,
            "signals_count": self.signals_count,
            "wins_count": self.wins_count,
            "losses_count": self.losses_count,
            "win_rate": self.win_rate,
            "avg_r": self.avg_r,
            "sum_r": self.sum_r,
            "mfe_avg_r": self.mfe_avg_r,
            "mae_avg_r": self.mae_avg_r,
            "best_r": self.best_r,
            "worst_r": self.worst_r,
            "max_dd_r": self.max_dd_r,
        }


@dataclass(frozen=True)
class InstrumentMetricsRow:
    trade_date: str
    instrument_id: str
    contract_id: str
    mode: str
    signals_count: int
    wins_count: int
    losses_count: int
    win_rate: float
    avg_r: float
    sum_r: float
    mfe_avg_r: float
    mae_avg_r: float
    best_r: float
    worst_r: float
    max_dd_r: float

    def to_dict(self) -> dict[str, object]:
        return {
            "trade_date": self.trade_date,
            "instrument_id": self.instrument_id,
            "contract_id": self.contract_id,
            "mode": self.mode,
            "signals_count": self.signals_count,
            "wins_count": self.wins_count,
            "losses_count": self.losses_count,
            "win_rate": self.win_rate,
            "avg_r": self.avg_r,
            "sum_r": self.sum_r,
            "mfe_avg_r": self.mfe_avg_r,
            "mae_avg_r": self.mae_avg_r,
            "best_r": self.best_r,
            "worst_r": self.worst_r,
            "max_dd_r": self.max_dd_r,
        }


@dataclass(frozen=True)
class LatencyMetricRow:
    signal_id: str
    strategy_version_id: str
    contract_id: str
    mode: str
    opened_at: str
    activated_at: str
    open_fill_at: str
    closed_at: str
    close_fill_at: str
    decision_to_activation_ms: float | None
    activation_to_open_fill_ms: float | None
    decision_to_close_ms: float | None
    close_to_fill_ms: float | None
    latency_status: str

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "strategy_version_id": self.strategy_version_id,
            "contract_id": self.contract_id,
            "mode": self.mode,
            "opened_at": self.opened_at,
            "activated_at": self.activated_at,
            "open_fill_at": self.open_fill_at,
            "closed_at": self.closed_at,
            "close_fill_at": self.close_fill_at,
            "decision_to_activation_ms": self.decision_to_activation_ms,
            "activation_to_open_fill_ms": self.activation_to_open_fill_ms,
            "decision_to_close_ms": self.decision_to_close_ms,
            "close_to_fill_ms": self.close_to_fill_ms,
            "latency_status": self.latency_status,
        }


@dataclass(frozen=True)
class Phase5ReviewReport:
    strategy_dashboard: list[StrategyMetricsRow]
    instrument_dashboard: list[InstrumentMetricsRow]
    latency_metrics: list[LatencyMetricRow]
    summary: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "strategy_dashboard": [item.to_dict() for item in self.strategy_dashboard],
            "instrument_dashboard": [item.to_dict() for item in self.instrument_dashboard],
            "latency_metrics": [item.to_dict() for item in self.latency_metrics],
            "summary": self.summary,
        }


def phase5_review_store_contract() -> dict[str, dict[str, object]]:
    return {
        "analytics_strategy_metrics_daily": {
            "format": "delta",
            "partition_by": ["trade_date", "strategy_version_id", "mode"],
            "inputs": ["analytics.signal_outcomes"],
            "columns": {
                "trade_date": "date",
                "strategy_version_id": "string",
                "mode": "string",
                "signals_count": "int",
                "wins_count": "int",
                "losses_count": "int",
                "win_rate": "double",
                "avg_r": "double",
                "sum_r": "double",
                "mfe_avg_r": "double",
                "mae_avg_r": "double",
                "best_r": "double",
                "worst_r": "double",
                "max_dd_r": "double",
            },
        },
        "analytics_instrument_metrics_daily": {
            "format": "delta",
            "partition_by": ["trade_date", "instrument_id", "mode"],
            "inputs": ["analytics.signal_outcomes"],
            "columns": {
                "trade_date": "date",
                "instrument_id": "string",
                "contract_id": "string",
                "mode": "string",
                "signals_count": "int",
                "wins_count": "int",
                "losses_count": "int",
                "win_rate": "double",
                "avg_r": "double",
                "sum_r": "double",
                "mfe_avg_r": "double",
                "mae_avg_r": "double",
                "best_r": "double",
                "worst_r": "double",
                "max_dd_r": "double",
            },
        },
        "observability_latency_metrics": {
            "format": "delta",
            "partition_by": ["mode", "latency_status"],
            "inputs": ["signal.signal_events"],
            "columns": {
                "signal_id": "string",
                "strategy_version_id": "string",
                "contract_id": "string",
                "mode": "string",
                "opened_at": "timestamp",
                "activated_at": "timestamp",
                "open_fill_at": "timestamp",
                "closed_at": "timestamp",
                "close_fill_at": "timestamp",
                "decision_to_activation_ms": "double",
                "activation_to_open_fill_ms": "double",
                "decision_to_close_ms": "double",
                "close_to_fill_ms": "double",
                "latency_status": "string",
            },
        },
    }


def _aggregate_r_metrics(items: list[SignalOutcome]) -> dict[str, float]:
    pnl = [item.pnl_r for item in items]
    mfe = [item.mfe_r for item in items]
    mae = [item.mae_r for item in items]
    total = len(items)
    wins = sum(1 for item in items if item.pnl_r > 0)
    losses = sum(1 for item in items if item.pnl_r < 0)
    return {
        "signals_count": float(total),
        "wins_count": float(wins),
        "losses_count": float(losses),
        "win_rate": float(wins / total) if total else 0.0,
        "avg_r": float(sum(pnl) / total) if total else 0.0,
        "sum_r": float(sum(pnl)),
        "mfe_avg_r": float(sum(mfe) / total) if total else 0.0,
        "mae_avg_r": float(sum(mae) / total) if total else 0.0,
        "best_r": float(max(pnl)) if pnl else 0.0,
        "worst_r": float(min(pnl)) if pnl else 0.0,
        "max_dd_r": float(_max_drawdown_r(pnl)),
    }


def build_strategy_metrics_dashboard(outcomes: list[SignalOutcome | dict[str, object]]) -> list[StrategyMetricsRow]:
    rows = _as_outcomes(outcomes)
    grouped: dict[tuple[str, str, str], list[SignalOutcome]] = {}
    for item in rows:
        trade_date = item.closed_at[:10]
        grouped.setdefault((trade_date, item.strategy_version_id, item.mode), []).append(item)

    dashboard: list[StrategyMetricsRow] = []
    for key in sorted(grouped):
        trade_date, strategy_version_id, mode = key
        metrics = _aggregate_r_metrics(grouped[key])
        dashboard.append(
            StrategyMetricsRow(
                trade_date=trade_date,
                strategy_version_id=strategy_version_id,
                mode=mode,
                signals_count=int(metrics["signals_count"]),
                wins_count=int(metrics["wins_count"]),
                losses_count=int(metrics["losses_count"]),
                win_rate=metrics["win_rate"],
                avg_r=metrics["avg_r"],
                sum_r=metrics["sum_r"],
                mfe_avg_r=metrics["mfe_avg_r"],
                mae_avg_r=metrics["mae_avg_r"],
                best_r=metrics["best_r"],
                worst_r=metrics["worst_r"],
                max_dd_r=metrics["max_dd_r"],
            )
        )
    return dashboard


def build_instrument_metrics_dashboard(outcomes: list[SignalOutcome | dict[str, object]]) -> list[InstrumentMetricsRow]:
    rows = _as_outcomes(outcomes)
    grouped: dict[tuple[str, str, str, str], list[SignalOutcome]] = {}
    for item in rows:
        trade_date = item.closed_at[:10]
        instrument_id = _instrument_id(item.contract_id)
        grouped.setdefault((trade_date, instrument_id, item.contract_id, item.mode), []).append(item)

    dashboard: list[InstrumentMetricsRow] = []
    for key in sorted(grouped):
        trade_date, instrument_id, contract_id, mode = key
        metrics = _aggregate_r_metrics(grouped[key])
        dashboard.append(
            InstrumentMetricsRow(
                trade_date=trade_date,
                instrument_id=instrument_id,
                contract_id=contract_id,
                mode=mode,
                signals_count=int(metrics["signals_count"]),
                wins_count=int(metrics["wins_count"]),
                losses_count=int(metrics["losses_count"]),
                win_rate=metrics["win_rate"],
                avg_r=metrics["avg_r"],
                sum_r=metrics["sum_r"],
                mfe_avg_r=metrics["mfe_avg_r"],
                mae_avg_r=metrics["mae_avg_r"],
                best_r=metrics["best_r"],
                worst_r=metrics["worst_r"],
                max_dd_r=metrics["max_dd_r"],
            )
        )
    return dashboard


def build_latency_metrics(signal_events: list[dict[str, object]]) -> list[LatencyMetricRow]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for event in signal_events:
        signal_id = str(event.get("signal_id", "")).strip()
        if not signal_id:
            continue
        grouped.setdefault(signal_id, []).append(event)

    rows: list[LatencyMetricRow] = []
    for signal_id in sorted(grouped):
        events = grouped[signal_id]
        opened_events = [item for item in events if str(item.get("event_type")) == "signal_opened"]
        if not opened_events:
            rows.append(
                LatencyMetricRow(
                    signal_id=signal_id,
                    strategy_version_id="",
                    contract_id="",
                    mode="",
                    opened_at="",
                    activated_at="",
                    open_fill_at="",
                    closed_at="",
                    close_fill_at="",
                    decision_to_activation_ms=None,
                    activation_to_open_fill_ms=None,
                    decision_to_close_ms=None,
                    close_to_fill_ms=None,
                    latency_status="missing_open_event",
                )
            )
            continue

        opened = sorted(opened_events, key=lambda item: str(item.get("event_ts", "")))[0]
        opened_payload = opened.get("payload_json")
        payload = opened_payload if isinstance(opened_payload, dict) else {}
        opened_at = str(opened.get("event_ts", ""))

        activated_events = [item for item in events if str(item.get("event_type")) == "signal_activated"]
        activated_at = (
            str(sorted(activated_events, key=lambda item: str(item.get("event_ts", "")))[0].get("event_ts", ""))
            if activated_events
            else ""
        )

        fill_events = [item for item in events if str(item.get("event_type")) == "execution_fill"]
        open_fills = []
        close_fills = []
        for fill_event in fill_events:
            role = str(fill_event.get("reason_code", ""))
            payload_json = fill_event.get("payload_json")
            if isinstance(payload_json, dict) and payload_json.get("role"):
                role = str(payload_json.get("role"))
            if role == "open":
                open_fills.append(fill_event)
            if role == "close":
                close_fills.append(fill_event)
        open_fill_at = (
            str(sorted(open_fills, key=lambda item: str(item.get("event_ts", "")))[0].get("event_ts", ""))
            if open_fills
            else ""
        )
        close_fill_at = (
            str(sorted(close_fills, key=lambda item: str(item.get("event_ts", "")))[0].get("event_ts", ""))
            if close_fills
            else ""
        )

        closed_events = [
            item for item in events if str(item.get("event_type")) in {"signal_closed", "signal_canceled", "signal_expired"}
        ]
        closed_at = (
            str(sorted(closed_events, key=lambda item: str(item.get("event_ts", "")))[-1].get("event_ts", ""))
            if closed_events
            else ""
        )

        decision_to_activation_ms = _latency_ms(opened_at, activated_at)
        activation_to_open_fill_ms = _latency_ms(activated_at, open_fill_at)
        decision_to_close_ms = _latency_ms(opened_at, closed_at)
        close_to_fill_ms = _latency_ms(closed_at, close_fill_at)

        durations = [
            value
            for value in [
                decision_to_activation_ms,
                activation_to_open_fill_ms,
                decision_to_close_ms,
                close_to_fill_ms,
            ]
            if value is not None
        ]
        if any(value < 0 for value in durations):
            status = "clock_skew"
        elif not activated_at:
            status = "missing_activation"
        elif not open_fill_at:
            status = "missing_open_fill"
        elif not closed_at:
            status = "missing_close_event"
        elif not close_fill_at:
            status = "missing_close_fill"
        else:
            status = "ok"

        rows.append(
            LatencyMetricRow(
                signal_id=signal_id,
                strategy_version_id=str(payload.get("strategy_version_id", "")),
                contract_id=str(payload.get("contract_id", "")),
                mode=str(payload.get("mode", "")),
                opened_at=opened_at,
                activated_at=activated_at,
                open_fill_at=open_fill_at,
                closed_at=closed_at,
                close_fill_at=close_fill_at,
                decision_to_activation_ms=decision_to_activation_ms,
                activation_to_open_fill_ms=activation_to_open_fill_ms,
                decision_to_close_ms=decision_to_close_ms,
                close_to_fill_ms=close_to_fill_ms,
                latency_status=status,
            )
        )
    return rows


def build_phase5_review_report(
    *,
    outcomes: list[SignalOutcome | dict[str, object]],
    signal_events: list[dict[str, object]],
) -> Phase5ReviewReport:
    strategy_dashboard = build_strategy_metrics_dashboard(outcomes)
    instrument_dashboard = build_instrument_metrics_dashboard(outcomes)
    latency_metrics = build_latency_metrics(signal_events)

    activation_latencies = [
        item.decision_to_activation_ms for item in latency_metrics if item.decision_to_activation_ms is not None
    ]
    close_latencies = [item.decision_to_close_ms for item in latency_metrics if item.decision_to_close_ms is not None]
    status_counts: dict[str, int] = {}
    for item in latency_metrics:
        status_counts[item.latency_status] = status_counts.get(item.latency_status, 0) + 1

    summary = {
        "strategy_rows": len(strategy_dashboard),
        "instrument_rows": len(instrument_dashboard),
        "latency_rows": len(latency_metrics),
        "latency_status_counts": status_counts,
        "decision_to_activation_p50_ms": _percentile(activation_latencies, 0.50),
        "decision_to_activation_p95_ms": _percentile(activation_latencies, 0.95),
        "decision_to_close_p50_ms": _percentile(close_latencies, 0.50),
        "decision_to_close_p95_ms": _percentile(close_latencies, 0.95),
    }
    return Phase5ReviewReport(
        strategy_dashboard=strategy_dashboard,
        instrument_dashboard=instrument_dashboard,
        latency_metrics=latency_metrics,
        summary=summary,
    )


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\"", "\\\"")


def export_prometheus_metrics(report: Phase5ReviewReport) -> str:
    lines: list[str] = []

    lines.append("# HELP ta3000_strategy_signals_total Signals count by strategy/day/mode.")
    lines.append("# TYPE ta3000_strategy_signals_total gauge")
    for row in report.strategy_dashboard:
        labels = (
            f'strategy_version_id="{_escape_label(row.strategy_version_id)}",'
            f'trade_date="{_escape_label(row.trade_date)}",mode="{_escape_label(row.mode)}"'
        )
        lines.append(f"ta3000_strategy_signals_total{{{labels}}} {row.signals_count}")
        lines.append(f"ta3000_strategy_win_rate{{{labels}}} {row.win_rate:.10f}")
        lines.append(f"ta3000_strategy_sum_r{{{labels}}} {row.sum_r:.10f}")
        lines.append(f"ta3000_strategy_max_drawdown_r{{{labels}}} {row.max_dd_r:.10f}")

    lines.append("# HELP ta3000_instrument_signals_total Signals count by instrument/day/mode.")
    lines.append("# TYPE ta3000_instrument_signals_total gauge")
    for row in report.instrument_dashboard:
        labels = (
            f'instrument_id="{_escape_label(row.instrument_id)}",'
            f'contract_id="{_escape_label(row.contract_id)}",'
            f'trade_date="{_escape_label(row.trade_date)}",mode="{_escape_label(row.mode)}"'
        )
        lines.append(f"ta3000_instrument_signals_total{{{labels}}} {row.signals_count}")
        lines.append(f"ta3000_instrument_win_rate{{{labels}}} {row.win_rate:.10f}")
        lines.append(f"ta3000_instrument_sum_r{{{labels}}} {row.sum_r:.10f}")

    lines.append("# HELP ta3000_latency_status_total Latency row count by status.")
    lines.append("# TYPE ta3000_latency_status_total gauge")
    status_counts = report.summary.get("latency_status_counts", {})
    if isinstance(status_counts, dict):
        for status, count in sorted(status_counts.items()):
            lines.append(f'ta3000_latency_status_total{{status="{_escape_label(str(status))}"}} {int(count)}')

    activation_p50 = report.summary.get("decision_to_activation_p50_ms")
    activation_p95 = report.summary.get("decision_to_activation_p95_ms")
    close_p50 = report.summary.get("decision_to_close_p50_ms")
    close_p95 = report.summary.get("decision_to_close_p95_ms")
    lines.append("# HELP ta3000_latency_quantile_ms Latency quantiles in milliseconds.")
    lines.append("# TYPE ta3000_latency_quantile_ms gauge")
    quantile_rows = [
        ("decision_to_activation", "0.50", activation_p50),
        ("decision_to_activation", "0.95", activation_p95),
        ("decision_to_close", "0.50", close_p50),
        ("decision_to_close", "0.95", close_p95),
    ]
    for metric, quantile, value in quantile_rows:
        if value is None:
            continue
        lines.append(
            f'ta3000_latency_quantile_ms{{metric="{metric}",quantile="{quantile}"}} {float(value):.6f}'
        )

    return "\n".join(lines) + "\n"


def build_loki_event_lines(report: Phase5ReviewReport) -> list[str]:
    lines: list[str] = []
    for row in report.latency_metrics:
        payload = {
            "stream": "latency",
            "signal_id": row.signal_id,
            "strategy_version_id": row.strategy_version_id,
            "contract_id": row.contract_id,
            "mode": row.mode,
            "status": row.latency_status,
            "decision_to_activation_ms": row.decision_to_activation_ms,
            "decision_to_close_ms": row.decision_to_close_ms,
            "timestamp": row.closed_at or row.activated_at or row.opened_at,
        }
        lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))

    for row in report.strategy_dashboard:
        payload = {
            "stream": "strategy_dashboard",
            "trade_date": row.trade_date,
            "strategy_version_id": row.strategy_version_id,
            "mode": row.mode,
            "signals_count": row.signals_count,
            "win_rate": row.win_rate,
            "sum_r": row.sum_r,
            "max_dd_r": row.max_dd_r,
        }
        lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return lines
