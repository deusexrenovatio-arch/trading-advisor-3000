from __future__ import annotations

from dataclasses import dataclass

from trading_advisor_3000.product_plane.contracts import BrokerFill, PositionSnapshot


@dataclass(frozen=True)
class SignalOutcome:
    signal_id: str
    strategy_version_id: str
    contract_id: str
    mode: str
    opened_at: str
    closed_at: str
    pnl_r: float
    mfe_r: float
    mae_r: float
    close_reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "strategy_version_id": self.strategy_version_id,
            "contract_id": self.contract_id,
            "mode": self.mode,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "pnl_r": self.pnl_r,
            "mfe_r": self.mfe_r,
            "mae_r": self.mae_r,
            "close_reason": self.close_reason,
        }


def shadow_replay_outcome_store_contract() -> dict[str, dict[str, object]]:
    return {
        "research_forward_observations": {
            "format": "delta",
            "partition_by": ["mode", "result_state"],
            "constraints": ["unique(candidate_id, mode, opened_at)"],
            "columns": {
                "forward_obs_id": "string",
                "candidate_id": "string",
                "mode": "string",
                "opened_at": "timestamp",
                "closed_at": "timestamp",
                "result_state": "string",
                "pnl_r": "double",
                "mfe_r": "double",
                "mae_r": "double",
            },
        },
        "analytics_signal_outcomes": {
            "format": "delta",
            "partition_by": ["strategy_version_id", "contract_id", "mode"],
            "constraints": ["unique(signal_id, mode)"],
            "inputs": ["signal.signal_events", "execution.broker_fills", "execution.positions"],
            "columns": {
                "signal_id": "string",
                "strategy_version_id": "string",
                "contract_id": "string",
                "mode": "string",
                "opened_at": "timestamp",
                "closed_at": "timestamp",
                "pnl_r": "double",
                "mfe_r": "double",
                "mae_r": "double",
                "close_reason": "string",
            },
        },
    }


def _as_fill_map(broker_fills: list[BrokerFill | dict[str, object]]) -> dict[str, BrokerFill]:
    parsed: dict[str, BrokerFill] = {}
    for row in broker_fills:
        fill = row if isinstance(row, BrokerFill) else BrokerFill.from_dict(row)
        parsed[fill.fill_id] = fill
    return parsed


def _as_positions(positions: list[PositionSnapshot | dict[str, object]]) -> list[PositionSnapshot]:
    return [row if isinstance(row, PositionSnapshot) else PositionSnapshot.from_dict(row) for row in positions]


def _risk_unit(entry_ref: float, stop_ref: float) -> float:
    value = abs(entry_ref - stop_ref)
    return value if value > 1e-9 else 1e-9


def _close_reason(events: list[dict[str, object]]) -> tuple[str, str]:
    closing_events = [item for item in events if str(item.get("event_type")) in {"signal_closed", "signal_canceled", "signal_expired"}]
    if not closing_events:
        return "", ""
    last = sorted(closing_events, key=lambda item: str(item.get("event_ts")))[-1]
    return str(last.get("event_ts")), str(last.get("reason_code", "closed"))


def _fill_ids_by_role(events: list[dict[str, object]]) -> dict[str, str]:
    roles: dict[str, str] = {}
    for event in events:
        if str(event.get("event_type")) != "execution_fill":
            continue
        payload = event.get("payload_json")
        if not isinstance(payload, dict):
            continue
        role = str(payload.get("role", ""))
        fill_id = str(payload.get("fill_id", ""))
        if role and fill_id:
            roles[role] = fill_id
    return roles


def _has_flat_position(
    *,
    positions: list[PositionSnapshot],
    contract_id: str,
    closed_at: str,
) -> bool:
    relevant = [
        item
        for item in positions
        if item.contract_id == contract_id and item.as_of_ts >= closed_at
    ]
    if not relevant:
        return False
    latest = sorted(relevant, key=lambda item: item.as_of_ts)[-1]
    return latest.qty == 0


def build_signal_outcomes(
    *,
    signal_events: list[dict[str, object]],
    broker_fills: list[BrokerFill | dict[str, object]],
    positions: list[PositionSnapshot | dict[str, object]],
) -> list[SignalOutcome]:
    events_by_signal: dict[str, list[dict[str, object]]] = {}
    for row in signal_events:
        signal_id = str(row.get("signal_id", ""))
        if not signal_id:
            continue
        events_by_signal.setdefault(signal_id, []).append(row)

    fills = _as_fill_map(broker_fills)
    position_rows = _as_positions(positions)
    outcomes: list[SignalOutcome] = []
    for signal_id, events in sorted(events_by_signal.items()):
        opened_events = [item for item in events if str(item.get("event_type")) == "signal_opened"]
        if not opened_events:
            continue
        opened = sorted(opened_events, key=lambda item: str(item.get("event_ts")))[0]
        opened_payload = opened.get("payload_json")
        if not isinstance(opened_payload, dict):
            continue

        closed_at, close_reason = _close_reason(events)
        if not closed_at:
            continue

        fill_ids = _fill_ids_by_role(events)
        open_fill = fills.get(fill_ids.get("open", ""))
        close_fill = fills.get(fill_ids.get("close", ""))
        if open_fill is None or close_fill is None:
            continue

        side = str(opened_payload.get("side", ""))
        contract_id = str(opened_payload.get("contract_id", ""))
        mode = str(opened_payload.get("mode", ""))
        if not _has_flat_position(positions=position_rows, contract_id=contract_id, closed_at=closed_at):
            continue

        if side == "long":
            pnl_raw = close_fill.price - open_fill.price
        elif side == "short":
            pnl_raw = open_fill.price - close_fill.price
        else:
            continue

        entry_ref = float(opened_payload.get("entry_ref", open_fill.price))
        stop_ref = float(opened_payload.get("stop_ref", entry_ref))
        pnl_r = pnl_raw / _risk_unit(entry_ref, stop_ref)
        outcomes.append(
            SignalOutcome(
                signal_id=signal_id,
                strategy_version_id=str(opened_payload.get("strategy_version_id", "")),
                contract_id=contract_id,
                mode=mode,
                opened_at=str(opened.get("event_ts", "")),
                closed_at=closed_at,
                pnl_r=pnl_r,
                mfe_r=max(0.0, pnl_r),
                mae_r=min(0.0, pnl_r),
                close_reason=close_reason,
            )
        )
    return sorted(outcomes, key=lambda row: (row.opened_at, row.signal_id))
