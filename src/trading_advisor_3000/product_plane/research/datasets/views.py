from __future__ import annotations

import math
from dataclasses import dataclass

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.canonical import RollMapEntry, SessionCalendarEntry

from .manifest import ResearchDatasetManifest


@dataclass(frozen=True)
class ResearchBarView:
    dataset_version: str
    contract_id: str
    instrument_id: str
    timeframe: str
    ts: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: int
    session_date: str
    session_open_ts: str
    session_close_ts: str
    active_contract_id: str
    ret_1: float | None
    log_ret_1: float | None
    true_range: float
    hl_range: float
    oc_range: float
    bar_index: int
    slice_role: str

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_version": self.dataset_version,
            "contract_id": self.contract_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "ts": self.ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "session_date": self.session_date,
            "session_open_ts": self.session_open_ts,
            "session_close_ts": self.session_close_ts,
            "active_contract_id": self.active_contract_id,
            "ret_1": self.ret_1,
            "log_ret_1": self.log_ret_1,
            "true_range": self.true_range,
            "hl_range": self.hl_range,
            "oc_range": self.oc_range,
            "bar_index": self.bar_index,
            "slice_role": self.slice_role,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ResearchBarView":
        return cls(
            dataset_version=str(payload["dataset_version"]),
            contract_id=str(payload["contract_id"]),
            instrument_id=str(payload["instrument_id"]),
            timeframe=str(payload["timeframe"]),
            ts=str(payload["ts"]),
            open=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=int(payload["volume"]),
            open_interest=int(payload["open_interest"]),
            session_date=str(payload["session_date"]),
            session_open_ts=str(payload["session_open_ts"]),
            session_close_ts=str(payload["session_close_ts"]),
            active_contract_id=str(payload["active_contract_id"]),
            ret_1=None if payload.get("ret_1") is None else float(payload["ret_1"]),
            log_ret_1=None if payload.get("log_ret_1") is None else float(payload["log_ret_1"]),
            true_range=float(payload["true_range"]),
            hl_range=float(payload["hl_range"]),
            oc_range=float(payload["oc_range"]),
            bar_index=int(payload["bar_index"]),
            slice_role=str(payload["slice_role"]),
        )


def _session_calendar_index(
    session_calendar: list[SessionCalendarEntry],
) -> dict[tuple[str, str, str], SessionCalendarEntry]:
    return {
        (entry.instrument_id, entry.timeframe, entry.session_date): entry
        for entry in session_calendar
    }


def _roll_map_index(roll_map: list[RollMapEntry]) -> dict[tuple[str, str], RollMapEntry]:
    return {
        (entry.instrument_id, entry.session_date): entry
        for entry in roll_map
    }


def build_research_bar_views(
    *,
    dataset_version: str,
    bars: list[CanonicalBar],
    session_calendar: list[SessionCalendarEntry],
    roll_map: list[RollMapEntry],
    manifest: ResearchDatasetManifest,
) -> list[ResearchBarView]:
    filtered_bars = [row for row in bars if row.timeframe.value in manifest.timeframes]
    if manifest.end_ts is not None:
        filtered_bars = [row for row in filtered_bars if row.ts <= manifest.end_ts]

    session_index = _session_calendar_index(session_calendar)
    roll_index = _roll_map_index(roll_map)
    grouped: dict[tuple[str, str], list[CanonicalBar]] = {}

    for bar in sorted(filtered_bars, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe.value, item.ts)):
        session_date = bar.ts[:10]
        if manifest.series_mode == "continuous_front":
            active = roll_index.get((bar.instrument_id, session_date))
            if active is None or active.active_contract_id != bar.contract_id:
                continue
            key = (bar.instrument_id, bar.timeframe.value)
        else:
            key = (bar.contract_id, bar.timeframe.value)
        grouped.setdefault(key, []).append(bar)

    selected_views: list[ResearchBarView] = []
    for _, series in sorted(grouped.items()):
        analysis_indices = [
            idx
            for idx, row in enumerate(series)
            if (manifest.start_ts is None or row.ts >= manifest.start_ts)
            and (manifest.end_ts is None or row.ts <= manifest.end_ts)
        ]
        if not analysis_indices:
            continue

        analysis_start = analysis_indices[0]
        analysis_stop = analysis_indices[-1] + 1
        selection_start = max(0, analysis_start - manifest.warmup_bars)
        selection_series = series[selection_start:analysis_stop]
        prev_close: float | None = None

        for index, bar in enumerate(selection_series):
            session_date = bar.ts[:10]
            session_entry = session_index.get((bar.instrument_id, bar.timeframe.value, session_date))
            if session_entry is None:
                raise ValueError(
                    "missing canonical session calendar entry for "
                    f"{bar.instrument_id}|{bar.timeframe.value}|{session_date}"
                )
            active_roll = roll_index.get((bar.instrument_id, session_date))
            active_contract_id = active_roll.active_contract_id if active_roll is not None else bar.contract_id
            ret_1 = None if prev_close in {None, 0.0} else (bar.close / prev_close) - 1.0
            log_ret_1 = None if ret_1 is None else math.log(bar.close / prev_close)
            hl_range = bar.high - bar.low
            oc_range = bar.close - bar.open
            true_range = hl_range if prev_close is None else max(hl_range, abs(bar.high - prev_close), abs(bar.low - prev_close))
            slice_role = "analysis" if bar.ts >= series[analysis_start].ts else "warmup"
            selected_views.append(
                ResearchBarView(
                    dataset_version=dataset_version,
                    contract_id=bar.contract_id,
                    instrument_id=bar.instrument_id,
                    timeframe=bar.timeframe.value,
                    ts=bar.ts,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    open_interest=bar.open_interest,
                    session_date=session_date,
                    session_open_ts=session_entry.session_open_ts,
                    session_close_ts=session_entry.session_close_ts,
                    active_contract_id=active_contract_id,
                    ret_1=ret_1,
                    log_ret_1=log_ret_1,
                    true_range=true_range,
                    hl_range=hl_range,
                    oc_range=oc_range,
                    bar_index=index,
                    slice_role=slice_role,
                )
            )
            prev_close = bar.close

    return sorted(selected_views, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe, item.ts))
