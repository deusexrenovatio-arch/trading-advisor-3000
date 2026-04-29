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
    series_id: str = ""
    series_mode: str = "contract"
    roll_epoch: int = 0
    roll_event_id: str | None = None
    is_roll_bar: bool = False
    is_first_bar_after_roll: bool = False
    bars_since_roll: int = 0
    price_space: str = "native"
    native_open: float | None = None
    native_high: float | None = None
    native_low: float | None = None
    native_close: float | None = None
    continuous_open: float | None = None
    continuous_high: float | None = None
    continuous_low: float | None = None
    continuous_close: float | None = None
    execution_open: float | None = None
    execution_high: float | None = None
    execution_low: float | None = None
    execution_close: float | None = None
    previous_contract_id: str | None = None
    candidate_contract_id: str | None = None
    adjustment_mode: str = ""
    cumulative_additive_offset: float = 0.0
    ratio_factor: float | None = None

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
            "series_id": self.series_id or self.contract_id,
            "series_mode": self.series_mode,
            "roll_epoch": self.roll_epoch,
            "roll_event_id": self.roll_event_id,
            "is_roll_bar": self.is_roll_bar,
            "is_first_bar_after_roll": self.is_first_bar_after_roll,
            "bars_since_roll": self.bars_since_roll,
            "price_space": self.price_space,
            "native_open": self.native_open if self.native_open is not None else self.open,
            "native_high": self.native_high if self.native_high is not None else self.high,
            "native_low": self.native_low if self.native_low is not None else self.low,
            "native_close": self.native_close if self.native_close is not None else self.close,
            "continuous_open": self.continuous_open if self.continuous_open is not None else self.open,
            "continuous_high": self.continuous_high if self.continuous_high is not None else self.high,
            "continuous_low": self.continuous_low if self.continuous_low is not None else self.low,
            "continuous_close": self.continuous_close if self.continuous_close is not None else self.close,
            "execution_open": self.execution_open if self.execution_open is not None else (self.native_open if self.native_open is not None else self.open),
            "execution_high": self.execution_high if self.execution_high is not None else (self.native_high if self.native_high is not None else self.high),
            "execution_low": self.execution_low if self.execution_low is not None else (self.native_low if self.native_low is not None else self.low),
            "execution_close": self.execution_close if self.execution_close is not None else (self.native_close if self.native_close is not None else self.close),
            "previous_contract_id": self.previous_contract_id,
            "candidate_contract_id": self.candidate_contract_id,
            "adjustment_mode": self.adjustment_mode,
            "cumulative_additive_offset": self.cumulative_additive_offset,
            "ratio_factor": self.ratio_factor,
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
            series_id=str(payload.get("series_id") or payload["contract_id"]),
            series_mode=str(payload.get("series_mode") or "contract"),
            roll_epoch=int(payload.get("roll_epoch") or 0),
            roll_event_id=None if payload.get("roll_event_id") is None else str(payload["roll_event_id"]),
            is_roll_bar=bool(payload.get("is_roll_bar")),
            is_first_bar_after_roll=bool(payload.get("is_first_bar_after_roll")),
            bars_since_roll=int(payload.get("bars_since_roll") or 0),
            price_space=str(payload.get("price_space") or "native"),
            native_open=float(payload.get("native_open", payload["open"])),
            native_high=float(payload.get("native_high", payload["high"])),
            native_low=float(payload.get("native_low", payload["low"])),
            native_close=float(payload.get("native_close", payload["close"])),
            continuous_open=float(payload.get("continuous_open", payload["open"])),
            continuous_high=float(payload.get("continuous_high", payload["high"])),
            continuous_low=float(payload.get("continuous_low", payload["low"])),
            continuous_close=float(payload.get("continuous_close", payload["close"])),
            execution_open=float(payload.get("execution_open", payload.get("native_open", payload["open"]))),
            execution_high=float(payload.get("execution_high", payload.get("native_high", payload["high"]))),
            execution_low=float(payload.get("execution_low", payload.get("native_low", payload["low"]))),
            execution_close=float(payload.get("execution_close", payload.get("native_close", payload["close"]))),
            previous_contract_id=None if payload.get("previous_contract_id") is None else str(payload["previous_contract_id"]),
            candidate_contract_id=None if payload.get("candidate_contract_id") is None else str(payload["candidate_contract_id"]),
            adjustment_mode=str(payload.get("adjustment_mode") or ""),
            cumulative_additive_offset=float(payload.get("cumulative_additive_offset") or 0.0),
            ratio_factor=None if payload.get("ratio_factor") is None else float(payload["ratio_factor"]),
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


def _series_id_for_bar(*, bar: CanonicalBar, manifest: ResearchDatasetManifest) -> str:
    if manifest.series_mode == "continuous_front":
        return str(
            getattr(
                bar,
                "series_id",
                f"{bar.instrument_id}|{bar.timeframe.value}|continuous_front",
            )
        )
    return str(bar.contract_id)


def _float_attr(bar: CanonicalBar, name: str, fallback: float) -> float:
    return float(getattr(bar, name, fallback))


def _optional_str_attr(bar: CanonicalBar, name: str) -> str | None:
    value = getattr(bar, name, None)
    return None if value is None else str(value)


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

    sort_key = (
        (lambda item: (item.instrument_id, item.timeframe.value, item.ts, item.contract_id))
        if manifest.series_mode == "continuous_front"
        else (lambda item: (item.instrument_id, item.contract_id, item.timeframe.value, item.ts))
    )
    for bar in sorted(filtered_bars, key=sort_key):
        session_date = bar.ts[:10]
        if manifest.series_mode == "continuous_front":
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
            active_contract_id = (
                str(getattr(bar, "active_contract_id", bar.contract_id))
                if manifest.series_mode == "continuous_front"
                else active_roll.active_contract_id if active_roll is not None else bar.contract_id
            )
            series_id = _series_id_for_bar(bar=bar, manifest=manifest)
            native_open = _float_attr(bar, "native_open", bar.open)
            native_high = _float_attr(bar, "native_high", bar.high)
            native_low = _float_attr(bar, "native_low", bar.low)
            native_close = _float_attr(bar, "native_close", bar.close)
            continuous_open = _float_attr(bar, "continuous_open", bar.open)
            continuous_high = _float_attr(bar, "continuous_high", bar.high)
            continuous_low = _float_attr(bar, "continuous_low", bar.low)
            continuous_close = _float_attr(bar, "continuous_close", bar.close)
            price_space = str(
                getattr(
                    bar,
                    "price_space",
                    "continuous_adjusted" if manifest.series_mode == "continuous_front" else "native",
                )
            )
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
                    series_id=series_id,
                    series_mode=manifest.series_mode,
                    roll_epoch=int(getattr(bar, "roll_epoch", 0) or 0),
                    roll_event_id=_optional_str_attr(bar, "roll_event_id"),
                    is_roll_bar=bool(getattr(bar, "is_roll_bar", False)),
                    is_first_bar_after_roll=bool(getattr(bar, "is_first_bar_after_roll", False)),
                    bars_since_roll=int(getattr(bar, "bars_since_roll", index) or 0),
                    price_space=price_space,
                    native_open=native_open,
                    native_high=native_high,
                    native_low=native_low,
                    native_close=native_close,
                    continuous_open=continuous_open,
                    continuous_high=continuous_high,
                    continuous_low=continuous_low,
                    continuous_close=continuous_close,
                    execution_open=native_open,
                    execution_high=native_high,
                    execution_low=native_low,
                    execution_close=native_close,
                    previous_contract_id=_optional_str_attr(bar, "previous_contract_id"),
                    candidate_contract_id=_optional_str_attr(bar, "candidate_contract_id"),
                    adjustment_mode=str(getattr(bar, "adjustment_mode", "")),
                    cumulative_additive_offset=float(getattr(bar, "cumulative_additive_offset", 0.0) or 0.0),
                    ratio_factor=None if getattr(bar, "ratio_factor", None) is None else float(getattr(bar, "ratio_factor")),
                )
            )
            prev_close = bar.close

    if manifest.series_mode == "continuous_front":
        return sorted(selected_views, key=lambda item: (item.instrument_id, item.timeframe, item.ts, item.contract_id))
    return sorted(selected_views, key=lambda item: (item.instrument_id, item.contract_id, item.timeframe, item.ts))
