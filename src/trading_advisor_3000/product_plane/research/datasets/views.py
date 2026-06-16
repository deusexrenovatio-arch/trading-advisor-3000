from __future__ import annotations

from dataclasses import dataclass


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
    session_open_ts: str | None
    session_close_ts: str | None
    active_contract_id: str
    ret_1: float | None
    log_ret_1: float | None
    true_range: float
    hl_range: float
    oc_range: float
    bar_index: int
    slice_role: str
    contour_id: str = "native_tradable"
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
    bar_start_ts: str | None = None
    bar_end_ts: str | None = None
    session_interval_id: str | None = None
    session_class: str = "regular"
    bar_usage_profile: str = "regular_trading"
    bar_usage_flags: int = 127
    bar_usage_policy_id: str = "moex_bar_usage_v1"

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_version": self.dataset_version,
            "contour_id": self.contour_id,
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
            "bar_start_ts": self.bar_start_ts,
            "bar_end_ts": self.bar_end_ts,
            "session_interval_id": self.session_interval_id,
            "session_class": self.session_class,
            "bar_usage_profile": self.bar_usage_profile,
            "bar_usage_flags": self.bar_usage_flags,
            "bar_usage_policy_id": self.bar_usage_policy_id,
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
            "continuous_open": self.continuous_open
            if self.continuous_open is not None
            else self.open,
            "continuous_high": self.continuous_high
            if self.continuous_high is not None
            else self.high,
            "continuous_low": self.continuous_low if self.continuous_low is not None else self.low,
            "continuous_close": self.continuous_close
            if self.continuous_close is not None
            else self.close,
            "execution_open": self.execution_open if self.execution_open is not None else self.open,
            "execution_high": self.execution_high if self.execution_high is not None else self.high,
            "execution_low": self.execution_low if self.execution_low is not None else self.low,
            "execution_close": self.execution_close
            if self.execution_close is not None
            else self.close,
            "previous_contract_id": self.previous_contract_id,
            "candidate_contract_id": self.candidate_contract_id,
            "adjustment_mode": self.adjustment_mode,
            "cumulative_additive_offset": self.cumulative_additive_offset,
            "ratio_factor": self.ratio_factor,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ResearchBarView":
        def nullable_str(key: str) -> str | None:
            value = payload[key]
            return None if value is None else str(value)

        return cls(
            dataset_version=str(payload["dataset_version"]),
            contour_id=str(payload.get("contour_id") or "native_tradable"),
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
            session_open_ts=nullable_str("session_open_ts"),
            session_close_ts=nullable_str("session_close_ts"),
            bar_start_ts=None
            if payload.get("bar_start_ts") is None
            else str(payload["bar_start_ts"]),
            bar_end_ts=None if payload.get("bar_end_ts") is None else str(payload["bar_end_ts"]),
            session_interval_id=None
            if payload.get("session_interval_id") is None
            else str(payload["session_interval_id"]),
            session_class=str(payload.get("session_class") or "regular"),
            bar_usage_profile=str(payload.get("bar_usage_profile") or "regular_trading"),
            bar_usage_flags=int(
                127 if payload.get("bar_usage_flags") is None else payload["bar_usage_flags"]
            ),
            bar_usage_policy_id=str(payload.get("bar_usage_policy_id") or "moex_bar_usage_v1"),
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
            roll_event_id=None
            if payload.get("roll_event_id") is None
            else str(payload["roll_event_id"]),
            is_roll_bar=bool(payload.get("is_roll_bar") or False),
            is_first_bar_after_roll=bool(payload.get("is_first_bar_after_roll") or False),
            bars_since_roll=int(payload.get("bars_since_roll") or 0),
            price_space=str(payload.get("price_space") or "native"),
            native_open=None
            if payload.get("native_open") is None
            else float(payload["native_open"]),
            native_high=None
            if payload.get("native_high") is None
            else float(payload["native_high"]),
            native_low=None if payload.get("native_low") is None else float(payload["native_low"]),
            native_close=None
            if payload.get("native_close") is None
            else float(payload["native_close"]),
            continuous_open=None
            if payload.get("continuous_open") is None
            else float(payload["continuous_open"]),
            continuous_high=None
            if payload.get("continuous_high") is None
            else float(payload["continuous_high"]),
            continuous_low=None
            if payload.get("continuous_low") is None
            else float(payload["continuous_low"]),
            continuous_close=None
            if payload.get("continuous_close") is None
            else float(payload["continuous_close"]),
            execution_open=None
            if payload.get("execution_open") is None
            else float(payload["execution_open"]),
            execution_high=None
            if payload.get("execution_high") is None
            else float(payload["execution_high"]),
            execution_low=None
            if payload.get("execution_low") is None
            else float(payload["execution_low"]),
            execution_close=None
            if payload.get("execution_close") is None
            else float(payload["execution_close"]),
            previous_contract_id=None
            if payload.get("previous_contract_id") is None
            else str(payload["previous_contract_id"]),
            candidate_contract_id=None
            if payload.get("candidate_contract_id") is None
            else str(payload["candidate_contract_id"]),
            adjustment_mode=str(payload.get("adjustment_mode") or ""),
            cumulative_additive_offset=float(payload.get("cumulative_additive_offset") or 0.0),
            ratio_factor=None
            if payload.get("ratio_factor") is None
            else float(payload["ratio_factor"]),
        )
