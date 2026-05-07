from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DEFAULT_CONTINUOUS_FRONT_POLICY: dict[str, object] = {
    "roll_source": "continuous_front_roll_events",
    "active_contract_field": "active_contract_id",
    "require_point_in_time_alignment": True,
    "preserve_roll_gap_columns": True,
    "roll_policy_version": "front_liquidity_oi_v1",
    "adjustment_policy_version": "backward_current_anchor_additive_v1",
    "roll_policy_mode": "liquidity_oi_v1",
    "primary_metric": "open_interest",
    "secondary_metric": "volume",
    "confirmation_bars": 2,
    "candidate_share_min": 0.0,
    "advantage_ratio_min": 1.0,
    "switch_timing": "next_tradable_bar_after_decision_watermark",
    "tie_breaker": "higher_open_interest_then_volume_then_contract_id",
    "adjustment_mode": "additive",
    "gap_type": "close_to_close",
    "reference_price_policy": "decision_bar_close",
    "price_space": "continuous_backward_current_anchor_additive",
    "decision_uses_closed_bar": True,
    "effective_after_watermark": True,
    "calendar_roll_offset_trading_days": 0,
    "expected_timeline_mode": "liquidity_ranked_contracts",
    "session_policy": "",
    "session_timezone": "",
    "session_start_time": "",
    "session_end_time": "",
}


CALENDAR_EXPIRY_CONTINUOUS_FRONT_POLICY: dict[str, object] = {
    **DEFAULT_CONTINUOUS_FRONT_POLICY,
    "roll_policy_version": "front_calendar_expiry_t2_session_0900_2350_v1",
    "roll_policy_mode": "calendar_expiry_v1",
    "primary_metric": "volume",
    "secondary_metric": "open_interest",
    "confirmation_bars": 1,
    "switch_timing": "first_active_bar_on_or_after_roll_session",
    "tie_breaker": "maturity_order_then_contract_id",
    "reference_price_policy": "last_old_active_close_to_first_new_active_close",
    "calendar_roll_offset_trading_days": 2,
    "expected_timeline_mode": "active_contract_bars",
    "session_policy": "research_regular_0900_2350",
    "session_timezone": "Europe/Moscow",
    "session_start_time": "09:00",
    "session_end_time": "23:50",
}


@dataclass(frozen=True)
class ContinuousFrontPolicy:
    roll_source: str = "continuous_front_roll_events"
    active_contract_field: str = "active_contract_id"
    require_point_in_time_alignment: bool = True
    preserve_roll_gap_columns: bool = True
    roll_policy_version: str = "front_liquidity_oi_v1"
    adjustment_policy_version: str = "backward_current_anchor_additive_v1"
    roll_policy_mode: str = "liquidity_oi_v1"
    primary_metric: str = "open_interest"
    secondary_metric: str = "volume"
    confirmation_bars: int = 2
    candidate_share_min: float = 0.0
    advantage_ratio_min: float = 1.0
    switch_timing: str = "next_tradable_bar_after_decision_watermark"
    tie_breaker: str = "higher_open_interest_then_volume_then_contract_id"
    adjustment_mode: str = "additive"
    gap_type: str = "close_to_close"
    reference_price_policy: str = "decision_bar_close"
    price_space: str = "continuous_backward_current_anchor_additive"
    decision_uses_closed_bar: bool = True
    effective_after_watermark: bool = True
    calendar_roll_offset_trading_days: int = 0
    expected_timeline_mode: str = "liquidity_ranked_contracts"
    session_policy: str = ""
    session_timezone: str = ""
    session_start_time: str = ""
    session_end_time: str = ""

    def __post_init__(self) -> None:
        if self.roll_policy_mode not in {"calendar_expiry_v1", "liquidity_oi_v1", "liquidity_volume_oi_v1"}:
            raise ValueError(f"unsupported continuous_front roll_policy_mode: {self.roll_policy_mode}")
        if self.adjustment_mode != "additive":
            raise ValueError("continuous_front v1 supports only additive adjustment")
        if self.confirmation_bars < 1:
            raise ValueError("confirmation_bars must be >= 1")
        if self.candidate_share_min < 0.0 or self.candidate_share_min > 1.0:
            raise ValueError("candidate_share_min must be between 0 and 1")
        if self.advantage_ratio_min < 0.0:
            raise ValueError("advantage_ratio_min must be non-negative")
        if self.calendar_roll_offset_trading_days < 0:
            raise ValueError("calendar_roll_offset_trading_days must be non-negative")
        if not self.roll_policy_version.strip():
            raise ValueError("roll_policy_version must be non-empty")
        if not self.adjustment_policy_version.strip():
            raise ValueError("adjustment_policy_version must be non-empty")

    @classmethod
    def from_config(cls, payload: dict[str, Any] | None = None) -> "ContinuousFrontPolicy":
        merged = dict(DEFAULT_CONTINUOUS_FRONT_POLICY)
        if payload:
            merged.update({str(key): value for key, value in payload.items()})
        return cls(
            roll_source=str(merged["roll_source"]),
            active_contract_field=str(merged["active_contract_field"]),
            require_point_in_time_alignment=bool(merged["require_point_in_time_alignment"]),
            preserve_roll_gap_columns=bool(merged["preserve_roll_gap_columns"]),
            roll_policy_version=str(merged["roll_policy_version"]),
            adjustment_policy_version=str(merged["adjustment_policy_version"]),
            roll_policy_mode=str(merged["roll_policy_mode"]),
            primary_metric=str(merged["primary_metric"]),
            secondary_metric=str(merged["secondary_metric"]),
            confirmation_bars=int(merged["confirmation_bars"]),
            candidate_share_min=float(merged["candidate_share_min"]),
            advantage_ratio_min=float(merged["advantage_ratio_min"]),
            switch_timing=str(merged["switch_timing"]),
            tie_breaker=str(merged["tie_breaker"]),
            adjustment_mode=str(merged["adjustment_mode"]),
            gap_type=str(merged["gap_type"]),
            reference_price_policy=str(merged["reference_price_policy"]),
            price_space=str(merged["price_space"]),
            decision_uses_closed_bar=bool(merged["decision_uses_closed_bar"]),
            effective_after_watermark=bool(merged["effective_after_watermark"]),
            calendar_roll_offset_trading_days=int(merged["calendar_roll_offset_trading_days"]),
            expected_timeline_mode=str(merged["expected_timeline_mode"]),
            session_policy=str(merged["session_policy"]),
            session_timezone=str(merged["session_timezone"]),
            session_start_time=str(merged["session_start_time"]),
            session_end_time=str(merged["session_end_time"]),
        )

    def to_config_dict(self) -> dict[str, object]:
        return {
            "roll_source": self.roll_source,
            "active_contract_field": self.active_contract_field,
            "require_point_in_time_alignment": self.require_point_in_time_alignment,
            "preserve_roll_gap_columns": self.preserve_roll_gap_columns,
            "roll_policy_version": self.roll_policy_version,
            "adjustment_policy_version": self.adjustment_policy_version,
            "roll_policy_mode": self.roll_policy_mode,
            "primary_metric": self.primary_metric,
            "secondary_metric": self.secondary_metric,
            "confirmation_bars": self.confirmation_bars,
            "candidate_share_min": self.candidate_share_min,
            "advantage_ratio_min": self.advantage_ratio_min,
            "switch_timing": self.switch_timing,
            "tie_breaker": self.tie_breaker,
            "adjustment_mode": self.adjustment_mode,
            "gap_type": self.gap_type,
            "reference_price_policy": self.reference_price_policy,
            "price_space": self.price_space,
            "decision_uses_closed_bar": self.decision_uses_closed_bar,
            "effective_after_watermark": self.effective_after_watermark,
            "calendar_roll_offset_trading_days": self.calendar_roll_offset_trading_days,
            "expected_timeline_mode": self.expected_timeline_mode,
            "session_policy": self.session_policy,
            "session_timezone": self.session_timezone,
            "session_start_time": self.session_start_time,
            "session_end_time": self.session_end_time,
        }

