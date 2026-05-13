from __future__ import annotations

from datetime import date

from trading_advisor_3000.product_plane.data_plane.moex.bar_interval_policy import (
    is_clearing_day_d1_period,
    is_native_calendar_d1_period,
    is_unified_session_period,
    moex_local_date,
    resolve_moex_bar_policy,
)


def test_moex_policy_date_uses_moscow_timezone() -> None:
    assert moex_local_date("2022-09-04T21:30:00Z") == date(2022, 9, 5)

    policy = resolve_moex_bar_policy("1d", "2022-09-04T21:30:00Z")

    assert policy.local_date == date(2022, 9, 5)
    assert policy.anchor_type == "calendar_day"
    assert policy.source_mode == "native_authoritative"
    assert policy.session_model == "legacy_session"


def test_d1_policy_boundaries_are_explicit() -> None:
    assert is_clearing_day_d1_period(date(2022, 9, 2))
    assert is_native_calendar_d1_period(date(2022, 9, 5))
    assert is_native_calendar_d1_period(date(2022, 11, 25))
    assert is_clearing_day_d1_period(date(2022, 11, 28))

    early = resolve_moex_bar_policy("1d", "2022-09-02T10:00:00Z")
    native = resolve_moex_bar_policy("1d", "2022-09-05T10:00:00Z")
    late = resolve_moex_bar_policy("1d", "2022-11-28T10:00:00Z")

    assert early.anchor_type == "clearing_day"
    assert early.source_mode == "aggregate_intraday"
    assert native.anchor_type == "calendar_day"
    assert native.source_mode == "native_authoritative"
    assert late.anchor_type == "clearing_day"
    assert late.source_mode == "aggregate_intraday"


def test_intraday_policy_uses_session_segment_aggregate_intraday() -> None:
    policy = resolve_moex_bar_policy("15m", "2026-04-02T10:00:00Z")

    assert policy.anchor_type == "session_segment"
    assert policy.source_mode == "aggregate_intraday"
    assert policy.session_model == "unified_session"
    assert is_unified_session_period(date(2026, 3, 29))
