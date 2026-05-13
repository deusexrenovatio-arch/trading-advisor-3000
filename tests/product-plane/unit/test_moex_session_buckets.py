from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.historical_canonical_route import (
    ChangedWindowScope,
)
from trading_advisor_3000.product_plane.data_plane.moex.session_buckets import (
    CanonicalBucketBuildError,
    build_canonical_bucket_rows,
)


def _window(
    *,
    start: str,
    end: str,
    internal_id: str = "FUT_BR",
    moex_secid: str = "BRM6",
    source_interval: int = 1,
) -> ChangedWindowScope:
    return ChangedWindowScope(
        internal_id=internal_id,
        source_timeframe="1m",
        source_interval=source_interval,
        moex_secid=moex_secid,
        window_start_utc=start,
        window_end_utc=end,
        incremental_rows=1,
    )


def test_bucket_intervals_are_half_open_and_limited_to_changed_window() -> None:
    rows = build_canonical_bucket_rows(
        changed_windows=[
            _window(
                start="2026-04-02T10:00:00Z",
                end="2026-04-02T10:11:00Z",
            )
        ],
        selected_source_intervals={("BRM6@MOEX", "FUT_BR", "5m"): 1},
        available_intervals_by_contract={("BRM6@MOEX", "FUT_BR"): {1}},
        target_timeframes=("5m",),
    )

    starts = [row.bucket_start_ts for row in rows]
    assert "2026-04-02T10:00:00Z" in starts
    assert "2026-04-02T10:05:00Z" in starts
    assert "2026-04-02T10:10:00Z" in starts
    assert "2026-04-02T10:15:00Z" not in starts
    first = rows[0]
    assert first.bucket_start_ts == first.canonical_ts
    assert first.bucket_end_ts > first.bucket_start_ts


def test_d1_native_period_uses_source_interval_1440() -> None:
    rows = build_canonical_bucket_rows(
        changed_windows=[
            _window(
                start="2022-09-05T10:00:00Z",
                end="2022-09-05T10:10:00Z",
            )
        ],
        selected_source_intervals={("BRM6@MOEX", "FUT_BR", "1d"): 1440},
        available_intervals_by_contract={("BRM6@MOEX", "FUT_BR"): {1, 1440}},
        target_timeframes=("1d",),
    )

    assert rows
    assert {row.source_interval for row in rows} == {1440}
    assert {row.source_mode for row in rows} == {"native_authoritative"}
    assert {row.anchor_type for row in rows} == {"calendar_day"}


def test_d1_clearing_period_uses_intraday_source_interval() -> None:
    rows = build_canonical_bucket_rows(
        changed_windows=[
            _window(
                start="2022-09-02T10:00:00Z",
                end="2022-09-02T10:10:00Z",
            )
        ],
        selected_source_intervals={("BRM6@MOEX", "FUT_BR", "1d"): 1440},
        available_intervals_by_contract={("BRM6@MOEX", "FUT_BR"): {1, 1440}},
        target_timeframes=("1d",),
    )

    assert rows
    assert {row.source_interval for row in rows} == {1}
    assert {row.source_mode for row in rows} == {"aggregate_intraday"}
    assert {row.anchor_type for row in rows} == {"clearing_day"}


def test_native_d1_without_native_source_blocks_build() -> None:
    with pytest.raises(CanonicalBucketBuildError) as exc:
        build_canonical_bucket_rows(
            changed_windows=[
                _window(
                    start="2022-09-05T10:00:00Z",
                    end="2022-09-05T10:10:00Z",
                )
            ],
            selected_source_intervals={("BRM6@MOEX", "FUT_BR", "1d"): 1},
            available_intervals_by_contract={("BRM6@MOEX", "FUT_BR"): {1}},
            target_timeframes=("1d",),
        )

    message = str(exc.value)
    assert "BRM6@MOEX" in message
    assert "FUT_BR" in message
    assert "1d" in message
    assert "native_authoritative" in message
    assert "available_intervals=[1]" in message


def test_bucket_rows_carry_policy_metadata() -> None:
    rows = build_canonical_bucket_rows(
        changed_windows=[
            _window(
                start="2026-04-02T10:00:00Z",
                end="2026-04-02T10:05:00Z",
            )
        ],
        selected_source_intervals={("BRM6@MOEX", "FUT_BR", "15m"): 1},
        available_intervals_by_contract={("BRM6@MOEX", "FUT_BR"): {1}},
        target_timeframes=("15m",),
    )

    row = rows[0]
    assert row.policy_id == "moex:15m:session_segment:aggregate_intraday:unified_session:v1"
    assert row.session_model == "unified_session"
    assert row.target_minutes == 15
    assert row.to_dict()["bucket_end_ts"] == row.bucket_end_ts
