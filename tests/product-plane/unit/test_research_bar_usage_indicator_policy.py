from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from trading_advisor_3000.product_plane.research.bar_usage_policy import (
    assert_derived_bar_usage_policy_coverage,
    assert_indicator_bar_usage_policy_coverage,
    resolve_derived_bar_usage_rule,
    resolve_indicator_bar_usage_rule,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.datasets.bar_usage import (
    bar_usage_flags_for_profile,
)
from trading_advisor_3000.product_plane.research.derived_indicators import (
    DerivedIndicatorProfile,
    build_derived_indicator_frames,
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.derived_indicators.materialize import (
    _compute_derived_frame,
)
from trading_advisor_3000.product_plane.research.indicators import (
    VOLUME_PROFILE_INDICATOR_COLUMNS,
    IndicatorFrameRow,
    IndicatorParameter,
    IndicatorProfile,
    IndicatorSpec,
    build_indicator_frames,
    default_indicator_profile,
)


def _p(name: str, value: object) -> IndicatorParameter:
    return IndicatorParameter(name=name, value=value)


def _indicator_profile_for_bar_usage() -> IndicatorProfile:
    return IndicatorProfile(
        version="bar_usage_test",
        description="Minimal profile for bar usage policy tests.",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_2",
                category="trend",
                operation_key="sma",
                parameters=(_p("length", 2),),
                required_input_columns=("close",),
                output_columns=("sma_2",),
                warmup_bars=2,
            ),
            IndicatorSpec(
                indicator_id="donchian_2",
                category="range",
                operation_key="donchian",
                parameters=(_p("lower_length", 2), _p("upper_length", 2)),
                required_input_columns=("high", "low"),
                output_columns=(
                    "donchian_high_2",
                    "donchian_low_2",
                    "donchian_mid_2",
                    "donchian_width_2",
                ),
                warmup_bars=2,
            ),
        ),
    )


def _view(ts_index: int, close: float, *, usage_profile: str) -> ResearchBarView:
    ts = datetime(2026, 3, 16, 9, 0, tzinfo=UTC) + timedelta(minutes=15 * ts_index)
    ts_text = ts.isoformat().replace("+00:00", "Z")
    session_class = "regular" if usage_profile == "regular_trading" else "holiday"
    return ResearchBarView(
        dataset_version="dataset-bar-usage-v1",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe="15m",
        ts=ts_text,
        open=close - 0.5,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1000 + ts_index,
        open_interest=2000 + ts_index,
        session_date=ts.date().isoformat(),
        session_open_ts=f"{ts.date().isoformat()}T09:00:00Z",
        session_close_ts=f"{ts.date().isoformat()}T23:45:00Z",
        active_contract_id="BR-6.26",
        ret_1=None if ts_index == 0 else 0.01,
        log_ret_1=None if ts_index == 0 else 0.00995,
        true_range=2.0,
        hl_range=2.0,
        oc_range=0.5,
        bar_index=ts_index,
        slice_role="analysis",
        session_class=session_class,
        bar_usage_profile=usage_profile,
        bar_usage_flags=bar_usage_flags_for_profile(usage_profile),
    )


def _cf_view(ts_index: int, close: float, *, timeframe: str) -> ResearchBarView:
    if timeframe == "15m":
        timestamp = datetime(2026, 3, 16, 9, 0, tzinfo=UTC) + timedelta(minutes=15 * ts_index)
    elif timeframe == "1h":
        timestamp = datetime(2026, 3, 16, 9, 0, tzinfo=UTC) + timedelta(hours=ts_index)
    else:
        raise AssertionError(f"unexpected timeframe: {timeframe}")
    base = _view(0, close, usage_profile="regular_trading")
    ts_text = timestamp.isoformat().replace("+00:00", "Z")
    return replace(
        base,
        contour_id="pit_active_front",
        series_mode="continuous_front",
        series_id=f"BR|{timeframe}|continuous_front",
        timeframe=timeframe,
        ts=ts_text,
        session_date=timestamp.date().isoformat(),
        session_open_ts=f"{timestamp.date().isoformat()}T09:00:00Z",
        session_close_ts=f"{timestamp.date().isoformat()}T23:45:00Z",
        close=close,
        high=close + 1.0,
        low=close - 1.0,
        bar_index=ts_index,
    )


def _indicator_row_for_bar(bar: ResearchBarView, values: dict[str, float]) -> IndicatorFrameRow:
    return IndicatorFrameRow(
        dataset_version=bar.dataset_version,
        indicator_set_version="indicators-bar-usage-v1",
        profile_version="indicator-test",
        contract_id=bar.contract_id,
        instrument_id=bar.instrument_id,
        timeframe=bar.timeframe,
        ts=bar.ts,
        values=values,
        source_bars_hash="SOURCE-BARS",
        row_count=1,
        warmup_span=0,
        null_warmup_span=0,
        created_at="2026-03-16T00:00:00Z",
        output_columns_hash="INDICATOR-COLUMNS",
        contour_id=bar.contour_id,
        series_mode=bar.series_mode,
        series_id=bar.series_id,
    )


def _derived_profile(*columns: str) -> DerivedIndicatorProfile:
    return DerivedIndicatorProfile(
        version="bar_usage_derived_test",
        description="Derived profile for bar usage policy tests.",
        output_columns=tuple(columns),
        warmup_bars=0,
    )


def _derived_base_frame(
    usage_profiles: list[str],
    *,
    highs: list[float] | None = None,
    closes: list[float] | None = None,
    session_dates: list[str] | None = None,
    timeframe: str = "15m",
    start: datetime | None = None,
) -> pd.DataFrame:
    start = start or datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for index, usage_profile in enumerate(usage_profiles):
        ts = start + timedelta(minutes=15 * index)
        close = closes[index] if closes is not None else float(index)
        high = highs[index] if highs is not None else close + 1.0
        session_date = session_dates[index] if session_dates is not None else ts.date().isoformat()
        rows.append(
            {
                "dataset_version": "dataset-bar-usage-v1",
                "contract_id": "BR-6.26",
                "instrument_id": "BR",
                "timeframe": timeframe,
                "ts": ts.isoformat().replace("+00:00", "Z"),
                "open": close - 0.5,
                "high": high,
                "low": close - 1.0,
                "close": close,
                "volume": 100.0,
                "open_interest": 2000.0 + index,
                "session_date": session_date,
                "session_open_ts": f"{session_date}T09:00:00Z",
                "session_close_ts": f"{session_date}T23:45:00Z",
                "bar_usage_profile": usage_profile,
                "bar_usage_flags": bar_usage_flags_for_profile(usage_profile),
                "bar_usage_policy_id": "moex_bar_usage_v1",
                "atr_14": 1.0,
                "sma_20": 10.0,
                "ema_20": 10.0,
                "ema_50": 11.0,
                "adx_14": 20.0,
                "rsi_14": 50.0,
                "roc_10": 0.0,
                "mom_10": 0.0,
            }
        )
    return pd.DataFrame(rows)


def test_bar_usage_policy_catalog_covers_current_outputs_fail_closed() -> None:
    indicator_profile = default_indicator_profile()
    derived_profile = current_derived_indicator_profile()

    assert_indicator_bar_usage_policy_coverage(indicator_profile)
    assert_derived_bar_usage_policy_coverage(derived_profile)
    assert resolve_indicator_bar_usage_rule("rsi_14").group_id == "price_close_state"
    assert (
        resolve_derived_bar_usage_rule("cross_close_session_vwap_code").mode == "event_update_zero"
    )

    with pytest.raises(ValueError, match="missing bar usage policy"):
        resolve_indicator_bar_usage_rule("new_unmapped_indicator")
    with pytest.raises(ValueError, match="missing bar usage policy"):
        resolve_derived_bar_usage_rule("new_unmapped_derived_output")


def test_base_indicator_windows_skip_noneligible_signal_bars() -> None:
    rows = [
        _view(0, 10.0, usage_profile="regular_trading"),
        _view(1, 12.0, usage_profile="regular_trading"),
        _view(2, 50.0, usage_profile="risk_only"),
        _view(3, 14.0, usage_profile="regular_trading"),
    ]

    frames = build_indicator_frames(
        dataset_version="dataset-bar-usage-v1",
        indicator_set_version="indicators-bar-usage-v1",
        bar_views=rows,
        profile=_indicator_profile_for_bar_usage(),
    )
    values = [row.values for row in frames]

    assert values[2]["sma_2"] == pytest.approx(31.0)
    assert values[2]["donchian_high_2"] == pytest.approx(13.0)
    assert values[3]["donchian_high_2"] == pytest.approx(15.0)


def test_continuous_front_base_indicator_windows_skip_noneligible_signal_bars() -> None:
    rows = [
        replace(
            _view(0, 10.0, usage_profile="regular_trading"),
            contour_id="pit_active_front",
            series_id="FUT_BR",
            series_mode="continuous_front",
        ),
        replace(
            _view(1, 12.0, usage_profile="regular_trading"),
            contour_id="pit_active_front",
            series_id="FUT_BR",
            series_mode="continuous_front",
        ),
        replace(
            _view(2, 50.0, usage_profile="risk_only"),
            contour_id="pit_active_front",
            series_id="FUT_BR",
            series_mode="continuous_front",
        ),
        replace(
            _view(3, 14.0, usage_profile="regular_trading"),
            contour_id="pit_active_front",
            series_id="FUT_BR",
            series_mode="continuous_front",
        ),
    ]

    frames = build_indicator_frames(
        dataset_version="dataset-bar-usage-v1",
        indicator_set_version="indicators-bar-usage-v1",
        bar_views=rows,
        series_mode="continuous_front",
        profile=_indicator_profile_for_bar_usage(),
    )
    values = [row.values for row in frames]

    assert values[2]["sma_2"] == pytest.approx(31.0)
    assert values[2]["donchian_high_2"] == pytest.approx(13.0)
    assert values[3]["donchian_high_2"] == pytest.approx(15.0)


def test_volume_profile_columns_skip_noneligible_signal_bars_without_source_rows() -> None:
    rows = [
        _view(0, 10.0, usage_profile="risk_only"),
        _view(1, 12.0, usage_profile="regular_trading"),
        _view(2, 14.0, usage_profile="risk_only"),
    ]
    profile = IndicatorProfile(
        version="bar_usage_volume_profile_test",
        description="Minimal volume-profile profile for bar usage policy tests.",
        indicators=(
            IndicatorSpec(
                indicator_id="volume_profile",
                category="volume",
                operation_key="volume_profile",
                parameters=(),
                required_input_columns=("open", "high", "low", "close", "volume"),
                output_columns=VOLUME_PROFILE_INDICATOR_COLUMNS,
                warmup_bars=0,
            ),
        ),
    )

    frames = build_indicator_frames(
        dataset_version="dataset-bar-usage-v1",
        indicator_set_version="indicators-bar-usage-v1",
        bar_views=rows,
        profile=profile,
    )
    values = [row.values for row in frames]

    assert values[0]["vp_shape_code"] is None
    assert values[1]["vp_shape_code"] == 0
    assert values[2]["vp_shape_code"] == 0


@pytest.mark.parametrize("usage_profile", ["incomplete", "boundary_risk", "shortened_risk"])
def test_nonregular_risk_profiles_do_not_update_signal_groups(usage_profile: str) -> None:
    rows = [
        _view(0, 10.0, usage_profile="regular_trading"),
        _view(1, 12.0, usage_profile="regular_trading"),
        _view(2, 50.0, usage_profile=usage_profile),
    ]

    frames = build_indicator_frames(
        dataset_version="dataset-bar-usage-v1",
        indicator_set_version="indicators-bar-usage-v1",
        bar_views=rows,
        profile=_indicator_profile_for_bar_usage(),
    )

    assert frames[2].values["sma_2"] == pytest.approx(31.0)
    assert frames[2].values["donchian_high_2"] == pytest.approx(13.0)


def test_derived_rolling_windows_skip_noneligible_bars() -> None:
    usage_profiles = ["regular_trading"] * 20 + ["risk_only", "regular_trading"]
    highs = [float(index) for index in range(20)] + [999.0, 21.0]
    frame = _derived_base_frame(usage_profiles, highs=highs)

    computed = _compute_derived_frame(
        base_frame=frame,
        current_timeframe="15m",
        source_frames={},
        profile=_derived_profile("rolling_high_20"),
    )

    assert computed.loc[19, "rolling_high_20"] == pytest.approx(19.0)
    assert computed.loc[20, "rolling_high_20"] == pytest.approx(19.0)
    assert computed.loc[21, "rolling_high_20"] == pytest.approx(21.0)


def test_derived_session_state_resets_without_cross_session_leakage() -> None:
    frame = _derived_base_frame(
        ["regular_trading", "risk_only", "risk_only", "regular_trading"],
        highs=[10.0, 99.0, 200.0, 20.0],
        session_dates=["2026-03-16", "2026-03-16", "2026-03-17", "2026-03-17"],
    )

    computed = _compute_derived_frame(
        base_frame=frame,
        current_timeframe="15m",
        source_frames={},
        profile=_derived_profile("session_high"),
    )

    assert computed.loc[0, "session_high"] == pytest.approx(10.0)
    assert computed.loc[1, "session_high"] == pytest.approx(10.0)
    assert math.isnan(computed.loc[2, "session_high"])
    assert computed.loc[3, "session_high"] == pytest.approx(20.0)


def test_derived_event_columns_are_neutral_on_noneligible_bars() -> None:
    frame = _derived_base_frame(
        ["regular_trading", "regular_trading", "risk_only"],
        closes=[9.0, 12.0, 13.0],
    )

    computed = _compute_derived_frame(
        base_frame=frame,
        current_timeframe="15m",
        source_frames={},
        profile=_derived_profile("cross_close_session_vwap_code"),
    )

    assert computed.loc[1, "cross_close_session_vwap_code"] == 1
    assert computed.loc[2, "cross_close_session_vwap_code"] == 0


def test_mtf_projection_uses_only_source_bars_eligible_for_carried_column() -> None:
    target = _derived_base_frame(
        ["regular_trading"],
        start=datetime(2026, 3, 16, 9, 45, tzinfo=UTC),
    )
    source = _derived_base_frame(
        ["regular_trading", "risk_only"],
        timeframe="1h",
        start=datetime(2026, 3, 16, 8, 0, tzinfo=UTC),
    )
    source["adx_14"] = [20.0, 999.0]
    source["ema_20"] = [10.0, 999.0]
    source["ema_50"] = [11.0, 999.0]
    source["rsi_14"] = [50.0, 999.0]

    computed = _compute_derived_frame(
        base_frame=target,
        current_timeframe="15m",
        source_frames={"1h": source},
        profile=_derived_profile("mtf_1h_to_15m_adx_14"),
    )

    assert computed.loc[0, "mtf_1h_to_15m_adx_14"] == pytest.approx(20.0)


def test_continuous_front_build_mtf_projection_uses_source_timeframe_family() -> None:
    bar_15m = _cf_view(3, 100.0, timeframe="15m")
    bar_1h = _cf_view(0, 101.0, timeframe="1h")
    indicator_rows = [
        _indicator_row_for_bar(
            bar_15m, {"ema_20": 10.0, "ema_50": 11.0, "adx_14": 20.0, "rsi_14": 50.0}
        ),
        _indicator_row_for_bar(
            bar_1h, {"ema_20": 123.0, "ema_50": 124.0, "adx_14": 25.0, "rsi_14": 55.0}
        ),
    ]

    rows = build_derived_indicator_frames(
        dataset_version="dataset-bar-usage-v1",
        indicator_set_version="indicators-bar-usage-v1",
        derived_indicator_set_version="derived-bar-usage-v1",
        bar_views=[bar_15m, bar_1h],
        indicator_rows=indicator_rows,
        series_mode="continuous_front",
        profile=_derived_profile("mtf_1h_to_15m_ema_20"),
    )

    row_15m = next(row for row in rows if row.timeframe == "15m")
    assert row_15m.values["mtf_1h_to_15m_ema_20"] == pytest.approx(123.0)
