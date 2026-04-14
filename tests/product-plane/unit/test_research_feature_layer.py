from __future__ import annotations

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.features import (
    build_feature_frames,
    build_feature_profile_registry,
    phase1_feature_profile,
    phase2b_feature_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators import IndicatorFrameRow


def _view(*, timeframe: str, ts_index: int, close: float, volume: int = 1000) -> ResearchBarView:
    if timeframe == "15m":
        ts = f"2026-03-16T{9 + (ts_index // 4):02d}:{(ts_index % 4) * 15:02d}:00Z"
    elif timeframe == "1h":
        ts = f"2026-03-16T{9 + ts_index:02d}:00:00Z"
    else:
        raise AssertionError(f"unexpected timeframe: {timeframe}")
    return ResearchBarView(
        dataset_version="dataset-v4",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe=timeframe,
        ts=ts,
        open=close - 0.3,
        high=close + 0.6,
        low=close - 0.7,
        close=close,
        volume=volume,
        open_interest=20000 + ts_index,
        session_date="2026-03-16",
        session_open_ts="2026-03-16T09:00:00Z",
        session_close_ts="2026-03-16T23:45:00Z",
        active_contract_id="BR-6.26",
        ret_1=None if ts_index == 0 else 0.01,
        log_ret_1=None if ts_index == 0 else 0.00995,
        true_range=1.2,
        hl_range=1.2,
        oc_range=0.3,
        bar_index=ts_index,
        slice_role="analysis",
    )


def _indicator_row(
    *,
    timeframe: str,
    ts_index: int,
    close: float,
    ema20: float,
    ema50: float,
    rsi14: float | None = 55.0,
    adx14: float | None = 28.0,
) -> IndicatorFrameRow:
    row = _view(timeframe=timeframe, ts_index=ts_index, close=close, volume=1000 + ts_index * 10)
    return IndicatorFrameRow(
        dataset_version="dataset-v4",
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
        contract_id=row.contract_id,
        instrument_id=row.instrument_id,
        timeframe=row.timeframe,
        ts=row.ts,
        values={
            "ema_10": ema20 + 0.4,
            "ema_20": ema20,
            "ema_50": ema50,
            "atr_14": 1.4,
            "rsi_14": rsi14,
            "adx_14": adx14,
            "bb_upper_20_2": close + 1.0,
            "bb_lower_20_2": close - 1.0,
            "bb_width_20_2": 0.12,
            "kc_upper_20_1_5": close + 1.2,
            "kc_mid_20_1_5": close,
            "kc_lower_20_1_5": close - 1.2,
            "vwma_20": close - 0.2,
            "rvol_20": 1.25,
            "volume_z_20": 0.6,
            "donchian_high_20": close + 1.4,
            "donchian_low_20": close - 1.4,
        },
        source_bars_hash="SRC-BARS",
        row_count=64,
        warmup_span=20,
        null_warmup_span=0,
        created_at="2026-03-16T12:00:00Z",
    )


def test_feature_registry_exposes_versioned_profiles_and_stage4_columns() -> None:
    registry = build_feature_profile_registry()
    assert registry.versions() == ("core_v1", "core_intraday_v1", "core_swing_v1")

    profile = registry.get("core_v1")
    mtf = next(spec for spec in profile.features if spec.feature_id == "mtf_overlay_1h_to_15m")
    assert mtf.operation_key == "mtf_overlay"
    assert mtf.params_dict()["source_timeframe"] == "1h"
    assert {
        "trend_state_fast_slow_code",
        "trend_strength",
        "rolling_high_20",
        "opening_range_high",
        "session_vwap",
        "bb_width_20_2",
        "squeeze_on_code",
        "breakout_ready_flag",
        "reversion_ready_flag",
        "atr_stop_ref_1x",
        "atr_target_ref_2x",
        "rvol_20",
        "volume_zscore_20",
        "htf_rsi_14",
    } <= set(profile.expected_output_columns())


def test_feature_store_contract_contains_stage4_lineage_columns() -> None:
    contract = phase2b_feature_store_contract()
    columns = contract["research_feature_frames"]["columns"]
    assert {
        "dataset_version",
        "indicator_set_version",
        "feature_set_version",
        "profile_version",
        "source_bars_hash",
        "source_indicators_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
    } <= set(columns)


def test_feature_build_is_point_in_time_safe_for_existing_prefix() -> None:
    base_views = [_view(timeframe="15m", ts_index=index, close=80.0 + index * 0.25, volume=1000 + index * 15) for index in range(32)]
    extended_views = [*base_views, _view(timeframe="15m", ts_index=32, close=120.0, volume=9000)]
    base_indicators = [
        _indicator_row(timeframe="15m", ts_index=index, close=80.0 + index * 0.25, ema20=79.0 + index * 0.2, ema50=78.0 + index * 0.15)
        for index in range(32)
    ]
    extended_indicators = [
        *base_indicators,
        _indicator_row(timeframe="15m", ts_index=32, close=120.0, ema20=101.0, ema50=95.0),
    ]

    base_rows = build_feature_frames(
        dataset_version="dataset-v4",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        bar_views=base_views,
        indicator_rows=base_indicators,
        series_mode="contract",
    )
    extended_rows = build_feature_frames(
        dataset_version="dataset-v4",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        bar_views=extended_views,
        indicator_rows=extended_indicators,
        series_mode="contract",
    )

    ignored = {"source_bars_hash", "source_indicators_hash", "row_count", "null_warmup_span", "created_at"}
    base_payload = [{k: v for k, v in row.to_dict().items() if k not in ignored} for row in base_rows]
    extended_payload = [{k: v for k, v in row.to_dict().items() if k not in ignored} for row in extended_rows[: len(base_rows)]]
    assert base_payload == extended_payload


def test_feature_mtf_overlay_is_available_only_after_higher_timeframe_close() -> None:
    ltf_views = [_view(timeframe="15m", ts_index=index, close=90.0 + index * 0.3) for index in range(8)]
    htf_views = [_view(timeframe="1h", ts_index=index, close=95.0 + index * 0.8) for index in range(2)]
    indicator_rows = [
        *[
            _indicator_row(timeframe="15m", ts_index=index, close=90.0 + index * 0.3, ema20=89.0 + index * 0.2, ema50=88.0 + index * 0.15)
            for index in range(8)
        ],
        _indicator_row(timeframe="1h", ts_index=0, close=95.0, ema20=94.0, ema50=92.0, rsi14=58.0, adx14=31.0),
        _indicator_row(timeframe="1h", ts_index=1, close=95.8, ema20=94.8, ema50=92.5, rsi14=61.0, adx14=33.0),
    ]

    rows = build_feature_frames(
        dataset_version="dataset-v4",
        indicator_set_version="indicators-v1",
        feature_set_version="features-v1",
        bar_views=[*ltf_views, *htf_views],
        indicator_rows=indicator_rows,
        series_mode="contract",
        profile=phase1_feature_profile(),
    )
    ltf_rows = [row for row in rows if row.timeframe == "15m"]

    assert ltf_rows[0].values["htf_rsi_14"] is None
    assert ltf_rows[1].values["htf_rsi_14"] is None
    assert ltf_rows[2].values["htf_rsi_14"] is None
    assert ltf_rows[3].values["htf_rsi_14"] == 58.0
    assert ltf_rows[3].values["htf_trend_state_code"] == 1
    assert ltf_rows[7].values["htf_rsi_14"] == 61.0
