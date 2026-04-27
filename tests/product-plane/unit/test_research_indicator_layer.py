from __future__ import annotations

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.indicators import (
    build_indicator_frames,
    build_indicator_profile_registry,
    default_indicator_profile,
    indicator_store_contract,
)


EXPECTED_WIDE_TECHNICAL_GOLD_V2_BASE_COLUMNS = {
    "sma_10",
    "sma_20",
    "sma_50",
    "sma_100",
    "sma_200",
    "ema_10",
    "ema_20",
    "ema_50",
    "ema_100",
    "ema_200",
    "hma_20",
    "hma_100",
    "hma_200",
    "vwma_20",
    "vwma_100",
    "vwma_200",
    "adx_14",
    "dmp_14",
    "dmn_14",
    "aroon_up_25",
    "aroon_down_25",
    "chop_14",
    "close_slope_20",
    "supertrend_10_3",
    "supertrend_direction_10_3",
    "roc_10",
    "mom_10",
    "macd_12_26_9",
    "macd_signal_12_26_9",
    "macd_hist_12_26_9",
    "ppo_12_26_9",
    "ppo_signal_12_26_9",
    "ppo_hist_12_26_9",
    "tsi_25_13",
    "trix_30_9",
    "trix_signal_30_9",
    "kst_10_15_20_30",
    "kst_signal_9",
    "rsi_14",
    "stoch_k_14_3_3",
    "stoch_d_14_3_3",
    "cci_20",
    "willr_14",
    "stochrsi_k_14_14_3_3",
    "stochrsi_d_14_14_3_3",
    "ultimate_oscillator_7_14_28",
    "atr_14",
    "natr_14",
    "bb_upper_20_2",
    "bb_mid_20_2",
    "bb_lower_20_2",
    "bb_width_20_2",
    "bb_percent_b_20_2",
    "kc_upper_20_1_5",
    "kc_mid_20_1_5",
    "kc_lower_20_1_5",
    "donchian_high_20",
    "donchian_low_20",
    "donchian_mid_20",
    "donchian_width_20",
    "donchian_high_55",
    "donchian_low_55",
    "donchian_mid_55",
    "donchian_width_55",
    "true_range",
    "realized_volatility_20",
    "ulcer_index_14",
    "rvol_20",
    "volume_z_20",
    "obv",
    "mfi_14",
    "cmf_20",
    "ad",
    "adosc_3_10",
    "force_index_13",
    "pvt",
    "pvo_12_26_9",
    "pvo_signal_12_26_9",
    "pvo_hist_12_26_9",
    "oi_change_1",
    "oi_roc_10",
    "oi_z_20",
    "oi_relative_activity_20",
    "volume_oi_ratio",
}


def _view(*, ts_index: int, close: float, volume: int = 1000) -> ResearchBarView:
    ts = f"2026-03-16T{9 + (ts_index // 4):02d}:{(ts_index % 4) * 15:02d}:00Z"
    return ResearchBarView(
        dataset_version="dataset-v3",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe="15m",
        ts=ts,
        open=close - 0.2,
        high=close + 0.5,
        low=close - 0.6,
        close=close,
        volume=volume,
        open_interest=20000 + ts_index,
        session_date=ts[:10],
        session_open_ts="2026-03-16T09:00:00Z",
        session_close_ts="2026-03-16T23:45:00Z",
        active_contract_id="BR-6.26",
        ret_1=None if ts_index == 0 else 0.01,
        log_ret_1=None if ts_index == 0 else 0.00995,
        true_range=1.1,
        hl_range=1.1,
        oc_range=0.2,
        bar_index=ts_index,
        slice_role="analysis",
    )


def test_indicator_registry_exposes_versioned_profiles_and_richer_specs() -> None:
    registry = build_indicator_profile_registry()
    assert registry.versions() == ("core_v1", "core_intraday_v1", "core_swing_v1")

    profile = registry.get("core_v1")
    macd = next(spec for spec in profile.indicators if spec.indicator_id == "macd_12_26_9")
    assert macd.operation_key == "macd"
    assert macd.params_dict() == {"fast": 12, "slow": 26, "signal": 9}
    assert macd.required_input_columns == ("close",)
    assert macd.output_columns == ("macd_12_26_9", "macd_signal_12_26_9", "macd_hist_12_26_9")
    assert profile.required_input_columns() == ("close", "high", "low", "true_range", "log_ret_1", "volume", "open_interest")
    assert profile.max_warmup_bars() == 200


def test_indicator_profile_covers_wide_technical_gold_v2_base_columns() -> None:
    profile = default_indicator_profile()
    columns = {column for spec in profile.indicators for column in spec.output_columns}
    assert EXPECTED_WIDE_TECHNICAL_GOLD_V2_BASE_COLUMNS <= columns
    assert not EXPECTED_WIDE_TECHNICAL_GOLD_V2_BASE_COLUMNS - set(indicator_store_contract()["research_indicator_frames"]["columns"])


def test_indicator_store_contract_contains_stage3_metadata_columns() -> None:
    contract = indicator_store_contract()
    columns = contract["research_indicator_frames"]["columns"]
    assert {
        "dataset_version",
        "indicator_set_version",
        "profile_version",
        "source_bars_hash",
        "source_dataset_bars_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
        "output_columns_hash",
    } <= set(columns)


def test_indicator_build_is_point_in_time_safe_for_existing_prefix() -> None:
    base_views = [_view(ts_index=index, close=80.0 + index * 0.4, volume=1000 + index * 15) for index in range(60)]
    extended_views = [*base_views, _view(ts_index=60, close=120.0, volume=9000)]

    base_rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=base_views,
        series_mode="contract",
    )
    extended_rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=extended_views,
        series_mode="contract",
    )

    ignored = {"source_bars_hash", "source_dataset_bars_hash", "row_count", "null_warmup_span", "created_at"}
    base_payload = [{k: v for k, v in row.to_dict().items() if k not in ignored} for row in base_rows]
    extended_payload = [{k: v for k, v in row.to_dict().items() if k not in ignored} for row in extended_rows[: len(base_rows)]]
    assert base_payload == extended_payload


def test_indicator_build_produces_real_values_and_null_warmup_span() -> None:
    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[_view(ts_index=index, close=90.0 + index * 0.3, volume=900 + index * 10) for index in range(70)],
        series_mode="contract",
    )

    tail = rows[-1]
    assert tail.values["ema_20"] is not None
    assert tail.values["macd_12_26_9"] is not None
    assert tail.values["rsi_14"] is not None
    assert tail.values["cci_20"] is not None
    assert tail.values["willr_14"] is not None
    assert tail.values["tsi_25_13"] is not None
    assert tail.values["bb_width_20_2"] is not None
    assert tail.values["rvol_20"] is not None
    assert tail.profile_version == "core_v1"
    assert tail.warmup_span >= tail.null_warmup_span >= 0


def test_indicator_build_produces_wide_v2_tail_values_after_full_warmup() -> None:
    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[
            _view(
                ts_index=index,
                close=90.0 + index * 0.11 + (0.35 if index % 11 == 0 else 0.0),
                volume=900 + index * 13 + (75 if index % 9 == 0 else 0),
            )
            for index in range(320)
        ],
        series_mode="contract",
    )

    tail = rows[-1]
    for column in (
        "sma_200",
        "ema_200",
        "hma_200",
        "vwma_200",
        "supertrend_10_3",
        "trix_30_9",
        "kst_10_15_20_30",
        "stochrsi_k_14_14_3_3",
        "ultimate_oscillator_7_14_28",
        "ad",
        "adosc_3_10",
        "force_index_13",
        "pvt",
        "pvo_12_26_9",
        "oi_z_20",
        "volume_oi_ratio",
    ):
        assert tail.values[column] is not None
