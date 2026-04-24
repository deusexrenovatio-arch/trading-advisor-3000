from __future__ import annotations

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.indicators import (
    build_indicator_frames,
    build_indicator_profile_registry,
    default_indicator_profile,
    indicator_store_contract,
)


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
    assert profile.required_input_columns() == ("close", "high", "low", "volume")


def test_indicator_profile_covers_stage3_minimum_contract_columns() -> None:
    profile = default_indicator_profile()
    columns = {column for spec in profile.indicators for column in spec.output_columns}
    assert {
        "sma_10",
        "ema_50",
        "hma_20",
        "atr_14",
        "natr_14",
        "rsi_14",
        "stoch_k_14_3_3",
        "macd_12_26_9",
        "adx_14",
        "aroon_up_25",
        "donchian_high_20",
        "bb_width_20_2",
        "kc_upper_20_1_5",
        "obv",
        "cmf_20",
        "mfi_14",
        "vwma_20",
        "rvol_20",
        "volume_z_20",
    } <= columns


def test_indicator_store_contract_contains_stage3_metadata_columns() -> None:
    contract = indicator_store_contract()
    columns = contract["research_indicator_frames"]["columns"]
    assert {
        "dataset_version",
        "indicator_set_version",
        "profile_version",
        "source_bars_hash",
        "row_count",
        "warmup_span",
        "null_warmup_span",
        "created_at",
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

    ignored = {"source_bars_hash", "row_count", "created_at"}
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
    assert tail.values["bb_width_20_2"] is not None
    assert tail.values["rvol_20"] is not None
    assert tail.profile_version == "core_v1"
    assert tail.warmup_span >= tail.null_warmup_span >= 0
