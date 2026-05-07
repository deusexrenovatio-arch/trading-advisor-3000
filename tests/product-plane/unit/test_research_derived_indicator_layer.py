from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.derived_indicators import (
    WIDE_TECHNICAL_GOLD_V2_DERIVED_COLUMNS,
    build_derived_indicator_frames,
    build_derived_indicator_profile_registry,
    load_derived_indicator_frames,
    research_derived_indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    write_derived_indicator_frame_batches,
)
from trading_advisor_3000.product_plane.research.indicators import IndicatorFrameRow, build_indicator_frames


def _angle_slope(current: float, previous: float, *, length: int) -> float:
    return math.degrees(math.atan((current - previous) / float(length)))


def _view(*, ts_index: int, close: float, timeframe: str = "15m") -> ResearchBarView:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)
    if timeframe == "15m":
        ts = start + timedelta(minutes=15 * ts_index)
    elif timeframe == "1h":
        ts = start + timedelta(hours=ts_index)
    elif timeframe == "4h":
        ts = start + timedelta(hours=4 * ts_index)
    else:
        raise AssertionError(f"unexpected timeframe: {timeframe}")
    ts_text = ts.isoformat().replace("+00:00", "Z")
    return ResearchBarView(
        dataset_version="dataset-v5",
        contract_id="BR-6.26",
        instrument_id="BR",
        timeframe=timeframe,
        ts=ts_text,
        open=close - 0.3,
        high=close + 0.6,
        low=close - 0.7,
        close=close,
        volume=1000 + ts_index * 10,
        open_interest=20000 + ts_index,
        session_date=ts_text[:10],
        session_open_ts=f"{ts_text[:10]}T09:00:00Z",
        session_close_ts=f"{ts_text[:10]}T23:45:00Z",
        active_contract_id="BR-6.26",
        ret_1=None if ts_index == 0 else 0.01,
        log_ret_1=None if ts_index == 0 else 0.00995,
        true_range=1.2,
        hl_range=1.2,
        oc_range=0.3,
        bar_index=ts_index,
        slice_role="analysis",
    )


def _indicator_row(*, ts_index: int, close: float) -> IndicatorFrameRow:
    row = _view(ts_index=ts_index, close=close)
    return IndicatorFrameRow(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        profile_version="core_v1",
        contract_id=row.contract_id,
        instrument_id=row.instrument_id,
        timeframe=row.timeframe,
        ts=row.ts,
        values={
            "ema_10": close - 0.1,
            "ema_20": close - 0.2,
            "ema_50": close - 0.5,
            "atr_14": 1.5,
            "rsi_14": 55.0,
            "adx_14": 28.0,
            "natr_14": 1.5,
            "roc_10": 0.6,
            "mom_10": 0.5,
            "ppo_12_26_9": 0.4,
            "tsi_25_13": 0.3,
            "cci_20": 75.0,
            "willr_14": -35.0,
            "bb_upper_20_2": close + 1.0,
            "bb_mid_20_2": close,
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


def test_derived_indicator_store_contract_is_separate_wide_layer() -> None:
    registry = build_derived_indicator_profile_registry()
    profile = registry.get("core_v1")
    contract = research_derived_indicator_store_contract()
    columns = contract["research_derived_indicator_frames"]["columns"]

    derived_columns = tuple(
        column
        for column in columns
        if column
        not in {
            "dataset_version",
            "indicator_set_version",
            "derived_indicator_set_version",
            "profile_version",
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
            "source_bars_hash",
            "source_dataset_bars_hash",
            "source_indicators_hash",
            "source_indicator_profile_version",
            "source_indicator_output_columns_hash",
            "row_count",
            "warmup_span",
            "null_warmup_span",
            "created_at",
            "output_columns_hash",
        }
    )
    assert contract["research_derived_indicator_frames"]["format"] == "delta"
    assert "feature_set_version" not in columns
    assert registry.versions() == ("core_v1",)
    assert profile.output_columns == WIDE_TECHNICAL_GOLD_V2_DERIVED_COLUMNS
    assert derived_columns == WIDE_TECHNICAL_GOLD_V2_DERIVED_COLUMNS
    forbidden_feature_outputs = {
        "trend_state_fast_slow_code",
        "ma_stack_state_code",
        "regime_state_code",
        "squeeze_on_code",
        "breakout_ready_state_code",
        "volatility_regime_code",
        "momentum_state_code",
        "oscillator_pressure_code",
        "breakout_ready_flag",
        "above_below_vwma_code",
        "session_volume_state_code",
        "volume_price_confirmation_code",
        "oi_price_confirmation_code",
        "volume_oi_confirmation_code",
        "reversion_ready_flag",
        "atr_stop_ref_1x",
        "atr_target_ref_2x",
    }
    assert forbidden_feature_outputs.isdisjoint(columns)
    assert {
        "dataset_version",
        "indicator_set_version",
        "derived_indicator_set_version",
        "distance_to_ema_20_atr",
        "donchian_position_20",
        "price_volume_corr_20",
        "divergence_price_rsi_14_score",
        "divergence_price_stoch_d_14_3_3_score",
        "divergence_price_stochrsi_k_14_14_3_3_score",
        "divergence_price_oi_z_20_score",
        "source_indicators_hash",
    } <= set(columns)


def test_derived_indicator_build_keeps_feature_sets_out_of_layer_identity() -> None:
    bars = [_view(ts_index=index, close=80.0 + index * 0.25) for index in range(32)]
    indicators = [_indicator_row(ts_index=index, close=80.0 + index * 0.25) for index in range(32)]

    rows = build_derived_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="contract",
    )

    tail = rows[-1]
    payload = tail.to_dict()
    assert len(rows) == len(bars)
    assert tail.derived_indicator_set_version == "derived-v1"
    assert payload["distance_to_ema_20_atr"] is not None
    assert payload["donchian_position_20"] is not None
    assert "oscillator_pressure_code" not in payload
    assert "feature_set_version" not in payload


def test_continuous_front_derived_rows_keep_chronological_alignment_across_contract_chunks() -> None:
    start = datetime(2026, 3, 16, 9, 0, tzinfo=UTC)

    def continuous_view(index: int) -> ResearchBarView:
        ts = (start + timedelta(minutes=15 * index)).isoformat().replace("+00:00", "Z")
        contract_id = "ZZ_OLD@MOEX" if index < 30 else "AA_NEW@MOEX"
        close = 100.0 + index if index < 30 else 95.0 + (index - 30)
        return ResearchBarView(
            dataset_version="dataset-v5",
            contract_id=contract_id,
            instrument_id="BR",
            timeframe="15m",
            ts=ts,
            open=close - 0.3,
            high=close + 0.6,
            low=close - 0.7,
            close=close,
            volume=1000 + index,
            open_interest=20000 + index,
            session_date=ts[:10],
            session_open_ts=f"{ts[:10]}T09:00:00Z",
            session_close_ts=f"{ts[:10]}T23:45:00Z",
            active_contract_id=contract_id,
            ret_1=None if index == 0 else 0.01,
            log_ret_1=None if index == 0 else 0.00995,
            true_range=1.3,
            hl_range=1.3,
            oc_range=0.3,
            bar_index=index,
            slice_role="analysis",
            series_id="BR|15m|continuous_front",
            series_mode="continuous_front",
            price_space="continuous_backward_current_anchor_additive",
            native_open=close - 0.3,
            native_high=close + 0.6,
            native_low=close - 0.7,
            native_close=close,
            continuous_open=close - 0.3,
            continuous_high=close + 0.6,
            continuous_low=close - 0.7,
            continuous_close=close,
            execution_open=close - 0.3,
            execution_high=close + 0.6,
            execution_low=close - 0.7,
            execution_close=close,
            adjustment_mode="additive",
        )

    bars = [continuous_view(index) for index in range(40)]
    indicators = [
        IndicatorFrameRow(
            dataset_version=row.dataset_version,
            indicator_set_version="indicators-v1",
            profile_version="core_v1",
            contract_id=row.contract_id,
            instrument_id=row.instrument_id,
            timeframe=row.timeframe,
            ts=row.ts,
            values={"atr_14": 1.5, "ema_20": row.close - 0.2, "sma_20": row.close - 0.3},
            source_bars_hash="SRC-BARS",
            row_count=len(bars),
            warmup_span=20,
            null_warmup_span=0,
            created_at="2026-03-16T12:00:00Z",
        )
        for row in bars
    ]

    rows = build_derived_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="continuous_front",
    )

    assert [row.ts for row in rows] == [row.ts for row in bars]
    probe = rows[35].to_dict()
    assert probe["ts"] == bars[35].ts
    assert probe["rolling_high_20"] == pytest.approx(129.6)
    assert probe["rolling_high_20"] > bars[35].high


def test_derived_indicator_batch_writer_coalesces_delta_writes(tmp_path) -> None:
    bars = [_view(ts_index=index, close=80.0 + index * 0.25) for index in range(32)]
    indicators = [_indicator_row(ts_index=index, close=80.0 + index * 0.25) for index in range(32)]
    rows = build_derived_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="contract",
    )
    row_batches = [rows[index : index + 8] for index in range(0, len(rows), 8)]

    paths, row_count, write_batch_count = write_derived_indicator_frame_batches(
        output_dir=tmp_path,
        row_batches=row_batches,
        max_rows_per_delta_write=15,
    )
    reloaded = load_derived_indicator_frames(
        output_dir=tmp_path,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )

    assert row_count == len(rows)
    assert write_batch_count == 2
    assert len(reloaded) == len(rows)
    assert (tmp_path / "research_derived_indicator_frames.delta" / "_delta_log").exists()
    assert "research_derived_indicator_frames" in paths


def test_derived_indicator_build_produces_wide_v2_values_and_causal_mtf_overlay() -> None:
    bars_15m = [
        _view(ts_index=index, close=90.0 + index * 0.05 + ((index % 16) - 8) * 0.06, timeframe="15m")
        for index in range(1200)
    ]
    bars_1h = [
        _view(ts_index=index, close=91.0 + index * 0.18 + ((index % 8) - 4) * 0.08, timeframe="1h")
        for index in range(300)
    ]
    bars_4h = [
        _view(ts_index=index, close=92.0 + index * 0.42 + ((index % 6) - 3) * 0.12, timeframe="4h")
        for index in range(80)
    ]
    bars = [*bars_15m, *bars_1h, *bars_4h]
    indicators = build_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
    )

    rows = build_derived_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="contract",
    )

    indicator_15m = [row for row in indicators if row.timeframe == "15m"]
    indicator_1h = [row for row in indicators if row.timeframe == "1h"]
    indicator_4h = [row for row in indicators if row.timeframe == "4h"]
    rows_15m = [row for row in rows if row.timeframe == "15m"]
    tail = rows_15m[-1]
    for column in (
        "session_vwap",
        "opening_range_high",
        "swing_high_10",
        "distance_to_sma_200_atr",
        "distance_to_ema_200_atr",
        "distance_to_vwma_200_atr",
        "donchian_position_55",
        "cross_close_sma_20_code",
        "macd_signal_cross_code",
        "trix_signal_cross_code",
        "divergence_price_rsi_14_score",
        "divergence_price_pvt_score",
        "mtf_1h_to_15m_ema_20",
        "mtf_1h_to_15m_ema_50",
        "mtf_1h_to_15m_rsi_14",
        "mtf_4h_to_15m_ema_20",
        "mtf_4h_to_15m_ema_50",
        "mtf_4h_to_15m_adx_14",
        "mtf_4h_to_15m_rsi_14",
    ):
        assert tail.values[column] is not None
    assert tail.values["sma_20_slope_5"] == pytest.approx(
        _angle_slope(indicator_15m[-1].values["sma_20"], indicator_15m[-6].values["sma_20"], length=5)
    )
    assert tail.values["ema_20_slope_5"] == pytest.approx(
        _angle_slope(indicator_15m[-1].values["ema_20"], indicator_15m[-6].values["ema_20"], length=5)
    )
    assert tail.values["mtf_1h_to_15m_ema_20"] == pytest.approx(indicator_1h[-1].values["ema_20"])
    assert tail.values["mtf_1h_to_15m_ema_50"] == pytest.approx(indicator_1h[-1].values["ema_50"])
    current_close_ts = pd.Timestamp(tail.ts) + pd.Timedelta(minutes=15)
    source_4h = [
        row
        for row in indicator_4h
        if pd.Timestamp(row.ts) + pd.Timedelta(hours=4) <= current_close_ts
    ][-1]
    assert tail.values["mtf_4h_to_15m_ema_20"] == pytest.approx(source_4h.values["ema_20"])
    assert tail.values["mtf_4h_to_15m_adx_14"] == pytest.approx(source_4h.values["adx_14"])
    assert "mtf_4h_to_15m_donchian_high_55" not in tail.values
    assert tail.null_warmup_span < tail.row_count


def test_continuous_front_derived_rows_keep_chronological_timestamp_binding_after_roll() -> None:
    old_bar = _view(ts_index=0, close=100.0)
    old_bar = replace(
        old_bar,
        contract_id="Z-OLD",
        active_contract_id="Z-OLD",
        series_mode="continuous_front",
        native_open=old_bar.open,
        native_high=old_bar.high,
        native_low=old_bar.low,
        native_close=old_bar.close,
    )
    new_bar = _view(ts_index=1, close=105.0)
    new_bar = replace(
        new_bar,
        contract_id="A-NEW",
        active_contract_id="A-NEW",
        series_mode="continuous_front",
        roll_epoch=1,
        roll_event_id="roll-1",
        is_roll_bar=True,
        is_first_bar_after_roll=True,
        native_open=new_bar.open,
        native_high=new_bar.high,
        native_low=new_bar.low,
        native_close=new_bar.close,
    )
    indicators = [
        replace(_indicator_row(ts_index=0, close=100.0), contract_id=old_bar.contract_id),
        replace(_indicator_row(ts_index=1, close=105.0), contract_id=new_bar.contract_id),
    ]

    rows = build_derived_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=[old_bar, new_bar],
        indicator_rows=indicators,
        series_mode="continuous_front",
        adjustment_ladder_rows=(
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "additive_gap": 0.0,
            },
        ),
    )

    assert [row.ts for row in rows] == [old_bar.ts, new_bar.ts]
    values_by_ts = {row.ts: row.values for row in rows}
    assert values_by_ts[old_bar.ts]["session_position"] == pytest.approx(
        (old_bar.close - old_bar.low) / (old_bar.high - old_bar.low)
    )
    assert values_by_ts[new_bar.ts]["session_position"] == pytest.approx(
        (new_bar.close - old_bar.low) / (new_bar.high - old_bar.low)
    )


def test_derived_indicator_edge_rules_avoid_misleading_signals() -> None:
    closes = [100.0] * 20 + [100.1, 100.2, 102.0, 102.1, 102.2, 102.3, 102.4, 102.5]
    bars = [_view(ts_index=index, close=close) for index, close in enumerate(closes)]
    indicators = [_indicator_row(ts_index=index, close=close) for index, close in enumerate(closes)]
    for index, row in enumerate(indicators):
        row.values["rsi_14"] = 50.0 + float(index % 10)
    indicators[22].values["rsi_14"] = 52.0
    indicators[-1].values.update(
        {
            "rvol_20": 1.25,
            "volume_z_20": -0.6,
            "rsi_14": 25.0,
            "cci_20": 150.0,
            "willr_14": -50.0,
        }
    )

    rows = build_derived_indicator_frames(
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        bar_views=bars,
        indicator_rows=indicators,
        series_mode="contract",
    )

    assert rows[22].values["cross_close_rolling_high_20_code"] == 1
    assert rows[22].values["divergence_price_rsi_14_score"] == pytest.approx(
        _angle_slope(102.0, 100.0, length=20) - _angle_slope(52.0, 52.0, length=20)
    )
    assert "session_volume_state_code" not in rows[-1].values
    assert "oscillator_pressure_code" not in rows[-1].values
