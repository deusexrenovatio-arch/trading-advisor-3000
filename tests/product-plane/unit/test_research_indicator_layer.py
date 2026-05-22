from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS
from trading_advisor_3000.product_plane.research.continuous_front_indicators import (
    default_indicator_roll_rules,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.indicators import (
    VOLUME_PROFILE_INDICATOR_COLUMNS,
    IndicatorFrameRow,
    IndicatorParameter,
    IndicatorProfile,
    IndicatorSpec,
    build_indicator_frames,
    build_indicator_profile_registry,
    compute_volume_profile_features,
    default_indicator_profile,
    indicator_store_contract,
)
from trading_advisor_3000.product_plane.research.indicators.materialize import _timeframe_delta
from trading_advisor_3000.product_plane.research.indicators.volume_profile import (
    _price_to_tick_floor,
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


def _minute_row(*, minute_index: int, price: float, volume: int) -> dict[str, object]:
    ts_open = datetime(2026, 3, 16, 9, 0, tzinfo=UTC) + timedelta(minutes=minute_index)
    ts_close = ts_open + timedelta(minutes=1)
    return {
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "1m",
        "ts_open": ts_open.isoformat().replace("+00:00", "Z"),
        "ts_close": ts_close.isoformat().replace("+00:00", "Z"),
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": volume,
    }


def _production_raw_1m_row(*, minute_index: int, price: float, volume: int) -> dict[str, object]:
    ts_open = datetime(2026, 3, 16, 9, 0, tzinfo=UTC) + timedelta(minutes=minute_index)
    ts_close = ts_open + timedelta(minutes=1)
    return {
        "internal_id": "FUT_BR",
        "finam_symbol": "BRM6@MOEX",
        "moex_engine": "futures",
        "moex_market": "forts",
        "moex_board": "RFUD",
        "moex_secid": "BRM6",
        "asset_group": "BR",
        "timeframe": "1m",
        "source_interval": 1,
        "ts_open": ts_open.isoformat().replace("+00:00", "Z"),
        "ts_close": ts_close.isoformat().replace("+00:00", "Z"),
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": volume,
        "open_interest": 20_000 + minute_index,
        "ingest_run_id": "test-raw-ingest",
        "ingested_at_utc": "2026-03-16T09:30:00Z",
        "provenance_json": "{}",
    }


def _continuous_view(
    *,
    ts_index: int,
    close: float,
    roll_epoch: int,
    active_contract_id: str,
    previous_contract_id: str | None = None,
    is_roll_bar: bool = False,
) -> ResearchBarView:
    row = _view(ts_index=ts_index, close=close)
    return ResearchBarView(
        **{
            **row.to_dict(),
            "contract_id": active_contract_id,
            "active_contract_id": active_contract_id,
            "series_id": "BR|15m|continuous_front",
            "series_mode": "continuous_front",
            "roll_epoch": roll_epoch,
            "roll_event_id": "roll-1" if roll_epoch else None,
            "is_roll_bar": is_roll_bar,
            "is_first_bar_after_roll": is_roll_bar,
            "bars_since_roll": 0 if is_roll_bar else ts_index,
            "price_space": "continuous_backward_current_anchor_additive",
            "native_open": close - 0.2,
            "native_high": close + 0.5,
            "native_low": close - 0.6,
            "native_close": close,
            "continuous_open": close - 0.2,
            "continuous_high": close + 0.5,
            "continuous_low": close - 0.6,
            "continuous_close": close,
            "execution_open": close - 0.2,
            "execution_high": close + 0.5,
            "execution_low": close - 0.6,
            "execution_close": close,
            "previous_contract_id": previous_contract_id,
            "adjustment_mode": "additive",
            "cumulative_additive_offset": 0.0,
        }
    )


def _sma2_profile() -> IndicatorProfile:
    return IndicatorProfile(
        version="sma2_test",
        description="Two-bar SMA test profile",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_2",
                category="trend",
                operation_key="sma",
                parameters=(IndicatorParameter(name="length", value=2),),
                required_input_columns=("close",),
                output_columns=("sma_2",),
                warmup_bars=2,
            ),
        ),
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
    assert profile.required_input_columns() == (
        "close",
        "high",
        "low",
        "true_range",
        "log_ret_1",
        "volume",
        "open_interest",
    )
    assert profile.max_warmup_bars() == 200


def test_indicator_profile_covers_wide_technical_gold_v2_base_columns() -> None:
    profile = default_indicator_profile()
    columns = {column for spec in profile.indicators for column in spec.output_columns}
    assert EXPECTED_WIDE_TECHNICAL_GOLD_V2_BASE_COLUMNS <= columns
    assert not EXPECTED_WIDE_TECHNICAL_GOLD_V2_BASE_COLUMNS - set(
        indicator_store_contract()["research_indicator_frames"]["columns"]
    )


def test_indicator_profile_contains_volume_profile_as_base_outputs() -> None:
    profile = default_indicator_profile()
    columns = {column for spec in profile.indicators for column in spec.output_columns}
    contract_columns = indicator_store_contract()["research_indicator_frames"]["columns"]
    volume_profile_spec = next(
        spec for spec in profile.indicators if spec.indicator_id == "volume_profile"
    )
    intraday_profile = build_indicator_profile_registry().get("core_intraday_v1")
    intraday_columns = {
        column for spec in intraday_profile.indicators for column in spec.output_columns
    }

    assert set(VOLUME_PROFILE_INDICATOR_COLUMNS) <= columns
    assert set(VOLUME_PROFILE_INDICATOR_COLUMNS) <= intraday_columns
    assert volume_profile_spec.warmup_bars == 0
    assert contract_columns["vp_cluster_count"] == "int"
    assert contract_columns["vp_quality_code"] == "int"
    assert contract_columns["vp_poc_price"] == "double"


def test_volume_profile_timeframe_delta_supports_weekly_windows() -> None:
    assert _timeframe_delta("1w") == timedelta(weeks=1)
    assert _timeframe_delta("w") == timedelta(weeks=1)


def test_volume_profile_roll_rule_uses_price_volume_roll_aware_group() -> None:
    profile = IndicatorProfile(
        version="volume-profile-roll-group-test",
        description="Volume profile rule routing proof.",
        indicators=(
            IndicatorSpec(
                indicator_id="volume_profile",
                category="volume",
                operation_key="volume_profile",
                parameters=(),
                required_input_columns=("low", "high", "volume"),
                output_columns=("vp_poc_price",),
                warmup_bars=1,
            ),
        ),
    )

    rules = default_indicator_roll_rules(indicator_profile=profile, derived_profile=None)
    rule = next(rule for rule in rules if rule.output_column == "vp_poc_price")

    assert rule.calculation_group_id == "price_volume_roll_aware"


def test_volume_profile_floor_quantization_keeps_aligned_prices_in_bucket() -> None:
    assert _price_to_tick_floor(0.3, 0.1) == 3


def test_volume_profile_features_preserve_multimodal_shape_and_quality_codes() -> None:
    minute_bars = [
        {"low": 100.0, "high": 100.0, "volume": 80},
        {"low": 101.0, "high": 101.0, "volume": 120},
        {"low": 102.0, "high": 102.0, "volume": 220},
        {"low": 110.0, "high": 110.0, "volume": 150},
        {"low": 111.0, "high": 111.0, "volume": 210},
        {"low": 112.0, "high": 112.0, "volume": 180},
    ]

    features = compute_volume_profile_features(
        minute_bars,
        tick_size=1.0,
        target_volume=960.0,
        expected_source_bars=6,
    )

    assert set(VOLUME_PROFILE_INDICATOR_COLUMNS) == set(features)
    assert features["vp_quality_code"] == 0
    assert features["vp_shape_code"] == 2
    assert features["vp_cluster_count"] == 2
    assert features["vp_poc_price"] == 102.0
    assert features["vp_secondary_cluster_volume_share"] is not None
    assert features["vp_cluster_separation_ticks"] == 9
    assert features["vp_volume_conservation_ratio"] == pytest.approx(1.0)


def test_volume_profile_conservation_ratio_stays_finite_when_target_volume_is_zero() -> None:
    features = compute_volume_profile_features(
        [{"low": 100.0, "high": 100.0, "volume": 10.0}],
        tick_size=1.0,
        target_volume=0.0,
        expected_source_bars=1,
    )

    assert math.isfinite(float(features["vp_volume_conservation_ratio"]))
    assert features["vp_quality_code"] == 3


def test_volume_profile_spans_floor_low_and_ceil_high_ticks() -> None:
    features = compute_volume_profile_features(
        [{"low": 100.26, "high": 100.74, "volume": 90}],
        tick_size=0.5,
        target_volume=90.0,
        expected_source_bars=1,
    )

    assert features["vp_quality_code"] == 0
    assert features["vp_poc_price"] == 100.0
    assert features["vp_value_area_low"] == 100.0
    assert features["vp_value_area_high"] == 101.0


def test_volume_profile_keeps_zero_range_off_grid_bar_in_one_tick() -> None:
    features = compute_volume_profile_features(
        [{"low": 100.26, "high": 100.26, "volume": 90}],
        tick_size=0.5,
        target_volume=90.0,
        expected_source_bars=1,
    )

    assert features["vp_quality_code"] == 0
    assert features["vp_poc_price"] == 100.0
    assert features["vp_value_area_low"] == 100.0
    assert features["vp_value_area_high"] == 100.0


def test_indicator_row_rejects_fractional_volume_profile_int_columns() -> None:
    payload = {
        "dataset_version": "dataset-v1",
        "indicator_set_version": "indicators-v1",
        "profile_version": "core_v1",
        "contract_id": "BR-6.26",
        "instrument_id": "BR",
        "timeframe": "15m",
        "ts": "2026-04-01T10:00:00Z",
        "source_bars_hash": "bars-hash",
        "row_count": 1,
        "warmup_span": 0,
        "null_warmup_span": 0,
        "created_at": "2026-04-01T10:00:00Z",
        "output_columns_hash": "columns-hash",
        "vp_cluster_count": "1.5",
    }

    with pytest.raises(ValueError, match="vp_cluster_count"):
        IndicatorFrameRow.from_dict(payload)

    payload["vp_cluster_count"] = "2.0"
    assert IndicatorFrameRow.from_dict(payload).values["vp_cluster_count"] == 2


def test_volume_profile_quality_nulls_profile_when_raw_1m_coverage_is_low() -> None:
    features = compute_volume_profile_features(
        [
            {"low": 100.0, "high": 100.0, "volume": 100},
            {"low": 101.0, "high": 101.0, "volume": 100},
        ],
        tick_size=1.0,
        target_volume=200.0,
        expected_source_bars=10,
    )

    assert features["vp_quality_code"] == 2
    assert features["vp_shape_code"] == 0
    assert features["vp_poc_price"] is None
    assert features["vp_source_1m_coverage_ratio"] == pytest.approx(0.2)


def test_volume_profile_value_area_uses_unsmoothed_histogram() -> None:
    features = compute_volume_profile_features(
        [
            {"low": 100.0, "high": 100.0, "volume": 10},
            {"low": 101.0, "high": 101.0, "volume": 80},
            {"low": 102.0, "high": 102.0, "volume": 10},
        ],
        tick_size=1.0,
        target_volume=100.0,
        expected_source_bars=3,
    )

    assert features["vp_poc_price"] == 101.0
    assert features["vp_value_area_low"] == 101.0
    assert features["vp_value_area_high"] == 101.0
    assert features["vp_cluster_count"] == 1


def test_core_v1_indicator_build_materializes_volume_profile_from_raw_1m_rows() -> None:
    bars = [_view(ts_index=0, close=100.0, volume=1000)]
    minute_rows = [
        *[_minute_row(minute_index=index, price=100.0, volume=100) for index in range(5)],
        *[_minute_row(minute_index=index, price=105.0, volume=20) for index in range(5, 10)],
        *[_minute_row(minute_index=index, price=110.0, volume=80) for index in range(10, 15)],
    ]

    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
        volume_profile_source_rows={("BR", "BR-6.26"): minute_rows},
        volume_profile_tick_size_by_instrument={"BR": 1.0},
    )

    payload = rows[0].values
    assert payload["vp_quality_code"] == 0
    assert payload["vp_source_1m_coverage_ratio"] == pytest.approx(1.0)
    assert payload["vp_volume_conservation_ratio"] == pytest.approx(1.0)
    assert payload["vp_poc_price"] == 100.0
    assert payload["vp_cluster_count"] >= 1
    assert (
        rows[0].source_bars_hash
        != build_indicator_frames(
            dataset_version="dataset-v3",
            indicator_set_version="indicators-v1",
            bar_views=bars,
            series_mode="contract",
            volume_profile_source_rows={("BR", "BR-6.26"): minute_rows[:-1]},
            volume_profile_tick_size_by_instrument={"BR": 1.0},
        )[0].source_bars_hash
    )


def test_volume_profile_source_hash_is_stable_for_row_order_and_tick_size() -> None:
    bars = [_view(ts_index=0, close=100.0, volume=300)]
    minute_rows = [
        _minute_row(minute_index=0, price=100.0, volume=100),
        _minute_row(minute_index=1, price=100.5, volume=100),
        _minute_row(minute_index=2, price=101.0, volume=100),
    ]

    base_hash = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
        volume_profile_source_rows={("BR", "BR-6.26"): minute_rows},
        volume_profile_tick_size_by_instrument={"BR": 0.5},
    )[0].source_bars_hash
    shuffled_hash = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
        volume_profile_source_rows={("BR", "BR-6.26"): list(reversed(minute_rows))},
        volume_profile_tick_size_by_instrument={"BR": 0.5},
    )[0].source_bars_hash
    different_tick_hash = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
        volume_profile_source_rows={("BR", "BR-6.26"): minute_rows},
        volume_profile_tick_size_by_instrument={"BR": 0.25},
    )[0].source_bars_hash

    assert shuffled_hash == base_hash
    assert different_tick_hash != base_hash


def test_core_v1_indicator_build_reads_production_raw_moex_schema(tmp_path) -> None:
    raw_path = tmp_path / "raw_moex_history.delta"
    minute_rows = [
        *[
            _production_raw_1m_row(minute_index=index, price=100.0, volume=100)
            for index in range(5)
        ],
        *[
            _production_raw_1m_row(minute_index=index, price=100.03, volume=20)
            for index in range(5, 10)
        ],
        *[
            _production_raw_1m_row(minute_index=index, price=100.07, volume=80)
            for index in range(10, 15)
        ],
    ]
    write_delta_table_rows(table_path=raw_path, rows=minute_rows, columns=RAW_COLUMNS)

    target_volume = sum(int(row["volume"]) for row in minute_rows)
    bars = [
        replace(
            _view(ts_index=0, close=100.0),
            contract_id="BRM6@MOEX",
            instrument_id="FUT_BR",
            active_contract_id="BRM6@MOEX",
            volume=target_volume,
        )
    ]

    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
        volume_profile_raw_1m_table_path=raw_path,
        volume_profile_tick_size_by_instrument={"FUT_BR": 0.01},
    )

    payload = rows[0].values
    assert payload["vp_quality_code"] == 0
    assert payload["vp_source_1m_coverage_ratio"] == pytest.approx(1.0)
    assert payload["vp_volume_conservation_ratio"] == pytest.approx(1.0)
    assert payload["vp_poc_price"] == 100.0


def test_core_v1_indicator_build_reads_canonical_1m_source_schema(tmp_path) -> None:
    canonical_path = tmp_path / "canonical_bars.delta"
    minute_rows = []
    for index in range(15):
        raw_row = _production_raw_1m_row(minute_index=index, price=100.0, volume=100)
        minute_rows.append(
            {
                "contract_id": raw_row["finam_symbol"],
                "instrument_id": raw_row["internal_id"],
                "timeframe": "1m",
                "ts": raw_row["ts_open"],
                "open": raw_row["open"],
                "high": raw_row["high"],
                "low": raw_row["low"],
                "close": raw_row["close"],
                "volume": raw_row["volume"],
                "open_interest": raw_row["open_interest"],
            }
        )
    write_delta_table_rows(
        table_path=canonical_path,
        rows=minute_rows,
        columns={
            "contract_id": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "ts": "timestamp",
            "open": "double",
            "high": "double",
            "low": "double",
            "close": "double",
            "volume": "bigint",
            "open_interest": "bigint",
        },
    )

    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[
            replace(
                _view(ts_index=0, close=100.0),
                contract_id="BRM6@MOEX",
                instrument_id="FUT_BR",
                active_contract_id="BRM6@MOEX",
                volume=sum(int(row["volume"]) for row in minute_rows),
            )
        ],
        series_mode="contract",
        volume_profile_raw_1m_table_path=canonical_path,
        volume_profile_tick_size_by_instrument={"FUT_BR": 0.01},
    )

    payload = rows[0].values
    assert payload["vp_quality_code"] == 0
    assert payload["vp_source_1m_coverage_ratio"] == pytest.approx(1.0)
    assert payload["vp_volume_conservation_ratio"] == pytest.approx(1.0)


def test_core_v1_indicator_build_reads_raw_1m_table_keyed_by_ts(tmp_path) -> None:
    raw_path = tmp_path / "raw_moex_history_ts.delta"
    minute_rows = []
    for index in range(15):
        row = _production_raw_1m_row(minute_index=index, price=100.0, volume=100)
        row["ts"] = row.pop("ts_open")
        minute_rows.append(row)
    write_delta_table_rows(
        table_path=raw_path,
        rows=minute_rows,
        columns={
            **{key: value for key, value in RAW_COLUMNS.items() if key != "ts_open"},
            "ts": "timestamp",
        },
    )

    bars = [
        replace(
            _view(ts_index=0, close=100.0),
            contract_id="BRM6@MOEX",
            instrument_id="FUT_BR",
            active_contract_id="BRM6@MOEX",
            volume=sum(int(row["volume"]) for row in minute_rows),
        )
    ]

    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=bars,
        series_mode="contract",
        volume_profile_raw_1m_table_path=raw_path,
        volume_profile_tick_size_by_instrument={"FUT_BR": 0.01},
    )

    assert rows[0].values["vp_quality_code"] == 0
    assert rows[0].values["vp_source_1m_coverage_ratio"] == pytest.approx(1.0)


def test_core_v1_indicator_build_prefers_raw_ts_open_when_both_timestamp_columns_exist(
    tmp_path,
) -> None:
    raw_path = tmp_path / "raw_moex_history_both_timestamps.delta"
    minute_rows = []
    for index in range(15):
        row = _production_raw_1m_row(minute_index=index, price=100.0, volume=100)
        row["ts"] = "1970-01-01T00:00:00Z"
        minute_rows.append(row)
    write_delta_table_rows(
        table_path=raw_path,
        rows=minute_rows,
        columns={**RAW_COLUMNS, "ts": "timestamp"},
    )

    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[
            replace(
                _view(ts_index=0, close=100.0),
                contract_id="BRM6@MOEX",
                instrument_id="FUT_BR",
                active_contract_id="BRM6@MOEX",
                volume=sum(int(row["volume"]) for row in minute_rows),
            )
        ],
        series_mode="contract",
        volume_profile_raw_1m_table_path=raw_path,
        volume_profile_tick_size_by_instrument={"FUT_BR": 0.01},
    )

    assert rows[0].values["vp_quality_code"] == 0
    assert rows[0].values["vp_source_1m_coverage_ratio"] == pytest.approx(1.0)


def test_core_v1_indicator_build_marks_volume_profile_no_source_without_raw_config() -> None:
    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[_view(ts_index=0, close=100.0)],
        series_mode="contract",
    )

    payload = rows[0].values
    assert payload["vp_quality_code"] == 1
    assert payload["vp_poc_price"] is None


def test_core_v1_indicator_build_marks_volume_profile_no_source_for_empty_source_rows() -> None:
    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[_view(ts_index=0, close=100.0)],
        series_mode="contract",
        volume_profile_source_rows={("BR", "BR-6.26"): []},
    )

    payload = rows[0].values
    assert payload["vp_quality_code"] == 1
    assert payload["vp_poc_price"] is None


def test_continuous_front_volume_profile_keeps_native_price_levels() -> None:
    minute_rows = [
        *[_minute_row(minute_index=index, price=100.0, volume=60) for index in range(10)],
        *[_minute_row(minute_index=index, price=100.0, volume=80) for index in range(10, 15)],
    ]

    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[
            _continuous_view(
                ts_index=0,
                close=110.0,
                roll_epoch=1,
                active_contract_id="BR-6.26",
                previous_contract_id="BR-3.26",
                is_roll_bar=True,
            )
        ],
        series_mode="continuous_front",
        volume_profile_source_rows={("BR", "BR-6.26"): minute_rows},
        volume_profile_tick_size_by_instrument={"BR": 1.0},
        adjustment_ladder_rows=(
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:00:00Z",
                "additive_gap": 10.0,
            },
        ),
    )

    payload = rows[0].values
    assert payload["vp_quality_code"] == 0
    assert payload["vp_poc_price"] == 100.0
    assert payload["vp_value_area_low"] == 100.0
    assert payload["vp_value_area_high"] == 100.0


def test_indicator_store_contract_contains_stage3_metadata_columns() -> None:
    contract = indicator_store_contract()
    columns = contract["research_indicator_frames"]["columns"]
    assert {
        "dataset_version",
        "contour_id",
        "series_mode",
        "series_id",
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
    assert contract["research_indicator_frames"]["partition_by"] == [
        "dataset_version",
        "contour_id",
        "indicator_set_version",
        "instrument_id",
        "timeframe",
    ]
    assert (
        "unique(dataset_version, contour_id, series_mode, series_id, "
        "indicator_set_version, timeframe, ts)"
        in contract["research_indicator_frames"]["constraints"]
    )


def test_indicator_build_is_point_in_time_safe_for_existing_prefix() -> None:
    base_views = [
        _view(ts_index=index, close=80.0 + index * 0.4, volume=1000 + index * 15)
        for index in range(60)
    ]
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

    ignored = {
        "source_bars_hash",
        "source_dataset_bars_hash",
        "row_count",
        "null_warmup_span",
        "created_at",
    }
    base_payload = [
        {k: v for k, v in row.to_dict().items() if k not in ignored} for row in base_rows
    ]
    extended_payload = [
        {k: v for k, v in row.to_dict().items() if k not in ignored}
        for row in extended_rows[: len(base_rows)]
    ]
    assert base_payload == extended_payload


def test_continuous_front_indicators_apply_ladder_as_of_current_roll_epoch() -> None:
    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[
            _continuous_view(ts_index=0, close=100.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
            _continuous_view(ts_index=1, close=101.0, roll_epoch=0, active_contract_id="BRK2@MOEX"),
            _continuous_view(
                ts_index=2,
                close=112.0,
                roll_epoch=1,
                active_contract_id="BRM2@MOEX",
                previous_contract_id="BRK2@MOEX",
                is_roll_bar=True,
            ),
            _continuous_view(ts_index=3, close=113.0, roll_epoch=1, active_contract_id="BRM2@MOEX"),
        ],
        series_mode="continuous_front",
        profile=_sma2_profile(),
        adjustment_ladder_rows=(
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "effective_ts": "2026-03-16T09:30:00Z",
                "additive_gap": 10.0,
            },
        ),
    )

    values_by_ts = {row.ts: row.values["sma_2"] for row in rows}
    assert values_by_ts["2026-03-16T09:15:00Z"] == pytest.approx(100.5)
    assert values_by_ts["2026-03-16T09:30:00Z"] == pytest.approx(111.5)
    assert values_by_ts["2026-03-16T09:45:00Z"] == pytest.approx(112.5)
    assert [row.contract_id for row in rows] == [
        "BRK2@MOEX",
        "BRK2@MOEX",
        "BRM2@MOEX",
        "BRM2@MOEX",
    ]
    assert rows[0].partition_key(series_mode="continuous_front").contract_id is None


def test_continuous_front_indicators_require_ladder_for_rolled_series() -> None:
    with pytest.raises(ValueError, match="adjustment ladder"):
        build_indicator_frames(
            dataset_version="dataset-v3",
            indicator_set_version="indicators-v1",
            bar_views=[
                _continuous_view(
                    ts_index=0, close=100.0, roll_epoch=0, active_contract_id="BRK2@MOEX"
                ),
                _continuous_view(
                    ts_index=1,
                    close=112.0,
                    roll_epoch=1,
                    active_contract_id="BRM2@MOEX",
                    previous_contract_id="BRK2@MOEX",
                    is_roll_bar=True,
                ),
            ],
            series_mode="continuous_front",
            profile=_sma2_profile(),
        )


def test_indicator_build_produces_real_values_and_null_warmup_span() -> None:
    rows = build_indicator_frames(
        dataset_version="dataset-v3",
        indicator_set_version="indicators-v1",
        bar_views=[
            _view(ts_index=index, close=90.0 + index * 0.3, volume=900 + index * 10)
            for index in range(70)
        ],
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
    assert tail.values["close_slope_20"] == pytest.approx(math.degrees(math.atan(0.3)))
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
