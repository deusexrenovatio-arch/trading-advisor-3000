from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DerivedIndicatorProfile:
    version: str
    description: str
    output_columns: tuple[str, ...]
    warmup_bars: int


@dataclass(frozen=True)
class DerivedIndicatorProfileRegistry:
    profiles: tuple[DerivedIndicatorProfile, ...]

    def get(self, version: str) -> DerivedIndicatorProfile:
        for profile in self.profiles:
            if profile.version == version:
                return profile
        raise KeyError(f"unknown derived indicator profile version: {version}")

    def versions(self) -> tuple[str, ...]:
        return tuple(profile.version for profile in self.profiles)


MTF_MAPPINGS: tuple[tuple[str, str], ...] = (
    ("1h", "15m"),
    ("4h", "15m"),
    ("4h", "1h"),
    ("1d", "15m"),
    ("1d", "1h"),
    ("1d", "4h"),
)

MTF_TREND_CONTEXT_COLUMNS: tuple[str, ...] = ("ema_20", "ema_50", "adx_14", "rsi_14")


def mtf_carried_columns(source_timeframe: str, target_timeframe: str) -> tuple[str, ...]:
    return MTF_TREND_CONTEXT_COLUMNS


def mtf_column_name(source_timeframe: str, target_timeframe: str, source_column: str) -> str:
    return f"mtf_{source_timeframe}_to_{target_timeframe}_{source_column}"


def mtf_projected_columns() -> tuple[str, ...]:
    return tuple(
        mtf_column_name(source_timeframe, target_timeframe, source_column)
        for source_timeframe, target_timeframe in MTF_MAPPINGS
        for source_column in mtf_carried_columns(source_timeframe, target_timeframe)
    )


WIDE_TECHNICAL_GOLD_V2_DERIVED_COLUMNS: tuple[str, ...] = (
    "rolling_high_20",
    "rolling_low_20",
    "session_high",
    "session_low",
    "week_high",
    "week_low",
    "opening_range_high",
    "opening_range_low",
    "swing_high_10",
    "swing_low_10",
    "session_vwap",
    "distance_to_session_vwap",
    "distance_to_session_high",
    "distance_to_session_low",
    "distance_to_week_high",
    "distance_to_week_low",
    "distance_to_rolling_high_20",
    "distance_to_rolling_low_20",
    "distance_to_sma_20_atr",
    "distance_to_sma_50_atr",
    "distance_to_sma_100_atr",
    "distance_to_sma_200_atr",
    "distance_to_ema_20_atr",
    "distance_to_ema_50_atr",
    "distance_to_ema_100_atr",
    "distance_to_ema_200_atr",
    "distance_to_hma_20_atr",
    "distance_to_hma_100_atr",
    "distance_to_hma_200_atr",
    "distance_to_vwma_20_atr",
    "distance_to_vwma_100_atr",
    "distance_to_vwma_200_atr",
    "distance_to_bb_upper_20_2_atr",
    "distance_to_bb_lower_20_2_atr",
    "distance_to_kc_upper_20_1_5_atr",
    "distance_to_kc_lower_20_1_5_atr",
    "distance_to_donchian_high_20_atr",
    "distance_to_donchian_low_20_atr",
    "distance_to_donchian_high_55_atr",
    "distance_to_donchian_low_55_atr",
    "rolling_position_20",
    "session_position",
    "week_position",
    "bb_position_20_2",
    "kc_position_20_1_5",
    "donchian_position_20",
    "donchian_position_55",
    "cross_close_sma_20_code",
    "cross_close_ema_20_code",
    "cross_close_session_vwap_code",
    "cross_close_rolling_high_20_code",
    "cross_close_rolling_low_20_code",
    "macd_signal_cross_code",
    "ppo_signal_cross_code",
    "trix_signal_cross_code",
    "kst_signal_cross_code",
    "close_change_1",
    "close_slope_20",
    "sma_20_slope_5",
    "ema_20_slope_5",
    "roc_10_change_1",
    "mom_10_change_1",
    "volume_change_1",
    "oi_change_1",
    "rvol_20",
    "volume_zscore_20",
    "price_volume_corr_20",
    "price_oi_corr_20",
    "volume_oi_corr_20",
    "divergence_price_rsi_14_score",
    "divergence_price_stoch_k_14_3_3_score",
    "divergence_price_stoch_d_14_3_3_score",
    "divergence_price_cci_20_score",
    "divergence_price_willr_14_score",
    "divergence_price_stochrsi_k_14_14_3_3_score",
    "divergence_price_stochrsi_d_14_14_3_3_score",
    "divergence_price_ultimate_oscillator_7_14_28_score",
    "divergence_price_macd_hist_12_26_9_score",
    "divergence_price_ppo_hist_12_26_9_score",
    "divergence_price_tsi_25_13_score",
    "divergence_price_roc_10_score",
    "divergence_price_obv_score",
    "divergence_price_mfi_14_score",
    "divergence_price_cmf_20_score",
    "divergence_price_ad_score",
    "divergence_price_adosc_3_10_score",
    "divergence_price_force_index_13_score",
    "divergence_price_pvt_score",
    "divergence_price_pvo_12_26_9_score",
    "divergence_price_pvo_hist_12_26_9_score",
    "divergence_price_oi_change_1_score",
    "divergence_price_oi_roc_10_score",
    "divergence_price_oi_z_20_score",
    "divergence_price_oi_relative_activity_20_score",
    *mtf_projected_columns(),
)


def core_v1_derived_indicator_profile() -> DerivedIndicatorProfile:
    return DerivedIndicatorProfile(
        version="core_v1",
        description="Wide Technical Gold V2 derived indicator profile over bars and base indicators.",
        output_columns=WIDE_TECHNICAL_GOLD_V2_DERIVED_COLUMNS,
        warmup_bars=300,
    )


def build_derived_indicator_profile_registry() -> DerivedIndicatorProfileRegistry:
    return DerivedIndicatorProfileRegistry(profiles=(core_v1_derived_indicator_profile(),))


def current_derived_indicator_profile() -> DerivedIndicatorProfile:
    return core_v1_derived_indicator_profile()
