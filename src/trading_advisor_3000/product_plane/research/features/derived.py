from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FeatureParameter:
    name: str
    value: object


@dataclass(frozen=True)
class FeatureSpec:
    feature_id: str
    category: str
    operation_key: str
    parameters: tuple[FeatureParameter, ...]
    required_columns: tuple[str, ...]
    output_columns: tuple[str, ...]
    warmup_bars: int
    optional: bool = False

    def params_dict(self) -> dict[str, object]:
        return {item.name: item.value for item in self.parameters}


@dataclass(frozen=True)
class FeatureProfile:
    version: str
    description: str
    features: tuple[FeatureSpec, ...]

    def by_category(self) -> dict[str, tuple[FeatureSpec, ...]]:
        grouped: dict[str, list[FeatureSpec]] = {}
        for spec in self.features:
            grouped.setdefault(spec.category, []).append(spec)
        return {key: tuple(value) for key, value in grouped.items()}

    def required_columns(self) -> tuple[str, ...]:
        seen: list[str] = []
        for spec in self.features:
            for column in spec.required_columns:
                if column not in seen:
                    seen.append(column)
        return tuple(seen)

    def expected_output_columns(self) -> tuple[str, ...]:
        columns: list[str] = []
        for spec in self.features:
            columns.extend(spec.output_columns)
        return tuple(columns)

    def max_warmup_bars(self) -> int:
        return max(spec.warmup_bars for spec in self.features)


@dataclass(frozen=True)
class FeatureProfileRegistry:
    profiles: tuple[FeatureProfile, ...]

    def get(self, version: str) -> FeatureProfile:
        for profile in self.profiles:
            if profile.version == version:
                return profile
        raise KeyError(f"unknown feature profile version: {version}")

    def versions(self) -> tuple[str, ...]:
        return tuple(profile.version for profile in self.profiles)


def _p(name: str, value: object) -> FeatureParameter:
    return FeatureParameter(name=name, value=value)


def _spec(
    *,
    feature_id: str,
    category: str,
    operation_key: str,
    parameters: tuple[FeatureParameter, ...],
    required_columns: tuple[str, ...],
    output_columns: tuple[str, ...],
    warmup_bars: int,
    optional: bool = False,
) -> FeatureSpec:
    return FeatureSpec(
        feature_id=feature_id,
        category=category,
        operation_key=operation_key,
        parameters=parameters,
        required_columns=required_columns,
        output_columns=output_columns,
        warmup_bars=warmup_bars,
        optional=optional,
    )


def core_v1_feature_profile() -> FeatureProfile:
    return FeatureProfile(
        version="core_v1",
        description="Baseline derived feature profile built over research bars and materialized indicator frames.",
        features=(
            _spec(
                feature_id="trend_state_fast_slow",
                category="trend",
                operation_key="trend_state_fast_slow",
                parameters=(
                    _p("fast_column", "ema_20"),
                    _p("slow_column", "ema_50"),
                    _p("atr_column", "atr_14"),
                    _p("flat_band_atr", 0.05),
                ),
                required_columns=("ema_20", "ema_50", "atr_14"),
                output_columns=("trend_state_fast_slow_code",),
                warmup_bars=50,
            ),
            _spec(
                feature_id="trend_strength",
                category="trend",
                operation_key="trend_strength",
                parameters=(
                    _p("fast_column", "ema_20"),
                    _p("slow_column", "ema_50"),
                    _p("volatility_column", "atr_14"),
                ),
                required_columns=("ema_20", "ema_50", "atr_14"),
                output_columns=("trend_strength",),
                warmup_bars=50,
            ),
            _spec(
                feature_id="ma_stack_state",
                category="trend",
                operation_key="ma_stack_state",
                parameters=(
                    _p("fast_column", "ema_10"),
                    _p("mid_column", "ema_20"),
                    _p("slow_column", "ema_50"),
                ),
                required_columns=("ema_10", "ema_20", "ema_50"),
                output_columns=("ma_stack_state_code",),
                warmup_bars=50,
            ),
            _spec(
                feature_id="rolling_levels",
                category="levels",
                operation_key="rolling_levels",
                parameters=(_p("length", 20),),
                required_columns=("high", "low"),
                output_columns=("rolling_high_20", "rolling_low_20"),
                warmup_bars=20,
            ),
            _spec(
                feature_id="opening_range",
                category="levels",
                operation_key="opening_range",
                parameters=(_p("opening_bars", 4),),
                required_columns=("high", "low", "session_date", "bar_index"),
                output_columns=("opening_range_high", "opening_range_low"),
                warmup_bars=4,
            ),
            _spec(
                feature_id="swing_levels",
                category="levels",
                operation_key="swing_levels",
                parameters=(
                    _p("left_bars", 5),
                    _p("right_bars", 5),
                ),
                required_columns=("high", "low"),
                output_columns=("swing_high_10", "swing_low_10"),
                warmup_bars=10,
            ),
            _spec(
                feature_id="session_vwap",
                category="levels",
                operation_key="session_vwap",
                parameters=(),
                required_columns=("high", "low", "close", "volume", "session_date"),
                output_columns=("session_vwap",),
                warmup_bars=1,
            ),
            _spec(
                feature_id="level_distances",
                category="levels",
                operation_key="level_distances",
                parameters=(_p("volatility_column", "atr_14"),),
                required_columns=(
                    "close",
                    "atr_14",
                    "session_vwap",
                    "rolling_high_20",
                    "rolling_low_20",
                ),
                output_columns=(
                    "distance_to_session_vwap",
                    "distance_to_rolling_high_20",
                    "distance_to_rolling_low_20",
                ),
                warmup_bars=20,
            ),
            _spec(
                feature_id="volatility_context",
                category="volatility",
                operation_key="volatility_context",
                parameters=(_p("breakout_distance_atr", 1.0),),
                required_columns=(
                    "close",
                    "atr_14",
                    "bb_upper_20_2",
                    "bb_lower_20_2",
                    "bb_width_20_2",
                    "kc_upper_20_1_5",
                    "kc_mid_20_1_5",
                    "kc_lower_20_1_5",
                    "trend_state_fast_slow_code",
                    "rolling_high_20",
                    "rolling_low_20",
                ),
                output_columns=(
                    "bb_width_20_2",
                    "kc_width_20_1_5",
                    "squeeze_on_code",
                    "breakout_ready_state_code",
                ),
                warmup_bars=20,
            ),
            _spec(
                feature_id="breakout_ready_flag",
                category="labels",
                operation_key="breakout_ready_flag",
                parameters=(),
                required_columns=("breakout_ready_state_code",),
                output_columns=("breakout_ready_flag",),
                warmup_bars=20,
            ),
            _spec(
                feature_id="volume_context",
                category="volume",
                operation_key="volume_context",
                parameters=(
                    _p("high_rvol_threshold", 1.5),
                    _p("low_rvol_threshold", 0.8),
                ),
                required_columns=("close", "rvol_20", "volume_z_20", "vwma_20"),
                output_columns=(
                    "rvol_20",
                    "volume_zscore_20",
                    "above_below_vwma_code",
                    "session_volume_state_code",
                ),
                warmup_bars=20,
            ),
            _spec(
                feature_id="regime_state",
                category="regime",
                operation_key="regime_state",
                parameters=(
                    _p("adx_threshold", 25.0),
                    _p("trend_strength_threshold", 0.75),
                ),
                required_columns=("trend_state_fast_slow_code", "trend_strength", "adx_14", "squeeze_on_code"),
                output_columns=("regime_state_code",),
                warmup_bars=20,
            ),
            _spec(
                feature_id="reversion_ready_flag",
                category="labels",
                operation_key="reversion_ready_flag",
                parameters=(
                    _p("entry_distance_atr", 0.75),
                    _p("long_rsi_max", 35.0),
                    _p("short_rsi_min", 65.0),
                ),
                required_columns=("distance_to_session_vwap", "rsi_14", "regime_state_code"),
                output_columns=("reversion_ready_flag",),
                warmup_bars=20,
            ),
            _spec(
                feature_id="atr_reference_levels",
                category="references",
                operation_key="atr_reference_levels",
                parameters=(
                    _p("entry_distance_atr", 0.75),
                    _p("long_rsi_max", 35.0),
                    _p("short_rsi_min", 65.0),
                ),
                required_columns=(
                    "close",
                    "atr_14",
                    "trend_state_fast_slow_code",
                    "breakout_ready_state_code",
                    "distance_to_session_vwap",
                    "rsi_14",
                    "regime_state_code",
                ),
                output_columns=("atr_stop_ref_1x", "atr_target_ref_2x"),
                warmup_bars=20,
            ),
            _spec(
                feature_id="mtf_overlay_1h_to_15m",
                category="mtf",
                operation_key="mtf_overlay",
                parameters=(
                    _p("source_timeframe", "1h"),
                    _p("target_timeframe", "15m"),
                    _p("fast_column", "ema_20"),
                    _p("slow_column", "ema_50"),
                    _p("adx_column", "adx_14"),
                    _p("rsi_column", "rsi_14"),
                ),
                required_columns=(),
                output_columns=(
                    "htf_ma_relation_code",
                    "htf_trend_state_code",
                    "htf_adx_14",
                    "htf_rsi_14",
                ),
                warmup_bars=0,
                optional=True,
            ),
        ),
    )


def core_intraday_v1_feature_profile() -> FeatureProfile:
    base = core_v1_feature_profile()
    keep = {
        "trend_state_fast_slow",
        "trend_strength",
        "ma_stack_state",
        "rolling_levels",
        "opening_range",
        "session_vwap",
        "level_distances",
        "volatility_context",
        "breakout_ready_flag",
        "volume_context",
        "reversion_ready_flag",
        "regime_state",
        "atr_reference_levels",
        "mtf_overlay_1h_to_15m",
    }
    return FeatureProfile(
        version="core_intraday_v1",
        description="Intraday-oriented derived features with opening-range and higher-timeframe alignment.",
        features=tuple(spec for spec in base.features if spec.feature_id in keep),
    )


def core_swing_v1_feature_profile() -> FeatureProfile:
    base = core_v1_feature_profile()
    keep = {
        "trend_state_fast_slow",
        "trend_strength",
        "ma_stack_state",
        "rolling_levels",
        "swing_levels",
        "session_vwap",
        "level_distances",
        "volatility_context",
        "breakout_ready_flag",
        "volume_context",
        "reversion_ready_flag",
        "regime_state",
        "atr_reference_levels",
    }
    return FeatureProfile(
        version="core_swing_v1",
        description="Swing-oriented derived features without intraday opening-range or MTF intraday overlays.",
        features=tuple(spec for spec in base.features if spec.feature_id in keep),
    )


def build_feature_profile_registry() -> FeatureProfileRegistry:
    return FeatureProfileRegistry(
        profiles=(
            core_v1_feature_profile(),
            core_intraday_v1_feature_profile(),
            core_swing_v1_feature_profile(),
        )
    )


def phase1_feature_profile() -> FeatureProfile:
    return core_v1_feature_profile()
