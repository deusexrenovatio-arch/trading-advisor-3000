from __future__ import annotations

from dataclasses import dataclass

from .naming import indicator_column_name


@dataclass(frozen=True)
class IndicatorParameter:
    name: str
    value: object


@dataclass(frozen=True)
class IndicatorSpec:
    indicator_id: str
    category: str
    operation_key: str
    parameters: tuple[IndicatorParameter, ...]
    required_input_columns: tuple[str, ...]
    output_columns: tuple[str, ...]
    warmup_bars: int

    def params_dict(self) -> dict[str, object]:
        return {item.name: item.value for item in self.parameters}


@dataclass(frozen=True)
class IndicatorProfile:
    version: str
    description: str
    indicators: tuple[IndicatorSpec, ...]

    def by_category(self) -> dict[str, tuple[IndicatorSpec, ...]]:
        grouped: dict[str, list[IndicatorSpec]] = {}
        for spec in self.indicators:
            grouped.setdefault(spec.category, []).append(spec)
        return {key: tuple(value) for key, value in grouped.items()}

    def required_input_columns(self) -> tuple[str, ...]:
        seen: list[str] = []
        for spec in self.indicators:
            for column in spec.required_input_columns:
                if column not in seen:
                    seen.append(column)
        return tuple(seen)

    def expected_output_columns(self) -> tuple[str, ...]:
        columns: list[str] = []
        for spec in self.indicators:
            columns.extend(spec.output_columns)
        return tuple(columns)

    def max_warmup_bars(self) -> int:
        return max(spec.warmup_bars for spec in self.indicators)


@dataclass(frozen=True)
class IndicatorProfileRegistry:
    profiles: tuple[IndicatorProfile, ...]

    def get(self, version: str) -> IndicatorProfile:
        for profile in self.profiles:
            if profile.version == version:
                return profile
        raise KeyError(f"unknown indicator profile version: {version}")

    def versions(self) -> tuple[str, ...]:
        return tuple(profile.version for profile in self.profiles)


def _spec(
    *,
    indicator_id: str,
    category: str,
    operation_key: str,
    parameters: tuple[IndicatorParameter, ...],
    required_input_columns: tuple[str, ...],
    output_columns: tuple[str, ...],
    warmup_bars: int,
) -> IndicatorSpec:
    return IndicatorSpec(
        indicator_id=indicator_id,
        category=category,
        operation_key=operation_key,
        parameters=parameters,
        required_input_columns=required_input_columns,
        output_columns=output_columns,
        warmup_bars=warmup_bars,
    )


def _p(name: str, value: object) -> IndicatorParameter:
    return IndicatorParameter(name=name, value=value)


def core_v1_indicator_profile() -> IndicatorProfile:
    close = ("close",)
    hlc = ("high", "low", "close")
    hlcv = ("high", "low", "close", "volume")
    cv = ("close", "volume")

    return IndicatorProfile(
        version="core_v1",
        description="Baseline full indicator profile for the vectorized research-plane bootstrap.",
        indicators=(
            _spec(
                indicator_id="sma_10",
                category="trend",
                operation_key="sma",
                parameters=(_p("length", 10),),
                required_input_columns=close,
                output_columns=(indicator_column_name("sma", 10),),
                warmup_bars=10,
            ),
            _spec(
                indicator_id="sma_20",
                category="trend",
                operation_key="sma",
                parameters=(_p("length", 20),),
                required_input_columns=close,
                output_columns=(indicator_column_name("sma", 20),),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="sma_50",
                category="trend",
                operation_key="sma",
                parameters=(_p("length", 50),),
                required_input_columns=close,
                output_columns=(indicator_column_name("sma", 50),),
                warmup_bars=50,
            ),
            _spec(
                indicator_id="ema_10",
                category="trend",
                operation_key="ema",
                parameters=(_p("length", 10),),
                required_input_columns=close,
                output_columns=(indicator_column_name("ema", 10),),
                warmup_bars=10,
            ),
            _spec(
                indicator_id="ema_20",
                category="trend",
                operation_key="ema",
                parameters=(_p("length", 20),),
                required_input_columns=close,
                output_columns=(indicator_column_name("ema", 20),),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="ema_50",
                category="trend",
                operation_key="ema",
                parameters=(_p("length", 50),),
                required_input_columns=close,
                output_columns=(indicator_column_name("ema", 50),),
                warmup_bars=50,
            ),
            _spec(
                indicator_id="hma_20",
                category="trend",
                operation_key="hma",
                parameters=(_p("length", 20),),
                required_input_columns=close,
                output_columns=(indicator_column_name("hma", 20),),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="atr_14",
                category="volatility",
                operation_key="atr",
                parameters=(_p("length", 14),),
                required_input_columns=hlc,
                output_columns=(indicator_column_name("atr", 14),),
                warmup_bars=14,
            ),
            _spec(
                indicator_id="natr_14",
                category="volatility",
                operation_key="natr",
                parameters=(_p("length", 14),),
                required_input_columns=hlc,
                output_columns=(indicator_column_name("natr", 14),),
                warmup_bars=14,
            ),
            _spec(
                indicator_id="rsi_14",
                category="oscillator",
                operation_key="rsi",
                parameters=(_p("length", 14),),
                required_input_columns=close,
                output_columns=(indicator_column_name("rsi", 14),),
                warmup_bars=14,
            ),
            _spec(
                indicator_id="stoch_14_3_3",
                category="oscillator",
                operation_key="stoch",
                parameters=(_p("k", 14), _p("d", 3), _p("smooth_k", 3)),
                required_input_columns=hlc,
                output_columns=(
                    indicator_column_name("stoch_k", 14, 3, 3),
                    indicator_column_name("stoch_d", 14, 3, 3),
                ),
                warmup_bars=14,
            ),
            _spec(
                indicator_id="macd_12_26_9",
                category="momentum",
                operation_key="macd",
                parameters=(_p("fast", 12), _p("slow", 26), _p("signal", 9)),
                required_input_columns=close,
                output_columns=(
                    indicator_column_name("macd", 12, 26, 9),
                    indicator_column_name("macd_signal", 12, 26, 9),
                    indicator_column_name("macd_hist", 12, 26, 9),
                ),
                warmup_bars=26,
            ),
            _spec(
                indicator_id="adx_14",
                category="momentum",
                operation_key="adx",
                parameters=(_p("length", 14),),
                required_input_columns=hlc,
                output_columns=(
                    indicator_column_name("adx", 14),
                    indicator_column_name("dmp", 14),
                    indicator_column_name("dmn", 14),
                ),
                warmup_bars=14,
            ),
            _spec(
                indicator_id="aroon_25",
                category="momentum",
                operation_key="aroon",
                parameters=(_p("length", 25),),
                required_input_columns=("high", "low"),
                output_columns=(
                    indicator_column_name("aroon_up", 25),
                    indicator_column_name("aroon_down", 25),
                ),
                warmup_bars=25,
            ),
            _spec(
                indicator_id="donchian_20",
                category="volatility",
                operation_key="donchian",
                parameters=(_p("lower_length", 20), _p("upper_length", 20)),
                required_input_columns=("high", "low"),
                output_columns=(
                    indicator_column_name("donchian_high", 20),
                    indicator_column_name("donchian_low", 20),
                ),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="bbands_20_2",
                category="volatility",
                operation_key="bbands",
                parameters=(_p("length", 20), _p("std", 2)),
                required_input_columns=close,
                output_columns=(
                    indicator_column_name("bb_upper", 20, 2),
                    indicator_column_name("bb_mid", 20, 2),
                    indicator_column_name("bb_lower", 20, 2),
                    indicator_column_name("bb_width", 20, 2),
                ),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="kc_20_1_5",
                category="volatility",
                operation_key="kc",
                parameters=(_p("length", 20), _p("scalar", 1.5)),
                required_input_columns=hlc,
                output_columns=(
                    indicator_column_name("kc_upper", 20, 1.5),
                    indicator_column_name("kc_mid", 20, 1.5),
                    indicator_column_name("kc_lower", 20, 1.5),
                ),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="obv",
                category="volume",
                operation_key="obv",
                parameters=(),
                required_input_columns=cv,
                output_columns=(indicator_column_name("obv"),),
                warmup_bars=1,
            ),
            _spec(
                indicator_id="mfi_14",
                category="volume",
                operation_key="mfi",
                parameters=(_p("length", 14),),
                required_input_columns=hlcv,
                output_columns=(indicator_column_name("mfi", 14),),
                warmup_bars=14,
            ),
            _spec(
                indicator_id="cmf_20",
                category="volume",
                operation_key="cmf",
                parameters=(_p("length", 20),),
                required_input_columns=hlcv,
                output_columns=(indicator_column_name("cmf", 20),),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="vwma_20",
                category="volume",
                operation_key="vwma",
                parameters=(_p("length", 20),),
                required_input_columns=cv,
                output_columns=(indicator_column_name("vwma", 20),),
                warmup_bars=20,
            ),
            _spec(
                indicator_id="roc_10",
                category="momentum",
                operation_key="roc",
                parameters=(_p("length", 10),),
                required_input_columns=close,
                output_columns=(indicator_column_name("roc", 10),),
                warmup_bars=10,
            ),
            _spec(
                indicator_id="ppo_12_26_9",
                category="momentum",
                operation_key="ppo",
                parameters=(_p("fast", 12), _p("slow", 26), _p("signal", 9)),
                required_input_columns=close,
                output_columns=(indicator_column_name("ppo", 12, 26, 9),),
                warmup_bars=26,
            ),
            _spec(
                indicator_id="volume_norm_20",
                category="volume",
                operation_key="volume_norm",
                parameters=(_p("length", 20),),
                required_input_columns=("volume",),
                output_columns=(
                    indicator_column_name("rvol", 20),
                    indicator_column_name("volume_z", 20),
                ),
                warmup_bars=20,
            ),
        ),
    )


def core_intraday_v1_indicator_profile() -> IndicatorProfile:
    base = core_v1_indicator_profile()
    keep = {
        "ema_10",
        "ema_20",
        "ema_50",
        "hma_20",
        "atr_14",
        "natr_14",
        "rsi_14",
        "stoch_14_3_3",
        "macd_12_26_9",
        "adx_14",
        "aroon_25",
        "donchian_20",
        "bbands_20_2",
        "kc_20_1_5",
        "obv",
        "mfi_14",
        "cmf_20",
        "vwma_20",
        "roc_10",
        "ppo_12_26_9",
        "volume_norm_20",
    }
    return IndicatorProfile(
        version="core_intraday_v1",
        description="Intraday-oriented indicator profile with the core fast and context indicators.",
        indicators=tuple(spec for spec in base.indicators if spec.indicator_id in keep),
    )


def core_swing_v1_indicator_profile() -> IndicatorProfile:
    base = core_v1_indicator_profile()
    keep = {
        "sma_20",
        "sma_50",
        "ema_20",
        "ema_50",
        "hma_20",
        "atr_14",
        "natr_14",
        "rsi_14",
        "macd_12_26_9",
        "adx_14",
        "aroon_25",
        "donchian_20",
        "bbands_20_2",
        "kc_20_1_5",
        "obv",
        "mfi_14",
        "cmf_20",
        "vwma_20",
        "roc_10",
        "ppo_12_26_9",
    }
    return IndicatorProfile(
        version="core_swing_v1",
        description="Swing-oriented indicator profile with slower trend and momentum context.",
        indicators=tuple(spec for spec in base.indicators if spec.indicator_id in keep),
    )


def build_indicator_profile_registry() -> IndicatorProfileRegistry:
    return IndicatorProfileRegistry(
        profiles=(
            core_v1_indicator_profile(),
            core_intraday_v1_indicator_profile(),
            core_swing_v1_indicator_profile(),
        )
    )


def phase1_indicator_profile() -> IndicatorProfile:
    return build_indicator_profile_registry().get("core_v1")
