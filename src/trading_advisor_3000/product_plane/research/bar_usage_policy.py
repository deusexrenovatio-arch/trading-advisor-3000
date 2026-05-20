from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from fnmatch import fnmatchcase

from trading_advisor_3000.product_plane.research.datasets.bar_usage import (
    OI_UPDATE,
    PRICE_RISK,
    RANGE_UPDATE,
    SESSION_UPDATE,
    VOLUME_UPDATE,
)

STATE_UPDATE_HOLD = "state_update_hold"
STATE_UPDATE_RESET_SCOPE = "state_update_reset_scope"
POINT_UPDATE_NULL = "point_update_null"
EVENT_UPDATE_ZERO = "event_update_zero"


@dataclass(frozen=True)
class BarUsageCalculationRule:
    group_id: str
    required_flags: int
    mode: str
    output_patterns: tuple[str, ...]
    scope_id: str = ""

    def matches(self, output_column: str) -> bool:
        return any(_matches_pattern(output_column, pattern) for pattern in self.output_patterns)

    def execution_key(self) -> tuple[str, int, str, str]:
        return (self.group_id, self.required_flags, self.mode, self.scope_id)


def _matches_pattern(value: str, pattern: str) -> bool:
    return fnmatchcase(value, pattern)


INDICATOR_BAR_USAGE_RULES: tuple[BarUsageCalculationRule, ...] = (
    BarUsageCalculationRule(
        group_id="price_risk_volatility",
        required_flags=PRICE_RISK,
        mode=STATE_UPDATE_HOLD,
        output_patterns=(
            "true_range",
            "atr_*",
            "natr_*",
            "realized_volatility_*",
            "realized_vol_*",
            "ulcer_index_*",
        ),
    ),
    BarUsageCalculationRule(
        group_id="price_close_state",
        required_flags=PRICE_RISK,
        mode=STATE_UPDATE_HOLD,
        output_patterns=(
            "sma_*",
            "ema_*",
            "hma_*",
            "rsi_*",
            "stochrsi_*",
            "macd_*",
            "ppo_*",
            "tsi_*",
            "trix_*",
            "kst_*",
            "roc_*",
            "mom_*",
            "close_slope_*",
        ),
    ),
    BarUsageCalculationRule(
        group_id="range_signal_state",
        required_flags=RANGE_UPDATE,
        mode=STATE_UPDATE_HOLD,
        output_patterns=(
            "donchian_*",
            "bb_*",
            "kc_*",
            "supertrend_*",
            "adx_*",
            "dmp_*",
            "dmn_*",
            "aroon_*",
            "chop_*",
            "stoch_k_*",
            "stoch_d_*",
            "cci_*",
            "willr_*",
            "ultimate_oscillator_*",
        ),
    ),
    BarUsageCalculationRule(
        group_id="volume_signal_state",
        required_flags=VOLUME_UPDATE,
        mode=STATE_UPDATE_HOLD,
        output_patterns=(
            "obv",
            "mfi_*",
            "cmf_*",
            "vwma_*",
            "ad",
            "adosc_*",
            "force_index_*",
            "pvt",
            "pvo_*",
            "rvol_*",
            "volume_z_*",
            "vp_*",
        ),
    ),
    BarUsageCalculationRule(
        group_id="oi_signal_state",
        required_flags=OI_UPDATE,
        mode=STATE_UPDATE_HOLD,
        output_patterns=("oi_change_*", "oi_roc_*", "oi_z_*", "oi_relative_activity_*"),
    ),
    BarUsageCalculationRule(
        group_id="volume_oi_state",
        required_flags=VOLUME_UPDATE | OI_UPDATE,
        mode=STATE_UPDATE_HOLD,
        output_patterns=("volume_oi_ratio",),
    ),
)


DERIVED_BAR_USAGE_RULES: tuple[BarUsageCalculationRule, ...] = (
    BarUsageCalculationRule(
        group_id="derived_range_level",
        required_flags=RANGE_UPDATE,
        mode=STATE_UPDATE_HOLD,
        output_patterns=(
            "rolling_high_*",
            "rolling_low_*",
            "swing_high_*",
            "swing_low_*",
            "rolling_position_*",
            "donchian_position_*",
            "distance_to_rolling_*",
            "distance_to_donchian_*",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_session_level",
        required_flags=SESSION_UPDATE,
        mode=STATE_UPDATE_RESET_SCOPE,
        scope_id="session",
        output_patterns=(
            "session_high",
            "session_low",
            "session_vwap",
            "opening_range_high",
            "opening_range_low",
            "session_position",
            "distance_to_session_*",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_week_level",
        required_flags=RANGE_UPDATE,
        mode=STATE_UPDATE_RESET_SCOPE,
        scope_id="week",
        output_patterns=("week_high", "week_low", "week_position", "distance_to_week_*"),
    ),
    BarUsageCalculationRule(
        group_id="derived_price_distance",
        required_flags=PRICE_RISK,
        mode=POINT_UPDATE_NULL,
        output_patterns=(
            "distance_to_close_atr",
            "distance_to_sma_*_atr",
            "distance_to_ema_*_atr",
            "distance_to_hma_*_atr",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_price_distance",
        required_flags=PRICE_RISK | RANGE_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("distance_to_bb_*_atr", "distance_to_kc_*_atr"),
    ),
    BarUsageCalculationRule(
        group_id="derived_volume_distance",
        required_flags=PRICE_RISK | VOLUME_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("distance_to_vwma_*_atr",),
    ),
    BarUsageCalculationRule(
        group_id="derived_position",
        required_flags=PRICE_RISK | RANGE_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("bb_position_*", "kc_position_*"),
    ),
    BarUsageCalculationRule(
        group_id="derived_cross_event",
        required_flags=PRICE_RISK,
        mode=EVENT_UPDATE_ZERO,
        output_patterns=(
            "cross_close_sma_*_code",
            "cross_close_ema_*_code",
            "macd_signal_cross_code",
            "ppo_signal_cross_code",
            "trix_signal_cross_code",
            "kst_signal_cross_code",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_cross_event",
        required_flags=PRICE_RISK | RANGE_UPDATE,
        mode=EVENT_UPDATE_ZERO,
        output_patterns=("cross_close_rolling_*_code",),
    ),
    BarUsageCalculationRule(
        group_id="derived_cross_event",
        required_flags=PRICE_RISK | SESSION_UPDATE,
        mode=EVENT_UPDATE_ZERO,
        output_patterns=("cross_close_session_vwap_code",),
    ),
    BarUsageCalculationRule(
        group_id="derived_price_change",
        required_flags=PRICE_RISK,
        mode=POINT_UPDATE_NULL,
        output_patterns=(
            "close_change_*",
            "close_slope_*",
            "sma_*_slope_*",
            "ema_*_slope_*",
            "roc_*_change_*",
            "mom_*_change_*",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_volume_change",
        required_flags=VOLUME_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("volume_change_*",),
    ),
    BarUsageCalculationRule(
        group_id="derived_volume_state",
        required_flags=VOLUME_UPDATE,
        mode=STATE_UPDATE_HOLD,
        output_patterns=("rvol_*", "volume_zscore_*", "volume_z_*"),
    ),
    BarUsageCalculationRule(
        group_id="derived_oi_change",
        required_flags=OI_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("oi_change_*",),
    ),
    BarUsageCalculationRule(
        group_id="derived_relationship",
        required_flags=PRICE_RISK | VOLUME_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("price_volume_corr_*",),
    ),
    BarUsageCalculationRule(
        group_id="derived_relationship",
        required_flags=PRICE_RISK | OI_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("price_oi_corr_*",),
    ),
    BarUsageCalculationRule(
        group_id="derived_relationship",
        required_flags=VOLUME_UPDATE | OI_UPDATE,
        mode=POINT_UPDATE_NULL,
        output_patterns=("volume_oi_corr_*",),
    ),
    BarUsageCalculationRule(
        group_id="derived_divergence_price",
        required_flags=PRICE_RISK,
        mode=EVENT_UPDATE_ZERO,
        output_patterns=(
            "divergence_price_rsi_*_score",
            "divergence_price_stochrsi_*_score",
            "divergence_price_macd_*_score",
            "divergence_price_ppo_*_score",
            "divergence_price_tsi_*_score",
            "divergence_price_trix_*_score",
            "divergence_price_kst_*_score",
            "divergence_price_roc_*_score",
            "divergence_price_mom_*_score",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_divergence_price",
        required_flags=PRICE_RISK | RANGE_UPDATE,
        mode=EVENT_UPDATE_ZERO,
        output_patterns=(
            "divergence_price_stoch_k_*_score",
            "divergence_price_stoch_d_*_score",
            "divergence_price_cci_*_score",
            "divergence_price_willr_*_score",
            "divergence_price_ultimate_oscillator_*_score",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_divergence_price",
        required_flags=PRICE_RISK | VOLUME_UPDATE,
        mode=EVENT_UPDATE_ZERO,
        output_patterns=(
            "divergence_price_obv_score",
            "divergence_price_mfi_*_score",
            "divergence_price_cmf_*_score",
            "divergence_price_ad_score",
            "divergence_price_adosc_*_score",
            "divergence_price_force_index_*_score",
            "divergence_price_pvt_score",
            "divergence_price_pvo_*_score",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_divergence_price",
        required_flags=PRICE_RISK | OI_UPDATE,
        mode=EVENT_UPDATE_ZERO,
        output_patterns=(
            "divergence_price_oi_change_*_score",
            "divergence_price_oi_roc_*_score",
            "divergence_price_oi_z_*_score",
            "divergence_price_oi_relative_activity_*_score",
        ),
    ),
    BarUsageCalculationRule(
        group_id="derived_mtf_projection",
        required_flags=PRICE_RISK,
        mode=STATE_UPDATE_HOLD,
        output_patterns=("mtf_*",),
    ),
)


def resolve_indicator_bar_usage_rule(output_column: str) -> BarUsageCalculationRule:
    return _resolve_bar_usage_rule(output_column, INDICATOR_BAR_USAGE_RULES)


def resolve_derived_bar_usage_rule(output_column: str) -> BarUsageCalculationRule:
    return _resolve_bar_usage_rule(output_column, DERIVED_BAR_USAGE_RULES)


def _resolve_bar_usage_rule(
    output_column: str, rules: tuple[BarUsageCalculationRule, ...]
) -> BarUsageCalculationRule:
    matches = tuple(rule for rule in rules if rule.matches(output_column))
    if not matches:
        raise ValueError(f"missing bar usage policy for output column: {output_column}")
    if len(matches) > 1:
        group_ids = ", ".join(rule.group_id for rule in matches)
        raise ValueError(
            "ambiguous bar usage policy for output column: "
            f"{output_column}; matching_groups={group_ids}"
        )
    return matches[0]


def group_outputs_by_bar_usage_rule(
    output_columns: Iterable[str],
    resolver: Callable[[str], BarUsageCalculationRule],
) -> tuple[tuple[BarUsageCalculationRule, tuple[str, ...]], ...]:
    grouped: list[tuple[BarUsageCalculationRule, list[str]]] = []
    for output_column in output_columns:
        rule = resolver(output_column)
        for existing_rule, columns in grouped:
            if existing_rule.execution_key() == rule.execution_key():
                columns.append(output_column)
                break
        else:
            grouped.append((rule, [output_column]))
    return tuple((rule, tuple(columns)) for rule, columns in grouped)


def indicator_bar_usage_groups_for_outputs(
    output_columns: Iterable[str],
) -> tuple[tuple[BarUsageCalculationRule, tuple[str, ...]], ...]:
    return group_outputs_by_bar_usage_rule(output_columns, resolve_indicator_bar_usage_rule)


def derived_bar_usage_groups_for_outputs(
    output_columns: Iterable[str],
) -> tuple[tuple[BarUsageCalculationRule, tuple[str, ...]], ...]:
    return group_outputs_by_bar_usage_rule(output_columns, resolve_derived_bar_usage_rule)


def assert_indicator_bar_usage_policy_coverage(profile: object) -> None:
    missing = _missing_policy_columns(
        _profile_output_columns(profile), resolve_indicator_bar_usage_rule
    )
    if missing:
        raise ValueError(f"missing bar usage policy for indicator outputs: {', '.join(missing)}")


def assert_derived_bar_usage_policy_coverage(profile: object) -> None:
    missing = _missing_policy_columns(
        _profile_output_columns(profile), resolve_derived_bar_usage_rule
    )
    if missing:
        raise ValueError(f"missing bar usage policy for derived outputs: {', '.join(missing)}")


def _profile_output_columns(profile: object) -> tuple[str, ...]:
    if hasattr(profile, "expected_output_columns"):
        return tuple(str(column) for column in profile.expected_output_columns())
    if hasattr(profile, "output_columns"):
        return tuple(str(column) for column in profile.output_columns)
    return tuple(str(column) for column in profile)  # type: ignore[arg-type]


def _missing_policy_columns(
    output_columns: tuple[str, ...],
    resolver: Callable[[str], BarUsageCalculationRule],
) -> tuple[str, ...]:
    missing: list[str] = []
    for output_column in output_columns:
        try:
            resolver(output_column)
        except ValueError:
            missing.append(output_column)
    return tuple(missing)


def has_required_bar_usage_flags(flags: object, required_flags: int) -> bool:
    try:
        value = int(flags)
    except (TypeError, ValueError):
        return False
    return (value & required_flags) == required_flags


def required_flags_for_mtf_source_column(source_column: str) -> int:
    return resolve_indicator_bar_usage_rule(source_column).required_flags
