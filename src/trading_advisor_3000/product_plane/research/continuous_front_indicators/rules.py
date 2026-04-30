from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json

from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    DerivedIndicatorProfile,
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.indicators.registry import (
    IndicatorProfile,
    IndicatorSpec,
    default_indicator_profile,
)


DEFAULT_RULE_SET_VERSION = "roll_rules_hybrid_v1"
ADAPTER_VERSION = "continuous_front_indicator_storage_hybrid_v2"

ROLL_BOUNDARY_DERIVED_SOURCE_COLUMNS = frozenset(
    {
        "obv",
        "mfi_14",
        "cmf_20",
        "ad",
        "adosc_3_10",
        "force_index_13",
        "pvt",
        "pvo_12_26_9",
        "pvo_hist_12_26_9",
        "oi_change_1",
        "oi_roc_10",
        "oi_z_20",
        "oi_relative_activity_20",
    }
)


@dataclass(frozen=True)
class CalculationGroup:
    group_id: str
    roll_class: str
    adapter_id: str
    input_price_space: str
    output_price_space: str
    state_space: str
    state_transform_on_roll: str
    contract_boundary_policy: str
    allow_cross_contract_window: bool
    reset_on_roll: bool
    anchor_sensitive: bool
    pandas_ta_allowed: bool
    description: str


CALCULATION_GROUPS: dict[str, CalculationGroup] = {
    "price_level_post_transform": CalculationGroup(
        group_id="price_level_post_transform",
        roll_class="A",
        adapter_id="pandas_ta_with_post_transform_adapter",
        input_price_space="causal_zero_anchor",
        output_price_space="continuous_current_anchor",
        state_space="price_level",
        state_transform_on_roll="Y0_t = F(P0_window); Y_t = Y0_t + A_t",
        contract_boundary_policy="allow_cross_contract_price_window",
        allow_cross_contract_window=True,
        reset_on_roll=False,
        anchor_sensitive=False,
        pandas_ta_allowed=True,
        description="Price-level outputs are computed on causal P0 prices and shifted only to the bar's known current anchor.",
    ),
    "price_range_on_p0": CalculationGroup(
        group_id="price_range_on_p0",
        roll_class="B",
        adapter_id="pandas_ta_on_p0_adapter",
        input_price_space="causal_zero_anchor",
        output_price_space="points",
        state_space="price_difference",
        state_transform_on_roll="Y_t = F(P0_window); additive shift is forbidden",
        contract_boundary_policy="allow_cross_contract_price_window",
        allow_cross_contract_window=True,
        reset_on_roll=False,
        anchor_sensitive=False,
        pandas_ta_allowed=True,
        description="Range and difference outputs use causal P0 and do not receive A_t post-transform.",
    ),
    "oscillator_on_p0": CalculationGroup(
        group_id="oscillator_on_p0",
        roll_class="C",
        adapter_id="pandas_ta_on_p0_adapter",
        input_price_space="causal_zero_anchor",
        output_price_space="dimensionless",
        state_space="shift_invariant",
        state_transform_on_roll="Y_t = F(P0_window); no additive post-transform",
        contract_boundary_policy="allow_cross_contract_price_window",
        allow_cross_contract_window=True,
        reset_on_roll=False,
        anchor_sensitive=False,
        pandas_ta_allowed=True,
        description="Shift-invariant oscillators and position codes are computed on causal P0 inputs.",
    ),
    "anchor_sensitive_roll_aware": CalculationGroup(
        group_id="anchor_sensitive_roll_aware",
        roll_class="D",
        adapter_id="custom_roll_aware_adapter",
        input_price_space="current_anchor_window",
        output_price_space="percent_or_dimensionless",
        state_space="anchor_sensitive_ratio",
        state_transform_on_roll="Y_t = F(P0_window + A_t); pandas-ta direct P0 output is rejected",
        contract_boundary_policy="allow_cross_contract_with_target_anchor",
        allow_cross_contract_window=True,
        reset_on_roll=False,
        anchor_sensitive=True,
        pandas_ta_allowed=False,
        description="Ratios whose denominator changes when a constant is added use custom current-anchor formulas.",
    ),
    "price_volume_roll_aware": CalculationGroup(
        group_id="price_volume_roll_aware",
        roll_class="E",
        adapter_id="custom_roll_aware_adapter",
        input_price_space="causal_zero_anchor_and_native_volume",
        output_price_space="flow_native_or_current_anchor_level",
        state_space="price_volume_flow",
        state_transform_on_roll="price uses P0/current anchor, volume stays native, cumulative state follows reset policy",
        contract_boundary_policy="explicit_flow_reset_policy",
        allow_cross_contract_window=True,
        reset_on_roll=True,
        anchor_sensitive=False,
        pandas_ta_allowed=False,
        description="Price-volume flows keep native volume and make reset/carry behavior explicit at roll boundaries.",
    ),
    "native_volume_oi_roll_aware": CalculationGroup(
        group_id="native_volume_oi_roll_aware",
        roll_class="F",
        adapter_id="custom_roll_aware_adapter",
        input_price_space="contract_native_volume_oi",
        output_price_space="native_volume_oi_state",
        state_space="native_volume_oi",
        state_transform_on_roll="windows crossing roll_epoch_id are NULL or state resets by rule",
        contract_boundary_policy="mask_cross_contract_native_window",
        allow_cross_contract_window=False,
        reset_on_roll=True,
        anchor_sensitive=False,
        pandas_ta_allowed=False,
        description="Native volume/OI windows are masked or reset instead of being blended across contracts.",
    ),
    "native_state_relationship_roll_aware": CalculationGroup(
        group_id="native_state_relationship_roll_aware",
        roll_class="F-DERIVED",
        adapter_id="custom_roll_aware_adapter",
        input_price_space="target_anchor_price_and_native_state",
        output_price_space="dimensionless_or_points",
        state_space="derived_native_state_relationship",
        state_transform_on_roll="derived windows over reset/native state are NULL when roll_epoch changes inside the window",
        contract_boundary_policy="mask_cross_contract_native_state_relationship_window",
        allow_cross_contract_window=False,
        reset_on_roll=True,
        anchor_sensitive=False,
        pandas_ta_allowed=False,
        description="Derived relationships over volume/OI/reset state stay inside one roll epoch.",
    ),
    "grid_locked_native": CalculationGroup(
        group_id="grid_locked_native",
        roll_class="G",
        adapter_id="custom_roll_aware_adapter",
        input_price_space="contract_native_price_grid",
        output_price_space="contract_native",
        state_space="price_grid_locked",
        state_transform_on_roll="automatic additive shift is forbidden",
        contract_boundary_policy="single_roll_epoch_native_grid",
        allow_cross_contract_window=False,
        reset_on_roll=True,
        anchor_sensitive=False,
        pandas_ta_allowed=False,
        description="Native price-grid levels remain contract-native unless a separate non-tradable projection is defined.",
    ),
    "pandas_window_derived_level": CalculationGroup(
        group_id="pandas_window_derived_level",
        roll_class="A",
        adapter_id="pandas_window_adapter",
        input_price_space="causal_zero_anchor",
        output_price_space="continuous_current_anchor",
        state_space="derived_price_level",
        state_transform_on_roll="level0_t = window/session F(P0); level_t = level0_t + A_t",
        contract_boundary_policy="allow_cross_contract_price_window",
        allow_cross_contract_window=True,
        reset_on_roll=False,
        anchor_sensitive=False,
        pandas_ta_allowed=False,
        description="Derived rolling/session/week levels are computed from causal normalized state before shifting to the bar's known current anchor.",
    ),
    "derived_relationship_roll_aware": CalculationGroup(
        group_id="derived_relationship_roll_aware",
        roll_class="C",
        adapter_id="custom_roll_aware_adapter",
        input_price_space="declared_source_price_space",
        output_price_space="dimensionless_points_or_degrees",
        state_space="derived_relationship",
        state_transform_on_roll="validate price-space compatibility before distance, position, cross, angle slope, or movement divergence",
        contract_boundary_policy="validate_source_price_space",
        allow_cross_contract_window=True,
        reset_on_roll=False,
        anchor_sensitive=False,
        pandas_ta_allowed=False,
        description="Derived relationships consume declared base/input spaces and fail closed on mismatches; slope outputs are angles and divergence outputs compare movement angles.",
    ),
    "mtf_causal_overlay": CalculationGroup(
        group_id="mtf_causal_overlay",
        roll_class="MTF",
        adapter_id="pandas_window_adapter",
        input_price_space="source_declared",
        output_price_space="source_declared",
        state_space="closed_bar_mtf_overlay",
        state_transform_on_roll="latest source bar where source_ts_close <= target_ts_close",
        contract_boundary_policy="matching_policy_versions_only",
        allow_cross_contract_window=True,
        reset_on_roll=False,
        anchor_sensitive=False,
        pandas_ta_allowed=False,
        description="MTF overlays use closed source bars only and require matching dataset/roll/indicator versions.",
    ),
}


@dataclass(frozen=True)
class IndicatorRollRule:
    rule_set_version: str
    indicator_id: str
    output_column: str
    output_family: str
    formula_id: str
    calculation_group_id: str
    adapter_input_columns: tuple[str, ...]
    adapter_output_columns: tuple[str, ...]
    warmup_bars: int
    requires_base_columns: tuple[str, ...] = ()
    requires_input_columns: tuple[str, ...] = ()
    causal_safe: bool = True
    notes: str = ""

    @property
    def group(self) -> CalculationGroup:
        return CALCULATION_GROUPS[self.calculation_group_id]

    @property
    def rule_hash(self) -> str:
        payload = self.to_dict(include_hash=False, include_created_at=False)
        normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()

    def to_dict(
        self,
        *,
        include_hash: bool = True,
        include_created_at: bool = True,
        created_at_utc: str | None = None,
    ) -> dict[str, object]:
        group = self.group
        row: dict[str, object] = {
            "rule_set_version": self.rule_set_version,
            "indicator_id": self.indicator_id,
            "output_column": self.output_column,
            "output_family": self.output_family,
            "formula_id": self.formula_id,
            "calculation_group_id": self.calculation_group_id,
            "calculation_group_description": group.description,
            "roll_class": group.roll_class,
            "input_price_space": group.input_price_space,
            "output_price_space": group.output_price_space,
            "state_space": group.state_space,
            "adapter_id": group.adapter_id,
            "adapter_version": ADAPTER_VERSION,
            "adapter_input_columns": self.adapter_input_columns,
            "adapter_output_columns": self.adapter_output_columns,
            "pandas_ta_allowed": group.pandas_ta_allowed,
            "state_transform_on_roll": group.state_transform_on_roll,
            "contract_boundary_policy": group.contract_boundary_policy,
            "allow_cross_contract_window": group.allow_cross_contract_window,
            "reset_on_roll": group.reset_on_roll,
            "anchor_sensitive": group.anchor_sensitive,
            "warmup_bars": self.warmup_bars,
            "requires_base_columns": self.requires_base_columns,
            "requires_input_columns": self.requires_input_columns,
            "causal_safe": self.causal_safe,
            "notes": self.notes,
        }
        if include_hash:
            row["rule_hash"] = self.rule_hash
        if include_created_at:
            row["created_at_utc"] = created_at_utc or _utc_now_iso()
        return row


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _group_for_indicator_output(spec: IndicatorSpec, output_column: str) -> str:
    operation = spec.operation_key
    if operation in {"sma", "ema", "hma"}:
        return "price_level_post_transform"
    if operation in {"atr", "true_range", "macd", "mom", "slope"}:
        return "price_range_on_p0"
    if operation in {
        "rsi",
        "stoch",
        "adx",
        "chop",
        "aroon",
        "cci",
        "willr",
        "stochrsi",
        "ultimate_oscillator",
        "tsi",
        "trix",
        "kst",
    }:
        return "oscillator_on_p0"
    if operation in {"natr", "roc", "ppo", "realized_volatility", "ulcer_index"}:
        return "anchor_sensitive_roll_aware"
    if operation in {"obv", "mfi", "cmf", "ad", "adosc", "force_index", "pvt"}:
        return "price_volume_roll_aware"
    if operation in {"pvo", "volume_norm", "oi_change", "oi_roc", "oi_z", "oi_relative_activity", "volume_oi_ratio"}:
        return "native_volume_oi_roll_aware"
    if operation == "vwma":
        return "price_level_post_transform"
    if operation in {"donchian", "bbands", "kc", "supertrend"}:
        if output_column.startswith(("bb_upper_", "bb_mid_", "bb_lower_")):
            return "price_level_post_transform"
        if output_column.startswith("bb_width_"):
            return "anchor_sensitive_roll_aware"
        if output_column.startswith("bb_percent_b_"):
            return "oscillator_on_p0"
        if output_column.startswith(("donchian_high_", "donchian_low_", "donchian_mid_")):
            return "price_level_post_transform"
        if output_column.startswith("donchian_width_"):
            return "price_range_on_p0"
        if output_column.startswith(("kc_upper_", "kc_mid_", "kc_lower_")):
            return "price_level_post_transform"
        if output_column.startswith("supertrend_direction_"):
            return "oscillator_on_p0"
        if output_column.startswith("supertrend_"):
            return "price_level_post_transform"
        return "oscillator_on_p0"
    return "anchor_sensitive_roll_aware"


def _input_columns_for_group(spec: IndicatorSpec, group_id: str) -> tuple[str, ...]:
    mapping = {
        "open": "open0",
        "high": "high0",
        "low": "low0",
        "close": "close0",
        "volume": "native_volume",
        "open_interest": "native_open_interest",
        "true_range": "true_range0",
        "log_ret_1": "close0",
    }
    columns = tuple(mapping.get(column, column) for column in spec.required_input_columns)
    if group_id in {"anchor_sensitive_roll_aware", "price_level_post_transform"}:
        columns = tuple(dict.fromkeys((*columns, "cumulative_additive_offset")))
    if group_id in {"price_volume_roll_aware", "native_volume_oi_roll_aware"}:
        columns = tuple(dict.fromkeys((*columns, "roll_epoch_id", "is_first_bar_after_roll")))
    return columns


def rules_for_indicator_profile(
    profile: IndicatorProfile | None = None,
    *,
    rule_set_version: str = DEFAULT_RULE_SET_VERSION,
) -> tuple[IndicatorRollRule, ...]:
    resolved = profile or default_indicator_profile()
    rules: list[IndicatorRollRule] = []
    for spec in resolved.indicators:
        for output_column in spec.output_columns:
            group_id = _group_for_indicator_output(spec, output_column)
            rules.append(
                IndicatorRollRule(
                    rule_set_version=rule_set_version,
                    indicator_id=spec.indicator_id,
                    output_column=output_column,
                    output_family="base",
                    formula_id=spec.operation_key,
                    calculation_group_id=group_id,
                    adapter_input_columns=_input_columns_for_group(spec, group_id),
                    adapter_output_columns=(output_column,),
                    warmup_bars=spec.warmup_bars,
                    requires_input_columns=spec.required_input_columns,
                )
            )
    return tuple(rules)


def _group_for_derived_output(column: str) -> str:
    if column.startswith("mtf_"):
        return "mtf_causal_overlay"
    if column in {"volume_change_1", "oi_change_1", "price_volume_corr_20", "price_oi_corr_20", "volume_oi_corr_20"}:
        return "native_volume_oi_roll_aware"
    if column.startswith("divergence_price_"):
        source_column = column.removeprefix("divergence_price_").removesuffix("_score")
        if source_column in ROLL_BOUNDARY_DERIVED_SOURCE_COLUMNS:
            return "native_state_relationship_roll_aware"
    if column.startswith(("rolling_", "session_", "week_", "opening_range_", "swing_")):
        if column.endswith("_position"):
            return "derived_relationship_roll_aware"
        return "pandas_window_derived_level"
    if column.startswith("distance_to_") or column.endswith("_position"):
        return "derived_relationship_roll_aware"
    if column.endswith("_code") or column.endswith("_change_1") or column.endswith("_slope_5"):
        return "derived_relationship_roll_aware"
    if "_corr_" in column or column.startswith("divergence_"):
        return "derived_relationship_roll_aware"
    return "derived_relationship_roll_aware"


def _warmup_for_derived_output(column: str, *, default: int) -> int:
    if column.endswith("_change_1"):
        return 1
    if column.endswith("_slope_5"):
        return 5
    if "_corr_20" in column or column.startswith("divergence_price_"):
        return 20
    if column.startswith("rolling_") and column.endswith("_20"):
        return 20
    if "_55" in column:
        return 55
    return default


def rules_for_derived_profile(
    profile: DerivedIndicatorProfile | None = None,
    *,
    rule_set_version: str = DEFAULT_RULE_SET_VERSION,
) -> tuple[IndicatorRollRule, ...]:
    resolved = profile or current_derived_indicator_profile()
    rules: list[IndicatorRollRule] = []
    for column in resolved.output_columns:
        group_id = _group_for_derived_output(column)
        rules.append(
            IndicatorRollRule(
                rule_set_version=rule_set_version,
                indicator_id=column,
                output_column=column,
                output_family="derived",
                formula_id=column,
                calculation_group_id=group_id,
                adapter_input_columns=("cf_indicator_input_frame", "continuous_front_indicator_frames"),
                adapter_output_columns=(column,),
                warmup_bars=_warmup_for_derived_output(column, default=resolved.warmup_bars),
                requires_base_columns=() if group_id == "pandas_window_derived_level" else ("declared_by_formula",),
                requires_input_columns=("open0", "high0", "low0", "close0", "cumulative_additive_offset"),
            )
        )
    return tuple(rules)


def default_indicator_roll_rules(
    *,
    indicator_profile: IndicatorProfile | None = None,
    derived_profile: DerivedIndicatorProfile | None = None,
    rule_set_version: str = DEFAULT_RULE_SET_VERSION,
) -> tuple[IndicatorRollRule, ...]:
    return (
        *rules_for_indicator_profile(indicator_profile, rule_set_version=rule_set_version),
        *rules_for_derived_profile(derived_profile, rule_set_version=rule_set_version),
    )


def rules_to_rows(rules: tuple[IndicatorRollRule, ...], *, created_at_utc: str | None = None) -> list[dict[str, object]]:
    timestamp = created_at_utc or _utc_now_iso()
    return [rule.to_dict(created_at_utc=timestamp) for rule in rules]


def rule_set_hash(rules: tuple[IndicatorRollRule, ...]) -> str:
    payload = [rule.to_dict(include_created_at=False) for rule in sorted(rules, key=lambda item: item.output_column)]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def adapter_bundle_hash(rules: tuple[IndicatorRollRule, ...], *, dependency_lock_hash: str = "") -> str:
    payload = {
        "adapters": sorted({(rule.group.adapter_id, ADAPTER_VERSION) for rule in rules}),
        "rule_hashes": sorted(rule.rule_hash for rule in rules),
        "output_columns": sorted(rule.output_column for rule in rules),
        "dependency_lock_hash": dependency_lock_hash,
    }
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def assert_rule_coverage(*, output_columns: set[str], rules: tuple[IndicatorRollRule, ...]) -> None:
    covered = {rule.output_column for rule in rules}
    missing = sorted(output_columns - covered)
    if missing:
        raise ValueError("missing indicator roll rules for output columns: " + ", ".join(missing))
    duplicates = sorted(
        column
        for column in covered
        if sum(1 for rule in rules if rule.output_column == column) > 1
    )
    if duplicates:
        raise ValueError("duplicate indicator roll rules for output columns: " + ", ".join(duplicates))
