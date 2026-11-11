"""Sealed observed-CF metadata; existing owners retain formulas and lifecycle."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

from .continuous_front_indicators.rules import (
    ADAPTER_VERSION,
    DEFAULT_RULE_SET_VERSION,
    rule_set_hash,
    rules_for_derived_profile,
    rules_for_indicator_profile,
)
from .derived_indicators.registry import core_v1_derived_indicator_profile
from .indicators.registry import core_v1_indicator_profile
from .native_core_policy import core_policy

LAYERS = ("CF", "CF-L1", "CF-L2")
TABLES = {
    "bars": "continuous_front_bars",
    "events": "continuous_front_roll_events",
    "ladder": "continuous_front_adjustment_ladder",
    "qc": "continuous_front_qc_report",
    "indicators": "continuous_front_indicator_frames",
    "derived": "continuous_front_derived_indicator_frames",
}
NEW_ROLES = {
    "CF": ("bars", "events", "ladder", "qc"),
    "CF-L1": ("indicators",),
    "CF-L2": ("derived",),
}
RESET_OPERATIONS = frozenset({"obv", "ad", "adosc", "pvt", "pvo"})
LIMITS = {
    "definitions": 2,
    "sequences": 8,
    "rows": 20000,
    "files": 128,
    "physical_bytes": 67108864,
    "receipt_depth": 4,
}
LIMITATIONS = {
    "snapshot_bound": True,
    "canonical_Q_verified": False,
    "historical_PIT_verified": False,
    "timezone_verified": False,
    "utc_closure_verified": False,
    "session_complete_verified": False,
    "execution_eligible": False,
}
PREFIX_SCOPE = {
    "version": "same_effective_prefix_and_used_references.v1",
    "same_inputs": [
        "effective_prefix_source_values",
        "used_reference_pins",
        "policy_and_invocation_mask_domains",
    ],
    "comparison": "admitted_numeric_values_and_masks_only_not_cross_context_wire_equality",
    "future_irrelevant": "future_unused_rows_and_references_do_not_backfill_admitted_prefix",
    "changed_used_references": "new_immutable_context_snapshot_cache_identity_values_may_differ",
    "embargo": "max_own_and_USED_reference_source_end_labels_no_UTC_or_PIT",
}
PRICE_BASES = {
    "price_level_post_transform": "P0",
    "price_range_on_p0": "P0",
    "oscillator_on_p0": "P0",
    "anchor_sensitive_roll_aware": "current_anchor",
    "price_volume_roll_aware": "current_anchor",
    "native_volume_oi_roll_aware": "native",
    "grid_locked_native": "native",
}


def _sha(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


def _field(name, sql_type="string", nullable=False):
    return {"name": name, "sql_type": sql_type, "nullable": nullable}


def cf_layouts(layer):
    if layer not in LAYERS:
        raise ValueError("unsupported observed CF layer")
    authority = json.loads(
        (Path(__file__).parents[1] / "contracts/schemas/market_identity.v3.json").read_text(
            encoding="utf-8"
        )
    )["$defs"]
    native = copy.deepcopy(
        authority["native_observation_snapshot"]["properties"]["storage_layout"]["const"][
            "native_bars"
        ]["fields"]
    )
    for field in native:
        if field["name"] == "instrument_id":
            field["name"] = "source_instrument_id"
    common = [_field("cf_definition_json"), _field("timeframe"), _field("cf_context_id")]
    bars = native + [
        _field("cf_definition_json"),
        _field("cf_context_id"),
        _field("cf_recipe_version"),
        _field("instrument_id", nullable=True),
        _field("active_instrument_id"),
        _field("planned_roll_ordinal", "bigint"),
        _field("roll_epoch", "bigint"),
        _field("is_first_bar_after_roll", "boolean"),
        _field("reference_calculation_embargo_source_label"),
        _field("true_range", "double", True),
        _field("log_ret_1", "double", True),
    ]
    event = common + [
        _field("roll_ordinal", "bigint"),
        _field("planned_roll_ordinal", "bigint"),
        _field("switch_source_date"),
        _field("observed_switch_source_begin"),
        _field("old_instrument_id"),
        _field("new_instrument_id"),
    ]
    for prefix in ("old", "new"):
        event += [
            _field(prefix + "_source_begin"),
            _field(prefix + "_source_end"),
            _field(prefix + "_body_sha256"),
            _field(prefix + "_receipt_sha256"),
            _field(prefix + "_row_ordinal", "bigint"),
            _field(prefix + "_reference_close", "decimal(38,10)"),
        ]
    event += [
        _field("additive_gap", "double"),
        _field("reference_calculation_embargo_source_label"),
    ]
    qc = (
        common
        + [
            _field(name, "bigint")
            for name in (
                "parent_inspected_rows",
                "eligible_component_rows",
                "selected_rows",
                "analysis_rows",
                "observed_events",
                "ladder_rows",
                "missing_reference_count",
            )
        ]
        + [_field("status")]
    )
    grain = ["cf_definition_json", "timeframe", "source_begin"]
    layouts = {
        "bars": {"fields": bars, "grain": grain},
        "events": {"fields": event, "grain": ["cf_definition_json", "timeframe", "roll_ordinal"]},
        "ladder": {
            "fields": copy.deepcopy(event),
            "grain": ["cf_definition_json", "timeframe", "roll_ordinal"],
        },
        "qc": {"fields": qc, "grain": ["cf_definition_json", "timeframe"]},
    }
    if layer in {"CF-L1", "CF-L2"}:
        fields = copy.deepcopy(bars)
        for current, role in [("CF-L1", "indicators"), ("CF-L2", "derived")]:
            policy = cf_policy(current, include_layouts=False)
            fields += [
                _field(row["output"], "double", True)
                for row in policy["catalog"]
                if row["materialized"] and row["passthrough"] is None
            ]
            fields += [_field(role + "_context_id"), _field(role + "_profile_version")]
            layouts[role] = {"fields": copy.deepcopy(fields), "grain": grain}
            if current == layer:
                break
    return layouts


def cf_policy(layer, *, include_layouts=True):
    if layer not in LAYERS:
        raise ValueError("unsupported observed CF layer")
    origin_layer = "L1" if layer == "CF" else layer.removeprefix("CF-")
    origin = core_policy(origin_layer)
    profile = (
        core_v1_indicator_profile() if origin_layer == "L1" else core_v1_derived_indicator_profile()
    )
    rules = (
        rules_for_indicator_profile(profile)
        if origin_layer == "L1"
        else rules_for_derived_profile(profile)
    )
    rules_by = {rule.output_column: rule for rule in rules}
    specs = (
        {name: spec for spec in profile.indicators for name in spec.output_columns}
        if origin_layer == "L1"
        else {}
    )
    safe = {row["spec_id"]: row for row in origin["spec_admission"]}
    catalog = []
    for original in origin["catalog"] if layer != "CF" else []:
        row = copy.deepcopy(original)
        name = row["output"]
        rule = rules_by[name]
        group = rule.group
        row.update(
            calculation_group=rule.calculation_group_id,
            allow_cross_contract_window=group.allow_cross_contract_window,
            declared_group_reset_on_roll=group.reset_on_roll,
        )
        if origin_layer == "L1":
            spec = specs[name]
            reset = spec.operation_key in RESET_OPERATIONS and group.reset_on_roll
            domain = "current_epoch_prefix" if reset else "composed_prefix"
            mask = (
                "current_epoch_prefix"
                if reset or not group.allow_cross_contract_window
                else "composed_prefix"
            )
            row.update(
                spec_id=spec.indicator_id,
                operation_key=spec.operation_key,
                parameters=spec.params_dict(),
                price_basis="native"
                if reset
                else PRICE_BASES.get(rule.calculation_group_id, "current_anchor"),
                add_offset=rule.calculation_group_id == "price_level_post_transform",
                invocation_owner="reset_epoch_profile" if reset else "epoch_prefix_spec",
                actual_reset=reset,
                invocation_domain=domain,
                mask_domain=mask,
                quantity_domain=mask,
                observed_call_safe=safe.get(spec.indicator_id, {}).get("observed_call_safe"),
                configured_group_minimum=safe.get(spec.indicator_id, {}).get(
                    "configured_group_minimum"
                ),
                mask_window_observations=max(
                    1, int(spec.params_dict().get("length", spec.warmup_bars))
                )
                if not group.allow_cross_contract_window
                else None,
            )
            if name == "true_range":
                row.update(
                    passthrough="cf_l0__true_range",
                    invocation_owner="composed_CF_L0_existing_P0_metric_kernel",
                    price_basis="P0",
                    invocation_domain="composed_prefix",
                    mask_domain="composed_prefix",
                    quantity_domain="composed_prefix",
                )
        else:
            minimum = (
                2 if name in {"close_change_1", "volume_change_1"} else row["admission_minimum"]
            )
            mask = (
                "current_epoch_prefix"
                if not group.allow_cross_contract_window or name == "volume_change_1"
                else "composed_prefix"
            )
            if name == "close_slope_20":
                row["passthrough"], minimum = "cf_l1__close_slope_20", 21
            if name == "rvol_20":
                row["passthrough"], minimum, mask = "cf_l1__rvol_20", 20, "current_epoch_prefix"
            row.update(
                admission_minimum=minimum,
                invocation_owner="exact_CF_L1_passthrough"
                if row["passthrough"]
                else "native_unmasked_derived_kernel",
                invocation_domain=mask if row["passthrough"] else "composed_prefix",
                mask_domain=mask,
                quantity_domain=mask,
                dependency_admission="declared_finite_inputs_and_existing_formula_windows",
                origin_formula_warmup_bars=rule.warmup_bars,
            )
        row["quantity_admission"] = (
            "pointwise_all_required_states_known_in_domain_prefix_to_row_no_future_veto"
        )
        if not row["materialized"]:
            row.update(
                invocation_owner="not_admitted_no_formula_call",
                invocation_domain="not_admitted",
                mask_domain="not_admitted",
                quantity_domain="not_admitted",
            )
        catalog.append(row)
    descriptors = [rule.to_dict(include_hash=False, include_created_at=False) for rule in rules]
    parent = {"CF": "L0", "CF-L1": "CF", "CF-L2": "CF-L1"}[layer]
    result = {
        "version": "native_cf_policy.v1",
        "input_mode": "native_cf_observation.v1",
        "layer": layer,
        "profile_version": "native_cf_composition.v1"
        if layer == "CF"
        else "native_cf_prefix_" + origin_layer.lower() + ".v1",
        "parent_layer": parent,
        "parent_contract_version": "native_research_l0_completion.v1"
        if layer == "CF"
        else "native_cf_completion.v1",
        "new_roles": list(NEW_ROLES[layer]),
        "limits": dict(LIMITS),
        "limitations": dict(LIMITATIONS),
        "history_policy": "pointwise_configured_prefix_admission.v1",
        "prefix_scope": copy.deepcopy(PREFIX_SCOPE),
        "composition": {
            "roll_offsets": [1, 2],
            "date_policy": "source_label_date_selection.v1",
            "switch": "first_selected_source_label_on_or_after_planned_switch_date",
            "reference": "last_selected_old_close_and_first_selected_new_close",
            "adjustment": "additive_causal_forward_current_epoch",
            "embargo": "max_own_and_USED_reference_source_end_labels_no_UTC_or_PIT",
            "legacy_spark_offset_guard": 2,
        },
        "origin_core_policy_version": origin["version"],
        "origin_core_policy_sha256": _sha(origin),
        "origin_profile_sha256": origin["origin_profile_sha256"],
        "resolved_origin_profile_sha256": origin["resolved_profile_sha256"],
        "rule_set_version": DEFAULT_RULE_SET_VERSION,
        "adapter_version": ADAPTER_VERSION,
        "legacy_rule_set_fingerprint": rule_set_hash(rules),
        "rule_set_sha256": _sha(descriptors),
        "runtime_profile": copy.deepcopy(origin["runtime_profile"]),
        "catalog": catalog,
        "catalog_sha256": _sha(catalog),
    }
    if include_layouts:
        result["layouts"] = cf_layouts(layer)
    return result


def cf_policy_ref(layer):
    policy = cf_policy(layer, include_layouts=False)
    return {
        name: policy[name]
        for name in (
            "version",
            "input_mode",
            "layer",
            "profile_version",
            "parent_layer",
            "parent_contract_version",
        )
    }
