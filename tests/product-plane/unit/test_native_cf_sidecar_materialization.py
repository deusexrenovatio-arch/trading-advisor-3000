"""Literal CF sidecar results and immutable-parent guards; no native job claims."""

import copy
import runpy
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.research import native_cf as cf
from trading_advisor_3000.product_plane.research.native_cf_policy import cf_policy
from trading_advisor_3000.product_plane.research.semantic_fingerprint import SymbolGraph

PREFIX = runpy.run_path(str(Path(__file__).with_name("test_native_cf_prefix_profiles.py")))


def values(data, *, rows=()):
    policy = cf_policy("CF-L1")
    profile = cf.cf_l1_profile(policy)
    return cf._cf_l1_values(data, profile, cf.cf_call_admission(policy, profile), rows)


def test_l1_preserves_decimal_input_and_returns_only_new_values_with_literal_seeds():
    source = PREFIX["frame"](400, 200)
    for name in ["open", "high", "low", "close", "volume"]:
        source[name] = source[name].map(lambda value: Decimal(str(value)))
    source["true_range"] = 4.0
    original = source.copy(deep=True)
    actual = values(source, rows=PREFIX["ladder"]())
    pd.testing.assert_frame_equal(source, original)
    assert "true_range" not in actual and "volume" not in actual
    assert actual.loc[:18, "sma_20"].isna().all()
    assert actual.loc[19, "sma_20"] == 109.5
    assert actual.loc[200, "sma_20"] == 390.5
    assert actual.loc[19, "ema_20"] == 109.5
    assert actual.loc[20, "ema_20"] == 110.5
    assert (
        actual.loc[:32, ["macd_12_26_9", "macd_signal_12_26_9", "macd_hist_12_26_9"]]
        .isna()
        .all()
        .all()
    )
    assert actual.loc[
        33, ["macd_12_26_9", "macd_signal_12_26_9", "macd_hist_12_26_9"]
    ].to_list() == pytest.approx([7, 7, 0])
    assert pd.isna(actual.loc[200, "pvt"])
    assert actual.loc[201, "pvt"] == pytest.approx(52.75)
    assert actual.loc[19, "rvol_20"] == pytest.approx(29 / 19.5)
    assert actual.loc[200:218, "rvol_20"].isna().all()


def test_not_applicable_quantity_never_invokes_volume_library_specs(monkeypatch):
    from trading_advisor_3000.product_plane.research.indicators import materialize

    policy = cf_policy("CF-L1")
    quantity_specs = {r["spec_id"] for r in policy["catalog"] if r["quantity_requirements"]}
    native = materialize._compute_spec

    def guard(data, spec):
        assert spec.indicator_id not in quantity_specs, "N/A quantity entered native formula"
        return native(data, spec)

    monkeypatch.setattr(materialize, "_compute_spec", guard)
    source = PREFIX["frame"](400)
    source["volume"] = np.nan
    source["volume_state"] = "not_applicable"
    actual = values(source)
    names = [
        r["output"] for r in policy["catalog"] if r["materialized"] and r["quantity_requirements"]
    ]
    assert actual[names].isna().all().all()
    assert actual.loc[399, "sma_20"] == 489.5


def test_mixed_full_cf_key_rejects_before_any_library_call(monkeypatch):
    from trading_advisor_3000.product_plane.research.indicators import materialize

    monkeypatch.setattr(materialize, "_compute_spec", lambda *args: pytest.fail("mixed key call"))
    source = PREFIX["frame"](40)
    source.loc[39, "cf_definition_json"] = '{"synthetic":"OTHER"}'
    with pytest.raises(ValueError, match="one full definition/timeframe"):
        values(source)


def test_parent_float_signed_zero_is_preserved_bit_for_bit():
    parent = pa.table(
        {"cf_definition_json": ["key"], "timeframe": ["1m"], "source_begin": ["a"], "value": [-0.0]}
    )
    grain = ["cf_definition_json", "timeframe", "source_begin"]
    good = parent.append_column("sma_20", pa.array([None], type=pa.float64()))
    cf._cf_inherited_values(good, parent, grain)
    changed = good.set_column(3, "value", pa.array([0.0]))
    assert changed.select(parent.column_names).equals(parent)
    with pytest.raises(ValueError, match="parent float bits"):
        cf._cf_inherited_values(changed, parent, grain)


def measured_frame(n=40):
    data = PREFIX["frame"](n)
    data["cf_definition_json"] = "{}"
    for name in [
        "namespace",
        "provider",
        "venue",
        "engine",
        "market",
        "board",
        "source_instrument_id",
    ]:
        data[name] = name
    data["true_range"] = 4.0
    actual = values(data)
    data = pd.concat(
        [data, actual.drop(columns=["cf_definition_json", "timeframe", "source_begin"])], axis=1
    )
    keys = frozenset(
        tuple(row)
        for row in data[
            [
                "namespace",
                "provider",
                "venue",
                "engine",
                "market",
                "board",
                "source_instrument_id",
                "timeframe",
                "source_begin",
            ]
        ].itertuples(index=False, name=None)
    )
    return data, keys


def test_pointwise_capability_does_not_erase_prior_ready_rows_for_late_quantity_unknown():
    data, keys = measured_frame()
    policy = cf_policy("CF-L1")
    data.loc[39, "volume_state"] = "unknown"
    data.loc[39, "volume"] = np.nan
    # Actual re-invocation, not a slice of the old full result.
    actual = values(
        data.drop(
            columns=[
                r["output"]
                for r in policy["catalog"]
                if r["materialized"] and r["passthrough"] is None
            ]
        )
    )
    for name in actual.columns[3:]:
        data[name] = actual[name]
    entries = {r["feature"]: r for r in cf.cf_sidecar_capabilities(data, policy, keys, ())}
    assert entries["pvt"]["state"] == "ready"
    assert entries["pvt"]["first_finite_bar_index"] == 1
    assert entries["pvt"]["analysis_finite_count"] == 38
    assert pd.isna(data.loc[39, "pvt"])


def test_capability_rejects_finite_value_before_atomic_macd_admission():
    data, keys = measured_frame()
    cf.cf_sidecar_capabilities(data, cf_policy("CF-L1"), keys, ())
    data.loc[32, "macd_12_26_9"] = 7.0
    with pytest.raises(ValueError, match="without pointwise admission: macd"):
        cf.cf_sidecar_capabilities(data, cf_policy("CF-L1"), keys, ())


def test_self_consistent_policy_catalog_omission_is_rejected():
    policy = cf_policy("CF-L1")
    forged = copy.deepcopy(policy)
    forged["catalog"] = [row for row in forged["catalog"] if row["output"] != "pvt"]
    forged["catalog_sha256"] = cf.sha(forged["catalog"])
    context = {"layer": "CF-L1", "policy_payload": forged}
    with pytest.raises(ValueError, match="fixed family policy/catalog"):
        cf._validate_cf_sidecar(context, {}, {})


def test_numerical_closure_includes_real_cf_kernel_and_pointwise_admission():
    graph = SymbolGraph(Path(cf.__file__).resolve().parents[2])
    root = "trading_advisor_3000.product_plane.research.native_cf._cf_l1_values"
    identity = graph.fingerprint((root,))
    assert isinstance(identity, str) and len(identity) == 64
    required = {
        "_compute_continuous_front_profile_frame",
        "_enforce_continuous_front_native_boundaries",
        "_cf_call_frame",
        "_cf_row_admission",
        "_cf_quantity_prefix_mask",
    }
    for name in required:
        symbol = "trading_advisor_3000.product_plane.research.indicators.materialize." + name
        assert symbol in graph.selected and graph.selected[symbol] != "native-boundary.v1"


def test_unexplained_all_null_after_actual_macd_admission_is_rejected():
    data, keys = measured_frame()
    cf.cf_sidecar_capabilities(data, cf_policy("CF-L1"), keys, ())
    data["macd_12_26_9"] = np.nan
    with pytest.raises(ValueError, match="unexplained all-NULL CF output.*macd"):
        cf.cf_sidecar_capabilities(data, cf_policy("CF-L1"), keys, ())


@pytest.mark.parametrize("layer,role", [("CF-L1", "indicators"), ("CF-L2", "derived")])
def test_cf_sidecars_use_existing_receipt_replay_protocol_and_real_completion_contract(
    monkeypatch, tmp_path_factory, layer, role
):
    """External frames/parent are protocol doubles; actual shared lifecycle/schema stay live."""
    import json

    from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract
    from trading_advisor_3000.product_plane.research import native_feature_materialization as writer

    tmp_path = tmp_path_factory.mktemp("s")
    fixtures = runpy.run_path(str(Path(__file__).with_name("test_native_cf_materialization.py")))
    intent, trace = fixtures["protocol"](monkeypatch, tmp_path, family="cf")
    intent["layer"] = layer
    l0_pin = {"path": "l0.json", "sha256": "1" * 64}
    parent = {"l0_graph": {}, "l0_completion": l0_pin}
    monkeypatch.setattr(
        cf,
        "_parent",
        lambda *args: (
            type(
                "Reader",
                (),
                {
                    "compute_metadata": lambda self: {
                        "namespace": fixtures["SHAPES"]["seed"]()["namespace"],
                        "snapshot": {"snapshot_id": "parent"},
                    }
                },
            )(),
            parent,
        ),
    )
    inherited = {
        "snapshot_id": "parent",
        "tables": {"bars": {"old": "exact"}},
        "required_tables": ["bars"],
    }
    monkeypatch.setattr(
        cf,
        "build_cf_frames",
        lambda *args: (
            {role: object()},
            {role: {}},
            {"policy_payload": {"version": layer}},
            inherited,
        ),
    )
    facts = []
    monkeypatch.setattr(
        cf, "validate_cf_facts", lambda *args, **kwargs: facts.append(kwargs["l0_completion"])
    )

    def validate(name, payload, namespace):
        if name == "native_cf_completion.v1.json":
            validate_identity_contract(name, payload, namespace)

    monkeypatch.setattr(writer, "validate_identity_contract", validate)
    with pytest.raises(RuntimeError, match="lost acknowledgement"):
        writer.materialize_feature_command(
            tmp_path, intent, spark=object(), fault="after_completion"
        )
    receipt_path = next(tmp_path.glob("commands/*/completion.json"))
    before = receipt_path.read_bytes()
    receipt = json.loads(before)
    assert receipt["layer"] == layer and receipt["input_mode"] == intent["input_mode"]
    assert facts == [l0_pin]
    assert trace == ["table:" + cf.TABLES[role], "snapshot"]
    monkeypatch.setattr(
        cf, "build_cf_frames", lambda *args: pytest.fail("replay recomputed values")
    )
    result = writer.materialize_feature_command(tmp_path, intent, spark=object())
    assert result["replayed"] and receipt_path.read_bytes() == before
    assert trace == ["table:" + cf.TABLES[role], "snapshot"]


def subset_capabilities(operation, data, *, analysis_first=0, rows=()):
    """Isolate actual sealed rows and native specs; not a synthetic full-graph admission."""
    policy = cf_policy("CF-L1")
    policy["catalog"] = [row for row in policy["catalog"] if row["operation_key"] == operation]
    data = data.copy()
    data["cf_definition_json"] = "{}"
    rows = tuple({**row, "cf_definition_json": "{}"} for row in rows)
    profile = cf.cf_l1_profile(policy)
    actual = cf._cf_l1_values(data, profile, cf.cf_call_admission(policy, profile), rows)
    for name in actual.columns[3:]:
        data[name] = actual[name]
    for name in [
        "namespace",
        "provider",
        "venue",
        "engine",
        "market",
        "board",
        "source_instrument_id",
    ]:
        data[name] = name
    names = [
        "namespace",
        "provider",
        "venue",
        "engine",
        "market",
        "board",
        "source_instrument_id",
        "timeframe",
        "source_begin",
    ]
    analysis = frozenset(
        tuple(row) for row in data[names].iloc[analysis_first:].itertuples(index=False, name=None)
    )
    return data, {
        row["feature"]: row for row in cf.cf_sidecar_capabilities(data, policy, analysis, rows)
    }


def test_pvo_zero_denominator_is_classified_in_actual_reset_epoch_not_old_warmup():
    data = PREFIX["frame"](41, 1)
    data["volume"] = 0.0
    data.loc[0, "volume"] = 100.0
    actual, entries = subset_capabilities("pvo", data, analysis_first=1, rows=PREFIX["ladder"]())
    assert actual[["pvo_12_26_9", "pvo_signal_12_26_9", "pvo_hist_12_26_9"]].isna().all().all()
    assert {row["state"] for row in entries.values()} == {"undefined_arithmetic"}
    assert all("zero_volume" in row["reason"] for row in entries.values())
    # Same invocation/domain, genuine nonzero changing denominator remains ready.
    data.loc[1:, "volume"] = np.arange(1.0, 41.0)
    _, positive = subset_capabilities("pvo", data, analysis_first=1, rows=PREFIX["ladder"]())
    assert {row["state"] for row in positive.values()} == {"ready"}


@pytest.mark.parametrize("current_volume", [0.0, 50.0])
def test_current_epoch_rvol_and_zscore_zero_denominators_ignore_prior_epoch_quantity(
    current_volume,
):
    data = PREFIX["frame"](41, 1)
    data["volume"] = current_volume
    data.loc[0, "volume"] = 100.0
    _, entries = subset_capabilities("volume_norm", data, analysis_first=1, rows=PREFIX["ladder"]())
    assert entries["rvol_20"]["state"] == (
        "undefined_arithmetic" if current_volume == 0 else "ready"
    )
    assert entries["volume_z_20"]["state"] == "undefined_arithmetic"


def test_flat_directional_prices_have_positive_true_range_but_undefined_adx():
    data = PREFIX["frame"](80)
    data["open"], data["high"], data["low"], data["close"] = 99.5, 102.0, 98.0, 100.0
    actual, entries = subset_capabilities("adx", data)
    assert (actual["high"] - actual["low"]).eq(4.0).all()
    assert actual["adx_14"].isna().all()
    assert set(actual["dmp_14"].dropna()) == {0.0}
    assert set(actual["dmn_14"].dropna()) == {0.0}
    assert entries["adx_14"]["state"] == "undefined_arithmetic"
    assert entries["dmp_14"]["state"] == entries["dmn_14"]["state"] == "ready"


def test_future_unknown_quantity_does_not_veto_zero_denominator_prefix_classification():
    data = PREFIX["frame"](41, 1)
    data["volume"] = 0.0
    data.loc[0, "volume"] = 100.0
    prefix, _ = subset_capabilities("pvo", data, analysis_first=1, rows=PREFIX["ladder"]())
    extended = PREFIX["frame"](42, 1)
    extended["volume"] = 0.0
    extended.loc[[0, 41], "volume"] = 100.0
    extended.loc[41, "volume_state"] = "unknown"
    full, entries = subset_capabilities("pvo", extended, analysis_first=1, rows=PREFIX["ladder"]())
    names = ["pvo_12_26_9", "pvo_signal_12_26_9", "pvo_hist_12_26_9"]
    pd.testing.assert_frame_equal(prefix[names], full.loc[:40, names])
    assert {row["state"] for row in entries.values()} == {"undefined_arithmetic"}


def test_flat_typical_price_explains_zero_mfi_flow_with_known_positive_volume():
    data = PREFIX["frame"](80)
    data["open"], data["high"], data["low"], data["close"] = 99.5, 102.0, 98.0, 100.0
    actual, entries = subset_capabilities("mfi", data)
    assert actual["volume"].gt(0).all() and actual["mfi_14"].isna().all()
    assert entries["mfi_14"]["state"] == "undefined_arithmetic"
