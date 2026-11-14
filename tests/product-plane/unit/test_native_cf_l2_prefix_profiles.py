"""CF L2 prefix re-invocations, literal values and dependency/epoch boundaries."""

import runpy
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_advisor_3000.product_plane.research import native_cf as cf
from trading_advisor_3000.product_plane.research.native_cf_policy import cf_policy

PREFIX = runpy.run_path(str(Path(__file__).with_name("test_native_cf_prefix_profiles.py")))


def source(n, old_rows=None, *, flat=False):
    frame = PREFIX["frame"](n, old_rows)
    if flat:
        for name in ["open", "high", "low", "close"]:
            frame[name] = 100.0
    frame["true_range"] = 0.0 if flat else 4.0
    p = cf_policy("CF-L1")
    profile = cf.cf_l1_profile(p)
    rows = PREFIX["ladder"]() if old_rows is not None else ()
    l1 = cf._cf_l1_values(frame, profile, cf.cf_call_admission(p, profile), rows)
    return pd.concat(
        [frame, l1.drop(columns=["cf_definition_json", "timeframe", "source_begin"])], axis=1
    )


def compute(frame, rows=()):
    p = cf_policy("CF-L2")
    return cf._cf_l2_values(frame, cf.cf_l2_profile(p), p, rows)


def test_l2_short_changes_use_two_observations_and_do_not_return_passthroughs():
    frame = source(2)
    original = frame.copy(deep=True)
    actual = compute(frame)
    pd.testing.assert_frame_equal(frame, original)
    assert pd.isna(actual.loc[0, "close_change_1"])
    assert actual.loc[1, "close_change_1"] == 1.0
    assert actual.loc[1, "volume_change_1"] == 1.0
    assert "close_slope_20" not in actual and "rvol_20" not in actual
    assert "ts" not in frame and "session_date" not in frame


def test_l2_actual_299_300_reinvocation_guards_formula_requests_and_masks(monkeypatch):
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize

    native = materialize._compute_derived_frame_unmasked
    requested = []

    def observed(*, base_frame, profile):
        requested.append((len(base_frame), set(profile.output_columns)))
        return native(base_frame=base_frame, profile=profile)

    monkeypatch.setattr(materialize, "_compute_derived_frame_unmasked", observed)
    prefix = compute(source(299))
    assert requested == [(299, {"close_change_1", "volume_change_1"})]
    actual = compute(source(300))
    assert "rolling_high_20" in requested[1][1]
    assert "close_slope_20" not in requested[1][1] and "rvol_20" not in requested[1][1]
    assert actual.loc[:298, "rolling_high_20"].isna().all()
    assert actual.loc[299, "rolling_high_20"] == 401.0
    assert actual.loc[299, "rolling_low_20"] == 378.0
    assert actual.loc[299, "distance_to_rolling_high_20"] == -0.5
    assert actual.loc[299, "sma_20_slope_5"] == pytest.approx(45.0)
    pd.testing.assert_frame_equal(prefix, actual.iloc[:299])


def test_l2_full_cf_key_isolates_gap_and_composed_change_crosses_roll():
    frame = source(301, 300)
    own = PREFIX["ladder"]()[0]
    foreign = {**own, "cf_definition_json": '{"synthetic":"OTHER"}', "additive_gap": 50.0}
    actual = compute(frame, (own, foreign))
    assert actual.loc[300, "close_change_1"] == 1.0
    assert pd.isna(actual.loc[300, "volume_change_1"])
    assert actual.loc[300, "rolling_high_20"] == 502.0
    assert actual.loc[300, "distance_to_rolling_high_20"] == -0.5


def test_l2_late_unknown_quantity_does_not_veto_actual_earlier_reinvocation():
    prefix = compute(source(339))
    frame = source(340)
    frame.loc[339, "volume"] = np.nan
    frame.loc[339, "volume_state"] = "unknown"
    actual = compute(frame)
    pd.testing.assert_frame_equal(prefix, actual.iloc[:339])
    assert pd.isna(actual.loc[339, "volume_change_1"])
    assert pd.isna(actual.loc[339, "volume_zscore_20"])
    assert actual.loc[339, "rolling_high_20"] == 441.0


def test_l2_missing_atr_rejects_before_derived_kernel(monkeypatch):
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize

    monkeypatch.setattr(
        materialize,
        "_compute_derived_frame_unmasked",
        lambda **kwargs: pytest.fail("missing input entered formula"),
    )
    with pytest.raises(ValueError, match="missing CF L2 input dependencies.*atr_14"):
        compute(source(300).drop(columns=["atr_14"]))


def test_l2_finite_parent_dependency_required_without_filling_or_other_feature_veto():
    frame = source(301)
    frame.loc[300, "sma_20"] = np.nan
    actual = compute(frame)
    assert pd.isna(actual.loc[300, "distance_to_sma_20_atr"])
    assert pd.isna(actual.loc[300, "cross_close_sma_20_code"])
    assert actual.loc[300, "rolling_high_20"] == 402.0


def capability_for_feature(name, frame):
    """Actual sealed output descriptor/native formula; no full graph admission claim."""
    policy = cf_policy("CF-L2")
    policy["catalog"] = [row for row in policy["catalog"] if row["output"] == name]
    values = cf._cf_l2_values(frame, cf.cf_l2_profile(policy), policy, ())
    measured = pd.concat(
        [frame, values.drop(columns=["cf_definition_json", "timeframe", "source_begin"])], axis=1
    )
    for key in [
        "namespace",
        "provider",
        "venue",
        "engine",
        "market",
        "board",
        "source_instrument_id",
    ]:
        measured[key] = key
    keys = [
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
        tuple(row) for row in measured[keys].iloc[299:].itertuples(index=False, name=None)
    )
    return values[name], cf.cf_sidecar_capabilities(measured, policy, analysis, ())[0]


def test_l2_constant_price_and_volume_changes_explain_undefined_correlation():
    actual, entry = capability_for_feature("price_volume_corr_20", source(400))
    assert actual.isna().all()
    assert entry["state"] == "undefined_arithmetic" and "variance" in entry["reason"]


def test_l2_flat_native_atr_epsilon_is_preserved_without_rounding_to_zero():
    normal, positive = capability_for_feature("distance_to_sma_20_atr", source(400))
    assert normal.loc[399] == pytest.approx(2.375) and positive["state"] == "ready"
    flat = source(400, flat=True)
    assert set(flat["atr_14"].dropna()) == {np.finfo(float).eps}
    actual, entry = capability_for_feature("distance_to_sma_20_atr", flat)
    assert actual.loc[399] == 0.0 and entry["state"] == "ready"


def test_l2_zero_band_range_explains_undefined_position():
    frame = source(400, flat=True)
    assert frame.loc[399, "bb_lower_20_2"] == frame.loc[399, "bb_upper_20_2"] == 100.0
    actual, entry = capability_for_feature("bb_position_20_2", frame)
    assert actual.isna().all()
    assert entry["state"] == "undefined_arithmetic" and "position_range" in entry["reason"]


@pytest.mark.parametrize("name", ["swing_high_10", "swing_low_10"])
def test_l2_strictly_monotonic_price_has_no_confirmed_swing(name):
    actual, entry = capability_for_feature(name, source(400))
    assert actual.isna().all()
    assert entry["state"] == "undefined_arithmetic" and "no_confirmed_swing" in entry["reason"]


def test_l2_numerical_closure_covers_real_projection_formula_and_dependency_masks():
    from trading_advisor_3000.product_plane.research.semantic_fingerprint import SymbolGraph

    graph = SymbolGraph(Path(cf.__file__).resolve().parents[2])
    graph.fingerprint(("trading_advisor_3000.product_plane.research.native_cf._cf_l2_values",))
    required = [
        "native_cf._cf_output_admission",
        "indicators.materialize._cf_row_admission",
        "indicators.materialize._cf_quantity_prefix_mask",
        "derived_indicators.materialize._with_continuous_front_projection",
        "derived_indicators.materialize._compute_derived_frame_unmasked",
    ]
    for name in required:
        symbol = "trading_advisor_3000.product_plane.research." + name
        assert symbol in graph.selected and graph.selected[symbol] != "native-boundary.v1"


def test_l2_quantity_divergence_waits_for_current_epoch300_without_future_backfill():
    prefix = compute(source(699, 400), PREFIX["ladder"]())
    actual = compute(source(700, 400), PREFIX["ladder"]())
    name = "divergence_price_pvt_score"
    assert prefix.loc[400:, name].isna().all()
    assert np.isfinite(actual.loc[699, name])
    pd.testing.assert_frame_equal(prefix, actual.iloc[:699])
