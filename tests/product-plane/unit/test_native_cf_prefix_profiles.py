"""Experimental CF pointwise call-site regressions; no producer or oracle claims."""

import numpy as np
import pandas as pd
import pytest

from trading_advisor_3000.product_plane.research.indicators import materialize as owner
from trading_advisor_3000.product_plane.research.indicators.registry import (
    IndicatorProfile,
    core_v1_indicator_profile,
)


def profile(operation):
    specs = tuple(
        s
        for s in core_v1_indicator_profile().indicators
        if s.operation_key == operation and (operation != "sma" or s.indicator_id == "sma_20")
    )
    assert len(specs) == 1
    return IndicatorProfile("native_cf_prefix_l1.v1", "pointwise CF regression", specs)


def admission(selected):
    from trading_advisor_3000.product_plane.research.native_cf_policy import cf_policy

    ids = {s.indicator_id for s in selected.indicators}
    rows = [
        r
        for r in cf_policy("CF-L1")["catalog"]
        if r["materialized"] and r["passthrough"] is None and r["spec_id"] in ids
    ]
    return {
        "version": "pointwise_configured_prefix_admission.v1",
        "series_columns": ("cf_definition_json", "timeframe"),
        "specs": {spec: [r for r in rows if r["spec_id"] == spec] for spec in ids},
    }


def frame(n, old_rows=None):
    close = np.arange(n, dtype=float) + 100.0
    epoch = np.zeros(n, dtype=int)
    if old_rows is not None:
        epoch[old_rows:] = 1
        close[old_rows:] += 100.0
    return pd.DataFrame(
        {
            "instrument_id": [None] * n,
            "cf_definition_json": ['{"synthetic":"CF"}'] * n,
            "timeframe": ["1m"] * n,
            "source_begin": pd.date_range("2026-09-14 09:00:00", periods=n, freq="min").strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "open": close - 0.5,
            "high": close + 2.0,
            "low": close - 2.0,
            "close": close,
            "volume": np.arange(n, dtype=float) + 10.0,
            "volume_state": ["known"] * n,
            "open_interest": [np.nan] * n,
            "open_interest_state": ["missing_source"] * n,
            "roll_epoch": epoch,
        }
    )


def ladder():
    return (
        {
            "instrument_id": None,
            "cf_definition_json": '{"synthetic":"CF"}',
            "timeframe": "1m",
            "roll_sequence": 1,
            "roll_ordinal": 1,
            "additive_gap": 100.0,
        },
    )


def test_cf_macd33_does_not_invoke_native_spec(monkeypatch):
    native = owner._compute_spec

    def observed(data, spec):
        assert len(data) >= 34, "unsafe CF atomic MACD call below34"
        return native(data, spec)

    monkeypatch.setattr(owner, "_compute_spec", observed)
    actual = owner._compute_continuous_front_profile_frame(
        frame(33),
        profile("macd"),
        adjustment_ladder_rows=(),
        call_admission=admission(profile("macd")),
    )
    assert actual[list(profile("macd").expected_output_columns())].isna().all().all()


def test_cf_one_bar_pvt_epoch_does_not_invoke_reset_profile(monkeypatch):
    native = owner._compute_profile_frame

    def observed(data, selected):
        assert len(data) >= 2, "unsafe CF reset PVT call atonebar"
        return native(data, selected)

    monkeypatch.setattr(owner, "_compute_profile_frame", observed)
    actual = owner._compute_continuous_front_profile_frame(
        frame(41, 40),
        profile("pvt"),
        adjustment_ladder_rows=ladder(),
        call_admission=admission(profile("pvt")),
    )
    assert pd.isna(actual.iloc[-1]["pvt"])


def test_cf_actual_epoch_call_never_uses_future_epoch(monkeypatch):
    native = owner._compute_spec
    seen = []

    def observed(data, spec):
        seen.append((len(data), int(data["roll_epoch"].max())))
        return native(data, spec)

    monkeypatch.setattr(owner, "_compute_spec", observed)
    owner._compute_continuous_front_profile_frame(
        frame(31, 30),
        profile("sma"),
        adjustment_ladder_rows=ladder(),
        call_admission=admission(profile("sma")),
    )
    assert seen[0] == (30, 0)


def compute(data, operation, *, rows=()):
    selected = profile(operation)
    return owner._compute_continuous_front_profile_frame(
        data, selected, adjustment_ladder_rows=rows, call_admission=admission(selected)
    )


def test_macd_independent_prefix_reinvocations_keep_first33_null():
    names = profile("macd").expected_output_columns()
    previous = None
    for n in [33, 34, 35, 80]:
        actual = compute(frame(n), "macd")[list(names)]
        assert actual.iloc[:33].isna().all().all()
        if n >= 34:
            assert actual.iloc[33].to_numpy(dtype=float) == pytest.approx(
                [7.0, 7.0, 0.0], abs=1e-12
            )
        if previous is not None:
            pd.testing.assert_frame_equal(actual.iloc[: len(previous)], previous)
        previous = actual


def test_pvt_reset_reinvocations_and_actual_calls_use_only_epoch_segments(monkeypatch):
    native = owner._compute_spec
    seen = []

    def observed(data, spec):
        seen.append((len(data), set(data["roll_epoch"])))
        return native(data, spec)

    monkeypatch.setattr(owner, "_compute_spec", observed)
    one = compute(frame(41, 40), "pvt", rows=ladder())
    assert seen == [(40, {0})]
    seen.clear()
    two = compute(frame(42, 40), "pvt", rows=ladder())
    assert seen == [(40, {0}), (2, {1})]
    assert pd.isna(two.loc[40, "pvt"]) and two.loc[41, "pvt"] == pytest.approx(21.25)
    pd.testing.assert_series_equal(one["pvt"], two.loc[:40, "pvt"])


def test_late_quantity_unknown_does_not_veto_prior_admitted_prefix(monkeypatch):
    native = owner._compute_spec
    seen = []

    def observed(data, spec):
        seen.append(len(data))
        return native(data, spec)

    monkeypatch.setattr(owner, "_compute_spec", observed)
    prefix = compute(frame(40), "pvt")
    later = frame(41)
    later.loc[40, "volume"] = np.nan
    later.loc[40, "volume_state"] = "missing_source"
    full = compute(later, "pvt")
    pd.testing.assert_series_equal(prefix["pvt"], full.loc[:39, "pvt"])
    assert pd.isna(full.loc[40, "pvt"]) and seen == [40, 40]
    assert prefix.loc[1, "pvt"] == pytest.approx(11.0)


def test_reset_quantity_domain_recovers_after_prior_epoch_unknown():
    data = frame(42, 40)
    data.loc[10, "volume"] = np.nan
    data.loc[10, "volume_state"] = "missing_source"
    actual = compute(data, "pvt", rows=ladder())
    assert actual.loc[10:39, "pvt"].isna().all()
    assert pd.isna(actual.loc[40, "pvt"]) and actual.loc[41, "pvt"] == pytest.approx(21.25)


def test_full_definition_key_filters_own_ladder_with_null_instrument():
    data = frame(41, 40)
    own = ladder()[0]
    foreign = {**own, "cf_definition_json": '{"synthetic":"OTHER"}', "additive_gap": 50.0}
    actual = compute(data, "sma", rows=(own, foreign))
    assert actual.loc[40, "sma_20"] == pytest.approx(230.5)
    assert actual.loc[39, "sma_20"] == pytest.approx(129.5)


def test_future_unused_gap_does_not_change_separately_computed_prefix():
    prefix = frame(30)
    ordinary = compute(prefix, "sma", rows=ladder())
    changed = tuple({**r, "additive_gap": 9999.0} for r in ladder())
    modified = compute(prefix, "sma", rows=changed)
    pd.testing.assert_series_equal(ordinary["sma_20"], modified["sma_20"])


@pytest.mark.parametrize("operation", ["force_index", "cmf", "mfi"])
def test_declared_reset_flag_does_not_create_additional_reset_calls(monkeypatch, operation):
    native = owner._compute_spec
    seen = []

    def observed(data, spec):
        seen.append(len(data))
        return native(data, spec)

    monkeypatch.setattr(owner, "_compute_spec", observed)
    selected = profile(operation)
    assert all(
        not row["actual_reset"]
        for row in admission(selected)["specs"][selected.indicators[0].indicator_id]
    )
    compute(frame(41, 40), operation, rows=ladder())
    assert seen == [40, 41]
