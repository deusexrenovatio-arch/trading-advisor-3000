"""KST must use the current causal anchor and retain bounded replay."""

from dataclasses import replace

import numpy as np
import pandas as pd
import pandas_ta_classic as ta
import pytest
from test_research_indicator_layer import _continuous_view

from trading_advisor_3000.product_plane.research.continuous_front_indicators.rules import (
    rules_for_indicator_profile,
)
from trading_advisor_3000.product_plane.research.indicators import materialize as m
from trading_advisor_3000.product_plane.research.indicators.registry import (
    default_indicator_profile,
)


def profile():
    original = default_indicator_profile()
    return m._profile_for_specs(
        original, tuple(s for s in original.indicators if s.operation_key == "kst")
    )


def bars(n=1500):
    x = np.arange(n, dtype=float)
    prices = 100 + x * 0.015 + 3 * np.sin(x / 11)
    result = [
        _continuous_view(
            ts_index=i,
            close=float(prices[i]) + (1000.0 if i >= 400 else 0),
            roll_epoch=int(i >= 400),
            active_contract_id="BR-next" if i >= 400 else "BR-first",
            is_roll_bar=i == 400,
        )
        for i in range(n)
    ]
    # Legitimate positive native quote, but zero in the erroneous P0 space.
    result[950] = replace(result[950], close=1000.0, native_close=1000.0)
    return result


def options(rows):
    return dict(
        dataset_version="dataset-v3",
        indicator_set_version="kst-anchor-proof",
        profile=profile(),
        series_mode="continuous_front",
        adjustment_ladder_rows=(
            dict(
                instrument_id="BR",
                timeframe="15m",
                roll_sequence=1,
                additive_gap=1000.0,
                effective_ts=rows[400].ts,
            ),
        ),
    )


def same_values(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected, strict=True):
        assert a.ts == e.ts
        for column in profile().expected_output_columns():
            assert (a.values[column] is None) == (e.values[column] is None)
            if a.values[column] is not None:
                assert a.values[column] == pytest.approx(e.values[column], rel=1e-8, abs=1e-8)


def test_kst_is_anchor_sensitive_like_its_roc_components():
    assert {r.calculation_group_id for r in rules_for_indicator_profile(profile())} == {
        "anchor_sensitive_roll_aware"
    }


def test_kst_matches_native_formula_in_each_bars_known_anchor():
    rows = bars()
    frame = pd.DataFrame([r.to_dict() for r in rows])
    actual = m._compute_continuous_front_profile_frame(
        frame, profile(), adjustment_ladder_rows=options(rows)["adjustment_ladder_rows"]
    )
    for epoch in (0, 1):
        indices = frame.index[frame.roll_epoch == epoch]
        prices = frame.loc[frame.roll_epoch <= epoch, "native_close"].copy()
        if epoch:
            prices.loc[frame.roll_epoch == 0] += 1000.0
        expected = ta.kst(prices)
        for column, native_column in zip(profile().expected_output_columns(), expected.columns):
            np.testing.assert_allclose(
                actual.loc[indices, column],
                expected.loc[indices, native_column],
                rtol=1e-10,
                atol=1e-10,
                equal_nan=True,
            )


@pytest.mark.parametrize("bad", [0.0, 6.8e-13, np.inf])
def test_invalid_roc_base_is_null_before_kst_smoothing_and_has_reason(bad):
    close = pd.Series(100 + np.sin(np.arange(200) / 4))
    close.iloc[80] = bad
    frame = pd.DataFrame({"close": close, "native_close": 1300.0})
    outputs = m._compute_spec(frame, profile().indicators[0])
    for output in outputs.values():
        assert output.loc[110:124].isna().all()
        assert output.loc[150:].notna().all()
        assert output.attrs["invalid_price_inputs"]["reason"] == "invalid_roc_price_base"
        assert output.attrs["invalid_price_inputs"]["indices"] == [80]


def test_guard_preserves_valid_tiny_prices_and_native_module_globals():
    spec = profile().indicators[0]
    close = pd.Series((100 + np.sin(np.arange(200) / 4)) * 1e-20)
    frame = pd.DataFrame({"close": close, "native_close": close})
    original_roc = ta.kst.__globals__["roc"]
    expected = ta.kst(close)
    actual = m._compute_spec(frame, spec)
    for column, native_column in zip(spec.output_columns, expected.columns):
        np.testing.assert_array_equal(actual[column], expected[native_column])
    frame.loc[80, "close"] = 0.0
    guarded = m._compute_spec(frame, spec)
    assert guarded[spec.output_columns[0]].loc[80:89].notna().all()
    assert ta.kst.__globals__["roc"] is original_roc


def test_invalid_base_reason_reaches_materializer_diagnostics():
    rows = bars(1100)
    rows[950] = replace(rows[950], close=0.0, native_close=0.0)
    diagnostic = {}
    actual = m._build_partition_rows(series=rows, compute_diagnostics=diagnostic, **options(rows))
    assert actual[980].values[profile().expected_output_columns()[0]] is None
    reasons = diagnostic["invalid_price_inputs"]
    assert any(
        v["reason"] == "invalid_roc_price_base" and 950 in v["indices"] for v in reasons.values()
    )


@pytest.mark.parametrize("change", ["append", "old_bar_correction"])
def test_kst_restart_matches_full_and_bounds_formula_work(change):
    rows = bars()
    opts = options(rows)
    seed = m._build_partition_rows(series=rows[:1480], **opts)
    seed = [m.IndicatorFrameRow.from_dict(r.to_dict()) for r in seed]
    if change == "old_bar_correction":
        rows[1220] = replace(
            rows[1220],
            close=rows[1220].close + 0.4,
            native_close=rows[1220].native_close + 0.4,
        )
    diagnostic = {}
    actual = m._build_partition_rows(
        series=rows, existing_rows=seed, compute_diagnostics=diagnostic, **opts
    )
    expected = m._build_partition_rows(series=rows, **opts)
    same_values(actual, expected)
    assert diagnostic.get("cf_tail_partition_count") == 1, diagnostic
    assert diagnostic["cf_replay_start"] <= (1220 if change != "append" else 1480)
    assert all(w["full_history_rows"] == 0 for w in diagnostic["cf_formula_work"].values())
    assert max(w["formula_rows"] for w in diagnostic["cf_formula_work"].values()) <= 1024


def test_future_roll_gap_does_not_change_past_kst():
    rows = bars()
    for i in range(1300, len(rows)):
        rows[i] = replace(rows[i], roll_epoch=2)
    opts = options(rows)
    future = dict(
        instrument_id="BR",
        timeframe="15m",
        roll_sequence=2,
        additive_gap=50.0,
        effective_ts=rows[1300].ts,
    )
    first = m._build_partition_rows(
        series=rows, **{**opts, "adjustment_ladder_rows": (*opts["adjustment_ladder_rows"], future)}
    )
    second = m._build_partition_rows(
        series=rows,
        **{
            **opts,
            "adjustment_ladder_rows": (
                *opts["adjustment_ladder_rows"],
                {**future, "additive_gap": 5000.0},
            ),
        },
    )
    same_values(first[:1300], second[:1300])


def test_checkpoint_context_binds_price_rules(monkeypatch):
    rows = bars(1100)
    opts = options(rows)
    seed = m._build_partition_rows(series=rows[:1080], **opts)
    original = m.rules_for_indicator_profile

    def previous_rules(p):
        return tuple(replace(r, calculation_group_id="oscillator_on_p0") for r in original(p))

    monkeypatch.setattr(m, "rules_for_indicator_profile", previous_rules)
    old = m._build_partition_rows(series=rows[:1080], **opts)
    monkeypatch.setattr(m, "rules_for_indicator_profile", original)
    # A self-consistent checksum does not certify a different calculation policy.
    assert any(r.compute_state_json for r in old)
    assert any(r.compute_state_json for r in seed)
    diagnostic = {}
    actual = m._build_partition_rows(
        series=rows, existing_rows=old, compute_diagnostics=diagnostic, **opts
    )
    assert not diagnostic.get("cf_tail_partition_count"), diagnostic
    assert diagnostic.get("cf_full_replacement_required"), diagnostic
    same_values(actual, m._build_partition_rows(series=rows, **opts))
