"""Formula work, not just short writes, must be bounded after a restart."""

import json

import numpy as np
import pandas as pd
import pytest

from trading_advisor_3000.product_plane.research.indicators import materialize as m
from trading_advisor_3000.product_plane.research.indicators.native_checkpoint import (
    NativeCheckpoint,
)
from trading_advisor_3000.product_plane.research.indicators.registry import (
    default_indicator_profile,
)


def market_frame(n=1100):
    x = np.arange(n, dtype=float)
    close = 110 + 0.015 * x + 3 * np.sin(x / 11) + np.cos(x / 3)
    return pd.DataFrame(
        dict(
            close=close,
            high=close + 1.1,
            low=close - 0.9,
            volume=100 + x % 37,
            open_interest=500 + x % 53,
            true_range=2 + 0 * x,
            log_ret_1=pd.Series(close).pct_change(),
        )
    )


def test_nonfinite_hidden_state_survives_strict_json_roundtrip():
    initial = NativeCheckpoint()
    values = pd.Series([1.0, np.inf, -np.inf, np.nan])
    initial._remember("probe", values, upper=np.inf, lower=-np.inf, missing=np.nan)
    exported = initial.export()
    state = json.loads(json.dumps(exported, allow_nan=False))
    restored = NativeCheckpoint(previous=state, cutoff=4)
    old, cached = restored._cached("probe", values.index)
    np.testing.assert_array_equal(cached.to_numpy(), values.to_numpy())
    assert old["upper"] == np.inf and old["lower"] == -np.inf
    assert np.isnan(old["missing"])
    # Export must not replace live native numeric state with transport tokens.
    assert initial.streams["probe"]["values"][1] == np.inf


@pytest.mark.parametrize("token", ["infinity", None, [], {"extra": 1}])
def test_malformed_native_float_token_is_rejected(token):
    from trading_advisor_3000.product_plane.research.indicators.native_checkpoint import (
        CheckpointUnavailable,
    )

    state = NativeCheckpoint().export()
    state["streams"] = {"nested": [{"__native_float__": token}]}
    with pytest.raises(CheckpointUnavailable, match="malformed native float token"):
        NativeCheckpoint(previous=state, cutoff=1)


def test_native_float_tokens_do_not_replace_ordinary_nested_metadata():
    initial = NativeCheckpoint()
    initial.streams = {"nested": [True, False, None, "nan", 2.5, {"upper": np.inf}]}
    state = json.loads(json.dumps(initial.export(), allow_nan=False))
    restored = NativeCheckpoint(previous=state, cutoff=1)
    assert restored.previous["streams"] == initial.streams
    assert state["streams"]["nested"][-1] == {"upper": {"__native_float__": "+inf"}}
    state["streams"]["nested"][-1]["upper"]["extra"] = True
    from trading_advisor_3000.product_plane.research.indicators.native_checkpoint import (
        CheckpointUnavailable,
    )

    with pytest.raises(CheckpointUnavailable, match="malformed native float token"):
        NativeCheckpoint(previous=state, cutoff=1)


@pytest.mark.parametrize("old_format", ["native-checkpoint.v1", "native-checkpoint.v2"])
def test_legacy_checkpoint_format_requests_native_bootstrap(old_format):
    from trading_advisor_3000.product_plane.research.indicators.native_checkpoint import (
        CheckpointUnavailable,
    )

    state = NativeCheckpoint().export()
    state["format"] = old_format
    with pytest.raises(CheckpointUnavailable, match="identity changed"):
        NativeCheckpoint(previous=state, cutoff=1)


def test_pvt_infinite_native_state_continues_after_strict_json_checkpoint():
    frame = market_frame()
    frame.loc[990, "close"] = 0.0
    spec = next(s for s in default_indicator_profile().indicators if s.operation_key == "pvt")
    initial = NativeCheckpoint()
    initial.compute(m._compute_spec, frame.iloc[:1000], spec)
    assert any(
        np.isinf(v)
        for stream in initial.streams.values()
        for v in stream.get("values", [])
        if v is not None
    ), "fixture must exercise an actual native nonfinite hidden stream"
    state = json.loads(json.dumps(initial.export(), allow_nan=False))
    resumed = NativeCheckpoint(previous=state, cutoff=1000)
    actual = resumed.compute(m._compute_spec, frame.iloc[488:], spec)
    expected = m._compute_spec(frame, spec)
    for column in spec.output_columns:
        np.testing.assert_allclose(
            actual[column].reindex(frame.index[1000:]),
            expected[column].reindex(frame.index[1000:]),
            rtol=1e-10,
            atol=1e-10,
            equal_nan=True,
        )
    assert resumed.full_history_rows == 0
    assert resumed.formula_rows <= 612


@pytest.mark.parametrize(
    "spec", default_indicator_profile().indicators, ids=lambda s: s.indicator_id
)
def test_every_core_spec_continues_after_serialized_checkpoint(spec):
    frame = market_frame()
    if spec.operation_key == "volume_profile":
        return  # The separate minute/grid producer already has exact per-bar refresh.
    initial = NativeCheckpoint()
    initial.compute(m._compute_spec, frame.iloc[:1000], spec)
    state = json.loads(json.dumps(initial.export()))
    resumed = NativeCheckpoint(previous=state, cutoff=1000)
    actual = resumed.compute(m._compute_spec, frame.iloc[488:], spec)
    expected = m._compute_spec(frame, spec)
    for column in spec.output_columns:
        a = pd.to_numeric(actual[column].reindex(frame.index[1000:]), errors="coerce")
        e = pd.to_numeric(expected[column].reindex(frame.index[1000:]), errors="coerce")
        np.testing.assert_array_equal(a.isna(), e.isna())
        np.testing.assert_allclose(a, e, rtol=1e-10, atol=1e-10, equal_nan=True)
    assert resumed.full_history_rows == 0
    assert resumed.formula_rows <= 612


@pytest.mark.parametrize(
    "spec", default_indicator_profile().indicators, ids=lambda s: s.indicator_id
)
def test_historical_native_snapshot_replays_corrected_suffix(spec):
    if spec.operation_key == "volume_profile":
        return
    frame = market_frame()
    initial = NativeCheckpoint(snapshot_counts=(900,))
    initial.compute(m._compute_spec, frame.iloc[:1000], spec)
    state = json.loads(json.dumps(initial.export_at(900), allow_nan=False))
    frame.loc[950, "close"] += 0.4
    frame.loc[950, "volume"] += 19
    frame.loc[950, "open_interest"] += 11
    resumed = NativeCheckpoint(previous=state, cutoff=900)
    actual = resumed.compute(m._compute_spec, frame.iloc[388:], spec)
    expected = m._compute_spec(frame, spec)
    for column in spec.output_columns:
        np.testing.assert_allclose(
            pd.to_numeric(actual[column].reindex(frame.index[900:]), errors="coerce"),
            pd.to_numeric(expected[column].reindex(frame.index[900:]), errors="coerce"),
            rtol=1e-10,
            atol=1e-10,
            equal_nan=True,
            err_msg=column,
        )
    assert resumed.full_history_rows == 0
    assert resumed.formula_rows <= 712


@pytest.mark.parametrize("rolled", [False, True])
def test_cf_core_append_restores_hidden_states_and_never_recomputes_full_specs(rolled):
    from dataclasses import replace

    from test_research_indicator_layer import _continuous_view

    values = market_frame(1020)
    bars = [
        _continuous_view(
            ts_index=i,
            close=float(values.close.iloc[i]) + (10 if rolled and i >= 400 else 0),
            roll_epoch=int(rolled and i >= 400),
            active_contract_id="BR-6.26",
        )
        for i in range(1020)
    ]
    # Nullable OI is independent of the price recursive state.
    bars[40] = replace(bars[40], open_interest=None)
    options = dict(
        dataset_version="dataset-v3",
        indicator_set_version="core_v1",
        profile=default_indicator_profile(),
        series_mode="continuous_front",
    )
    if rolled:
        options["adjustment_ladder_rows"] = (
            {
                "instrument_id": "BR",
                "timeframe": "15m",
                "roll_sequence": 1,
                "additive_gap": 10.0,
                "effective_ts": bars[400].ts,
            },
        )
    initial = m._build_partition_rows(series=bars[:1000], **options)
    # Restart through the public serialization contract, not a live Python object.
    initial = [m.IndicatorFrameRow.from_dict(row.to_dict()) for row in initial]
    diagnostics = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=initial, compute_diagnostics=diagnostics, **options
    )
    expected = m._build_partition_rows(series=bars, **options)
    assert diagnostics.get("cf_tail_partition_count") == 1, diagnostics
    assert diagnostics["cf_formula_work"]
    assert all(w["full_history_rows"] == 0 for w in diagnostics["cf_formula_work"].values())
    assert all(w["formula_rows"] <= 532 for w in diagnostics["cf_formula_work"].values())
    for left, right in zip(actual, expected, strict=True):
        assert (left.ts, left.contract_id) == (right.ts, right.contract_id)
        assert left.values.keys() == right.values.keys()
        for column in left.values:
            a, b = left.values[column], right.values[column]
            assert (a is None) == (b is None), (left.ts, column, a, b)
            if a is not None:
                # Native pandas rolling variance has order-dependent rounding.
                # NULL/key/code parity remains exact; numerical tolerance is explicit.
                assert a == pytest.approx(b, rel=1e-8, abs=1e-8), (left.ts, column, a, b)


def test_l2_tail_core_append_matches_full_with_swing_and_week_scopes():
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as d
    from trading_advisor_3000.product_plane.research.derived_indicators.compute_tail import (
        compute,
        input_hashes,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
        current_derived_indicator_profile,
    )

    frame = market_frame(1700)
    for spec in default_indicator_profile().indicators:
        if spec.operation_key != "volume_profile":
            for name, series in m._compute_spec(frame, spec).items():
                frame[name] = series
    frame["ts"] = pd.date_range("2026-01-01", periods=len(frame), freq="15min", tz="UTC").astype(
        str
    )
    frame["session_date"] = frame.ts.str[:10]
    frame["contract_id"] = "BR-6.26"
    profile = current_derived_indicator_profile()
    old = d._compute_derived_frame(base_frame=frame.iloc[:1680], profile=profile)
    old["input_row_hash"] = input_hashes(frame.iloc[:1680], profile)
    old["compute_contract_hash"] = "numeric-test"
    diagnostics = {}
    actual, first = compute(
        frame, profile, old, input_hashes(frame, profile), "numeric-test", diagnostics
    )
    expected = d._compute_derived_frame(base_frame=frame, profile=profile)
    assert first == 1680
    assert diagnostics["l2_formula_rows"] < len(frame)
    for column in profile.output_columns:
        a = pd.to_numeric(actual[column], errors="coerce")
        b = pd.to_numeric(expected[column], errors="coerce")
        np.testing.assert_array_equal(a.isna(), b.isna(), err_msg=column)
        np.testing.assert_allclose(a, b, rtol=1e-10, atol=1e-10, equal_nan=True, err_msg=column)


def test_opaque_adapter_bytes_change_kernel_and_invalidate_cache(tmp_path, monkeypatch):
    from pathlib import Path

    from trading_advisor_3000.product_plane.research import compute_contract as c
    from trading_advisor_3000.product_plane.research.indicators import native_checkpoint as n

    target = tmp_path / "product_plane/research/indicators"
    target.mkdir(parents=True)
    for name in ("cf_tail", "native_checkpoint"):
        (target / (name + ".py")).write_bytes(Path(n.__file__).with_name(name + ".py").read_bytes())
    root = "trading_advisor_3000.product_plane.research.indicators.cf_tail.compute"
    monkeypatch.setattr(c, "_ROOTS", {"l1": (root,)})
    monkeypatch.setattr(c, "_LEAVES", ())
    before = c.kernel_contract(package=tmp_path)["l1"]
    for name in ("cf_tail", "native_checkpoint"):
        path = target / (name + ".py")
        path.write_text(path.read_text() + "\n# source pin mutation\n")
        after = c.kernel_contract(package=tmp_path)["l1"]
        assert after != before
        before = after


@pytest.mark.parametrize("operation", ["ad", "adosc", "atr", "stoch"])
def test_old_zero_range_regime_outside_overlap_matches_native(operation):
    frame = market_frame()
    # A flat source bar before the retained overlap must remain in the native
    # global epsilon regime even when the bounded window contains no flat bar.
    frame.loc[20, "high"] = frame.loc[20, "low"]
    spec = next(s for s in default_indicator_profile().indicators if s.operation_key == operation)
    initial = NativeCheckpoint()
    initial.compute(m._compute_spec, frame.iloc[:1000], spec)
    resumed = NativeCheckpoint(previous=json.loads(json.dumps(initial.export())), cutoff=1000)
    actual = resumed.compute(m._compute_spec, frame.iloc[488:], spec)
    expected = m._compute_spec(frame, spec)
    for name in spec.output_columns:
        np.testing.assert_allclose(
            actual[name].reindex(frame.index[1000:]),
            expected[name].reindex(frame.index[1000:]),
            rtol=1e-10,
            atol=1e-10,
            equal_nan=True,
        )
