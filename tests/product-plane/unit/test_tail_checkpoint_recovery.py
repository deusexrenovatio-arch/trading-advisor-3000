"""Independent-review regressions: malformed/unsupported state and eligibility gaps."""

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest
from test_native_checkpoint_compute import market_frame
from test_research_indicator_layer import _continuous_view, _sma2_profile

from trading_advisor_3000.product_plane.research.indicators import materialize as m
from trading_advisor_3000.product_plane.research.indicators.registry import (
    default_indicator_profile,
)


@pytest.mark.parametrize("bad", ["null", "1", '"x"', "[]", '{"states":null}'])
def test_malformed_cf_checkpoint_falls_back_to_full(bad):
    bars = [
        _continuous_view(ts_index=i, close=100 + i * 0.1, roll_epoch=0, active_contract_id="BR")
        for i in range(40)
    ]
    options = dict(
        dataset_version="dataset-v3",
        indicator_set_version="test",
        profile=_sma2_profile(),
        series_mode="continuous_front",
    )
    old = m._build_partition_rows(series=bars[:39], **options)
    old[-1] = replace(old[-1], compute_state_json=bad)
    diagnostic = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=old, compute_diagnostics=diagnostic, **options
    )
    expected = m._build_partition_rows(series=bars, **options)
    assert [r.values for r in actual] == [r.values for r in expected]
    assert not diagnostic.get("cf_tail_partition_count")
    assert diagnostic["cf_tail_fallback_reason"]


def test_unsupported_native_source_is_never_bootstrapped_as_compatible(monkeypatch):
    from trading_advisor_3000.product_plane.research.indicators import native_checkpoint as n

    monkeypatch.setattr(n, "identity", lambda: "unreviewed-native-source")
    with pytest.raises(n.CheckpointUnavailable, match="unsupported"):
        n.NativeCheckpoint()


def test_untouched_eligibility_state_survives_two_serialized_appends():
    profile = m._profile_for_specs(
        default_indicator_profile(),
        tuple(
            s
            for s in default_indicator_profile().indicators
            if s.indicator_id in {"sma_20", "oi_change_1"}
        ),
    )
    bars = [
        replace(
            _continuous_view(
                ts_index=i, close=100 + i * 0.1, roll_epoch=0, active_contract_id="BR"
            ),
            bar_usage_flags=1 if 1000 <= i < 1005 else 127,
        )
        for i in range(1006)
    ]
    options = dict(
        dataset_version="dataset-v3",
        indicator_set_version="test",
        profile=profile,
        series_mode="continuous_front",
    )
    old = m._build_partition_rows(series=bars[:1000], **options)
    for end in (1005, 1006):
        old = [m.IndicatorFrameRow.from_dict(r.to_dict()) for r in old]
        diag = {}
        old = m._build_partition_rows(
            series=bars[:end], existing_rows=old, compute_diagnostics=diag, **options
        )
        assert diag.get("cf_tail_partition_count") == 1, diag
        assert all(w["full_history_rows"] == 0 for w in diag["cf_formula_work"].values())
        expected = m._build_partition_rows(series=bars[:end], **options)
        assert [r.values for r in old] == [r.values for r in expected]


@pytest.mark.parametrize("side", ["high", "low"])
@pytest.mark.parametrize("change", ["append", "correction", "deletion"])
def test_l2_single_swing_profile_and_changed_history(side, change):
    from trading_advisor_3000.product_plane.research.derived_indicators import materialize as d
    from trading_advisor_3000.product_plane.research.derived_indicators.compute_tail import (
        compute,
        input_hashes,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
        current_derived_indicator_profile,
    )

    frame = market_frame()
    frame["ts"] = pd.date_range("2026-01-01", periods=len(frame), freq="15min", tz="UTC").astype(
        str
    )
    frame["session_date"] = frame.ts.str[:10]
    frame["atr_14"] = 2.0
    profile = replace(current_derived_indicator_profile(), output_columns=(f"swing_{side}_10",))
    old = d._compute_derived_frame(base_frame=frame.iloc[:1000], profile=profile)[
        ["ts", *profile.output_columns]
    ]
    old["input_row_hash"] = input_hashes(frame.iloc[:1000], profile)
    old["compute_contract_hash"] = "numeric-test"
    if change == "correction":
        frame.loc[990, side] += 5
    elif change == "deletion":
        frame = frame.drop(index=990).reset_index(drop=True)
    actual, start = compute(frame, profile, old, input_hashes(frame, profile), "numeric-test", {})
    expected = d._compute_derived_frame(base_frame=frame, profile=profile)
    assert 0 < start <= 1000
    for column in profile.output_columns:
        np.testing.assert_allclose(
            pd.to_numeric(actual[column]), pd.to_numeric(expected[column]), equal_nan=True
        )


def test_supertrend_minimum_length_revalidates_previous_nulls():
    profile = m._profile_for_specs(
        default_indicator_profile(),
        tuple(s for s in default_indicator_profile().indicators if s.operation_key == "supertrend"),
    )
    bars = [
        _continuous_view(ts_index=i, close=100 + i * 0.1, roll_epoch=0, active_contract_id="BR")
        for i in range(10)
    ]
    options = dict(
        dataset_version="dataset-v3",
        indicator_set_version="test",
        profile=profile,
        series_mode="continuous_front",
    )
    old = m._build_partition_rows(series=bars[:9], **options)
    diag = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=old, compute_diagnostics=diag, **options
    )
    expected = m._build_partition_rows(series=bars, **options)
    assert [r.values for r in actual] == [r.values for r in expected]
    assert diag.get("cf_full_replacement_required"), diag


def test_nonmonotonic_epoch_rejects_checkpoint_admission():
    bars = [
        _continuous_view(
            ts_index=i,
            close=100 + i * 0.1,
            roll_epoch=0 if i < 20 or i == 39 else 1,
            active_contract_id="BR",
        )
        for i in range(40)
    ]
    ladder = (
        {
            "instrument_id": "BR",
            "timeframe": "15m",
            "roll_sequence": 1,
            "effective_ts": bars[20].ts,
            "additive_gap": 10.0,
        },
    )
    options = dict(
        dataset_version="dataset-v3",
        indicator_set_version="test",
        profile=_sma2_profile(),
        series_mode="continuous_front",
        adjustment_ladder_rows=ladder,
    )
    old = m._build_partition_rows(series=bars[:39], **options)
    diag = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=old, compute_diagnostics=diag, **options
    )
    assert diag["cf_tail_fallback_reason"] == "nonmonotonic_roll_epoch"
    assert diag["cf_full_replacement_required"]
    assert not actual[-1].compute_state_json
    expected = m._build_partition_rows(series=bars, **options)
    assert [r.values for r in actual] == [r.values for r in expected]


def test_window_writer_keeps_predecessor_when_clearing_checkpoint(tmp_path, monkeypatch):
    from test_indicator_cache_identity_contract import _window_kwargs

    from trading_advisor_3000.product_plane.research import hash_compatibility
    from trading_advisor_3000.product_plane.research.indicators import store

    profile = _sma2_profile()
    bars = [
        _continuous_view(ts_index=i, close=100 + i * 0.1, roll_epoch=0, active_contract_id="BR")
        for i in range(41)
    ]
    options = dict(
        dataset_version="dataset-v3",
        indicator_set_version="test",
        profile=profile,
        series_mode="continuous_front",
        native_full_compute=True,
    )
    old = m._build_partition_rows(series=bars[:40], **options)
    key = old[0].partition_key(series_mode="continuous_front")
    store.write_indicator_frame_partition_batches(
        output_dir=tmp_path, partition_row_batches=[(key, old)], profile=profile
    )
    kwargs = _window_kwargs(tmp_path)
    kwargs["profile"] = profile
    metadata = dict(kwargs["existing_by_partition"].values().__iter__().__next__())
    metadata["row_count"] = 40
    metadata["refresh_context_hash"] = m._refresh_context_hash(
        profile,
        instrument_id="BR",
        ladder_rows=(),
        raw_1m_path=None,
        source_rows=None,
        tick_sizes=None,
    )
    kwargs.update(
        partition_counts={key: 41},
        existing_by_partition={key: metadata},
        windows=(
            {
                "instrument_id": "BR",
                "contract_id": None,
                "timeframe": "15m",
                "start_ts": pd.Timestamp(bars[-1].ts),
                "end_ts": pd.Timestamp(bars[-1].ts),
            },
        ),
    )
    monkeypatch.setattr(hash_compatibility, "migration_source", lambda: None)
    monkeypatch.setattr(m, "_load_bar_partition_rows", lambda **_: bars)
    m._materialize_indicator_window_tails(**kwargs)
    stored = store.load_indicator_partition_rows(
        output_dir=tmp_path, partition=key, value_columns=profile.expected_output_columns()
    )
    assert [r.ts for r in stored] == [b.ts for b in bars]
    assert sum(bool(r.compute_state_json) for r in stored) == 1
