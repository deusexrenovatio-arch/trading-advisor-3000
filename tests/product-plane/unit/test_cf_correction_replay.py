"""Acceptance requires native parity AND bounded formulas for old-bar corrections."""

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest
from test_native_checkpoint_compute import market_frame
from test_research_indicator_layer import _continuous_view

from trading_advisor_3000.product_plane.research.indicators import cf_tail
from trading_advisor_3000.product_plane.research.indicators import materialize as m
from trading_advisor_3000.product_plane.research.indicators.registry import (
    default_indicator_profile,
)


def bars_for(n):
    frame = market_frame(n)
    return [
        replace(
            _continuous_view(
                ts_index=i, close=float(frame.close.iloc[i]), roll_epoch=0, active_contract_id="BR"
            ),
            volume=int(frame.volume.iloc[i]),
            open_interest=None if i == 40 else int(frame.open_interest.iloc[i]),
        )
        for i in range(n)
    ]


def options(ladder=()):
    return dict(
        dataset_version="dataset-v3",
        indicator_set_version="core_v1",
        profile=default_indicator_profile(),
        series_mode="continuous_front",
        adjustment_ladder_rows=ladder,
    )


def native_oracle(bars, monkeypatch, kwargs=None):
    def full(frame, profile, ladder, cached, prefix, diagnostics, state, *args, **kwargs):
        state.clear()
        return m._compute_profile_frame_for_series(
            frame, profile, series_mode="continuous_front", adjustment_ladder_rows=ladder
        )

    with monkeypatch.context() as patch:
        patch.setattr(cf_tail, "compute", full)
        return m._build_partition_rows(series=bars, **(kwargs or options()))


def assert_values(actual, expected):
    assert [(r.ts, r.contract_id) for r in actual] == [(r.ts, r.contract_id) for r in expected]
    a = pd.DataFrame([r.values for r in actual])
    b = pd.DataFrame([r.values for r in expected])
    assert a.columns.equals(b.columns)
    np.testing.assert_array_equal(a.isna(), b.isna())
    np.testing.assert_allclose(
        a.to_numpy(dtype=float), b.to_numpy(dtype=float), rtol=1e-8, atol=1e-8
    )


@pytest.mark.parametrize("size", [2400, 4800])
@pytest.mark.parametrize("change", ["correction", "deletion", "insertion"])
def test_old_bar_change_requires_bounded_native_replay_and_full_parity(change, size, monkeypatch):
    original = bars_for(size)
    index = size - 193
    seed_source = original[:-100]
    if change == "insertion":
        seed_source = seed_source[:index] + seed_source[index + 1 :]
    seed = m._build_partition_rows(series=seed_source, **options())
    seed = [m.IndicatorFrameRow.from_dict(row.to_dict()) for row in seed]
    changed = list(original)
    if change == "correction":
        old = changed[index]
        changed[index] = replace(
            old,
            close=old.close + 0.4,
            native_close=old.close + 0.4,
            volume=old.volume + 19,
            open_interest=old.open_interest + 11,
        )
    elif change == "deletion":
        del changed[index]
    diagnostics = {}
    from trading_advisor_3000.product_plane.research.indicators.native_checkpoint import (
        NativeCheckpoint,
    )

    native_compute = NativeCheckpoint.compute
    full_compute = m._compute_profile_frame_for_series
    observed = []

    def bounded_native(engine, compute_spec, frame, spec):
        observed.append(len(frame))
        assert len(frame) <= 512 + len(changed) - (index - 256), "unbounded native formula call"
        return native_compute(engine, compute_spec, frame, spec)

    def no_hidden_full(*args, **kwargs):
        assert kwargs.get("checkpoint_runner") is not None, "hidden full fallback"
        return full_compute(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(NativeCheckpoint, "compute", bounded_native)
        patch.setattr(m, "_compute_profile_frame_for_series", no_hidden_full)
        for name in (
            "_zero_anchor_price_frame",
            "_native_price_frame",
            "_asof_adjusted_price_frame",
        ):
            original_transform = getattr(m, name)

            def bounded_transform(frame, *args, _transform=original_transform, **kwargs):
                assert len(frame) <= 512 + len(changed) - (index - 256), (
                    "unbounded price normalization"
                )
                return _transform(frame, *args, **kwargs)

            patch.setattr(m, name, bounded_transform)
        actual = m._build_partition_rows(
            series=changed, existing_rows=seed, compute_diagnostics=diagnostics, **options()
        )
    expected = native_oracle(changed, monkeypatch)
    assert_values(actual, expected)
    # This assertion is deliberately inseparable from parity: full fallback is failure.
    assert diagnostics.get("cf_tail_partition_count") == 1, diagnostics.get(
        "cf_tail_fallback_reason"
    )
    assert 0 < diagnostics["cf_replay_start"] <= index
    assert diagnostics["cf_replay_start"] >= index - 256
    assert diagnostics["cf_formula_work"]
    assert observed
    assert all(w["full_history_rows"] == 0 for w in diagnostics["cf_formula_work"].values())
    assert all(
        w["formula_rows"] <= 512 + len(changed) - diagnostics["cf_replay_start"]
        for w in diagnostics["cf_formula_work"].values()
    )


def test_execution_metadata_change_has_no_indicator_formula_work(monkeypatch):
    bars = bars_for(1200)
    seed = m._build_partition_rows(series=bars, **options())
    seed = [m.IndicatorFrameRow.from_dict(r.to_dict()) for r in seed]
    bars[1000] = replace(
        bars[1000],
        economics_effective_from_ts="2026-01-01T00:00:00Z",
        economics_model_version="metadata-only",
        execution_margin_required_estimate=9999.0,
    )
    diagnostics = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=seed, compute_diagnostics=diagnostics, **options()
    )
    assert_values(actual, native_oracle(bars, monkeypatch))
    assert diagnostics.get("cf_tail_noop_count") == 1, diagnostics.get("cf_tail_fallback_reason")
    assert not diagnostics.get("cf_formula_work")


def test_replay_crosses_known_roll_and_eligibility_gap(monkeypatch):
    bars = bars_for(2400)
    for i in range(2000, len(bars)):
        bars[i] = replace(
            bars[i], roll_epoch=1, contract_id="BR-next", active_contract_id="BR-next"
        )
    for i in range(1900, 1930):
        bars[i] = replace(bars[i], bar_usage_flags=1)
    ladder = (
        dict(
            instrument_id="BR",
            timeframe="15m",
            roll_sequence=1,
            effective_ts=bars[2000].ts,
            additive_gap=10.0,
        ),
    )
    kwargs = options(ladder)
    seed = [
        m.IndicatorFrameRow.from_dict(r.to_dict())
        for r in m._build_partition_rows(series=bars[:2300], **kwargs)
    ]
    bars[1950] = replace(
        bars[1950],
        close=bars[1950].close + 0.3,
        native_close=bars[1950].close + 0.3,
        volume=bars[1950].volume + 17,
    )
    diag = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=seed, compute_diagnostics=diag, **kwargs
    )
    assert_values(actual, native_oracle(bars, monkeypatch, kwargs))
    assert diag.get("cf_tail_partition_count") == 1, diag.get("cf_tail_fallback_reason")
    assert diag["cf_replay_start"] == 1792
    assert max(w["formula_rows"] for w in diag["cf_formula_work"].values()) <= 512 + 2400 - 1792


@pytest.mark.parametrize("change", ["first_zero_added", "last_zero_removed"])
def test_global_zero_regime_change_requires_full_prefix_replacement(change, monkeypatch):
    bars = bars_for(2400)

    def flat(row):
        return replace(
            row, high=row.close, low=row.close, native_high=row.close, native_low=row.close
        )

    if change == "last_zero_removed":
        bars[2207] = flat(bars[2207])
    seed = [
        m.IndicatorFrameRow.from_dict(r.to_dict())
        for r in m._build_partition_rows(series=bars[:2300], **options())
    ]
    bars[2207] = flat(bars[2207]) if change == "first_zero_added" else bars_for(2400)[2207]
    diag = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=seed, compute_diagnostics=diag, **options()
    )
    assert_values(actual, native_oracle(bars, monkeypatch))
    assert diag.get("cf_full_replacement_required"), diag
    assert "global zero-range" in diag["cf_tail_fallback_reason"]
    assert not diag.get("cf_tail_partition_count")


@pytest.mark.parametrize("deletion", ["anchor", "final"])
def test_deleting_anchor_or_final_bar_restores_earlier_valid_state(deletion, monkeypatch):
    from test_research_indicator_layer import _sma2_profile

    kwargs = dict(options(), profile=_sma2_profile())
    bars = bars_for(2300)
    seed = [
        m.IndicatorFrameRow.from_dict(r.to_dict())
        for r in m._build_partition_rows(series=bars, **kwargs)
    ]
    del bars[2047 if deletion == "anchor" else -1]
    diag = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=seed, compute_diagnostics=diag, **kwargs
    )
    assert_values(actual, native_oracle(bars, monkeypatch, kwargs))
    assert diag.get("cf_tail_partition_count") == 1
    assert diag["cf_replay_start"] == (1792 if deletion == "anchor" else 2048)


def test_bad_nearest_anchor_uses_previous_checksum_valid_anchor(monkeypatch):
    import json

    from test_research_indicator_layer import _sma2_profile

    kwargs = dict(options(), profile=_sma2_profile())
    bars = bars_for(2400)
    seed = m._build_partition_rows(series=bars[:2300], **kwargs)
    payload = json.loads(seed[2047].compute_state_json)
    payload["checksum"] = "tampered"
    seed[2047] = replace(seed[2047], compute_state_json=json.dumps(payload))
    bars[2207] = replace(bars[2207], native_close=bars[2207].close + 0.2)
    diag = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=seed, compute_diagnostics=diag, **kwargs
    )
    assert_values(actual, native_oracle(bars, monkeypatch, kwargs))
    assert diag.get("cf_tail_partition_count") == 1
    assert diag["cf_replay_start"] == 1792


def window_kwargs(tmp_path, bars, stored, profile, snapshot):
    from test_indicator_cache_identity_contract import _window_kwargs

    key = stored[-1].partition_key(series_mode="continuous_front")
    kwargs = _window_kwargs(tmp_path)
    kwargs.update(
        profile=profile,
        partition_counts={key: len(bars)},
        existing_by_partition={
            key: dict(
                row_count=len(stored),
                compute_contract_hash=m._indicator_compute_contract_hash(profile),
                refresh_context_hash=m._refresh_context_hash(
                    profile,
                    instrument_id="BR",
                    ladder_rows=(),
                    raw_1m_path=None,
                    source_rows=None,
                    tick_sizes=None,
                ),
                source_snapshot_hash=stored[-1].source_snapshot_hash,
            )
        },
        windows=m._normalize_indicator_windows(
            (
                {
                    "instrument_id": "BR",
                    "contract_id": None,
                    "timeframe": "15m",
                    "start_ts": bars[-1].ts,
                    "end_ts": bars[-1].ts,
                },
            )
        ),
        source_snapshot_hash=snapshot,
        source_dataset_bars_hash=snapshot + "-bars",
    )
    return key, kwargs


def test_native_delta_retention_cleanup_widens_payload_and_retry_is_noop(tmp_path, monkeypatch):
    from deltalake import DeltaTable
    from test_research_indicator_layer import _sma2_profile

    from trading_advisor_3000.product_plane.research import hash_compatibility
    from trading_advisor_3000.product_plane.research.indicators import store

    profile = _sma2_profile()
    bars = bars_for(4900)
    opts = dict(options(), profile=profile, native_full_compute=True, source_snapshot_hash="OLD")
    seed = m._build_partition_rows(series=bars[:4600], **opts)
    key = seed[-1].partition_key(series_mode="continuous_front")
    store.write_indicator_frame_partition_batches(
        output_dir=tmp_path, partition_row_batches=[(key, seed)], profile=profile
    )
    _, kwargs = window_kwargs(tmp_path, bars, seed, profile, "NEW")
    monkeypatch.setattr(hash_compatibility, "migration_source", lambda: None)
    monkeypatch.setattr(m, "_load_bar_partition_rows", lambda **_: bars)
    report = m._materialize_indicator_window_tails(**kwargs)
    stored = store.load_indicator_partition_rows(
        output_dir=tmp_path, partition=key, value_columns=profile.expected_output_columns()
    )
    expected = m._build_partition_rows(series=bars, **opts)
    assert_values(stored, expected)
    assert len(stored) == 4900 and len({r.ts for r in stored}) == 4900
    actual_anchors = {i + 1 for i, r in enumerate(stored) if r.compute_state_json}
    assert actual_anchors == set(cf_tail.checkpoint_counts(4900))
    assert len(actual_anchors) <= 18
    assert report["refreshed_row_count"] > 300, (
        "expired carrier cleanup must widen preserved payload"
    )
    before = DeltaTable(str(tmp_path / "research_indicator_frames.delta")).version()
    _, kwargs = window_kwargs(tmp_path, bars, stored, profile, "NEW")

    def no_formulas(**_):
        raise AssertionError("no-op must not enter formula producer")

    monkeypatch.setattr(m, "_build_partition_rows", no_formulas)
    again = m._materialize_indicator_window_tails(**kwargs)
    assert again["refreshed_partition_count"] == 0
    assert DeltaTable(str(tmp_path / "research_indicator_frames.delta")).version() == before


def test_native_metadata_only_update_advances_lineage_without_formulas(tmp_path, monkeypatch):
    from test_research_indicator_layer import _sma2_profile

    from trading_advisor_3000.product_plane.research import hash_compatibility
    from trading_advisor_3000.product_plane.research.indicators import store

    profile = _sma2_profile()
    bars = bars_for(1200)
    seed = m._build_partition_rows(
        series=bars,
        source_snapshot_hash="OLD",
        native_full_compute=True,
        **dict(options(), profile=profile),
    )
    key = seed[-1].partition_key(series_mode="continuous_front")
    store.write_indicator_frame_partition_batches(
        output_dir=tmp_path, partition_row_batches=[(key, seed)], profile=profile
    )
    bars[1000] = replace(
        bars[1000], economics_model_version="new-lineage", execution_step_price_rub=123.0
    )
    _, kwargs = window_kwargs(tmp_path, bars, seed, profile, "NEW")
    monkeypatch.setattr(hash_compatibility, "migration_source", lambda: None)
    monkeypatch.setattr(m, "_load_bar_partition_rows", lambda **_: bars)

    def no_formulas(**_):
        raise AssertionError("metadata-only must not enter formula producer")

    monkeypatch.setattr(m, "_build_partition_rows", no_formulas)
    report = m._materialize_indicator_window_tails(**kwargs)
    stored = store.load_indicator_partition_rows(
        output_dir=tmp_path, partition=key, value_columns=profile.expected_output_columns()
    )
    assert_values(stored, seed)
    assert stored[-1].source_snapshot_hash == "NEW"
    assert stored[-1].source_dataset_bars_hash == "NEW-bars"
    assert stored[-1].compute_state_json == seed[-1].compute_state_json
    assert report["metadata_rebased_partition_count"] == 1


def test_correction_before_retained_horizon_explicitly_falls_back(monkeypatch):
    from test_research_indicator_layer import _sma2_profile

    kwargs = dict(options(), profile=_sma2_profile())
    bars = bars_for(5500)
    seed = m._build_partition_rows(series=bars, **kwargs)
    bars[100] = replace(bars[100], native_close=bars[100].close + 0.2)
    diagnostic = {}
    actual = m._build_partition_rows(
        series=bars, existing_rows=seed, compute_diagnostics=diagnostic, **kwargs
    )
    assert_values(actual, native_oracle(bars, monkeypatch, kwargs))
    assert not diagnostic.get("cf_tail_partition_count")
    assert diagnostic["cf_tail_fallback_reason"]
    assert max(w["full_history_rows"] for w in diagnostic["cf_formula_work"].values()) > 4096


def test_bar_close_is_causal_identity_and_moves_native_year_partition(tmp_path, monkeypatch):
    from deltalake import DeltaTable
    from test_research_indicator_layer import _sma2_profile

    from trading_advisor_3000.product_plane.research import hash_compatibility
    from trading_advisor_3000.product_plane.research.indicators import store

    profile = _sma2_profile()
    base = pd.Timestamp("2026-12-31T23:30:00Z")
    bars = [
        replace(
            row,
            ts=(base + pd.Timedelta(minutes=15 * i)).isoformat().replace("+00:00", "Z"),
            bar_end_ts=(base + pd.Timedelta(minutes=15 * i + 14))
            .isoformat()
            .replace("+00:00", "Z"),
        )
        for i, row in enumerate(bars_for(3))
    ]
    seed = m._build_partition_rows(
        series=bars,
        native_full_compute=True,
        source_snapshot_hash="OLD",
        **dict(options(), profile=profile),
    )
    key = seed[-1].partition_key(series_mode="continuous_front")
    store.write_indicator_frame_partition_batches(
        output_dir=tmp_path, partition_row_batches=[(key, seed)], profile=profile
    )
    bars[1] = replace(bars[1], bar_end_ts="2027-01-01T00:00:00Z")
    _, kwargs = window_kwargs(tmp_path, bars, seed, profile, "NEW")
    monkeypatch.setattr(hash_compatibility, "migration_source", lambda: None)
    monkeypatch.setattr(m, "_load_bar_partition_rows", lambda **_: bars)

    def no_formulas(**_):
        raise AssertionError("physical metadata change must not enter formula producer")

    monkeypatch.setattr(m, "_build_partition_rows", no_formulas)
    m._materialize_indicator_window_tails(**kwargs)
    table = (
        DeltaTable(str(tmp_path / "research_indicator_frames.delta"))
        .to_pyarrow_table(columns=["ts", "ts_close_year"])
        .sort_by([("ts", "ascending")])
    )
    assert table.num_rows == 3
    assert table.column("ts_close_year").to_pylist() == [2026, 2027, 2027]
