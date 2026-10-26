from __future__ import annotations

import copy
import shutil
from dataclasses import replace

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from test_research_indicator_layer import _sma2_indicator_row, _sma2_profile, _view

from trading_advisor_3000.product_plane.data_plane import delta_runtime
from trading_advisor_3000.product_plane.research.indicators import materialize as owner
from trading_advisor_3000.product_plane.research.indicators import store
from trading_advisor_3000.product_plane.research.indicators.registry import (
    IndicatorProfile,
    default_indicator_profile,
)
from trading_advisor_3000.product_plane.research.indicators.store import IndicatorFrameRow


def _vp_profile():
    return IndicatorProfile(
        version="vp_test",
        description="VP identity contract",
        indicators=tuple(
            spec
            for spec in default_indicator_profile().indicators
            if spec.operation_key == "volume_profile"
        ),
    )


def _context(profile, path=None):
    return owner._refresh_context_hash(
        profile,
        instrument_id="BR",
        ladder_rows=(),
        raw_1m_path=path,
        source_rows=None,
        tick_sizes={"BR": 0.01},
    )


def _source(path):
    table = pa.table(
        {
            "instrument_id": ["BR"],
            "contract_id": ["BR-6.26"],
            "timeframe": ["1m"],
            "ts_open": ["2026-03-16T09:00:00Z"],
            "low": [80.0],
            "high": [80.5],
            "volume": [10],
        }
    )
    write_deltalake(str(path), table)
    return table


def test_numerical_compute_contract_binds_kernel_dependencies_and_not_description(monkeypatch):
    from trading_advisor_3000.product_plane.research import compute_contract

    profile = _sma2_profile()
    original = owner._indicator_compute_contract_hash(profile)
    assert (
        owner._indicator_compute_contract_hash(replace(profile, description="provenance only"))
        == original
    )
    identity = compute_contract.numerical_identity()
    for section, key in (("kernels", "l1"), ("dependencies", "pandas")):
        changed = copy.deepcopy(identity)
        changed[section][key] = "unreviewed-source-or-dependency"
        with monkeypatch.context() as patch:
            patch.setattr(compute_contract, "numerical_identity", lambda: changed)
            assert owner._indicator_compute_contract_hash(profile) != original


def test_vp_identity_reuses_relocation_and_append_but_rejects_replacement_and_schema(tmp_path):
    source, relocated, replaced = (tmp_path / name for name in ("source", "copy", "replacement"))
    table = _source(source)
    shutil.copytree(source, relocated)
    profile = _vp_profile()
    original = _context(profile, source)
    assert _context(profile, relocated) == original
    write_deltalake(str(relocated), table, mode="append")
    assert _context(profile, relocated) == original
    _source(replaced)
    assert _context(profile, replaced) != original
    evolved = table.append_column("unexpected_input", pa.array([1]))
    write_deltalake(str(relocated), evolved, mode="append", schema_mode="merge")
    assert _context(profile, relocated) != original
    assert (
        delta_runtime.delta_table_identity(source)["table_id"]
        == DeltaTable(str(source)).metadata().id
    )


def test_cache_metadata_roundtrips_as_reserved_fields():
    row = replace(
        _sma2_indicator_row(),
        compute_contract_hash="COMPUTE",
        input_row_hash="INPUT",
        refresh_context_hash="REFRESH",
        volume_profile_input_hash="MINUTES",
    )
    restored = IndicatorFrameRow.from_dict(row.to_dict())
    assert restored == row
    assert set(restored.values) == {"sma_2"}


def test_atomic_tail_predicate_keeps_prefix_in_one_commit(tmp_path):
    path = tmp_path / "research_indicator_frames.delta"
    columns = {"series_id": "string", "ts": "string", "value": "double"}
    rows = [{"series_id": "BR", "ts": str(i), "value": float(i)} for i in range(3)]
    delta_runtime.write_delta_table_rows(table_path=path, rows=rows, columns=columns)
    before = DeltaTable(str(path)).version()
    delta_runtime.write_delta_table_rows(
        table_path=path,
        rows=[{**rows[2], "value": 99.0}],
        columns=columns,
        replace_predicate="series_id = 'BR' AND ts >= '2'",
    )
    table = DeltaTable(str(path))
    assert table.version() == before + 1
    assert sorted(table.to_pyarrow_table().to_pylist(), key=lambda row: row["ts"]) == [
        rows[0],
        rows[1],
        {**rows[2], "value": 99.0},
    ]

    oversized = tmp_path / "oversized"
    oversized_table = oversized / "research_indicator_frames.delta"
    delta_runtime.write_delta_table_rows(table_path=oversized_table, rows=rows, columns=columns)
    oversized_before = DeltaTable(str(oversized_table)).version()
    row = _sma2_indicator_row()
    key = row.partition_key(series_mode="contract")
    with pytest.raises(ValueError, match="atomic indicator suffix exceeds"):
        store.write_indicator_frame_partition_batches(
            output_dir=oversized,
            partition_row_batches=[(key, [row, replace(row, ts="2026-03-16T09:15:00Z")])],
            partition_start_ts={key: row.ts},
            max_rows_per_delta_write=1,
            profile=_sma2_profile(),
        )
    assert DeltaTable(str(oversized_table)).version() == oversized_before


def _window_kwargs(tmp_path):
    profile = _sma2_profile()
    row = _sma2_indicator_row()
    key = row.partition_key(series_mode="contract")
    return dict(
        dataset_output_dir=tmp_path,
        indicator_output_dir=tmp_path,
        profile=profile,
        partition_counts={key: 1},
        existing_by_partition={
            key: {
                "row_count": 1,
                "refresh_context_hash": _context(profile),
                "compute_contract_hash": owner._indicator_compute_contract_hash(profile),
                "source_snapshot_hash": "SNAPSHOT",
            }
        },
        windows=(),
        adjustment_ladder_rows=(),
        source_dataset_bars_hash="BARS",
        source_snapshot_hash="SNAPSHOT",
        volume_profile_source_rows=None,
        volume_profile_raw_1m_table_path=None,
        volume_profile_tick_size_by_instrument=None,
        deleted_partitions=(),
        stage_timings={},
    )


def test_unchanged_window_owner_reuses_without_loading_or_writing(tmp_path, monkeypatch):
    kwargs = _window_kwargs(tmp_path)

    def forbidden(**_):
        pytest.fail("unchanged context must reuse before numerical load")

    monkeypatch.setattr(owner, "_load_bar_partition_rows", forbidden)
    monkeypatch.setattr(owner, "_build_partition_rows", forbidden)

    def drain(**call):
        assert list(call["partition_row_batches"]) == []
        return {}, 0, 0

    monkeypatch.setattr(owner, "write_indicator_frame_partition_batches", drain)
    report = owner._materialize_indicator_window_tails(**kwargs)
    assert (
        report["reused_partition_count"],
        report["refreshed_partition_count"],
        report["write_batch_count"],
    ) == (1, 0, 0)
    key = next(iter(kwargs["partition_counts"]))
    kwargs["existing_by_partition"][key]["compute_contract_hash"] = None

    def require_load(**_):
        raise RuntimeError("missing compute guard forces source load")

    monkeypatch.setattr(owner, "_load_bar_partition_rows", require_load)
    with pytest.raises(RuntimeError, match="missing compute guard forces source load"):
        owner._materialize_indicator_window_tails(**kwargs)


def test_changed_window_owner_keeps_native_compute_and_only_writes_suffix(tmp_path, monkeypatch):
    kwargs = _window_kwargs(tmp_path)
    key = next(iter(kwargs["partition_counts"]))
    bars = [_view(ts_index=i, close=80.0 + i) for i in range(3)]
    hashes = owner._indicator_input_row_hashes(
        bars,
        compute_contract_hash=owner._indicator_compute_contract_hash(kwargs["profile"]),
        volume_profile_source_rows=None,
        tick_size_by_instrument=None,
        has_volume_profile=False,
        required_columns=kwargs["profile"].required_input_columns(),
    )
    rows = [
        replace(_sma2_indicator_row(), ts=bar.ts, row_count=3, input_row_hash=value)
        for bar, value in zip(bars, hashes)
    ]
    kwargs["partition_counts"] = {key: 3}
    kwargs["existing_by_partition"][key]["row_count"] = 3
    kwargs["windows"] = owner._normalize_indicator_windows(
        [
            {
                "instrument_id": "BR",
                "contract_id": "BR-6.26",
                "timeframe": "15m",
                "start_ts": bars[2].ts,
                "end_ts": bars[2].ts,
            }
        ]
    )
    monkeypatch.setattr(owner, "_load_bar_partition_rows", lambda **_: bars)
    monkeypatch.setattr(owner, "load_indicator_partition_rows", lambda **_: rows)

    def build(**call):
        assert call["native_full_compute"] is True
        assert call["existing_rows"] == rows
        assert call["volume_profile_refresh_ts"] == {bars[2].ts}
        return rows

    monkeypatch.setattr(owner, "_build_partition_rows", build)

    def drain(**call):
        assert list(call["partition_row_batches"]) == [(key, [rows[2]])]
        assert call["partition_start_ts"] == {key: bars[2].ts}
        return {}, 1, 1

    monkeypatch.setattr(owner, "write_indicator_frame_partition_batches", drain)
    report = owner._materialize_indicator_window_tails(**kwargs)
    assert (
        report["reused_partition_count"],
        report["refreshed_partition_count"],
        report["write_batch_count"],
    ) == (0, 1, 1)


def test_full_reuse_and_extension_reject_stale_refresh_context():
    profile = _sma2_profile()
    current = _context(profile)
    compute = owner._indicator_compute_contract_hash(profile)
    metadata = {
        "source_bars_hash": "BARS",
        "profile_version": profile.version,
        "row_count": 3,
        "output_columns_hash": "OUTPUT",
        "compute_contract_hash": compute,
        "refresh_context_hash": current,
    }
    kwargs = dict(
        source_hash="BARS",
        profile_version=profile.version,
        row_count=3,
        output_columns_hash="OUTPUT",
        compute_contract_hash=compute,
        refresh_context_hash=current,
    )
    assert owner._existing_partition_matches(metadata, **kwargs)
    stale = {**metadata, "refresh_context_hash": "old-v1-or-other-producer"}
    assert not owner._existing_partition_matches(stale, **kwargs)
    context = dict(
        instrument_id="BR",
        ladder_rows=(),
        raw_1m_path=None,
        source_rows=None,
        tick_sizes={"BR": 0.01},
    )
    assert owner._profile_extension_preserves_existing_contract(
        profile,
        metadata,
        profile.expected_output_columns(),
        **context,
    )
    assert not owner._profile_extension_preserves_existing_contract(
        profile,
        stale,
        profile.expected_output_columns(),
        **context,
    )


def _read_metadata(table, tmp_path, monkeypatch):
    monkeypatch.setattr(store, "delta_table_columns", lambda _: tuple(table.column_names))
    monkeypatch.setattr(store, "read_delta_table_arrow", lambda *_, **__: table)
    return store.load_indicator_partition_metadata(
        output_dir=tmp_path,
        dataset_version="dataset-v3",
        indicator_set_version="sma2_test",
    )


def test_metadata_keeps_legacy_contract_identity_with_null_or_empty_series(tmp_path, monkeypatch):
    rows = [
        {**_sma2_indicator_row().to_dict(), "contract_id": contract, "series_id": series}
        for contract, series in [("A", None), ("B", None), ("C", ""), ("D", "")]
    ]
    metadata = _read_metadata(pa.Table.from_pylist(rows), tmp_path, monkeypatch)
    assert {row["contract_id"] for row in metadata} == {"A", "B", "C", "D"}
    assert len(metadata) == 4


def test_metadata_selects_one_latest_physical_carrier_including_null_guard(tmp_path, monkeypatch):
    old = {
        **_sma2_indicator_row().to_dict(),
        "created_at": "2026-03-16T12:00:00Z",
        "refresh_context_hash": "old-context",
        "compute_contract_hash": "old-compute",
        "row_count": 2,
    }
    latest = {
        **old,
        "created_at": "2026-03-16T13:00:00Z",
        "refresh_context_hash": None,
        "compute_contract_hash": None,
        "row_count": 3,
    }
    metadata = _read_metadata(pa.Table.from_pylist([old, latest]), tmp_path, monkeypatch)
    assert len(metadata) == 1
    assert metadata[0]["created_at"] == latest["created_at"]
    assert metadata[0]["row_count"] == 3
    assert metadata[0]["refresh_context_hash"] is None
    assert metadata[0]["compute_contract_hash"] is None


@pytest.mark.parametrize(
    "field,value",
    [("native_close", 99.0), ("roll_epoch", 1), ("session_close_ts", "2026-03-16T23:00:00Z")],
)
def test_effective_cf_input_proof_covers_legacy_row_native_and_projection_fields(field, value):
    bar = _view(ts_index=0, close=80.0)
    kwargs = dict(
        compute_contract_hash="same",
        volume_profile_source_rows=None,
        tick_size_by_instrument=None,
        has_volume_profile=False,
        required_columns=("close",),
        series_mode="continuous_front",
    )
    before = owner._indicator_input_row_hashes([bar], **kwargs)
    assert owner._indicator_input_row_hashes([replace(bar, **{field: value})], **kwargs) != before
    kwargs["series_mode"] = "contract"
    assert owner._indicator_input_row_hashes([bar], **kwargs) == owner._indicator_input_row_hashes(
        [replace(bar, **{field: value})], **kwargs
    )
