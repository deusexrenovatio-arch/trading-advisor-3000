from dataclasses import replace

import pandas as pd
import pyarrow as pa
import pytest
from test_research_derived_indicator_layer import _indicator_row, _view

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView
from trading_advisor_3000.product_plane.research.derived_indicators import materialize as m
from trading_advisor_3000.product_plane.research.derived_indicators import tails
from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    DerivedIndicatorProfile,
)
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    load_derived_indicator_frames,
    load_derived_indicator_partition_metadata,
    write_derived_indicator_frame_batches,
)
from trading_advisor_3000.product_plane.research.derived_indicators.tails import (
    changed_suffix_index,
)


def test_null_open_interest_roundtrip():
    bar = replace(_view(ts_index=0, close=100), open_interest=None)
    assert ResearchBarView.from_dict(bar.to_dict()).open_interest is None


@pytest.mark.parametrize(
    "old,new,start",
    [
        ([1, 2, 3], [1, 2, 3, 4], 3),
        ([1, 2, 3], [1, 2, 3], 2),
        ([1, 2, 3], [1, 3], 0),
        ([1, 2, 3], [1, 2], 1),
        ([1, 3], [1, 2, 3], 1),
        ([], [1], 0),
        ([1], [], 0),
    ],
)
def test_insert_delete_append_proof(old, new, start):
    def table(keys):
        return pa.table(
            {
                "ts": pa.array(keys, type=pa.int64()),
                "value": pa.array([None for _ in keys], type=pa.float64()),
            }
        )

    assert changed_suffix_index(table(old), table(new)) == start


def test_local_indicator_correction_native_suffix_equals_full(tmp_path):
    profile = DerivedIndicatorProfile("test", "native", ("distance_to_ema_20_atr",), 0)
    bars = [_view(ts_index=i, close=100 + i) for i in range(24)]
    indicators = [_indicator_row(ts_index=i, close=100 + i) for i in range(24)]

    def build(**kw):
        return m._build_prepared_partition_rows(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            series_mode="contract",
            profile=profile,
            local_series=bars,
            local_indicator_rows=indicators,
            **kw,
        )

    old = build()
    key = old[0].partition_key(series_mode="contract")
    write_derived_indicator_frame_batches(
        output_dir=tmp_path,
        row_batches=[old],
        replace_partitions=(key,),
        profile=profile,
        atomic_partition_batches=True,
    )
    indicators[12] = replace(indicators[12], values={**indicators[12].values, "ema_20": 1000.0})
    full = build()
    assert full[0].source_inputs_hash != old[0].source_inputs_hash
    assert not m._existing_partition_matches(
        [old[0].to_dict()],
        source_bars_hash=old[0].source_bars_hash,
        source_indicators_hash=old[0].source_indicators_hash,
        profile_version=profile.version,
        row_count=len(old),
        output_columns_hash=old[0].output_columns_hash,
        source_inputs_hash=full[0].source_inputs_hash,
    )
    tail = build(
        tail_output_dir=tmp_path, tail_compatible=True, previous_created_at="2099-01-01T00:00:00Z"
    )
    assert len(tail) == 12
    write_derived_indicator_frame_batches(
        output_dir=tmp_path,
        row_batches=[tail],
        replace_partitions=(key,),
        profile=profile,
        atomic_partition_batches=True,
        partition_start_ts={key: tail[0].ts},
    )
    actual = load_derived_indicator_frames(
        output_dir=tmp_path,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    assert {r.ts: r.values for r in actual} == {r.ts: r.values for r in full}
    metadata = load_derived_indicator_partition_metadata(
        output_dir=tmp_path,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )
    assert len(metadata) == 1 and metadata[0]["_metadata_valid"]
    assert metadata[0]["source_inputs_hash"] == full[0].source_inputs_hash
    assert pd.Timestamp(metadata[0]["created_at"]) > pd.Timestamp("2099-01-01T00:00:00Z")


@pytest.mark.parametrize("removed", [0, 12, 23])
def test_native_deleted_timestamp_does_not_survive(tmp_path, removed):
    profile = DerivedIndicatorProfile("test", "deletion", ("rolling_high_20",), 0)
    bars = [_view(ts_index=i, close=100 + i) for i in range(24)]
    indicators = [_indicator_row(ts_index=i, close=100 + i) for i in range(24)]

    def build(selected, **kw):
        return m._build_prepared_partition_rows(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            series_mode="contract",
            profile=profile,
            local_series=selected,
            local_indicator_rows=indicators,
            **kw,
        )

    old = build(bars)
    key = old[0].partition_key(series_mode="contract")
    write_derived_indicator_frame_batches(
        output_dir=tmp_path,
        row_batches=[old],
        replace_partitions=(key,),
        profile=profile,
        atomic_partition_batches=True,
    )
    selected = [b for i, b in enumerate(bars) if i != removed]
    full = build(selected)
    tail = build(selected, tail_output_dir=tmp_path, tail_compatible=True)
    cutoff = {key: tail[0].ts} if tail[0].ts != selected[0].ts else {}
    write_derived_indicator_frame_batches(
        output_dir=tmp_path,
        row_batches=[tail],
        replace_partitions=(key,),
        profile=profile,
        atomic_partition_batches=True,
        partition_start_ts=cutoff,
    )
    actual = load_derived_indicator_frames(
        output_dir=tmp_path,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    assert {r.ts: r.values for r in actual} == {r.ts: r.values for r in full}
    assert bars[removed].ts not in {r.ts for r in actual}


@pytest.fixture
def native_materializer(tmp_path, monkeypatch):
    profile = DerivedIndicatorProfile("test", "local L2", ("distance_to_ema_20_atr",), 0)
    (tmp_path / "research_indicator_frames.delta" / "_delta_log").mkdir(parents=True)
    inputs = {}
    for asset in ("A", "B"):
        inputs[asset] = []
        for i in range(24):
            bar = replace(
                _view(ts_index=i, close=100 + i),
                instrument_id=asset,
                series_mode="continuous_front",
                series_id=asset,
                contract_id="OLD" if i < 12 else "NEW",
            )
            inputs[asset].append(
                {
                    **bar.to_dict(),
                    **_indicator_row(ts_index=i, close=100 + i).values,
                    "indicator_set_version": "indicators-v1",
                    "indicator_profile_version": "l1-v1",
                    "indicator_output_columns_hash": "l1-columns",
                    "indicator_source_bars_hash": "bars",
                }
            )
    monkeypatch.setattr(
        m, "_load_dataset_manifest", lambda **_: {"series_mode": "continuous_front"}
    )
    monkeypatch.setattr(m.cf_input_projection, "load_adjustment_ladder_rows", lambda **_: ())
    monkeypatch.setattr(
        m,
        "existing_indicator_value_columns",
        lambda **_: m._source_indicator_columns_for_profile(profile),
    )
    monkeypatch.setattr(
        m, "run_research_derived_source_frames_spark_job", lambda **_: {"rows_by_table": {}}
    )
    monkeypatch.setattr(
        m,
        "load_derived_source_frame_partition_metadata",
        lambda **_: [
            {
                "dataset_version": "dataset-v5",
                "indicator_set_version": "indicators-v1",
                "contour_id": "native_tradable",
                "series_mode": "continuous_front",
                "series_id": rows[0]["series_id"],
                "contract_id": rows[0]["contract_id"],
                "instrument_id": rows[0]["instrument_id"],
                "timeframe": rows[0]["timeframe"],
                "partition_row_count": len(rows),
            }
            for asset, rows in inputs.items()
            if rows
        ],
    )

    # These tests exercise the slow preparation branch explicitly. Snapshot-only
    # reuse has separate main regressions and must bypass all wide reads.
    if "_integration_original_fast_path" not in m.__dict__:
        m.__dict__["_integration_original_fast_path"] = m._derived_metadata_fast_reuse
    monkeypatch.setattr(m, "_derived_metadata_fast_reuse", lambda **_: None)
    from deltalake import write_deltalake

    write_deltalake(str(tmp_path / "research_bar_views.delta"), pa.table({"probe": [1]}))
    write_deltalake(str(tmp_path / "research_indicator_frames.delta"), pa.table({"probe": [1]}))
    previous = None

    def run(override_profile=None, **request):
        nonlocal previous
        from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
            write_delta_table_rows,
        )

        columns = m.research_derived_source_frame_store_contract(
            source_indicator_columns=m._source_indicator_columns_for_profile(
                override_profile or profile
            )
        )[m.DERIVED_SOURCE_FRAME_TABLE]["columns"]
        import json

        signature = json.dumps([columns, inputs], sort_keys=True, default=str)
        if signature != previous:
            write_source = True
        else:
            write_source = False
        previous = signature
        if write_source:
            write_delta_table_rows(
                table_path=tmp_path / "research_derived_source_frames.delta",
                rows=[row for rows in inputs.values() for row in rows],
                columns=columns,
                compact=False,
            )
        return m.materialize_derived_indicator_frames(
            dataset_output_dir=tmp_path,
            indicator_output_dir=tmp_path,
            derived_indicator_output_dir=tmp_path,
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            profile=override_profile or profile,
            timeframes=request.pop("timeframes", ("15m",)),
            **request,
        )

    return inputs, run, tmp_path, profile


def test_metadata_only_update_cf_latest_and_noop(native_materializer):
    inputs, run, _, _ = native_materializer
    assert run()["refreshed_row_count"] == 48
    assert run()["reused_partition_count"] == 2

    for row in inputs["A"]:
        row["indicator_profile_version"] = "l1-v2"
    changed = run()
    assert changed["refreshed_row_count"] == 2
    assert changed["reused_partition_count"] == 1
    assert run()["reused_partition_count"] == 2


def test_source_and_old_wide_scans_once_with_full_exact_compare(native_materializer, monkeypatch):
    inputs, run, root, profile = native_materializer
    first = run()
    assert first["source_read_cache"]["wide_scans"] == 1
    assert first["old_read_cache"] is None
    unchanged = run()
    assert unchanged["source_read_cache"]["wide_scans"] == 1
    assert unchanged["old_read_cache"]["wide_scans"] == 0
    sizes = []
    original = tails.changed_suffix_index

    def compare(old, new):
        sizes.append((len(old), len(new)))
        return original(old, new)

    monkeypatch.setattr(tails, "changed_suffix_index", compare)
    for rows in inputs.values():
        rows[12]["ema_20"] += 10
    changed = run()
    assert changed["source_read_cache"]["wide_scans"] == 1
    assert changed["old_read_cache"]["wide_scans"] == 1
    assert sizes == [(24, 24), (24, 24)]
    from trading_advisor_3000.product_plane.research.derived_indicators.read_cache import (
        PartitionReadCache,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        load_derived_indicator_partition_rows,
    )

    snapshot = m.DeltaArrowReader(root / "research_derived_indicator_frames.delta")
    key = m.DerivedIndicatorFramePartitionKey(
        "dataset-v5",
        "indicators-v1",
        "derived-v1",
        "15m",
        "A",
        None,
        series_mode="continuous_front",
        series_id="A",
    )
    with PartitionReadCache(
        snapshot,
        columns=snapshot.schema.names,
        filters=[("dataset_version", "=", "dataset-v5")],
        enabled=True,
    ) as cache:
        direct = load_derived_indicator_partition_rows(
            output_dir=root, partition=key, value_columns=profile.output_columns
        )
        cached = load_derived_indicator_partition_rows(
            output_dir=root, partition=key, value_columns=profile.output_columns, reader=cache
        )
        assert cached == direct


def test_each_partition_is_read_and_prepared_once(native_materializer, monkeypatch):
    _, run, _, _ = native_materializer
    calls = []
    reads = m.load_derived_source_frame_partition_arrow
    prepare = m._source_frame_arrow_to_inputs

    def read(**kw):
        calls.append(("read", kw["partition"].instrument_id))
        return reads(**kw)

    def prepared(**kw):
        calls.append(("prepare", kw["table"]["instrument_id"][0].as_py()))
        return prepare(**kw)

    monkeypatch.setattr(m, "load_derived_source_frame_partition_arrow", read)
    monkeypatch.setattr(m, "_source_frame_arrow_to_inputs", prepared)
    assert run()["refreshed_partition_count"] == 2
    assert calls == [("read", "A"), ("prepare", "A"), ("read", "B"), ("prepare", "B")]


def test_columnar_hashes_once_per_partition(native_materializer, monkeypatch):
    _, run, _, _ = native_materializer
    calls = []

    def forbidden(*_, **__):
        raise AssertionError("active materializer must not serialize historical JSON")

    monkeypatch.setattr(m, "_columnar_source_hashes", forbidden)
    for name in ("_fast_source_hashes", "_source_inputs_hash"):
        original = getattr(m, name)

        def measured(*args, _original=original, _name=name, **kwargs):
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(m, name, measured)
    run()
    assert calls.count("_fast_source_hashes") == 2
    assert calls.count("_source_inputs_hash") == 2


def test_hash_bundle_cannot_be_used_with_other_inputs(native_materializer, monkeypatch):
    inputs, _, _, profile = native_materializer
    bars, indicators, frame = m._source_frame_rows_to_inputs(
        rows=inputs["A"],
        source_indicator_columns=m._source_indicator_columns_for_profile(profile),
    )
    hashes = m.PreparedSourceHashes(
        "bars", "indicators", "inputs", id(frame), profile.version, profile.output_columns
    )

    def forbidden(**_):
        pytest.fail("must reject stale hash binding before numerical work")

    monkeypatch.setattr(m, "_compute_derived_frame", forbidden)
    with pytest.raises(ValueError, match="different inputs/profile"):
        m._build_prepared_partition_rows(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            series_mode="continuous_front",
            profile=profile,
            local_series=bars,
            local_indicator_rows=indicators,
            base_frame=frame.copy(),
            prepared_hashes=hashes,
        )


@pytest.mark.parametrize("all_null_oi", [False, True])
@pytest.mark.parametrize("source_tz", ["UTC", "Europe/Moscow"])
def test_columnar_inputs_preserve_legacy_hashes_and_dtypes(
    native_materializer, all_null_oi, source_tz
):
    from datetime import date

    from trading_advisor_3000.product_plane.data_plane.delta_runtime import _normalize_loaded_rows

    inputs, _, _, profile = native_materializer
    rows = [dict(row) for row in inputs["A"]]
    for i, row in enumerate(rows):
        row["ts"] = (
            (pd.Timestamp(row["ts"]) + pd.Timedelta(microseconds=i))
            .tz_convert(source_tz)
            .to_pydatetime()
        )
        row["session_date"] = date(2026, 3, 16)
        row["session_open_ts"] = None if i % 2 else row["ts"]
        row["open_interest"] = None if all_null_oi or i % 2 else 100
        row["native_open"] = None
        row["ema_20"] = None if i == 1 else float("nan") if i == 2 else 0.25
        row["bar_usage_flags"] = None if i == 1 else 127
    table = pa.Table.from_pylist(rows)
    for name, dtype in (
        ("open_interest", pa.int64()),
        ("bar_index", pa.int32()),
        ("ema_20", pa.float32()),
    ):
        index = table.schema.get_field_index(name)
        table = table.set_column(index, name, table[name].cast(dtype))
    # Exercise multiple chunks and canonical array commas without full-history payloads.
    table = pa.concat_tables([table.slice(0, 3), table.slice(0, 0), table.slice(3)])
    columns = m._source_indicator_columns_for_profile(profile)
    bars, indicators, old_frame = m._source_frame_rows_to_inputs(
        rows=_normalize_loaded_rows(table.to_pylist()),
        source_indicator_columns=columns,
    )
    metadata = pa.Table.from_pandas(
        old_frame.set_axis(pd.RangeIndex(100, 100 + len(old_frame)))
    ).schema.metadata
    table = table.replace_schema_metadata(metadata)
    new_bars, _, new_frame = m._source_frame_arrow_to_inputs(
        table=table,
        source_indicator_columns=columns,
    )
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "error", message="Mismatched null-like values", category=FutureWarning
        )
        pd.testing.assert_frame_equal(old_frame, new_frame)
    assert m._columnar_source_hashes(new_bars.table, columns) == (
        m._bars_hash(bars),
        m._indicator_hash(indicators, value_columns=columns),
    )
    assert m._source_inputs_hash(profile=profile, base_frame=old_frame) == m._source_inputs_hash(
        profile=profile,
        base_frame=new_frame,
    )


def test_native_loaders_preserve_mixed_microsecond_order(native_materializer):
    from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
    from trading_advisor_3000.product_plane.research.derived_indicators import source_frames as sf

    inputs, _, root, profile = native_materializer
    rows = [dict(row) for row in inputs["A"][:3]]
    for row, ts in zip(
        rows,
        ("2026-03-16T00:00:00Z", "2026-03-16T00:00:00.000001Z", "2026-03-16T00:00:00.000002Z"),
        strict=True,
    ):
        row["ts"] = ts
    columns = m._source_indicator_columns_for_profile(profile)
    write_delta_table_rows(
        table_path=root / sf.DERIVED_SOURCE_FRAME_DELTA,
        rows=rows,
        columns=sf.research_derived_source_frame_store_contract(source_indicator_columns=columns)[
            sf.DERIVED_SOURCE_FRAME_TABLE
        ]["columns"],
        compact=False,
    )
    args = dict(
        output_dir=root,
        source_indicator_columns=columns,
        partition=sf.DerivedSourceFramePartitionKey(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            instrument_id="A",
            series_mode="continuous_front",
            series_id="A",
            timeframe="15m",
        ),
    )
    old_rows = sf.load_derived_source_frame_partition_rows(**args)
    table = sf.load_derived_source_frame_partition_arrow(**args)
    bars, indicators, old_frame = m._source_frame_rows_to_inputs(
        rows=old_rows,
        source_indicator_columns=columns,
    )
    native_bars, _, new_frame = m._source_frame_arrow_to_inputs(
        table=table,
        source_indicator_columns=columns,
    )
    assert old_frame["ts"].tolist() == [rows[1]["ts"], rows[2]["ts"], rows[0]["ts"]]
    pd.testing.assert_frame_equal(old_frame, new_frame)
    assert m._columnar_source_hashes(native_bars.table, columns) == (
        m._bars_hash(bars),
        m._indicator_hash(indicators, value_columns=columns),
    )
    assert m._source_inputs_hash(profile=profile, base_frame=old_frame) == m._source_inputs_hash(
        profile=profile,
        base_frame=new_frame,
    )


def test_onepass_closes_partition_compute_timers(native_materializer, monkeypatch):
    from collections import Counter

    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.common import stage_timings

    inputs, run, root, profile = native_materializer
    events = []
    monkeypatch.setattr(stage_timings, "_emit", lambda event, **kw: events.append((event, kw)))
    assert run()["refreshed_partition_count"] == 2
    inputs["A"][-1]["ema_20"] += 10
    assert run()["refreshed_partition_count"] == 1
    path = root / "research_derived_indicator_frames.delta"
    before = DeltaTable(str(path)).version()
    assert run()["reused_partition_count"] == 2
    assert DeltaTable(str(path)).version() == before
    assert Counter(p["stage"] for e, p in events if e == "stage_started") == Counter(
        p["stage"] for e, p in events if e == "stage_finished"
    )
    finished = [
        payload
        for event, payload in events
        if event == "stage_finished" and payload.get("stage") == "partition.compute"
    ]
    assert len(finished) == 3
    assert all(payload["status"] == "PASS" for payload in finished)
    assert not any(payload.get("status") == "INCOMPLETE" for _, payload in events)
    actual = load_derived_indicator_frames(
        output_dir=root,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    expected = []
    for rows in inputs.values():
        source = m.load_derived_source_frame_partition_arrow(
            output_dir=root,
            partition=m.DerivedSourceFramePartitionKey(
                "dataset-v5",
                "indicators-v1",
                "15m",
                rows[0]["instrument_id"],
                contour_id="native_tradable",
                series_mode="continuous_front",
                series_id=rows[0]["instrument_id"],
            ),
            source_indicator_columns=m._source_indicator_columns_for_profile(profile),
        )
        bars, indicators, frame = m._source_frame_arrow_to_inputs(
            table=source,
            source_indicator_columns=m._source_indicator_columns_for_profile(profile),
        )
        hashes = m._fast_source_hashes(bars.table, m._source_indicator_columns_for_profile(profile))
        expected.extend(
            m._build_prepared_partition_rows(
                dataset_version="dataset-v5",
                indicator_set_version="indicators-v1",
                derived_indicator_set_version="derived-v1",
                series_mode="continuous_front",
                profile=profile,
                local_series=bars,
                local_indicator_rows=indicators,
                base_frame=frame,
                prepared_hashes=m.PreparedSourceHashes(
                    *hashes,
                    m._source_inputs_hash(profile=profile, base_frame=frame),
                    id(frame),
                    profile.version,
                    profile.output_columns,
                ),
            )
        )

    def key(row):
        return row.instrument_id, row.timeframe, row.ts

    assert len(actual) == len({key(row) for row in actual}) == len(expected)
    assert {key(row): row.values for row in actual} == {key(row): row.values for row in expected}
    metadata = load_derived_indicator_partition_metadata(
        output_dir=root,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
    )
    assert len(metadata) == 2 and all(row["_metadata_valid"] for row in metadata)
    for head in metadata:
        reference = next(row for row in expected if row.instrument_id == head["instrument_id"])
        for name in ("source_bars_hash", "source_indicators_hash", "source_inputs_hash"):
            assert head[name] == getattr(reference, name)


def test_partition_decisions_do_not_infer_deletion(native_materializer):
    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.research.derived_indicators import store

    _, run, root, profile = native_materializer
    run()
    existing = load_derived_indicator_frames(
        output_dir=root,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    partitions = tuple({row.partition_key(series_mode=row.series_mode) for row in existing})
    path = root / "research_derived_indicator_frames.delta"
    before = DeltaTable(str(path)).version()
    with pytest.raises(ValueError, match="omitted"):
        store.write_derived_indicator_partition_updates(
            output_dir=root,
            updates=[store.DerivedIndicatorPartitionUpdate(partitions[0], "reuse", [])],
            expected_partitions=partitions,
            profile=profile,
        )
    assert DeltaTable(str(path)).version() == before
    with pytest.raises(ValueError, match="repeated"):
        store.write_derived_indicator_partition_updates(
            output_dir=root,
            updates=[store.DerivedIndicatorPartitionUpdate(partitions[0], "reuse", [])] * 2,
            expected_partitions=partitions,
            profile=profile,
        )
    assert DeltaTable(str(path)).version() == before


def test_failed_pack_retry_without_windows(native_materializer, monkeypatch):
    from trading_advisor_3000.product_plane.research.derived_indicators import store

    inputs, run, _, _ = native_materializer
    run()
    for rows in inputs.values():
        rows[-1]["ema_20"] += 10
    native_write = store.write_delta_table_scoped_batches
    monkeypatch.setattr(
        store,
        "write_delta_table_scoped_batches",
        lambda **kw: native_write(**kw, max_batch_scopes=1),
    )
    build = m._build_prepared_partition_rows

    def fail_second(**kw):
        if kw["local_series"][0].instrument_id == "B":
            raise RuntimeError("injected later scope failure")
        return build(**kw)

    monkeypatch.setattr(m, "_build_prepared_partition_rows", fail_second)
    with pytest.raises(RuntimeError, match="later scope"):
        run()
    monkeypatch.setattr(m, "_build_prepared_partition_rows", build)
    retried = run()
    assert retried["reused_partition_count"] == 1
    assert retried["refreshed_partition_count"] == 1
    assert retried["refreshed_row_count"] == 1
    assert run()["reused_partition_count"] == 2


def test_full_fallback_removes_old_leading_keys(native_materializer):
    inputs, run, root, profile = native_materializer
    run()
    # Profile mismatch forces full replacement while source start also moves.
    for asset in inputs:
        inputs[asset] = inputs[asset][4:]
    report = run(replace(profile, version="test-v2"))
    assert report["derived_indicator_row_count"] == 40
    actual = load_derived_indicator_frames(
        output_dir=root,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    assert len(actual) == 40


def test_vanished_partition_deleted(native_materializer):
    inputs, run, root, profile = native_materializer
    run()
    inputs["A"] = []
    report = run()
    assert report["deleted_partition_count"] == 1
    actual = load_derived_indicator_frames(
        output_dir=root,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    assert len(actual) == 24 and {r.instrument_id for r in actual} == {"B"}


def test_cf_ladder_only_correction_full_tail_parity(tmp_path):
    profile = DerivedIndicatorProfile("test", "CF ladder", ("rolling_high_20",), 0)
    bars = [
        replace(
            _view(ts_index=i, close=100 + i),
            series_mode="continuous_front",
            series_id="BR",
            contract_id="OLD" if i < 20 else "NEW",
            roll_epoch=int(i >= 20),
        )
        for i in range(40)
    ]
    indicators = [
        replace(_indicator_row(ts_index=i, close=100 + i), contract_id=bar.contract_id)
        for i, bar in enumerate(bars)
    ]

    def build(gap, **kw):
        return m._build_prepared_partition_rows(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            series_mode="continuous_front",
            profile=profile,
            local_series=bars,
            local_indicator_rows=indicators,
            adjustment_ladder_rows=(
                {
                    "instrument_id": "BR",
                    "timeframe": "15m",
                    "roll_sequence": 1,
                    "additive_gap": gap,
                },
            ),
            **kw,
        )

    old = build(10.0)
    key = old[0].partition_key(series_mode="continuous_front")
    write_derived_indicator_frame_batches(
        output_dir=tmp_path,
        row_batches=[old],
        replace_partitions=(key,),
        profile=profile,
        atomic_partition_batches=True,
    )
    full = build(20.0)
    assert old[0].source_inputs_hash != full[0].source_inputs_hash
    assert any(a.values != b.values for a, b in zip(old, full))
    tail = build(20.0, tail_output_dir=tmp_path, tail_compatible=True)
    assert 0 < len(tail) < len(full)
    write_derived_indicator_frame_batches(
        output_dir=tmp_path,
        row_batches=[tail],
        replace_partitions=(key,),
        profile=profile,
        atomic_partition_batches=True,
        partition_start_ts={key: tail[0].ts},
    )
    actual = load_derived_indicator_frames(
        output_dir=tmp_path,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    assert {r.ts: r.values for r in actual} == {r.ts: r.values for r in full}


def test_invalid_code_in_prefix_cannot_wrap_and_be_reused(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        DerivedIndicatorFramePartitionKey,
    )

    column = "cross_close_ema_20_code"
    dates = pd.date_range("2026-01-01", periods=2, tz="UTC")
    old = pa.table(
        {"ts": dates, "contract_id": ["A", "A"], column: pa.array([1, 0], type=pa.int32())}
    )
    for name, values in {
        "ts_close_year": [2026, 2026],
        "warmup_span": [0, 0],
        "null_warmup_span": [0, 0],
        "profile_version": ["test", "test"],
        "output_columns_hash": ["columns", "columns"],
    }.items():
        old = old.append_column(name, pa.array(values))
    frame = old.to_pandas()
    frame[column] = [1.9, 0.0]
    monkeypatch.setattr(tails, "has_delta_log", lambda _: True)
    monkeypatch.setattr(tails, "read_delta_table_arrow", lambda *_, **__: old)
    key = DerivedIndicatorFramePartitionKey("v", "l1", "l2", "15m", "A", "A")
    assert (
        tails.suffix_start_index(
            output_dir=tmp_path,
            partition=key,
            computed=frame,
            output_columns=(column,),
            compatible=True,
        )
        == 1
    )
    frame.loc[0, column] = float(2**40 + 1)
    with pytest.raises(pa.ArrowInvalid):
        tails.suffix_start_index(
            output_dir=tmp_path,
            partition=key,
            computed=frame,
            output_columns=(column,),
            compatible=True,
        )
