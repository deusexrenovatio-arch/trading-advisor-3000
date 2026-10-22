import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.research.derived_indicators import materialize as m
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    derived_indicator_output_columns_hash,
)
from trading_advisor_3000.spark_jobs import research_derived_source_frames_job as job


def test_narrow_source_contract_change_requires_metadata_adaptation(tmp_path):
    bars = tmp_path / "bars.delta"
    indicators = tmp_path / "indicators.delta"
    for path in (bars, indicators):
        write_delta_table_rows(table_path=path, rows=[], columns={"ema_20": "double"})
    write_delta_table_rows(
        table_path=tmp_path / "research_derived_source_frames.delta",
        rows=[
            {
                "dataset_version": "d",
                "contour_id": "native_tradable",
                "indicator_set_version": "i",
                "instrument_id": "BR",
                "timeframe": "15m",
                "source_indicator_columns_hash": "old",
            }
        ],
        columns={
            c: "string"
            for c in (
                "dataset_version",
                "contour_id",
                "indicator_set_version",
                "instrument_id",
                "timeframe",
                "source_indicator_columns_hash",
            )
        },
    )
    with pytest.raises(ValueError, match="metadata adaptation"):
        job.run_research_derived_source_frames_spark_job(
            operation="rebuild",
            bar_views_path=bars,
            indicator_frames_path=indicators,
            output_dir=tmp_path,
            dataset_version="d",
            contour_id="native_tradable",
            indicator_set_version="i",
            source_indicator_columns=("ema_20",),
            derived_profile_version="core_v1",
            timeframes=("15m",),
            dataset_instrument_ids=("BR",),
            spark_session_factory=lambda *_: pytest.fail(
                "narrow request expanded into Spark repair"
            ),
        )


def test_metadata_reuse_ignores_other_timeframe_identity(tmp_path, monkeypatch):
    p = m.current_derived_indicator_profile()
    rows = [
        dict(
            dataset_version="d",
            contour_id="pit_active_front",
            series_mode="continuous_front",
            series_id="BR",
            contract_id=None,
            instrument_id="BR",
            timeframe=tf,
            indicator_set_version="i",
            derived_indicator_set_version="j",
            source_snapshot_hash=snap,
            compute_contract_hash="compute",
            output_columns_hash=derived_indicator_output_columns_hash(p.output_columns),
            warmup_span=p.warmup_bars,
        )
        for tf, snap in [("15m", "snap"), ("1d", "stale")]
    ]
    monkeypatch.setattr(m, "has_delta_log", lambda _: True)
    monkeypatch.setattr(m, "load_derived_indicator_partition_metadata", lambda **_: rows)
    monkeypatch.setattr(
        m,
        "load_derived_source_frame_partition_metadata",
        lambda **_: [dict(row, partition_row_count=2) for row in rows],
    )

    def coverage(**kw):
        assert {key.timeframe for key in kw["expected_counts"]} == {"15m"}
        return False

    monkeypatch.setattr(m, "_derived_partition_coverage_changed", coverage)
    result = m._derived_metadata_fast_reuse(
        output_dir=tmp_path,
        dataset_version="d",
        indicator_set_version="i",
        derived_indicator_set_version="j",
        contour_id="pit_active_front",
        series_mode="continuous_front",
        profile=p,
        snapshot_hash="snap",
        compute_contract_hash="compute",
        instruments=("BR",),
        timeframes=("15m",),
    )
    assert result and result["reused_partition_count"] == 1


@pytest.fixture
def native_store(tmp_path, monkeypatch):
    import json
    from dataclasses import replace

    from test_research_derived_indicator_layer import _indicator_row, _view

    from trading_advisor_3000.product_plane.research.datasets import research_dataset_store_contract
    from trading_advisor_3000.product_plane.research.derived_indicators import (
        adapt_native_metadata as a,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
        research_derived_source_frame_store_contract,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.store import (
        research_derived_indicator_store_contract,
    )
    from trading_advisor_3000.product_plane.research.indicators.store import (
        indicator_store_contract,
    )

    # This fixture models an independently audited predecessor. The separate
    # authority regression below rejects the actual unreviewed fingerprint.
    monkeypatch.setattr(a, "native_kernel_digest", lambda: a.NATIVE_KERNEL)
    p = m.current_derived_indicator_profile()
    cols = m._source_indicator_columns_for_profile(p)
    bars = [
        replace(
            _view(ts_index=i, close=float(80 + i)),
            contour_id="pit_active_front",
            series_mode="continuous_front",
            series_id="BR",
        )
        for i in range(32)
    ]
    indicators = [
        replace(
            _indicator_row(ts_index=i, close=float(80 + i)),
            contour_id="pit_active_front",
            series_mode="continuous_front",
            series_id="BR",
            row_count=32,
            values={c: float(i + 1) for c in (*cols, "adx_14")},
            output_columns_hash="base-profile",
        )
        for i in range(32)
    ]
    source = []
    for bar, ind in zip(bars, indicators):
        row = {
            **bar.to_dict(),
            **ind.values,
            "indicator_set_version": "indicators-v1",
            "derived_profile_version": "core_v1",
            "indicator_profile_version": "core_v1",
            "indicator_source_bars_hash": ind.source_bars_hash,
            "indicator_source_dataset_bars_hash": ind.source_dataset_bars_hash,
            "indicator_row_count": 32,
            "indicator_warmup_span": 20,
            "indicator_null_warmup_span": 0,
            "indicator_created_at": ind.created_at,
            "indicator_output_columns_hash": "base-profile",
            "source_indicator_columns_hash": "1E6B1AD5B3491A4D",
            "joined_row_count": 32,
            "source_frame_created_at": ind.created_at,
        }
        source.append(row)
    # Generate only stored fixture values; formula entrypoints remain forbidden.
    raw = []
    for bar in bars:
        raw.append(
            dict(
                dataset_version=bar.dataset_version,
                contour_id=bar.contour_id,
                series_mode=bar.series_mode,
                series_id=bar.series_id,
                indicator_set_version="indicators-v1",
                derived_indicator_set_version="derived-v1",
                profile_version=p.version,
                contract_id=bar.contract_id,
                instrument_id=bar.instrument_id,
                timeframe=bar.timeframe,
                ts=bar.ts,
                **{c: 1.0 for c in p.output_columns},
                source_bars_hash=m._bars_hash(bars),
                source_indicators_hash=a._legacy_indicators(
                    indicators, tuple(dict.fromkeys((*cols, "adx_14")))
                ),
                source_dataset_bars_hash="dataset-bars",
                source_indicator_profile_version="core_v1",
                source_indicator_output_columns_hash="base-profile",
                row_count=32,
                warmup_span=p.warmup_bars,
                null_warmup_span=32,
                created_at=indicators[0].created_at,
                output_columns_hash=derived_indicator_output_columns_hash(p.output_columns),
                source_indicator_columns_hash="1E6B1AD5B3491A4D",
                source_snapshot_hash="legacy",
                compute_contract_hash=a.LEGACY_COMPUTE,
                source_inputs_hash=None,
            )
        )
    contracts = {
        "research_bar_views": (
            research_dataset_store_contract()["research_bar_views"]["columns"],
            [b.to_dict() for b in bars],
        ),
        "research_indicator_frames": (
            indicator_store_contract()["research_indicator_frames"]["columns"],
            [i.to_dict() for i in indicators],
        ),
        a.SOURCE: (
            research_derived_source_frame_store_contract(
                source_indicator_columns=(*cols, "adx_14")
            )[a.SOURCE]["columns"],
            source,
        ),
        a.DERIVED: (research_derived_indicator_store_contract()[a.DERIVED]["columns"], raw),
        "research_datasets": (
            {
                "dataset_version": "string",
                "contour_id": "string",
                "series_mode": "string",
                "bars_hash": "string",
            },
            [
                {
                    "dataset_version": "dataset-v5",
                    "contour_id": "pit_active_front",
                    "series_mode": "continuous_front",
                    "bars_hash": "dataset-bars",
                }
            ],
        ),
        "continuous_front_adjustment_ladder": ({"dataset_version": "string"}, []),
        "continuous_front_indicator_run_manifest": (
            {
                c: "string"
                for c in (
                    "dataset_version",
                    "run_id",
                    "publish_status",
                    "input_delta_versions_json",
                )
            },
            [
                dict(
                    dataset_version="dataset-v5",
                    run_id="legacy-run",
                    publish_status="accepted",
                    indicator_set_version="indicators-v1",
                    derived_set_version="derived-v1",
                    input_delta_versions_json=json.dumps(
                        {
                            name: {"delta_version": 0, "path": str(tmp_path / f"{name}.delta")}
                            for name in (
                                "research_bar_views",
                                "research_indicator_frames",
                                "continuous_front_adjustment_ladder",
                                a.DERIVED,
                            )
                        }
                    ),
                )
            ],
        ),
    }
    contracts["continuous_front_indicator_run_manifest"][0].update(
        indicator_set_version="string", derived_set_version="string"
    )
    for stem, (columns, rows) in contracts.items():
        write_delta_table_rows(
            table_path=tmp_path / f"{stem}.delta", columns=dict(columns), rows=rows
        )
    monkeypatch.setattr(a, "_legacy_compute", lambda _: a.LEGACY_COMPUTE)
    monkeypatch.setattr(a, "_assert_writer_owner", lambda _: None)
    for name in (
        "_compute_derived_frame",
        "_compute_derived_frame_unmasked",
        "_build_partition_rows",
    ):
        monkeypatch.setattr(m, name, lambda **_: pytest.fail("metadata adaptation called formulas"))

    # AST identity is read from file, so forbidden runtime sentinels do not bypass it.
    def plan():
        return a.plan_adaptation(
            tmp_path,
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            contour_id="pit_active_front",
            instruments=("BR",),
            timeframes=("15m",),
            legacy_ladder_version=0,
            legacy_indicator_version=0,
            legacy_bar_version=0,
            legacy_derived_version=0,
        )

    return a, plan, contracts


def test_adaptation_preserves_values_and_supports_rollback(native_store, tmp_path):
    from deltalake import DeltaTable

    a, plan, _ = native_store
    before = {stem: DeltaTable(str(tmp_path / f"{stem}.delta")).version() for stem in a.INPUTS}
    prepared = plan()
    receipt = a.apply_adaptation(
        prepared, writer_root=tmp_path, receipt_path=tmp_path / "receipt.json"
    )
    assert all(row["verified"] for row in receipt["tables"])
    assert [row["table"] for row in receipt["tables"]] == [a.SOURCE, a.DERIVED]
    assert all(
        DeltaTable(str(tmp_path / f"{stem}.delta")).version() == version
        for stem, version in before.items()
        if stem not in (a.SOURCE, a.DERIVED)
    )
    a.rollback_adaptation(receipt)
    rows = DeltaTable(str(tmp_path / f"{a.DERIVED}.delta")).to_pyarrow_table().to_pylist()
    assert {r["compute_contract_hash"] for r in rows} == {a.LEGACY_COMPUTE}


@pytest.mark.parametrize("change", ["unknown", "native_input", "partial", "ladder", "profile"])
def test_adaptation_rejects_unproved_identity_before_writes(
    native_store, tmp_path, change, monkeypatch
):
    from deltalake import DeltaTable

    a, plan, _ = native_store
    derived = DeltaTable(str(tmp_path / f"{a.DERIVED}.delta"))
    if change == "unknown":
        derived.update(new_values={"compute_contract_hash": "unknown"})
    elif change == "native_input":
        DeltaTable(str(tmp_path / "research_indicator_frames.delta")).update(
            new_values={"ema_20": 999.0}
        )
    elif change == "partial":
        derived.delete("ts = '2026-03-16T09:00:00Z'")
    elif change == "profile":
        derived.update(new_values={"output_columns_hash": "other"})
    else:
        original = m._source_inputs_hash
        calls = []

        def changed(**kw):
            calls.append(1)
            return original(**kw) if len(calls) == 1 else "changed-ladder"

        monkeypatch.setattr(m, "_source_inputs_hash", changed)
    before = a._inventory(tmp_path)
    with pytest.raises(ValueError):
        plan()
    assert a._inventory(tmp_path) == before


def test_adaptation_rejects_version_drift(native_store, tmp_path):
    from deltalake import DeltaTable

    a, plan, _ = native_store
    prepared = plan()
    DeltaTable(str(tmp_path / "research_datasets.delta")).update(
        new_values={"bars_hash": "changed"}
    )
    before = a._inventory(tmp_path)
    with pytest.raises(ValueError, match="drift"):
        a.apply_adaptation(prepared, writer_root=tmp_path, receipt_path=tmp_path / "receipt.json")
    assert a._inventory(tmp_path) == before and not (tmp_path / "receipt.json").exists()


def test_scope_from_windows_preserves_exact_disjoint_pairs():
    from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
        NativeRefreshScope,
    )

    scope = NativeRefreshScope.from_request(
        refresh_windows=[
            {"instrument_id": "BR", "timeframe": "15m"},
            {"instrument_id": "Si", "timeframe": "1d"},
        ]
    )
    assert scope.matches("BR", "15m") and scope.matches("Si", "1d")
    assert not scope.matches("BR", "1d") and not scope.matches("Si", "15m")
    assert scope.timeframes == ("15m", "1d")


def test_historical_adoption_keeps_corrections_and_append_pending(native_store, tmp_path):
    from datetime import timedelta

    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
        DerivedSourceFramePartitionKey,
        load_derived_source_frame_partition_rows,
    )

    a, plan, _ = native_store
    bars_path = tmp_path / "research_bar_views.delta"
    source_path = tmp_path / f"{a.SOURCE}.delta"
    indicators_path = tmp_path / "research_indicator_frames.delta"
    # The old immutable versions remain exact. Current has13corrections and37newbars.
    cutoff = "2026-03-16T13:45:00Z"
    for path in (bars_path, source_path):
        DeltaTable(str(path)).update(new_values={"close": 99.0}, predicate=f"ts >= '{cutoff}'")
    for path in (indicators_path, source_path):
        DeltaTable(str(path)).update(new_values={"ema_20": 999.0}, predicate=f"ts >= '{cutoff}'")
    for path in (bars_path, indicators_path, source_path):
        table = DeltaTable(str(path))
        data = table.to_pyarrow_table().sort_by([("ts", "ascending")])
        last = data.to_pylist()[-1]
        tail = []
        for i in range(1, 38):
            row = dict(last, ts=last["ts"] + timedelta(minutes=15 * i))
            if row.get("bar_end_ts") is not None:
                row["bar_end_ts"] = last["bar_end_ts"] + timedelta(minutes=15 * i)
            if "bar_index" in row:
                row["bar_index"] = last["bar_index"] + i
            tail.append(row)
        write_deltalake(str(path), pa.Table.from_pylist(tail, schema=data.schema), mode="append")
    prepared = plan()
    entry = prepared["partitions"][0]
    assert entry["retained_rows"] == 32 and entry["appended_source_rows"] == 37
    assert entry["current_inputs_match"] is False
    receipt = a.apply_adaptation(
        prepared, writer_root=tmp_path, receipt_path=tmp_path / "receipt.json"
    )
    assert all(item["verified"] for item in receipt["tables"])
    rows = load_derived_source_frame_partition_rows(
        output_dir=tmp_path,
        partition=DerivedSourceFramePartitionKey(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            timeframe="15m",
            instrument_id="BR",
            contour_id="pit_active_front",
            series_mode="continuous_front",
            series_id="BR",
        ),
        source_indicator_columns=m._source_indicator_columns_for_profile(
            m.current_derived_indicator_profile()
        ),
    )
    bars, indicators, frame = m._source_frame_rows_to_inputs(
        rows=rows,
        source_indicator_columns=m._source_indicator_columns_for_profile(
            m.current_derived_indicator_profile()
        ),
    )
    frame = m._with_continuous_front_projection(frame, adjustment_ladder_rows=())
    metadata = m.load_derived_indicator_partition_metadata(
        output_dir=tmp_path,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        contour_id="pit_active_front",
    )
    head = metadata[0]
    current_hash = m._source_inputs_hash(
        profile=m.current_derived_indicator_profile(), base_frame=frame
    )
    assert head["row_count"] == 32 and not head["source_snapshot_hash"]
    assert head["source_inputs_hash"] != current_hash
    assert not m._existing_partition_matches(
        [head],
        source_bars_hash=m._bars_hash(bars),
        source_indicators_hash=m._indicator_hash(
            indicators,
            value_columns=m._source_indicator_columns_for_profile(
                m.current_derived_indicator_profile()
            ),
        ),
        profile_version="core_v1",
        row_count=69,
        output_columns_hash=head["output_columns_hash"],
        source_inputs_hash=current_hash,
        compute_contract_hash=head["compute_contract_hash"],
        warmup_span=300,
    )
    assert (
        m._derived_metadata_fast_reuse(
            output_dir=tmp_path,
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            contour_id="pit_active_front",
            series_mode="continuous_front",
            profile=m.current_derived_indicator_profile(),
            snapshot_hash="current",
            compute_contract_hash=head["compute_contract_hash"],
            instruments=("BR",),
            timeframes=("15m",),
        )
        is None
    )


def test_typed_windows_and_absent_selected_pair(native_store, tmp_path):
    from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
        NativeRefreshScope,
    )

    scope = NativeRefreshScope.from_request(
        refresh_windows=[job.DerivedSourceFrameRefreshWindow("BR", "1d")]
    )
    assert scope.pairs == (("BR", "1d"),)
    a, _, _ = native_store
    cols = m._source_indicator_columns_for_profile(m.current_derived_indicator_profile())
    assert not job._source_projection_contract_changed(
        table_path=tmp_path / f"{a.SOURCE}.delta",
        expected_hash="new",
        dataset_version="dataset-v5",
        contour_id="pit_active_front",
        indicator_set_version="indicators-v1",
        source_indicator_columns=cols,
        scope=scope,
    )
    assert not m._derived_projection_contract_changed(
        table_path=tmp_path / f"{a.DERIVED}.delta",
        expected_hash="new",
        dataset_version="dataset-v5",
        contour_id="pit_active_front",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        series_mode="continuous_front",
        scope=scope,
    )
    with pytest.raises(ValueError, match="requested native pair is absent"):
        a.plan_adaptation(
            tmp_path,
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            contour_id="pit_active_front",
            instruments=("BR",),
            timeframes=("15m", "1d"),
            legacy_ladder_version=0,
            legacy_indicator_version=0,
            legacy_bar_version=0,
            legacy_derived_version=0,
        )


def test_missing_physical_native_column_rejects_before_writes(native_store, tmp_path):
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    a, plan, _ = native_store
    for name in (a.SOURCE, "research_indicator_frames"):
        table = DeltaTable(str(tmp_path / f"{name}.delta"))
        data = table.to_pyarrow_table()
        index = data.schema.get_field_index("ema_20")
        data = data.set_column(index, "ema_20", pa.nulls(len(data), type=pa.float64()))
        if name == a.SOURCE:
            data = data.drop(["ema_20"])
        write_deltalake(table, data, mode="overwrite", schema_mode="overwrite")
    before = a._inventory(tmp_path)
    with pytest.raises(ValueError, match="physical native source columns/types"):
        plan()
    assert a._inventory(tmp_path) == before


def test_writer_root_and_mutable_readback_fail_closed(tmp_path, monkeypatch):
    from trading_advisor_3000.product_plane.research.derived_indicators import (
        adapt_native_metadata as a,
    )

    monkeypatch.setattr(a, "_check_writer", lambda _: None)
    with pytest.raises(ValueError, match="promoted writer root"):
        a._assert_writer_owner(tmp_path)
    keys = ["instrument_id", "timeframe"]
    mutable = {"source_inputs_hash"}
    row = dict(instrument_id="BR", timeframe="15m", source_inputs_hash="new")
    old = {
        ("BR", "15m"): [dict(row, source_inputs_hash="old", instrument_id_count=32)],
        ("BR", "1d"): [
            dict(row, timeframe="1d", source_inputs_hash="other", instrument_id_count=2)
        ],
    }
    expected = {("BR", "15m"): 32}
    with pytest.raises(RuntimeError, match="metadata/count"):
        a._verify_metadata_groups(old, old, [row], keys, expected, mutable)
    changed = {key: [dict(item) for item in values] for key, values in old.items()}
    changed[("BR", "15m")][0]["source_inputs_hash"] = "new"
    changed[("BR", "1d")][0]["source_inputs_hash"] = "corrupted"
    with pytest.raises(RuntimeError, match="outside selected scope"):
        a._verify_metadata_groups(old, changed, [row], keys, expected, mutable)


def test_native_adoption_rejects_unreviewed_fingerprint_authority(monkeypatch):
    from trading_advisor_3000.product_plane.research import semantic_fingerprint
    from trading_advisor_3000.product_plane.research.derived_indicators import (
        adapt_native_metadata as a,
    )

    monkeypatch.setattr(semantic_fingerprint, "engine_identity", lambda: "unreviewed")
    with pytest.raises(ValueError, match="fingerprint authority changed"):
        a.native_kernel_digest()
