from dataclasses import replace

import pytest
from deltalake import DeltaTable

from trading_advisor_3000.product_plane.research.indicators.batch_writer import (
    write_indicator_frame_scoped_batches as write_indicator_frame_partition_batches,
)
from trading_advisor_3000.product_plane.research.indicators.registry import (
    IndicatorProfile,
    IndicatorSpec,
)
from trading_advisor_3000.product_plane.research.indicators.store import IndicatorFrameRow


def _row(symbol="BR", ts="2026-01-02T09:00:00Z", value=10.0):
    return IndicatorFrameRow(
        dataset_version="batch-test",
        indicator_set_version="test",
        profile_version="test",
        contract_id=symbol,
        instrument_id=symbol,
        timeframe="15m",
        ts=ts,
        bar_end_ts=ts,
        values={"sma_2": value},
        source_bars_hash="source",
        row_count=1,
        warmup_span=2,
        null_warmup_span=0,
        created_at="2026-01-03T00:00:00Z",
        series_id=symbol,
    )


def _profile():
    return IndicatorProfile(
        version="test",
        description="Native batching regression",
        indicators=(
            IndicatorSpec(
                indicator_id="sma_2",
                category="trend",
                operation_key="sma",
                parameters=(),
                required_input_columns=("close",),
                output_columns=("sma_2",),
                warmup_bars=2,
            ),
        ),
    )


def _write_l1(path, rows, *, deleted=()):
    keys = [row.partition_key(series_mode="contract") for row in rows]
    return write_indicator_frame_partition_batches(
        output_dir=path,
        partition_row_batches=((key, [row]) for key, row in zip(keys, rows, strict=True)),
        partition_start_ts={key: row.ts for key, row in zip(keys, rows, strict=True)},
        delete_partitions=deleted,
        profile=_profile(),
    )


def _rows(path):
    return DeltaTable(str(path)).to_pyarrow_table().to_pylist()


def test_daily_l1_groups_two_series_in_one_native_write(tmp_path):
    _, count, batches = _write_l1(tmp_path, [_row(), _row("SI")])
    assert (count, batches) == (2, 1)
    assert {
        row["instrument_id"] for row in _rows(tmp_path / "research_indicator_frames.delta")
    } == {
        "BR",
        "SI",
    }


def test_daily_l1_pack_keeps_prefix_other_series_and_deletes_empty_scope(tmp_path):
    table = tmp_path / "research_indicator_frames.delta"
    _write_l1(tmp_path, [_row(), _row("SI"), _row("RI")])
    _write_l1(tmp_path, [_row(ts="2026-01-01T09:00:00Z", value=9.0)])
    # The prior call replaces the BR suffix, so restore its second timestamp.
    _write_l1(tmp_path, [_row()])
    _, count, batches = _write_l1(
        tmp_path,
        [_row(value=11.0), _row("SI", value=20.0)],
        deleted=(_row("RI").partition_key(series_mode="contract"),),
    )
    result = _rows(table)
    assert (count, batches) == (2, 1)
    assert sorted((r["instrument_id"], r["sma_2"]) for r in result) == [
        ("BR", 9.0),
        ("BR", 11.0),
        ("SI", 20.0),
    ]


def test_daily_l1_invalid_later_series_keeps_pack_uncommitted(tmp_path):
    table = tmp_path / "research_indicator_frames.delta"
    _write_l1(tmp_path, [_row(), _row("SI")])
    before = _rows(table)
    version = DeltaTable(str(table)).version()
    good, bad = _row(value=99.0), _row("SI")
    keys = [row.partition_key(series_mode="contract") for row in (good, bad)]
    with pytest.raises(ValueError, match="duplicate logical key"):
        write_indicator_frame_partition_batches(
            output_dir=tmp_path,
            partition_row_batches=((keys[0], [good]), (keys[1], [bad, bad])),
            partition_start_ts={key: good.ts for key in keys},
            profile=_profile(),
        )
    assert DeltaTable(str(table)).version() == version
    assert _rows(table) == before


def test_daily_l1_mode_mismatch_cannot_escape_through_another_pack_scope(tmp_path):
    table = tmp_path / "research_indicator_frames.delta"
    row = _row()
    _write_l1(tmp_path, [row])
    partition = row.partition_key(series_mode="contract")
    foreign = replace(row, series_mode="continuous_front")
    version = DeltaTable(str(table)).version()
    with pytest.raises(ValueError, match="partition identity"):
        write_indicator_frame_partition_batches(
            output_dir=tmp_path,
            partition_row_batches=((partition, [foreign]),),
            partition_start_ts={partition: row.ts},
            delete_partitions=(foreign.partition_key(series_mode="continuous_front"),),
            profile=_profile(),
        )
    assert DeltaTable(str(table)).version() == version
    assert [r["series_mode"] for r in _rows(table)] == ["contract"]


def test_close_year_correction_preserves_delta_identity(tmp_path):
    row = _row(ts="2026-12-31T23:30:00Z")
    _write_l1(tmp_path, [row])
    path = tmp_path / "research_indicator_frames.delta"
    identity = DeltaTable(str(path)).metadata().id
    _write_l1(tmp_path, [replace(row, bar_end_ts="2027-01-01T00:00:00Z")])
    assert DeltaTable(str(path)).metadata().id == identity
    assert [item["ts_close_year"] for item in _rows(path)] == [2027]


def test_scoped_noop_schema_extension_preserves_layout(tmp_path):
    from trading_advisor_3000.product_plane.data_plane import delta_runtime as runtime

    row = _row()
    _write_l1(tmp_path, [row])
    path = tmp_path / "research_indicator_frames.delta"
    table = DeltaTable(str(path))
    identity = table.metadata().id
    columns = {field.name: "string" for field in table.to_pyarrow_table().schema}
    from trading_advisor_3000.product_plane.research.indicators.store import (
        _indicator_write_columns,
    )

    columns = {**_indicator_write_columns(path=path, profile=_profile()), "new_value": "double"}
    assert runtime.write_delta_table_scoped_batches(
        table_path=path, scoped_batches=(), columns=columns, partition_by=("ts_close_year",)
    ) == (0, 0)
    table = DeltaTable(str(path))
    assert table.metadata().partition_columns == ["ts_close_year"]
    assert table.metadata().id == identity
    assert "new_value" in table.to_pyarrow_table().schema.names


def test_public_l1_snapshot_noop_repairs_interrupted_finalization(tmp_path, monkeypatch):
    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.data_plane import delta_runtime as runtime
    from trading_advisor_3000.product_plane.research.indicators import batch_writer
    from trading_advisor_3000.product_plane.research.indicators import materialize as owner
    from trading_advisor_3000.product_plane.research.indicators.store import (
        indicator_output_columns_hash,
    )

    bars = tmp_path / "research_bar_views.delta"
    bases = [_row(), _row("SI")]
    write_deltalake(str(bars), pa.Table.from_pylist([row.to_dict() for row in bases]))
    profile = _profile()
    rows = [
        replace(
            row,
            source_dataset_bars_hash="dataset",
            output_columns_hash=indicator_output_columns_hash(("sma_2",)),
            source_snapshot_hash=owner._source_snapshot_hash(bars, None),
            compute_contract_hash=owner._indicator_compute_contract_hash(profile),
            refresh_context_hash=owner._refresh_context_hash(
                profile,
                instrument_id=row.instrument_id,
                ladder_rows=(),
                raw_1m_path=None,
                source_rows=None,
                tick_sizes=None,
            ),
        )
        for row in bases
    ]
    native = runtime.finalize_research_delta_table
    monkeypatch.setattr(
        batch_writer,
        "write_delta_table_scoped_batches",
        lambda **kw: runtime.write_delta_table_scoped_batches(**kw, max_batch_scopes=1),
    )
    monkeypatch.setattr(
        runtime,
        "finalize_research_delta_table",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("finalization interrupted")),
    )
    with pytest.raises(RuntimeError, match="finalization interrupted"):
        _write_l1(tmp_path, rows)
    path = tmp_path / "research_indicator_frames.delta"
    assert len(DeltaTable(str(path)).file_uris()) == 2
    monkeypatch.setattr(runtime, "finalize_research_delta_table", native)
    monkeypatch.setattr(
        owner,
        "_load_dataset_manifest",
        lambda **_: {"series_mode": "contract", "bars_hash": "dataset"},
    )
    monkeypatch.setattr(
        owner,
        "_load_bar_partition_rows",
        lambda **_: pytest.fail("source history read during fast reuse"),
    )
    kwargs = dict(
        dataset_output_dir=tmp_path,
        indicator_output_dir=tmp_path,
        dataset_version="batch-test",
        indicator_set_version="test",
        profile=profile,
    )
    assert (
        owner.materialize_indicator_frames(operation="rebuild", **kwargs)["reused_partition_count"]
        == 2
    )
    table = DeltaTable(str(path))
    assert len(table.file_uris()) == 1
    version = table.version()
    assert (
        owner.materialize_indicator_frames(operation="rebuild", **kwargs)["reused_partition_count"]
        == 2
    )
    assert DeltaTable(str(path)).version() == version

    def drift(*args, **kwargs):
        native(*args, **kwargs)
        write_deltalake(str(bars), pa.Table.from_pylist([bases[0].to_dict()]), mode="append")

    monkeypatch.setattr(runtime, "finalize_research_delta_table", drift)
    with pytest.raises(ValueError, match="inputs changed during snapshot reuse"):
        owner.materialize_indicator_frames(operation="rebuild", **kwargs)
