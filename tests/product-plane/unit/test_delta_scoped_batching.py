from datetime import UTC, datetime

import pytest
from deltalake import DeltaTable
from deltalake.table import TableOptimizer

from trading_advisor_3000.product_plane.data_plane import delta_runtime as runtime
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    finalize_research_delta_table as compact_default_table,
)

COLUMNS = {"id": "string", "ts": "timestamp", "value": "double", "ts_close_year": "int"}


def _row(key, year=2026, value=1.0):
    return {"id": key, "ts": f"{year}-01-02T00:00:00Z", "value": value, "ts_close_year": year}


def _table(tmp_path):
    return tmp_path / "research_indicator_frames.delta"


def _write(path, scopes, **kwargs):
    return runtime.write_delta_table_scoped_batches(
        table_path=path,
        columns=COLUMNS,
        partition_by=("ts_close_year",),
        scoped_batches=scopes,
        **kwargs,
    )


def _read(path):
    return sorted(DeltaTable(str(path)).to_pyarrow_table().to_pylist(), key=lambda r: r["id"])


@pytest.mark.parametrize("limits", [{"max_batch_scopes": 1}, {"max_batch_bytes": 1}])
def test_pack_budget_flushes_without_losing_complete_scopes(tmp_path, limits):
    path = _table(tmp_path)
    assert _write(path, [("id = 'a'", [_row("a")]), ("id = 'b'", [_row("b")])], **limits) == (2, 2)
    assert [r["id"] for r in _read(path)] == ["a", "b"]
    assert len(DeltaTable(str(path)).file_uris()) == 1


def test_generator_failure_leaves_pending_pack_and_old_values_unchanged(tmp_path):
    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a")])])
    version = DeltaTable(str(path)).version()

    def failed():
        yield "id = 'a'", [_row("a", value=9)]
        raise RuntimeError("compute failed")

    with pytest.raises(RuntimeError, match="compute failed"):
        _write(path, failed())
    assert DeltaTable(str(path)).version() == version
    assert _read(path)[0]["value"] == 1.0


def test_delete_only_and_year_move_include_removed_year(tmp_path, monkeypatch):
    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a", 2025)]), ("id = 'b'", [_row("b")])])
    calls = []
    native = TableOptimizer.compact

    def record(self, *args, **kwargs):
        calls.append(kwargs.get("partition_filters"))
        return native(self, *args, **kwargs)

    monkeypatch.setattr(TableOptimizer, "compact", record)
    _write(path, [("id = 'a'", [_row("a", 2027)]), ("id = 'b'", [])])
    assert calls == [[("ts_close_year", "in", ["2025", "2026", "2027"])]]
    assert _read(path) == [
        {**_row("a", 2027), "ts": datetime(2027, 1, 2, tzinfo=UTC), "ts_close_year": 2027}
    ]


def test_healthy_noop_has_no_commit_or_native_optimize(tmp_path, monkeypatch):
    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a")])])
    version = DeltaTable(str(path)).version()
    monkeypatch.setattr(
        TableOptimizer, "compact", lambda *a, **k: pytest.fail("healthy no-op optimized")
    )
    assert _write(path, []) == (0, 0)
    assert DeltaTable(str(path)).version() == version


def test_failure_after_write_is_repaired_on_noop_retry(tmp_path, monkeypatch):
    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a")])])
    native = runtime.finalize_research_delta_table
    monkeypatch.setattr(
        runtime,
        "finalize_research_delta_table",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("finalize failed")),
    )
    with pytest.raises(RuntimeError, match="finalize failed"):
        _write(path, [("id = 'b'", [_row("b")])])
    assert len(DeltaTable(str(path)).file_uris()) == 2
    monkeypatch.setattr(runtime, "finalize_research_delta_table", native)
    assert _write(path, []) == (0, 0)
    assert len(DeltaTable(str(path)).file_uris()) == 1
    assert [r["id"] for r in _read(path)] == ["a", "b"]


def test_unchanged_healthy_year_is_excluded_from_compaction(tmp_path, monkeypatch):
    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a", 2025)]), ("id = 'b'", [_row("b")])])
    old_files = {uri for uri in DeltaTable(str(path)).file_uris() if "2025" in uri}
    native = TableOptimizer.compact
    calls = []

    def record(self, *args, **kwargs):
        calls.append(kwargs.get("partition_filters"))
        return native(self, *args, **kwargs)

    monkeypatch.setattr(TableOptimizer, "compact", record)
    _write(path, [("id = 'b'", [_row("b", value=2)])])
    assert calls == [[("ts_close_year", "in", ["2026"])]]
    assert old_files <= set(DeltaTable(str(path)).file_uris())


def test_empty_table_and_empty_delete_keep_schema_without_files(tmp_path):
    path = _table(tmp_path)
    assert _write(path, [("id = 'missing'", [])]) == (0, 1)
    assert DeltaTable(str(path)).file_uris() == []
    compact_default_table(
        path, partition_by=("ts_close_year",), since_version=DeltaTable(str(path)).version()
    )


def test_native_predicate_rejection_keeps_previous_snapshot(tmp_path):
    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a")])])
    version = DeltaTable(str(path)).version()
    with pytest.raises(Exception, match="Invariant|predicate|constraint"):
        _write(path, [("id = 'b'", [_row("outside")])])
    assert DeltaTable(str(path)).version() == version
    assert _read(path)[0]["id"] == "a"


def test_failure_while_native_writer_consumes_arrow_stream_is_atomic(tmp_path, monkeypatch):
    import pyarrow as pa

    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a")])])
    version = DeltaTable(str(path)).version()
    native = runtime.write_deltalake

    def interrupted(path, reader, **kwargs):
        def batches():
            yield reader.read_next_batch()
            raise RuntimeError("Arrow stream interrupted")

        return native(path, pa.RecordBatchReader.from_batches(reader.schema, batches()), **kwargs)

    monkeypatch.setattr(runtime, "write_deltalake", interrupted)
    with pytest.raises(Exception, match="Arrow stream interrupted"):
        _write(path, [("id = 'a'", [_row("a", value=99)])])
    assert DeltaTable(str(path)).version() == version
    assert _read(path)[0]["value"] == 1.0


def test_owning_asset_captures_only_finalized_output(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.research import data_snapshots as snapshots

    pending = dict(
        tables={},
        dataset_version="test",
        run_id="test",
        code_version="test",
        quality_checks={},
        quality_evidence={},
    )
    monkeypatch.setattr(snapshots, "_initial", lambda *_: pending)
    monkeypatch.setattr(snapshots, "_pending_path", lambda *_: tmp_path / "pending.json")
    context = SimpleNamespace(
        run_id="test",
        op_execution_context=SimpleNamespace(
            op_config={
                "materialized_output_dir": str(tmp_path),
                "data_snapshot_enabled": True,
                "series_mode": "contract",
            }
        ),
    )
    path = _table(tmp_path)
    _write(path, [("id = 'a'", [_row("a")])])

    @snapshots.snapshot_materialization(("research_indicator_frames",))
    def asset(context):
        _write(path, [("id = 'b'", [_row("b")])])
        return {"success": True}

    native = runtime.finalize_research_delta_table
    monkeypatch.setattr(
        runtime,
        "finalize_research_delta_table",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("finalize failed")),
    )
    with pytest.raises(RuntimeError, match="finalize failed"):
        asset(context)
    assert pending["tables"] == {}
    assert not (tmp_path / "pending.json").exists()
    monkeypatch.setattr(runtime, "finalize_research_delta_table", native)
    asset(context)
    assert (
        pending["tables"]["research_indicator_frames"]["delta_version"]
        == DeltaTable(str(path)).version()
    )
    assert pending["tables"]["research_indicator_frames"]["file_count"] == 1
