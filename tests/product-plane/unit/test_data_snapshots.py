from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as snapshots
from trading_advisor_3000.product_plane.data_plane import delta_runtime

READERS = ("count", "arrow", "frame", "rows", "limited_rows", "batches")


def _table(ids, years, flags, amounts):
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "year": pa.array(years, type=pa.int32()),
            "flag": pa.array(flags, type=pa.bool_()),
            "amount": pa.array(amounts, type=pa.int64()),
            "ts_close": pa.array(
                [datetime(2026, 1, 1, tzinfo=UTC)] * len(ids), type=pa.timestamp("us", tz="UTC")
            ),
        }
    )


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "snapshot_stats.delta"
    write_deltalake(
        str(path),
        _table([1, 2, 3, 4], [2025, 2025, 2026, None], [True, False, None, True], [2, 1, 2, 3]),
        partition_by=["year"],
    )
    # Native Spark may omit predicate-column bounds; retain the PR180 oracle shape.
    log = path / "_delta_log/00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "add" in action:
            stats = json.loads(action["add"]["stats"])
            for bound in ("minValues", "maxValues"):
                for field in ("flag", "amount"):
                    stats[bound].pop(field, None)
            action["add"]["stats"] = json.dumps(stats)
    log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    return path


def _manifest(path, *, reference=None, run_id="run-one", quality="PASS"):
    return snapshots.create_snapshot(
        tables={"source": reference or snapshots.capture_table(path)},
        required_tables=["source"],
        dataset_version="fixture-v1",
        run_id=run_id,
        code_version="exact-fixture-code",
        quality_checks={"fixture-readback": quality},
    )


def _append(path):
    write_deltalake(
        str(path),
        _table(
            [99, 100, 101, 102], [2025, 2025, 2026, None], [True, False, None, True], [2, 1, 2, 3]
        ),
        mode="append",
        partition_by=["year"],
    )


def test_data_publication_rejects_advanced_economics_output(source, monkeypatch):
    from trading_advisor_3000.product_plane.data_plane import data_snapshots as owner

    reference = snapshots.capture_table(source)
    report = {
        "status": "PASS",
        "publish_decision": "publish",
        "economics_refresh": {"output_references": {"snapshot_stats": reference}},
    }
    provenance = {"fixture": "stable"}
    monkeypatch.setattr(owner, "runtime_provenance", lambda: provenance)
    _append(source)
    with pytest.raises(ValueError, match="Economics publication output changed"):
        owner.publish_data_snapshot(
            inputs={},
            canonical_paths=[source],
            canonical_roots=[],
            report=report,
            expected_current_id=None,
            provenance=provenance,
        )
    assert snapshots.current_snapshot(source.parent / ".data_snapshots") is None


def _read(reader, path, filters):
    if reader == "count":
        return delta_runtime.count_delta_table_rows(path, filters=filters)
    if reader == "arrow":
        return sorted(
            delta_runtime.read_delta_table_arrow(path, columns=["id"], filters=filters)[
                "id"
            ].to_pylist()
        )
    if reader == "frame":
        return sorted(
            delta_runtime.read_delta_table_frame(path, columns=["id"], filters=filters)[
                "id"
            ].tolist()
        )
    if reader == "batches":
        return sorted(
            row["id"]
            for batch in delta_runtime.iter_delta_table_row_batches(
                path, columns=["id"], filters=filters, batch_size=1
            )
            for row in batch
        )
    kwargs = {"limit": 100} if reader == "limited_rows" else {}
    return sorted(
        row["id"]
        for row in delta_runtime.read_delta_table_rows(
            path, columns=["id"], filters=filters, **kwargs
        )
    )


@pytest.mark.parametrize("reader", READERS)
@pytest.mark.parametrize(
    "filters,expected",
    [
        ([("year", "=", 2025), ("flag", "==", True)], [1]),
        (
            [
                [("year", "=", 2025), ("flag", "=", False)],
                [("year", "=", 2026), ("amount", "in", [2])],
            ],
            [2, 3],
        ),
        ([("flag", "in", [None])], [3]),
        ([("year", "in", [None]), ("flag", "=", True)], [4]),
        ([("ts_close", ">=", "2026-01-01T00:00:00Z"), ("year", "=", 2026)], [3]),
    ],
)
def test_snapshot_read_modes_preserve_old_version_and_pr180_filters(
    source, reader, filters, expected
):
    manifest = _manifest(source)
    _append(source)
    assert _read(reader, source, filters) != (len(expected) if reader == "count" else expected)
    with snapshots.snapshot_read_scope(manifest):
        assert _read(reader, source, filters) == (len(expected) if reader == "count" else expected)


@pytest.mark.parametrize(
    "reader",
    [
        lambda p: delta_runtime.delta_table_version(p),
        lambda p: delta_runtime.delta_table_columns(p),
        lambda p: delta_runtime.delta_table_column_value_counts(p, column="year"),
        lambda p: delta_runtime.count_delta_table_rows(p),
        lambda p: delta_runtime.read_delta_table_arrow(p),
        lambda p: delta_runtime.read_delta_table_frame(p),
        lambda p: delta_runtime.read_delta_table_rows(p),
        lambda p: list(delta_runtime.iter_delta_table_row_batches(p)),
    ],
)
def test_scope_unregistered_tables_have_no_latest_fallback(source, tmp_path, reader):
    other = tmp_path / "other.delta"
    write_deltalake(str(other), pa.table({"id": [0]}))
    with snapshots.snapshot_read_scope(_manifest(source)):
        with pytest.raises(snapshots.SnapshotUnavailableError, match="not pinned"):
            reader(other)


@pytest.mark.parametrize(
    "escape", [{"strict": False}, {"capture": True}, {"writable_paths": ["other.delta"]}]
)
def test_accepted_scope_rejects_latest_escapes(source, escape):
    with pytest.raises((TypeError, ValueError)):
        with snapshots.snapshot_read_scope(_manifest(source), **escape):
            pass


def test_nested_scope_restores_on_exception_and_threads_are_isolated(source):
    old = _manifest(source)
    _append(source)
    new = _manifest(source, run_id="run-two")
    with snapshots.snapshot_read_scope(old):
        assert delta_runtime.delta_table_version(source) == 0
        with pytest.raises(RuntimeError, match="leave child"):
            with snapshots.snapshot_read_scope(new):
                assert delta_runtime.delta_table_version(source) == 1
                raise RuntimeError("leave child")
        assert delta_runtime.delta_table_version(source) == 0
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(delta_runtime.delta_table_version, source).result() == 1
    assert delta_runtime.delta_table_version(source) == 1


def test_scope_reference_is_not_mutable_or_aliased(source):
    manifest = _manifest(source)
    with snapshots.snapshot_read_scope(manifest) as scope:
        manifest["tables"]["source"]["partition_columns"].append("id")
        assert snapshots.open_read_table(source).version() == 0
        with pytest.raises(TypeError):
            scope.tables["source"]["delta_version"] = 99


@pytest.mark.parametrize(
    "change",
    [
        {"schema_hash": "0" * 64},
        {"layout_version": "0" * 64},
        {"partition_columns": ["id"]},
        {"file_count": 99},
        {"row_count": 99},
        {"active_files_sha256": "0" * 64},
        {"delta_version": 999},
    ],
)
def test_declared_reference_guards_reject_mismatch(source, change):
    reference = {**snapshots.capture_table(source), **change}
    with pytest.raises(snapshots.SnapshotUnavailableError):
        snapshots.open_pinned_table(reference)


def test_new_capture_binds_active_file_set(source):
    reference = snapshots.capture_table(source)
    assert len(reference["active_files_sha256"]) == 64
    _append(source)
    assert (
        snapshots.capture_table(source)["active_files_sha256"] != reference["active_files_sha256"]
    )
    assert snapshots.open_pinned_table(reference).version() == 0


def test_metadata_and_schema_remain_pinned_after_schema_evolving_append(source):
    old = _manifest(source)
    old_columns = delta_runtime.delta_table_columns(source)
    evolved = _table([99], [2025], [True], [2]).append_column("new_value", pa.array(["new"]))
    write_deltalake(str(source), evolved, mode="append", schema_mode="merge", partition_by=["year"])
    assert "new_value" in delta_runtime.delta_table_columns(source)
    with snapshots.snapshot_read_scope(old):
        assert delta_runtime.delta_table_version(source) == 0
        assert delta_runtime.delta_table_columns(source) == old_columns
        assert delta_runtime.delta_table_column_value_counts(source, column="year") == {
            "2025": 2,
            "2026": 1,
            "<null>": 1,
        }
        assert delta_runtime.delta_table_column_value_counts(source, column="new_value") == {}


def test_accepted_scope_ignores_unscoped_factory(source, monkeypatch):
    old = _manifest(source)
    _append(source)

    def forbid_latest(*args, **kwargs):
        pytest.fail("unscoped factory bypassed accepted snapshot")

    monkeypatch.setattr(delta_runtime, "DeltaTable", forbid_latest)
    with snapshots.snapshot_read_scope(old):
        assert delta_runtime.count_delta_table_rows(source) == 4
        assert snapshots.open_read_table(source, unscoped_open=forbid_latest).version() == 0


@pytest.mark.parametrize("version", [-1, True, "0"])
def test_capture_rejects_untyped_or_negative_version(source, version):
    with pytest.raises(ValueError, match="nonnegative integer"):
        snapshots.capture_table(source, version=version)


def test_accepted_scope_cannot_capture_mutable_latest(source):
    manifest = _manifest(source)
    _append(source)
    with snapshots.snapshot_read_scope(manifest):
        with pytest.raises(ValueError, match="capture is forbidden"):
            snapshots.capture_table(source)


def test_missing_pinned_log_never_recaptures_latest(source):
    reference = snapshots.capture_table(source)
    _append(source)
    (source / "_delta_log/00000000000000000000.json").unlink()
    with pytest.raises(snapshots.SnapshotUnavailableError):
        snapshots.open_pinned_table(reference)


@pytest.mark.parametrize("damage", ["missing", "wrong_size", "different_table"])
def test_missing_or_replaced_pinned_data_never_recaptures_latest(source, tmp_path, damage):
    reference = snapshots.capture_table(source)
    if damage == "different_table":
        source.rename(tmp_path / "old.delta")
        write_deltalake(str(source), pa.table({"id": [999]}))
    else:
        file = snapshots._file_path(DeltaTable(str(source)).file_uris()[0])
        if damage == "missing":
            file.unlink()
        else:
            with file.open("ab") as stream:
                stream.write(b"not parquet data")
    with pytest.raises(snapshots.SnapshotUnavailableError):
        snapshots.open_pinned_table(reference)


def test_legacy_v1_manifest_keeps_hash_id_and_reads_without_recapture(
    source, tmp_path, monkeypatch
):
    reference = snapshots.capture_table(source)
    reference.pop("active_files_sha256", None)
    manifest = _manifest(source, reference=reference)
    registry = tmp_path / "registry"
    path = snapshots.save_snapshot(registry, manifest)
    original_bytes = path.read_bytes()
    _append(source)
    loaded = snapshots.load_snapshot(path)
    assert loaded == manifest
    assert path.read_bytes() == original_bytes

    def forbid_capture(*args, **kwargs):
        pytest.fail("accepted v1 was silently recaptured")

    monkeypatch.setattr(snapshots, "capture_table", forbid_capture)
    with snapshots.snapshot_read_scope(loaded):
        assert delta_runtime.count_delta_table_rows(source) == 4


def test_immutable_manifest_and_hash_tampering(source, tmp_path):
    root = tmp_path / "registry"
    manifest = _manifest(source)
    path = snapshots.save_snapshot(root, manifest)
    assert snapshots.save_snapshot(root, manifest) == path
    altered = copy.deepcopy(manifest)
    altered["run_id"] = "different"
    with pytest.raises(ValueError, match="hash"):
        snapshots.save_snapshot(root, altered)
    altered = snapshots.create_snapshot(
        tables=manifest["tables"],
        required_tables=["source"],
        dataset_version="fixture-v1",
        run_id="other",
        code_version="exact-fixture-code",
        quality_checks={"fixture": "PASS"},
    )
    altered["snapshot_id"] = manifest["snapshot_id"]
    altered["manifest_sha256"] = snapshots._hash(
        {k: v for k, v in altered.items() if k != "manifest_sha256"}
    )
    with pytest.raises(ValueError, match="immutable"):
        snapshots.save_snapshot(root, altered)
    assert snapshots.load_snapshot(path) == manifest


def test_pointer_cas_rejects_wrong_expected_and_held_lock(source, tmp_path):
    root = tmp_path / "registry"
    one, two = _manifest(source), _manifest(source, run_id="two")
    path_one, path_two = snapshots.save_snapshot(root, one), snapshots.save_snapshot(root, two)
    snapshots.publish_snapshot(root, path_one, expected_current_id=None)
    with pytest.raises(ValueError, match="changed"):
        snapshots.publish_snapshot(root, path_two, expected_current_id=None)
    assert snapshots.current_snapshot(root) == one
    lock = root / ".publication.lock"
    lock.write_text("other owner")
    with pytest.raises(ValueError, match="running"):
        snapshots.publish_snapshot(root, path_two, expected_current_id=one["snapshot_id"])
    assert lock.read_text() == "other owner"
    lock.unlink()
    snapshots.publish_snapshot(root, path_two, expected_current_id=one["snapshot_id"])
    assert snapshots.current_snapshot(root) == two


def test_publication_revalidates_data_inside_lock_before_replacing_pointer(
    source, tmp_path, monkeypatch
):
    root = tmp_path / "registry"
    one, two = _manifest(source), _manifest(source, run_id="two")
    path_one, path_two = snapshots.save_snapshot(root, one), snapshots.save_snapshot(root, two)
    snapshots.publish_snapshot(root, path_one, expected_current_id=None)
    original_pointer = (root / "published_current.json").read_bytes()
    original_current = snapshots.current_snapshot

    def remove_evidence_during_cas(registry):
        assert (root / ".publication.lock").exists()
        accepted = original_current(registry)
        snapshots._file_path(DeltaTable(str(source)).file_uris()[0]).unlink()
        return accepted

    monkeypatch.setattr(snapshots, "current_snapshot", remove_evidence_during_cas)
    with pytest.raises(snapshots.SnapshotUnavailableError):
        snapshots.publish_snapshot(root, path_two, expected_current_id=one["snapshot_id"])
    assert (root / "published_current.json").read_bytes() == original_pointer
    assert not (root / ".publication.lock").exists()


def test_pointer_rejects_manifest_id_alias(source, tmp_path):
    root = tmp_path / "registry"
    manifest = _manifest(source)
    path = snapshots.save_snapshot(root, manifest)
    other_id = "f" * 32
    (path.parent / f"{other_id}.json").write_bytes(path.read_bytes())
    snapshots.atomic_json(
        root / "published_current.json",
        {
            "schema_version": "data_snapshot_publication.v1",
            "snapshot_id": other_id,
            "manifest_sha256": manifest["manifest_sha256"],
        },
    )
    with pytest.raises(ValueError, match="ID"):
        snapshots.current_snapshot(root)


def test_failed_atomic_pointer_replace_keeps_previous_publication(source, tmp_path, monkeypatch):
    root = tmp_path / "registry"
    one, two = _manifest(source), _manifest(source, run_id="two")
    path_one, path_two = snapshots.save_snapshot(root, one), snapshots.save_snapshot(root, two)
    snapshots.publish_snapshot(root, path_one, expected_current_id=None)

    def reject_replace(*args, **kwargs):
        raise OSError("atomic replacement rejected")

    monkeypatch.setattr(snapshots.os, "replace", reject_replace)
    with pytest.raises(OSError, match="rejected"):
        snapshots.publish_snapshot(root, path_two, expected_current_id=one["snapshot_id"])
    assert snapshots.current_snapshot(root) == one
    assert not (root / ".publication.lock").exists()
    assert not list(root.rglob("*.tmp"))


def test_atomic_json_preserves_replace_error_when_temporary_cleanup_fails(tmp_path, monkeypatch):
    primary = OSError("primary replacement rejected")

    def reject_replace(*args, **kwargs):
        raise primary

    def reject_unlink(*args, **kwargs):
        raise PermissionError("temporary still locked")

    monkeypatch.setattr(snapshots.os, "replace", reject_replace)
    monkeypatch.setattr(Path, "unlink", reject_unlink)
    with pytest.raises(OSError) as failure:
        snapshots.atomic_json(tmp_path / "pointer.json", {"id": "one"})
    assert failure.value is primary
    assert any("temporary still locked" in note for note in primary.__notes__)


def test_atomic_json_never_cleans_another_writers_temporary(tmp_path, monkeypatch):
    fixed_uuid = "a" * 32
    temporary = tmp_path / f".{fixed_uuid}.tmp"
    temporary.write_bytes(b"other owner")
    monkeypatch.setattr(snapshots, "uuid4", lambda: SimpleNamespace(hex=fixed_uuid))
    with pytest.raises(FileExistsError):
        snapshots.atomic_json(tmp_path / "pointer.json", {"id": "one"})
    assert temporary.read_bytes() == b"other owner"


@pytest.mark.parametrize("cause", ["cas", "evidence", "replace"])
def test_publication_preserves_primary_error_when_owned_lock_cleanup_fails(
    source, tmp_path, monkeypatch, cause
):
    root = tmp_path / "registry"
    one, two = _manifest(source), _manifest(source, run_id="two")
    path_one, path_two = snapshots.save_snapshot(root, one), snapshots.save_snapshot(root, two)
    snapshots.publish_snapshot(root, path_one, expected_current_id=None)
    pointer_before = (root / "published_current.json").read_bytes()
    primary = None
    expected = one["snapshot_id"]
    error_type, message = ValueError, "publication changed"
    if cause == "cas":
        expected = None
    elif cause == "evidence":
        primary = snapshots.SnapshotUnavailableError("selected evidence lost")
        error_type, message = snapshots.SnapshotUnavailableError, "selected evidence lost"

        def reject_evidence(*args, **kwargs):
            raise primary

        monkeypatch.setattr(snapshots, "open_pinned_table", reject_evidence)
    else:
        primary = OSError("replacement rejected")
        error_type, message = OSError, "replacement rejected"

        def reject_replace(*args, **kwargs):
            raise primary

        monkeypatch.setattr(snapshots.os, "replace", reject_replace)
    original_unlink = Path.unlink

    def reject_owned_cleanup(path, *args, **kwargs):
        if path.name == ".publication.lock" or (cause == "replace" and path.suffix == ".tmp"):
            raise PermissionError(f"cleanup still locked: {path.name}")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", reject_owned_cleanup)
    with pytest.raises(error_type, match=message) as failure:
        snapshots.publish_snapshot(root, path_two, expected_current_id=expected)
    if primary is not None:
        assert failure.value is primary
    assert any(".publication.lock" in note for note in failure.value.__notes__)
    if cause == "replace":
        assert len(failure.value.__notes__) == 2
    assert (root / "published_current.json").read_bytes() == pointer_before
    assert (root / ".publication.lock").exists()


@pytest.mark.parametrize("operation", ["manifest", "publication"])
def test_cleanup_failure_without_primary_error_is_not_reported_as_success(
    source, tmp_path, monkeypatch, operation
):
    root = tmp_path / "registry"
    manifest = _manifest(source)
    saved = snapshots.save_snapshot(root, manifest) if operation == "publication" else None
    original_unlink = Path.unlink

    def reject_owned_cleanup(path, *args, **kwargs):
        if (operation == "manifest" and path.suffix == ".tmp") or (
            operation == "publication" and path.name == ".publication.lock"
        ):
            raise PermissionError("cleanup still locked")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", reject_owned_cleanup)
    with pytest.raises(PermissionError, match="cleanup still locked"):
        if operation == "manifest":
            snapshots.save_snapshot(root, manifest)
        else:
            snapshots.publish_snapshot(root, saved, expected_current_id=None)
    if operation == "publication":
        # Replacement succeeded before cleanup failed; callers must read the pointer.
        assert snapshots.current_snapshot(root) == manifest
        assert (root / ".publication.lock").exists()
    else:
        assert (
            snapshots.load_snapshot(root / "manifests" / f"{manifest['snapshot_id']}.json")
            == manifest
        )


def test_publication_never_cleans_another_publishers_lock(source, tmp_path, monkeypatch):
    root = tmp_path / "registry"
    path = snapshots.save_snapshot(root, _manifest(source))
    lock = root / ".publication.lock"
    lock.write_text("other owner")
    original_unlink = Path.unlink

    def forbid_foreign_cleanup(candidate, *args, **kwargs):
        if candidate == lock:
            pytest.fail("removed another publisher's lock")
        return original_unlink(candidate, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", forbid_foreign_cleanup)
    with pytest.raises(ValueError, match="already running"):
        snapshots.publish_snapshot(root, path, expected_current_id=None)
    assert lock.read_text() == "other owner"


def test_publication_reloads_saved_manifest_under_owned_lock(source, tmp_path, monkeypatch):
    root = tmp_path / "registry"
    manifest = _manifest(source)
    path = snapshots.save_snapshot(root, manifest)
    original_load = snapshots.load_snapshot
    loads = []

    def track_load(candidate):
        if candidate == path:
            loads.append((root / ".publication.lock").exists())
        return original_load(candidate)

    monkeypatch.setattr(snapshots, "load_snapshot", track_load)
    snapshots.publish_snapshot(root, path, expected_current_id=None)
    assert loads == [False, True]


def test_publication_rejects_manifest_change_at_owned_lock_acquisition(
    source, tmp_path, monkeypatch
):
    root = tmp_path / "registry"
    manifest = _manifest(source)
    path = snapshots.save_snapshot(root, manifest)
    original_open = Path.open

    def replace_manifest_on_lock(candidate, *args, **kwargs):
        stream = original_open(candidate, *args, **kwargs)
        if candidate == root / ".publication.lock" and args and args[0] == "x":
            altered = {**manifest, "run_id": "changed-before-lock"}
            altered["manifest_sha256"] = snapshots._hash(
                {key: value for key, value in altered.items() if key != "manifest_sha256"}
            )
            path.write_text(json.dumps(altered))
        return stream

    monkeypatch.setattr(Path, "open", replace_manifest_on_lock)
    with pytest.raises(ValueError, match="manifest changed"):
        snapshots.publish_snapshot(root, path, expected_current_id=None)
    assert not (root / "published_current.json").exists()
    assert not (root / ".publication.lock").exists()


def test_publication_blocks_nonpass_qc(source, tmp_path):
    root = tmp_path / "registry"
    path = snapshots.save_snapshot(root, _manifest(source, quality="FAIL"))
    with pytest.raises(ValueError, match="PASS QC"):
        snapshots.publish_snapshot(root, path, expected_current_id=None)
    assert snapshots.current_snapshot(root) is None


@pytest.mark.parametrize(
    "change",
    [
        {"delta_version": True},
        {"delta_version": "0"},
        {"delta_version": -1},
        {"table_path": "relative.delta"},
        {"partition_columns": ["year", "year"]},
        {"file_count": True},
        {"active_files_sha256": "invalid"},
    ],
)
def test_manifest_rejects_invalid_declared_reference_even_with_valid_hash(source, change):
    reference = {**snapshots.capture_table(source), **change}
    with pytest.raises(ValueError):
        _manifest(source, reference=reference)


@pytest.mark.parametrize("relative", [False, True])
def test_spark_adapter_selects_exact_version_and_blocks_unknown_path_before_load(
    source, tmp_path, monkeypatch, relative
):
    class RecordingReader:
        def __init__(self):
            self.options = {}
            self.loads = []

        def format(self, name):
            assert name == "delta"
            return self

        def option(self, name, value):
            self.options[name] = value
            return self

        def load(self, path):
            self.loads.append(path)
            return self.options.copy()

    class RecordingSpark:
        def __init__(self):
            self.read = RecordingReader()

    manifest = _manifest(source)
    _append(source)
    spark = RecordingSpark()
    requested = source
    if relative:
        monkeypatch.chdir(source.parent)
        requested = Path(source.name)
    with snapshots.snapshot_read_scope(manifest):
        assert snapshots.read_spark_delta(spark, requested) == {"versionAsOf": 0}
        with pytest.raises(snapshots.SnapshotUnavailableError, match="not pinned"):
            snapshots.read_spark_delta(spark, tmp_path / "unknown.delta")
    assert spark.read.loads == [manifest["tables"]["source"]["table_path"]]
    # This tests the adapter contract; native Spark old-version proof remains separate.


def test_materialization_owner_cannot_replace_accepted_scope(source):
    manifest = _manifest(source)
    with snapshots.snapshot_read_scope(manifest):
        with pytest.raises(ValueError, match="cannot replace an active snapshot scope"):
            with snapshots.materialization_snapshot_scope(manifest, writable_paths=[source]):
                pass
        with pytest.raises(ValueError, match="capture is forbidden"):
            snapshots.capture_table(source)


def test_materialization_owner_pins_inputs_and_reads_owned_outputs(source, tmp_path):
    output = tmp_path / "owner-output.delta"
    write_deltalake(str(output), _table([1], [2026], [True], [2]), partition_by=["year"])
    manifest = _manifest(source)
    _append(source)
    with snapshots.materialization_snapshot_scope(manifest, writable_paths=[output]) as reads:
        assert delta_runtime.count_delta_table_rows(source) == 4
        _append(output)
        assert delta_runtime.count_delta_table_rows(output) == 5
        assert set(reads.plain_tables()) == {"source"}
    with pytest.raises(snapshots.SnapshotUnavailableError, match="changed during calculation"):
        snapshots.assert_tables_unchanged(reads.plain_tables())


def test_materialization_owner_captures_additional_input_immutably(source, tmp_path):
    with snapshots.materialization_snapshot_scope(None) as reads:
        assert delta_runtime.count_delta_table_rows(source) == 4
        with pytest.raises(TypeError):
            reads.tables[source.stem]["delta_version"] = 999
        with pytest.raises(ValueError, match="capture is forbidden"):
            snapshots.capture_table(source)
        process_path = snapshots.save_snapshot_scope(tmp_path / "process")
    assert snapshots.load_snapshot(process_path)["tables"] == reads.plain_tables()
    snapshots.assert_tables_unchanged(reads.plain_tables())


def test_materialization_process_mapping_preserves_pins_and_checks_digest(source, tmp_path):
    manifest = _manifest(source)
    path = snapshots.save_snapshot(tmp_path / "registry", manifest)
    mapping_path = snapshots.snapshot_process_path_mapping(
        path, tmp_path / "mount-map.json", lambda value: value.as_posix()
    )
    child = snapshots.load_process_snapshot(path, mapping_path)
    assert child["tables"] == manifest["tables"]
    assert child["parameters"]["process_source_manifest_sha256"] == manifest["manifest_sha256"]
    payload = json.loads(path.read_text())
    payload["tables"]["source"]["delta_version"] = 999
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="source manifest digest mismatch"):
        snapshots.load_process_snapshot(path, mapping_path)
    payload = dict(manifest)
    payload["schema_version"] = "unsupported"
    payload.pop("manifest_sha256")
    payload["manifest_sha256"] = snapshots._hash(payload)
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="unsupported data snapshot schema"):
        snapshots.load_process_snapshot(path, mapping_path)


def test_materialization_research_direct_context_rejects_legacy_compute(tmp_path):
    from trading_advisor_3000.product_plane.research.data_snapshots import snapshot_materialization

    context = SimpleNamespace(run_id="legacy", op_config={"materialized_output_dir": str(tmp_path)})

    @snapshot_materialization(("research_datasets",))
    def existing_owner(actual):
        assert actual is context
        return {"success": True, "legacy": 7}

    with pytest.raises(ValueError, match="refresh admission required"):
        existing_owner(context)


def test_materialization_research_explicit_parent_requires_current_admission(source, tmp_path):
    from trading_advisor_3000.product_plane.research.data_snapshots import _initial, _pending_path

    canonical = tmp_path / "canonical"
    manifest = _manifest(source)
    path = snapshots.save_snapshot(canonical / ".data_snapshots", manifest)
    context = SimpleNamespace(run_id="owner-run")
    config = {
        "canonical_output_dir": str(canonical),
        "materialized_output_dir": str(tmp_path / "research"),
        "dataset_version": "owner-v1",
        "code_version": "owner-code",
        "series_mode": "contract",
        "data_snapshot_manifest_path": str(path),
    }
    with pytest.raises(ValueError, match="not the accepted current snapshot"):
        _initial(context, config)
    snapshots.publish_snapshot(canonical / ".data_snapshots", path, expected_current_id=None)
    pending = _initial(context, config)
    assert pending["parents"] == [manifest["snapshot_id"]]
    snapshots.atomic_json(_pending_path(context, config), pending)
    replacement = _manifest(source)
    replacement_path = snapshots.save_snapshot(canonical / ".data_snapshots", replacement)
    snapshots.publish_snapshot(
        canonical / ".data_snapshots", replacement_path, expected_current_id=manifest["snapshot_id"]
    )
    with pytest.raises(ValueError, match="accepted Data parent changed"):
        _initial(context, config)


def test_materialization_spark_explicit_version_cannot_override_scope(source):
    manifest = _manifest(source)
    spark = SimpleNamespace(read=None)
    with snapshots.snapshot_read_scope(manifest):
        with pytest.raises(
            snapshots.SnapshotUnavailableError, match="differs from the input snapshot pin"
        ):
            snapshots.read_spark_delta(spark, source, version=1)


def test_materialization_failed_status_never_captures_or_stamps_old_outputs(
    source, tmp_path, monkeypatch
):
    from trading_advisor_3000.product_plane.research import data_snapshots as owner

    pending = {
        "tables": {"source": snapshots.capture_table(source)},
        "dataset_version": "test",
        "run_id": "test",
        "code_version": "test",
        "quality_checks": {},
    }
    monkeypatch.setattr(owner, "_initial", lambda *_: pending)
    monkeypatch.setattr(
        owner, "capture_table", lambda *_: pytest.fail("failed stage captured old output")
    )
    context = SimpleNamespace(
        run_id="test",
        op_config={"data_snapshot_enabled": True, "materialized_output_dir": str(tmp_path)},
    )

    @owner.snapshot_materialization(("old_output",))
    def failed_stage(_):
        return {"success": True, "status": "FAIL"}

    with pytest.raises(ValueError, match="failed QC"):
        failed_stage.__wrapped__(context)
    assert pending["quality_checks"] == {}


@pytest.mark.parametrize("verified_version,covered", [(1, False), (2, True)])
def test_canonical_commit_ownership_binds_the_verified_output_version(
    tmp_path, verified_version, covered
):
    from trading_advisor_3000.product_plane.data_plane.data_snapshots import _downstream_scope

    before = {
        "table_path": (tmp_path / "canonical_bars.delta").as_posix(),
        "table_id": "same",
        "delta_version": 0,
    }
    after = {**before, "delta_version": 2}
    report = {
        "cf_catch_up": {"windows": [{"instrument_id": "BR", "timeframe": "15m"}]},
        "canonical_report": {
            "publish_protocol": {
                "pre_publish_versions": {"canonical_bars": 0},
                "target_paths": {"canonical_bars": before["table_path"]},
                "commit_owner_verified": {
                    "canonical_bars": {**after, "delta_version": verified_version, "verified": True}
                },
            },
            "sidecar_refresh": {
                "dependency_scope": {
                    "version": 1,
                    "output_delta_versions": {"canonical_bars": after},
                }
            },
        },
    }
    scope = _downstream_scope(
        report=report, tables={"canonical_bars": after}, previous_tables={"canonical_bars": before}
    )
    assert ("canonical_bars" in scope["covered_tables"]) is covered


@pytest.mark.parametrize(
    "version,proven,covered",
    [(1, True, True), (True, True, False), (1, 1, False), (None, True, False)],
)
def test_raw_scope_rejects_untyped_or_unversioned_evidence(tmp_path, version, proven, covered):
    from trading_advisor_3000.product_plane.data_plane.data_snapshots import _downstream_scope

    before = {
        "table_path": (tmp_path / "raw_moex_history.delta").as_posix(),
        "table_id": "raw",
        "delta_version": 0,
    }
    after = {**before, "delta_version": 1}
    report = {
        "cf_catch_up": {"windows": [{"instrument_id": "BR", "timeframe": "15m"}]},
        "windowed_input_references": {"raw_moex_history": after},
        "raw_ingest_scope": {
            "version": version,
            "proven": proven,
            "input_reference": before,
            "output_reference": after,
        },
    }
    scope = _downstream_scope(
        report=report,
        tables={"raw_moex_history": after},
        previous_tables={"raw_moex_history": before},
    )
    assert ("raw_moex_history" in scope["covered_tables"]) is covered


@pytest.mark.parametrize("mutation", [None, "input_path", "output_path", "version", "proven"])
def test_oi_scope_binds_typed_evidence_and_both_native_paths(tmp_path, mutation):
    from trading_advisor_3000.product_plane.data_plane.data_snapshots import _downstream_scope

    name = "daily_open_interest"
    before = {
        "table_path": (tmp_path / (name + ".delta")).as_posix(),
        "table_id": "oi",
        "delta_version": 0,
    }
    after = {**before, "delta_version": 1}
    proof = {
        "version": 1,
        "scope_proven": True,
        "input_reference": dict(before),
        "output_reference": dict(after),
        "changed_keys": [{"contract_id": "BRM6"}],
        "windows": [{"instrument_id": "BR", "timeframe": "15m"}],
    }
    if mutation in {"input_path", "output_path"}:
        key = "input_reference" if mutation == "input_path" else "output_reference"
        proof[key]["table_path"] = (tmp_path / "copied" / (name + ".delta")).as_posix()
    elif mutation == "version":
        proof["version"] = True
    elif mutation == "proven":
        proof["scope_proven"] = 1
    scope = _downstream_scope(
        report={"oi_support_scope": proof}, tables={name: after}, previous_tables={name: before}
    )
    assert (name in scope["covered_tables"]) is (mutation is None)
