"""Native Delta producer -> explicit snapshot descriptor -> pinned reader."""

from __future__ import annotations

import copy
import json

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.common import data_snapshots as s


@pytest.fixture
def table_path(tmp_path):
    path = tmp_path / "native.delta"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False, metadata={b"owner": b"native-test"}),
            pa.field("value", pa.string()),
            pa.field("ts_close_year", pa.int32()),
        ]
    )
    write_deltalake(
        str(path),
        pa.Table.from_pylist([{"id": 1, "value": "old", "ts_close_year": 2026}], schema=schema),
        partition_by=["ts_close_year"],
    )
    return path


def manifest(path, reference=None):
    return s.create_snapshot(
        tables={"native": reference or s.capture_table(path)},
        required_tables=["native"],
        dataset_version="native-v1",
        run_id="run",
        code_version="code",
        quality_checks={"native": "PASS"},
    )


def seal(value):
    value["manifest_sha256"] = s._hash({k: v for k, v in value.items() if k != "manifest_sha256"})
    return value


def test_new_manifest_describes_exact_native_schema_and_partitions(table_path):
    value = manifest(table_path)
    assert value["schema_version"] == "data_snapshot.v2"
    descriptor = value["physical_schemas"]["native"]
    assert descriptor == {
        "schema": json.loads(DeltaTable(str(table_path)).schema().to_json()),
        "partition_columns": ["ts_close_year"],
    }
    assert s._hash(descriptor["schema"]) == value["tables"]["native"]["schema_hash"]
    field = next(f for f in descriptor["schema"]["fields"] if f["name"] == "id")
    assert field["nullable"] is False and field["metadata"] == {"owner": "native-test"}
    with s.snapshot_read_scope(value):
        assert s.open_read_table(table_path).to_pyarrow_table(columns=["value"])[
            "value"
        ].to_pylist() == ["old"]


@pytest.mark.parametrize("mutation", ["add", "remove", "type", "nullable", "metadata"])
@pytest.mark.parametrize("rehash_schema", [False, True])
def test_corrupted_description_cannot_replace_native_pin(table_path, mutation, rehash_schema):
    value = manifest(table_path)
    schema = value["physical_schemas"]["native"]["schema"]
    field = schema["fields"][0]
    if mutation == "add":
        schema["fields"].append(
            {"name": "invented", "type": "long", "nullable": True, "metadata": {}}
        )
    elif mutation == "remove":
        schema["fields"].pop(0)
    elif mutation == "type":
        field["type"] = "string"
    elif mutation == "nullable":
        field["nullable"] = not field["nullable"]
    else:
        field["metadata"]["owner"] = "corrupt"
    if rehash_schema:
        value["tables"]["native"]["schema_hash"] = s._hash(schema)
    seal(value)
    with pytest.raises((ValueError, s.SnapshotUnavailableError)):
        with s.snapshot_read_scope(value):
            s.open_read_table(table_path)


@pytest.mark.parametrize(
    "corruption", ["missing_all", "missing_table", "missing_nullable", "partition"]
)
def test_v2_description_is_required_and_partition_fields_are_bound(table_path, corruption):
    value = manifest(table_path)
    if corruption == "missing_all":
        del value["physical_schemas"]
    elif corruption == "missing_table":
        value["physical_schemas"].clear()
    elif corruption == "missing_nullable":
        del value["physical_schemas"]["native"]["schema"]["fields"][0]["nullable"]
    else:
        value["physical_schemas"]["native"]["partition_columns"] = ["missing"]
    seal(value)
    with pytest.raises(ValueError):
        s._validate(value)


def test_legacy_reference_upgrade_keeps_identity_and_supplies_partition_metadata(table_path):
    reference = s.capture_table(table_path)
    del reference["partition_columns"], reference["layout_version"]
    original = copy.deepcopy(reference)
    value = manifest(table_path, reference)
    assert value["tables"]["native"] == original
    assert value["physical_schemas"]["native"]["partition_columns"] == ["ts_close_year"]
    value["physical_schemas"]["native"]["partition_columns"] = ["id"]
    seal(value)
    with pytest.raises(s.SnapshotUnavailableError):
        with s.snapshot_read_scope(value):
            s.open_read_table(table_path)


def test_immutable_legacy_load_and_process_remap_are_offline(table_path, tmp_path):
    value = manifest(table_path)
    value["schema_version"] = "data_snapshot.v1"
    del value["physical_schemas"]
    value["tables"]["native"]["table_path"] = (tmp_path / "unavailable.delta").as_posix()
    seal(value)
    source = tmp_path / "legacy.json"
    source.write_text(json.dumps(value))
    before = source.read_bytes()
    mapping = tmp_path / "mapping.json"
    mapped = (tmp_path / "also-unavailable.delta").as_posix()
    mapping.write_text(
        json.dumps(
            {
                "source_manifest_sha256": value["manifest_sha256"],
                "paths": {value["tables"]["native"]["table_path"]: mapped},
            }
        )
    )
    assert s.load_snapshot(source) == value
    result = s.load_process_snapshot(source, mapping)
    assert result["schema_version"] == "data_snapshot.v1"
    assert result["tables"]["native"]["table_path"] == mapped
    assert source.read_bytes() == before


@pytest.mark.parametrize("kind", ["add", "remove", "type", "nullable"])
def test_schema_evolution_changes_hash_but_old_description_stays_pinned(table_path, kind):
    old = manifest(table_path)
    schema = DeltaTable(str(table_path)).to_pyarrow_table().schema
    fields = list(schema)
    row = {"id": 2, "value": "new", "ts_close_year": 2026}
    if kind == "add":
        fields.append(pa.field("added", pa.int64()))
        row["added"] = 9
    elif kind == "remove":
        fields = [f for f in fields if f.name != "value"]
        row.pop("value")
    elif kind == "type":
        fields = [pa.field("value", pa.int64()) if f.name == "value" else f for f in fields]
        row["value"] = 3
    else:
        fields = [f.with_nullable(True) if f.name == "id" else f for f in fields]
    write_deltalake(
        str(table_path),
        pa.Table.from_pylist([row], schema=pa.schema(fields)),
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["ts_close_year"],
    )
    assert s.capture_table(table_path)["schema_hash"] != old["tables"]["native"]["schema_hash"]
    with s.snapshot_read_scope(old):
        assert s.open_read_table(table_path).version() == 0
        assert s.open_read_table(table_path).to_pyarrow_table(columns=["id"])["id"].to_pylist() == [
            1
        ]


@pytest.fixture
def semantic_pair(table_path):
    from trading_advisor_3000.product_plane.research import compute_contract as c
    from trading_advisor_3000.product_plane.research.derived_indicators.materialize import (
        _source_indicator_columns_for_profile,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
        current_derived_indicator_profile,
    )
    from trading_advisor_3000.product_plane.research.derived_indicators.source_frames import (
        research_derived_source_frame_store_contract,
    )
    from trading_advisor_3000.product_plane.research.indicators.registry import (
        default_indicator_profile,
    )

    name = "research_derived_source_frames"
    current = c.semantic_compute_contract()
    expected = research_derived_source_frame_store_contract(
        source_indicator_columns=_source_indicator_columns_for_profile(
            current_derived_indicator_profile()
        )
    )[name]
    assert current["schemas"][name] == expected["columns"]
    legacy = copy.deepcopy(current)
    legacy["schemas"][name] = research_derived_source_frame_store_contract(
        source_indicator_columns=default_indicator_profile().expected_output_columns()
    )[name]["columns"]
    legacy["digest"] = c.digest({k: v for k, v in legacy.items() if k != "digest"})
    return legacy, current, expected


def source_manifest(tmp_path, pair, *, extra=None, wrong_type=None):
    legacy, _, expected = pair
    kinds = {
        "string": pa.string(),
        "int": pa.int32(),
        "bigint": pa.int64(),
        "boolean": pa.bool_(),
        "double": pa.float64(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
        "json": pa.string(),
    }
    columns = {**expected["columns"], **expected["physical_columns"], **(extra or {})}
    fields = [
        pa.field(k, kinds[wrong_type[1] if wrong_type and wrong_type[0] == k else v])
        for k, v in columns.items()
    ]
    path = tmp_path / "source.delta"
    write_deltalake(
        str(path),
        pa.Table.from_pylist([{"ts_close_year": 2026}], schema=pa.schema(fields)),
        partition_by=["ts_close_year"],
    )
    accepted = s.create_snapshot(
        tables={"research_derived_source_frames": s.capture_table(path)},
        required_tables=["research_derived_source_frames"],
        dataset_version="native-v1",
        run_id="run",
        code_version="code",
        quality_checks={"native": "PASS"},
        parameters={"semantic_compute_contract": legacy},
    )
    return accepted


@pytest.mark.parametrize("retained", [False, True])
def test_exact_metadata_correction_proves_native_schema_without_writes(
    tmp_path, semantic_pair, retained
):
    from trading_advisor_3000.product_plane.research.hash_compatibility import compatibility_receipt

    accepted = source_manifest(
        tmp_path, semantic_pair, extra={"adx_14": "double"} if retained else None
    )
    before = copy.deepcopy(accepted)
    receipt = compatibility_receipt(accepted, {}, semantic_pair[1])
    assert receipt["version"] == 3
    assert receipt["old_compute_digest"] == semantic_pair[0]["digest"]
    assert receipt["compute_digest"] == semantic_pair[1]["digest"]
    assert receipt["schema_adaptation"]["retained_legacy_columns"] == (
        ["adx_14"] if retained else []
    )
    assert accepted == before
    assert s.open_pinned_table(accepted["tables"]["research_derived_source_frames"]).version() == 0


@pytest.mark.parametrize(
    "mutation", ["extra", "legacy_type", "input_type", "arbitrary_removal", "numeric"]
)
def test_metadata_correction_rejects_unproved_transition(tmp_path, semantic_pair, mutation):
    from trading_advisor_3000.product_plane.research.hash_compatibility import compatibility_receipt
    from trading_advisor_3000.product_plane.research.semantic_fingerprint import (
        ContractError,
        digest,
    )

    extra = (
        {"unknown": "double"}
        if mutation == "extra"
        else {"adx_14": "string"}
        if mutation == "legacy_type"
        else None
    )
    accepted = source_manifest(
        tmp_path,
        semantic_pair,
        extra=extra,
        wrong_type=("close", "string") if mutation == "input_type" else None,
    )
    current = copy.deepcopy(semantic_pair[1])
    if mutation == "arbitrary_removal":
        current["schemas"]["research_derived_source_frames"].pop("close")
    elif mutation == "numeric":
        current["kernels"]["l2"] = "a" * 64
    current["digest"] = digest({k: v for k, v in current.items() if k != "digest"})
    with pytest.raises(ContractError):
        compatibility_receipt(accepted, {}, current)


@pytest.mark.parametrize("failure", [None, "numeric", "missing_pin"])
def test_presemantic_adaptation_is_proved_or_blocks_without_full_refresh(
    tmp_path, semantic_pair, monkeypatch, failure
):
    from trading_advisor_3000.product_plane.research import hash_compatibility as bridge
    from trading_advisor_3000.product_plane.research.semantic_fingerprint import (
        ContractError,
        digest,
    )

    accepted = source_manifest(tmp_path, semantic_pair)
    accepted["parameters"].pop("semantic_compute_contract")
    accepted["parameters"]["runtime_provenance"] = {"source_sha256": bridge.NATIVE_LEGACY_SOURCE}
    monkeypatch.setattr(bridge, "BOOTSTRAP_CONTRACT_DIGEST", semantic_pair[0]["digest"])
    current = copy.deepcopy(semantic_pair[1])
    if failure == "numeric":
        current["kernels"]["l2"] = "a" * 64
        current["digest"] = digest({k: v for k, v in current.items() if k != "digest"})
    elif failure == "missing_pin":
        accepted["tables"].clear()
    if failure:
        with pytest.raises(ContractError):
            bridge.compatibility_receipt(accepted, {"source_sha256": "new"}, current)
    else:
        receipt = bridge.compatibility_receipt(accepted, {"source_sha256": "new"}, current)
        assert receipt["version"] == 3
        assert receipt["source_sha256"] == bridge.NATIVE_LEGACY_SOURCE


def test_nested_native_fields_and_v2_offline_remap_preserve_descriptors(tmp_path):
    path = tmp_path / "nested.delta"
    nested = pa.struct(
        [pa.field("inner", pa.int64(), nullable=False, metadata={b"meaning": b"native"})]
    )
    schema = pa.schema([pa.field("payload", pa.list_(nested))])
    write_deltalake(str(path), pa.Table.from_pylist([{"payload": [{"inner": 1}]}], schema=schema))
    value = manifest(path)
    descriptor = value["physical_schemas"]["native"]
    assert descriptor["schema"] == json.loads(DeltaTable(str(path)).schema().to_json())
    source = tmp_path / "source.json"
    source.write_text(json.dumps(value))
    target = (tmp_path / "offline.delta").as_posix()
    mapping = tmp_path / "paths.json"
    mapping.write_text(
        json.dumps(
            {"source_manifest_sha256": value["manifest_sha256"], "paths": {path.as_posix(): target}}
        )
    )
    remapped = s.load_process_snapshot(source, mapping)
    assert remapped["schema_version"] == "data_snapshot.v2"
    assert remapped["physical_schemas"]["native"] == descriptor


@pytest.mark.parametrize("hint", [{"type": "struct"}, {"type": "array"}, {"type": "map"}])
def test_native_field_metadata_is_opaque_json(table_path, hint):
    from deltalake import Schema

    value = manifest(table_path)
    schema = value["physical_schemas"]["native"]["schema"]
    schema["fields"][0]["metadata"] = {"hint": hint}
    assert json.loads(Schema.from_json(json.dumps(schema)).to_json()) == schema
    value["tables"]["native"]["schema_hash"] = s._hash(schema)
    seal(value)
    s._validate(value)
    # Opaque metadata remains part of the physical pin, never ignored on read.
    with pytest.raises(s.SnapshotUnavailableError):
        with s.snapshot_read_scope(value):
            s.open_read_table(table_path)
