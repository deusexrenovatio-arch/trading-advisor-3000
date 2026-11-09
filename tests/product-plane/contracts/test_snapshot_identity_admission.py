"""Synthetic oracles: snapshot identity never establishes historical applicability.

Expected results come from explicit source associations/pins and contract rules,
not current live IDs. Delta metadata is substituted only in unit tests; the real
read-only metadata proof exercises the actual existing Delta table separately.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from trading_advisor_3000.product_plane.contracts import identity_v3 as contract
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.data_plane.identity import (
    SnapshotIdentitySelector,
    resolve_snapshot_identity,
)
from trading_advisor_3000.product_plane.data_plane.moex import snapshot_identity as source

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/contracts"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def resolve(query: dict, namespace: dict, bindings: list[dict]) -> dict:
    return resolve_snapshot_identity(
        SnapshotIdentitySelector.from_payload(query, namespace),
        namespace=namespace,
        bindings=bindings,
    )


def test_schema_fixtures_and_public_resolution_preserve_metadata() -> None:
    namespace = fixture("instrument_namespace.v1.json")
    binding = fixture("snapshot_identity_binding.v1.json")
    query = fixture("snapshot_identity_selector.v1.json")
    original = copy.deepcopy(binding)
    result = resolve(query, namespace, [binding])
    assert result == original
    assert result["source_symbol"] == "SBER"
    assert result["historical_mapping_verified"] is False
    result["selector"]["pins"]["table"]["delta_version"] = 99
    assert binding == original  # caller cannot corrupt admitted inputs through readback
    assert resolve(query, namespace, [binding]) == original


@pytest.mark.parametrize("field", ["expiry", "specification"])
def test_unknown_native_future_metadata_does_not_invent_values(field: str) -> None:
    namespace = fixture("instrument_namespace.v1.json")
    future = next(x for x in namespace["instruments"] if x["instrument_id"] == "SRZ6@MOEX")
    future[field] = None
    payload = fixture("canonical_bar.v3.srz6.json")
    contract.validate_identity_contract("canonical_bar.v3.json", payload, namespace)
    assert future[field] is None
    with pytest.raises(SchemaValidationError):
        contract.logical_key(fixture("continuous_front_bars.v3.json"), namespace)


def test_unrelated_unknown_future_does_not_invalidate_confirmed_cf_references() -> None:
    namespace = fixture("instrument_namespace.v1.json")
    unrelated = copy.deepcopy(namespace["instruments"][-1])
    unrelated.update(instrument_id="UNREFERENCED-FUTURE", expiry=None, specification=None)
    namespace["instruments"].append(unrelated)
    contract.logical_key(fixture("continuous_front_bars.v3.json"), namespace)


@pytest.mark.parametrize(
    "field,value",
    [
        ("valid_at", "2020-01-01T00:00:00Z"),
        ("known_at", "2020-01-01T00:00:00Z"),
        ("applicability", "historical_pit"),
        ("series_id", "synthetic-group"),
        ("family_id", "synthetic-group"),
        ("product_id", "synthetic-group"),
    ],
)
def test_snapshot_selector_cannot_be_used_as_temporal_or_grouping_request(field, value) -> None:
    namespace = fixture("instrument_namespace.v1.json")
    query = fixture("snapshot_identity_selector.v1.json")
    query[field] = value
    # Even bypassing the convenience factory cannot bypass the public boundary.
    with pytest.raises(SchemaValidationError):
        resolve_snapshot_identity(
            SnapshotIdentitySelector(json.dumps(query)), namespace=namespace, bindings=[]
        )


def test_snapshot_binding_is_rejected_by_existing_temporal_resolver() -> None:
    with pytest.raises(SchemaValidationError):
        contract.resolve_temporal_mapping(
            [fixture("snapshot_identity_binding.v1.json")],
            namespace=fixture("instrument_namespace.v1.json"),
            source_symbol="SBER",
            source_context={
                "provider": "fixture_terminal",
                "venue": "MOEX",
                "market": "shares",
                "board": "TQBR",
            },
            target_kind="instrument",
            relation="alias",
            valid_at="2020-01-01T00:00:00Z",
            known_at="2020-01-01T00:00:00Z",
        )


def test_rolling_source_alias_cannot_become_a_native_snapshot_instrument() -> None:
    binding = fixture("snapshot_identity_binding.v1.json")
    binding["source_symbol"] = "FUT_SBRF"
    binding["source_descriptor"]["secid"] = "FUT_SBRF"
    with pytest.raises(SchemaValidationError):
        resolve(binding["selector"], fixture("instrument_namespace.v1.json"), [binding])


@pytest.mark.parametrize(
    "pin",
    [
        "source_manifest_sha256",
        "report_sha256",
        "configuration_sha256",
        "assignments_sha256",
        "assignment_version",
        "dataset_context_id",
        "table.source_path",
        "table.table_id",
        "table.delta_version",
    ],
)
def test_different_source_snapshot_config_assignment_or_table_never_resolves(pin: str) -> None:
    namespace = fixture("instrument_namespace.v1.json")
    query = fixture("snapshot_identity_selector.v1.json")
    target = query["pins"]
    parts = pin.split(".")
    for part in parts[:-1]:
        target = target[part]
    current = target[parts[-1]]
    target[parts[-1]] = current + 1 if isinstance(current, int) else "f" * 64
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve(query, namespace, [fixture("snapshot_identity_binding.v1.json")])


@pytest.mark.parametrize(
    "field,value",
    [
        ("secid", "DIFFERENT-CONTRACT"),
        ("engine", "currency"),
        ("board", "OTHER-BOARD"),
        ("provider", "other-feed"),
        ("price_basis", "adjusted"),
    ],
)
def test_contradictory_source_descriptor_fails_closed(field: str, value: str) -> None:
    binding = fixture("snapshot_identity_binding.v1.json")
    binding["source_descriptor"][field] = value
    with pytest.raises(SchemaValidationError):
        resolve(binding["selector"], fixture("instrument_namespace.v1.json"), [binding])


def test_empty_ambiguous_and_namespace_mismatched_inputs_fail_closed() -> None:
    namespace = fixture("instrument_namespace.v1.json")
    binding = fixture("snapshot_identity_binding.v1.json")
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve(binding["selector"], namespace, [])
    with pytest.raises(SchemaValidationError, match="ambiguous"):
        resolve(binding["selector"], namespace, [binding, copy.deepcopy(binding)])
    namespace["assets"].append({"asset_id": "unrelated-asset", "kind": "other"})
    with pytest.raises(SchemaValidationError, match="namespace fingerprint"):
        resolve(binding["selector"], namespace, [binding])


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider", "other-feed"),
        ("source_engine", "other-engine"),
        ("price_basis", "adjusted-space"),
    ],
)
def test_consumer_source_feed_engine_and_price_spaces_are_separate(field, value):
    namespace = fixture("instrument_namespace.v1.json")
    binding = fixture("snapshot_identity_binding.v1.json")
    query = copy.deepcopy(binding["selector"])
    target = query["source_context"] if field == "provider" else query
    target[field] = value
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve(query, namespace, [binding])


def test_distinct_native_instruments_never_collapse_by_equal_descriptors():
    namespace = fixture("instrument_namespace.v1.json")
    twin = copy.deepcopy(
        next(i for i in namespace["instruments"] if i["instrument_id"] == "SBER@MOEX")
    )
    twin["instrument_id"] = "distinct-native-instrument"
    namespace["instruments"].append(twin)
    binding = fixture("snapshot_identity_binding.v1.json")
    binding["selector"]["pins"]["namespace_sha256"] = contract.identity_input_fingerprint(namespace)
    query = copy.deepcopy(binding["selector"])
    query["instrument_id"] = twin["instrument_id"]
    with pytest.raises(SchemaValidationError, match="no applicable"):
        resolve(query, namespace, [binding])


def make_source(tmp_path: Path, monkeypatch, *, change=None) -> tuple[dict, list]:
    """Independent small producer metadata; no Delta tables or bar outputs created."""
    source_root, run_root = tmp_path / "source", tmp_path / "run"
    source_root.mkdir()
    (run_root / "proofs").mkdir(parents=True)
    artifacts = {}
    descriptors, assignments, requests = [], [], {}
    for symbol, native_id, asset_id, kind, engine, market, board in [
        ("SBER", "share-A", "ordinary-share", "common_share", "stock", "shares", "TQBR"),
        ("IMOEX", "index-A", "reference-index", "stock_index", "stock", "index", "SNDX"),
        ("SRZ6", "future-A", "ordinary-share", "futures", "futures", "forts", "RFUD"),
    ]:
        asset_class = {"common_share": "equity", "stock_index": "index", "futures": "futures"}[kind]
        descriptor = {
            "series_id": "legacy-" + symbol,
            "security_id": "security-" + symbol,
            "secid": symbol,
            "source": "moex_iss",
            "engine": engine,
            "market": market,
            "board": board,
            "price_basis": "source_native",
            "asset_class": asset_class,
            "metadata_asof": "2026-10-27T14:23:45Z",
            "underlying_security_id": "security-SBER" if kind == "futures" else None,
        }
        assignment = {
            "instrument_id": native_id,
            "asset_id": asset_id,
            "ticker": symbol,
            "source_series_id": descriptor["series_id"],
            "source_security_id": descriptor["security_id"],
            "source_descriptor": copy.deepcopy(descriptor),
            "exchange": "MOEX",
            "instrument_role": "derivative" if kind == "futures" else "base",
            "asset_class": asset_class,
            "contract_id": "SRZ6@MOEX" if kind == "futures" else None,
        }
        fields = {"SECID": symbol, "TYPE": kind, "EMITTER_ID": "issuer-A"}
        if kind == "futures":
            fields.update(
                ASSETCODE="reviewed-ordinary-share-code", LOTSIZE="100", LSTTRADE="2026-12-17"
            )
        envelope = {
            "fetched_at_utc": descriptor["metadata_asof"],
            "payload": {
                "description": {
                    "columns": ["name", "value"],
                    "data": list(map(list, fields.items())),
                },
                "boards": {
                    "columns": ["secid", "engine", "market", "boardid", "is_traded"],
                    "data": [[symbol, engine, market, board, 1]],
                },
            },
        }
        artifacts[symbol] = {
            "descriptor": descriptor,
            "assignment": assignment,
            "envelope": envelope,
        }
        descriptors.append(descriptor)
        assignments.append(assignment)
        requests[native_id] = symbol + ".json"
    if change:
        change(artifacts)

    def write(path: Path, data) -> str:
        raw = (json.dumps(data, sort_keys=True) + "\n").encode()
        path.write_bytes(raw)
        return hashlib.sha256(raw).hexdigest()

    hashes = {}
    for symbol, artifact in artifacts.items():
        envelope = artifact["envelope"]
        payload_bytes = (
            json.dumps(envelope["payload"], sort_keys=True, ensure_ascii=False, allow_nan=False)
            + "\n"
        ).encode()
        envelope["payload_sha256"] = hashlib.sha256(payload_bytes).hexdigest()
        hashes[symbol + ".json"] = write(source_root / (symbol + ".json"), envelope)
    raw = "".join(json.dumps(d) + "\n" for d in descriptors).encode()
    (source_root / "series.jsonl").write_bytes(raw)
    hashes["series.jsonl"] = hashlib.sha256(raw).hexdigest()
    manifest_hash = write(
        source_root / "manifest.json", {"descriptor_path": "series.jsonl", "source_hashes": hashes}
    )
    assignment_path = run_root / "proofs/assignments.json"
    assignment_hash = write(
        assignment_path,
        {
            "instruments": assignments,
            "registry_hash": "reviewed-registry",
            "mapping_version": "reviewed-version",
        },
    )
    config = json.dumps(
        {
            "source_manifest_sha256": manifest_hash,
            "registry_hash": "reviewed-registry",
            "mapping_version": "reviewed-version",
        }
    )
    report_path = run_root / "proofs/report.json"
    report_hash = write(
        report_path,
        {
            "role": "verification_only",
            "status": "PASS",
            "registry_hash": "reviewed-registry",
            "contexts": [
                {
                    "label": "source",
                    "context_json": config,
                    "dataset_context_id": "CTX:" + hashlib.sha256(config.encode()).hexdigest(),
                }
            ],
            "tables": {"analytical_bars": {"path": "/logical/run/data/bars.delta", "version": 5}},
        },
    )
    invocation_path = run_root / "proofs/invocation.json"
    invocation_hash = write(
        invocation_path,
        {
            "writable_run": str(run_root),
            "readonly_source": str(source_root),
            "logical_container_run": "/logical/run",
        },
    )
    reads = []

    def table(path, *, version):
        reads.append((path, version))
        return SimpleNamespace(
            metadata=lambda: SimpleNamespace(id="physical-table-A"), version=lambda: version
        )

    monkeypatch.setattr(source, "DeltaTable", table)
    return {
        "source_root": source_root,
        "report_path": report_path,
        "assignments_path": assignment_path,
        "invocation_path": invocation_path,
        "expected": {
            "source_manifest_sha256": manifest_hash,
            "report_sha256": report_hash,
            "assignments_sha256": assignment_hash,
            "invocation_sha256": invocation_hash,
            "table_id": "physical-table-A",
        },
        "namespace_name": "reviewed-test-pilot",
        "metadata_requests": requests,
    }, reads


def test_constructor_produces_only_sourced_native_bindings_with_exact_pins(tmp_path, monkeypatch):
    inputs, reads = make_source(tmp_path, monkeypatch)
    result = source.admit_native_snapshot(**inputs)
    by_symbol = {b["source_symbol"]: b for b in result["bindings"]}
    assert by_symbol["SRZ6"]["selector"]["source_context"]["market"] == "forts"
    assert by_symbol["SBER"]["asset_id"] == by_symbol["SRZ6"]["asset_id"] == "ordinary-share"
    assert by_symbol["IMOEX"]["asset_id"] == "reference-index"
    future = next(i for i in result["namespace"]["instruments"] if i["instrument_id"] == "future-A")
    assert future["expiry"] is None and future["specification"] is None
    assert result["evidence"]["source_fields"]["future-A"]["LOTSIZE"] == "100"
    assert {version for _, version in reads} == {5}  # no implicit latest read
    for binding in result["bindings"]:
        assert resolve(binding["selector"], result["namespace"], result["bindings"]) == binding


@pytest.mark.parametrize("input_name", ["report_path", "assignments_path", "invocation_path"])
def test_constructor_rejects_changed_original_bytes(tmp_path, monkeypatch, input_name):
    inputs, _ = make_source(tmp_path, monkeypatch)
    with inputs[input_name].open("ab") as handle:
        handle.write(b" ")
    with pytest.raises(SchemaValidationError, match="hash mismatch"):
        source.admit_native_snapshot(**inputs)


def test_constructor_rejects_wrong_physical_identity(tmp_path, monkeypatch):
    inputs, _ = make_source(tmp_path, monkeypatch)
    inputs["expected"]["table_id"] = "different-physical-table"
    with pytest.raises(SchemaValidationError, match="physical table"):
        source.admit_native_snapshot(**inputs)


@pytest.mark.parametrize(
    "corruption",
    [
        "assignment",
        "source_symbol",
        "board",
        "underlying",
        "wrong_contract",
        "explicit_underlying_link",
    ],
)
def test_constructor_rejects_self_consistently_rehashed_wrong_associations(
    tmp_path, monkeypatch, corruption
):
    def change(artifacts):
        if corruption == "assignment":
            artifacts["SRZ6"]["assignment"]["source_descriptor"]["secid"] = "OTHER-CONTRACT"
        elif corruption == "source_symbol":
            artifacts["SRZ6"]["envelope"]["payload"]["description"]["data"][0][1] = "OTHER-CONTRACT"
        elif corruption == "board":
            artifacts["SBER"]["envelope"]["payload"]["boards"]["data"][0][3] = "OTHER-BOARD"
        elif corruption == "underlying":
            artifacts["SRZ6"]["assignment"]["asset_id"] = "preferred-share"
        elif corruption == "wrong_contract":
            artifacts["SRZ6"]["assignment"]["contract_id"] = "OTHER-CONTRACT@OTHER-VENUE"
        else:
            artifacts["SRZ6"]["descriptor"]["underlying_security_id"] = "security-OTHER-SHARE"
            artifacts["SRZ6"]["assignment"]["source_descriptor"]["underlying_security_id"] = (
                "security-OTHER-SHARE"
            )

    inputs, _ = make_source(tmp_path, monkeypatch, change=change)
    with pytest.raises(SchemaValidationError):
        source.admit_native_snapshot(**inputs)


@pytest.mark.parametrize(
    "corruption", ["context_id", "config_source", "unversioned_table", "table_root", "receipt_root"]
)
def test_constructor_rejects_rehashed_but_incoherent_snapshot_references(
    tmp_path, monkeypatch, corruption
):
    inputs, _ = make_source(tmp_path, monkeypatch)
    key = "invocation_path" if corruption == "receipt_root" else "report_path"
    data = json.loads(inputs[key].read_text())
    if corruption == "context_id":
        data["contexts"][0]["dataset_context_id"] = "CTX:other-context"
    elif corruption == "config_source":
        context = data["contexts"][0]
        config = json.loads(context["context_json"])
        config["source_manifest_sha256"] = "f" * 64
        context["context_json"] = json.dumps(config)
        context["dataset_context_id"] = (
            "CTX:" + hashlib.sha256(context["context_json"].encode()).hexdigest()
        )
    elif corruption == "unversioned_table":
        data["tables"]["analytical_bars"]["version"] = None
    elif corruption == "table_root":
        data["tables"]["analytical_bars"]["path"] = "/other-run/data/bars.delta"
    else:
        data["writable_run"] = str(tmp_path / "other-run")
    raw = json.dumps(data).encode()
    inputs[key].write_bytes(raw)
    pin = "invocation_sha256" if key == "invocation_path" else "report_sha256"
    inputs["expected"][pin] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(SchemaValidationError):
        source.admit_native_snapshot(**inputs)
