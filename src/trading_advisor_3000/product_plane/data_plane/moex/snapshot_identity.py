"""Read-only admission of existing MOEX pilot metadata, never temporal mappings.

The supplied expected hashes identify reviewed inputs. This constructor verifies
their bytes and associations; pure resolution alone does not verify storage.
No market rows, output tables, pointers or source files are written here.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from deltalake import DeltaTable

from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    identity_input_fingerprint,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SchemaValidationError(message)


def _pinned_json(path: Path, expected: str) -> dict[str, Any]:
    raw = path.read_bytes()
    _require(hashlib.sha256(raw).hexdigest() == expected, f"source hash mismatch: {path}")
    result = json.loads(raw)
    _require(isinstance(result, dict), f"expected JSON object: {path}")
    return result


def _child(root: Path, relative: str) -> Path:
    result = (root / relative).resolve()
    _require(result.is_relative_to(root.resolve()), "source reference escapes its root")
    return result


def _one(rows: list[dict[str, Any]], **matches: Any) -> dict[str, Any]:
    selected = [row for row in rows if all(row.get(k) == v for k, v in matches.items())]
    _require(len(selected) == 1, f"absent or ambiguous source association: {matches}")
    return selected[0]


def admit_native_snapshot(
    *,
    source_root: Path,
    report_path: Path,
    assignments_path: Path,
    invocation_path: Path,
    expected: Mapping[str, str],
    namespace_name: str,
    metadata_requests: Mapping[str, str],
) -> dict[str, Any]:
    """Verify pinned producer metadata and admit only explicitly requested IDs.

    ``expected`` requires source_manifest/report/assignments/invocation_sha256
    and table_id. Metadata request paths are explicit manifest-relative inputs,
    keyed by existing concrete pilot instrument IDs. No symbol/ID generation.
    The result is supplied metadata for the public snapshot resolver, not a
    historical mapping, CF specification, execution approval or row read.
    """
    _require(
        set(expected)
        == {
            "source_manifest_sha256",
            "report_sha256",
            "assignments_sha256",
            "invocation_sha256",
            "table_id",
        },
        "complete explicit source pins required",
    )
    _require(bool(metadata_requests), "at least one concrete instrument required")
    manifest = _pinned_json(source_root / "manifest.json", expected["source_manifest_sha256"])
    report = _pinned_json(report_path, expected["report_sha256"])
    assignments = _pinned_json(assignments_path, expected["assignments_sha256"])
    invocation = _pinned_json(invocation_path, expected["invocation_sha256"])
    _require(report["role"] == "verification_only", "this admission route is verification-only")
    _require(
        report["status"] in {"PASS", "PASS_WITH_KNOWN_SOURCE_GAPS"},
        "snapshot report is not accepted",
    )
    run_root = report_path.resolve().parent.parent
    _require(Path(invocation["writable_run"]).resolve() == run_root, "report/root mismatch")
    _require(
        Path(invocation["readonly_source"]).resolve() == source_root.resolve(),
        "source/root mismatch",
    )
    context = _one(report["contexts"], label="source")
    config_hash = hashlib.sha256(context["context_json"].encode("utf-8")).hexdigest()
    _require(context["dataset_context_id"] == "CTX:" + config_hash, "configuration ID mismatch")
    config = json.loads(context["context_json"])
    _require(
        config["source_manifest_sha256"] == expected["source_manifest_sha256"]
        and config["registry_hash"] == assignments["registry_hash"] == report["registry_hash"]
        and config["mapping_version"] == assignments["mapping_version"],
        "report/config/source/assignment mismatch",
    )
    reference = report["tables"]["analytical_bars"]
    _require(type(reference["version"]) is int and reference["version"] >= 0, "unpinned table")
    logical = PurePosixPath(invocation["logical_container_run"])
    try:
        relative_table = PurePosixPath(reference["path"]).relative_to(logical)
    except ValueError as exc:
        raise SchemaValidationError("table is outside pinned invocation") from exc
    physical = _child(run_root, relative_table.as_posix())
    table = DeltaTable(str(physical), version=reference["version"])
    _require(
        table.metadata().id == expected["table_id"] and table.version() == reference["version"],
        "physical table identity/version mismatch",
    )
    descriptor_path = _child(source_root, manifest["descriptor_path"])
    descriptor_bytes = descriptor_path.read_bytes()
    _require(
        hashlib.sha256(descriptor_bytes).hexdigest()
        == manifest["source_hashes"][manifest["descriptor_path"]],
        "descriptor hash mismatch",
    )
    descriptors = [json.loads(line) for line in descriptor_bytes.decode("utf-8").splitlines()]
    selected = []
    for instrument_id, request_path in metadata_requests.items():
        assignment = _one(assignments["instruments"], instrument_id=instrument_id)
        descriptor = _one(descriptors, series_id=assignment["source_series_id"])
        _require(
            descriptor == assignment["source_descriptor"]
            and assignment["source_security_id"] == descriptor["security_id"]
            and assignment["ticker"] == descriptor["secid"],
            "descriptor/assignment association mismatch",
        )
        envelope = _pinned_json(
            _child(source_root, request_path), manifest["source_hashes"][request_path]
        )
        payload_bytes = (
            json.dumps(envelope["payload"], sort_keys=True, ensure_ascii=False, allow_nan=False)
            + "\n"
        ).encode("utf-8")
        _require(
            hashlib.sha256(payload_bytes).hexdigest() == envelope["payload_sha256"],
            "metadata payload hash mismatch",
        )
        description = envelope["payload"]["description"]
        fields = {
            row[description["columns"].index("name")]: row[description["columns"].index("value")]
            for row in description["data"]
        }
        _require(len(fields) == len(description["data"]), "ambiguous metadata fields")
        _require(
            fields["SECID"] == descriptor["secid"]
            and descriptor["source"] == "moex_iss"
            and descriptor["price_basis"] == "source_native"
            and assignment["exchange"] == "MOEX"
            and descriptor["metadata_asof"] == envelope["fetched_at_utc"],
            "metadata source/observation mismatch",
        )
        boards = envelope["payload"]["boards"]
        board = _one(
            [dict(zip(boards["columns"], row, strict=True)) for row in boards["data"]],
            secid=descriptor["secid"],
            engine=descriptor["engine"],
            market=descriptor["market"],
            boardid=descriptor["board"],
        )
        _require(
            type(board["is_traded"]) is int and board["is_traded"] in {0, 1},
            "invalid source tradability observation",
        )
        kind = {"common_share": "equity", "stock_index": "reference_index", "futures": "future"}
        _require(fields["TYPE"] in kind, "unsupported native source type")
        _require(
            assignment["asset_class"]
            == descriptor["asset_class"]
            == {"equity": "equity", "reference_index": "index", "future": "futures"}[
                kind[fields["TYPE"]]
            ],
            "source/assignment class mismatch",
        )
        _require(
            assignment["instrument_role"]
            == ("derivative" if fields["TYPE"] == "futures" else "base"),
            "source/assignment role mismatch",
        )
        _require(
            # Existing pilot producer convention, identity_registry.py:213.
            # This checks a legacy field; canonical IDs are never generated here.
            assignment["contract_id"] == descriptor["secid"] + "@MOEX"
            if fields["TYPE"] == "futures"
            else assignment["contract_id"] is None,
            "native contract/source association mismatch",
        )
        selected.append((assignment, descriptor, envelope, fields, board, request_path))
    assets: dict[str, dict[str, Any]] = {}
    instruments = []
    for assignment, descriptor, envelope, fields, board, request_path in selected:
        kind = {"common_share": "equity", "stock_index": "reference_index", "futures": "future"}[
            fields["TYPE"]
        ]
        if kind == "future":
            underlying = [
                item
                for item in selected
                if item[0]["asset_id"] == assignment["asset_id"]
                and item[1]["security_id"] == descriptor.get("underlying_security_id")
                and item[3]["TYPE"] == "common_share"
                and item[3].get("EMITTER_ID") == fields.get("EMITTER_ID")
                and fields.get("EMITTER_ID") is not None
            ]
            _require(
                len(underlying) == 1 and bool(fields.get("ASSETCODE")),
                "unconfirmed reviewed ordinary-share underlying",
            )
        else:
            asset = {"asset_id": assignment["asset_id"], "kind": kind}
            _require(assets.get(assignment["asset_id"], asset) == asset, "conflicting asset kinds")
            assets[assignment["asset_id"]] = asset
        instruments.append(
            {
                "instrument_id": assignment["instrument_id"],
                "asset_id": assignment["asset_id"],
                "kind": kind,
                "instrument_role": "derivative" if kind == "future" else "base",
                "venue": "MOEX",
                "market": descriptor["market"],
                "board": descriptor["board"],
                "tradable": False if kind == "reference_index" else board["is_traded"] == 1,
                "execution_eligible": False,
                "economics_state": "not_applicable" if kind == "reference_index" else "unknown",
                "size_variant": {
                    "state": "unknown" if kind == "future" else "not_applicable",
                    "value": None,
                },
                "specification": None,
                "expiry": None,
            }
        )
    namespace = {
        "schema_version": 1,
        "namespace": namespace_name,
        "assets": list(assets.values()),
        "instruments": instruments,
    }
    validate_identity_contract("instrument_namespace.v1.json", namespace, namespace)
    pins = {
        **{
            key: expected[key]
            for key in ("source_manifest_sha256", "report_sha256", "assignments_sha256")
        },
        "configuration_sha256": config_hash,
        "namespace_sha256": identity_input_fingerprint(namespace),
        "assignment_version": assignments["mapping_version"],
        "dataset_context_id": context["dataset_context_id"],
        "table": {
            "source_path": reference["path"],
            "table_id": expected["table_id"],
            "delta_version": reference["version"],
        },
    }
    bindings = []
    for assignment, descriptor, envelope, fields, board, request_path in selected:
        binding = {
            "schema_version": 1,
            "selector": {
                "schema_version": 1,
                "applicability": "snapshot_bound",
                "namespace": namespace_name,
                "instrument_id": assignment["instrument_id"],
                "source_context": {
                    "provider": descriptor["source"],
                    "venue": "MOEX",
                    "market": descriptor["market"],
                    "board": descriptor["board"],
                },
                "source_engine": descriptor["engine"],
                "price_basis": descriptor["price_basis"],
                "pins": pins,
            },
            "asset_id": assignment["asset_id"],
            "source_symbol": descriptor["secid"],
            "source_descriptor": {
                "provider": descriptor["source"],
                "venue": "MOEX",
                **{
                    key: descriptor[key]
                    for key in ("engine", "market", "board", "secid", "security_id", "price_basis")
                },
                "legacy_series_id": descriptor["series_id"],
                "legacy_contract_id": assignment["contract_id"],
            },
            "source_reference": request_path,
            "source_hash": manifest["source_hashes"][request_path],
            "observed_at": envelope["fetched_at_utc"],
            "historical_mapping_verified": False,
        }
        validate_identity_contract("snapshot_identity_binding.v1.json", binding, namespace)
        bindings.append(binding)
    return {
        "namespace": namespace,
        "bindings": bindings,
        "evidence": {
            "input_pins": dict(expected),
            "physical_table": str(physical),
            "original_table_reference": reference,
            "source_fields": {item[0]["instrument_id"]: item[3] for item in selected},
        },
    }
