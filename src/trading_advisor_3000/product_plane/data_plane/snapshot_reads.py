"""Public Market Data pins, native predicates and bounded snapshot bar reads."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds

from trading_advisor_3000.product_plane.common.data_snapshots import (
    SnapshotUnavailableError,
    open_pinned_table,
)
from trading_advisor_3000.product_plane.common.snapshot_read_cache import SnapshotReadCache
from trading_advisor_3000.product_plane.contracts.identity_v3 import identity_input_fingerprint
from trading_advisor_3000.product_plane.contracts.snapshot_reads import (
    ArrowReadResult,
    NativeReadRequest,
    require,
)

from .delta_runtime import delta_arrow_dataset
from .identity import SnapshotIdentitySelector, resolve_snapshot_identity
from .moex.snapshot_identity import admit_native_snapshot


@dataclass(frozen=True)
class NativeReadSource:
    dataset: ds.Dataset
    binding: dict[str, Any]
    metadata: dict[str, Any]
    context_id: str


class PinnedNativeSnapshot:
    """Immutable supplied pin bundle; source guards always precede cache lookup."""

    def __init__(self, raw_bundle: bytes, *, expected_sha256: str):
        require(
            hashlib.sha256(raw_bundle).hexdigest() == expected_sha256, "read pin bundle changed"
        )
        bundle = json.loads(raw_bundle)
        self._bundle_json = json.dumps(bundle, sort_keys=True, ensure_ascii=False, allow_nan=False)
        self._bundle_sha256 = expected_sha256
        inputs = copy.deepcopy(bundle["admission_inputs"])
        for field in ("source_root", "report_path", "assignments_path", "invocation_path"):
            inputs[field] = Path(inputs[field])
        admission = admit_native_snapshot(**inputs)
        self._admission_json = json.dumps(
            admission, sort_keys=True, ensure_ascii=False, allow_nan=False
        )

        def pinned_text(path: Path, expected: str) -> str:
            content = path.read_bytes()
            require(
                hashlib.sha256(content).hexdigest() == expected, "captured source bytes changed"
            )
            return content.decode("utf-8")

        self._report_json = pinned_text(inputs["report_path"], inputs["expected"]["report_sha256"])
        self._invocation_json = pinned_text(
            inputs["invocation_path"], inputs["expected"]["invocation_sha256"]
        )
        manifest_path = inputs["source_root"] / "manifest.json"
        manifest = json.loads(
            pinned_text(manifest_path, inputs["expected"]["source_manifest_sha256"])
        )
        self._targets = tuple(manifest["target_series_ids"])
        self._originals = {
            inputs[field]: inputs["expected"][pin]
            for field, pin in (
                ("report_path", "report_sha256"),
                ("assignments_path", "assignments_sha256"),
                ("invocation_path", "invocation_sha256"),
            )
        }
        self._originals[manifest_path] = inputs["expected"]["source_manifest_sha256"]
        for relative in [manifest["descriptor_path"], *inputs["metadata_requests"].values()]:
            self._originals[inputs["source_root"] / relative] = manifest["source_hashes"][relative]
        self.verify()

    @classmethod
    def from_file(cls, path: Path, *, expected_sha256: str) -> PinnedNativeSnapshot:
        return cls(path.read_bytes(), expected_sha256=expected_sha256)

    @property
    def bundle_sha256(self) -> str:
        return self._bundle_sha256

    @property
    def namespace(self) -> dict[str, Any]:
        return json.loads(self._admission_json)["namespace"]

    @property
    def bindings(self) -> list[dict[str, Any]]:
        return json.loads(self._admission_json)["bindings"]

    def verify(self) -> None:
        for path, expected in self._originals.items():
            try:
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError as exc:
                raise SnapshotUnavailableError(
                    f"original snapshot input unreadable: {path.name}"
                ) from exc
            require(digest == expected, f"original snapshot input changed: {path.name}")

    def resolve(self, request: NativeReadRequest) -> tuple[dict[str, Any], dict[str, Any]]:
        self.verify()
        query = NativeReadRequest.from_payload(request.to_payload(), self.namespace).to_payload()
        binding = resolve_snapshot_identity(
            SnapshotIdentitySelector.from_payload(query["identity"], self.namespace),
            namespace=self.namespace,
            bindings=self.bindings,
        )
        require(
            binding["source_descriptor"]["legacy_series_id"] in self._targets,
            "native descriptor is outside accepted output scope",
        )
        return query, binding

    def source(
        self, query: dict[str, Any], binding: dict[str, Any], *, table_name: str
    ) -> NativeReadSource | None:
        report, invocation = json.loads(self._report_json), json.loads(self._invocation_json)
        if table_name not in report["tables"]:
            return None
        bundle = json.loads(self._bundle_json)
        require(table_name in bundle["tables"], "existing output has no explicit read pins")
        read = bundle["tables"][table_name]
        reference, original = read["reference"], report["tables"][table_name]
        require(
            type(reference["delta_version"]) is int and type(original["version"]) is int,
            "exact integer table version required",
        )
        logical = PurePosixPath(invocation["logical_container_run"])
        relative = PurePosixPath(original["path"]).relative_to(logical)
        root = Path(invocation["writable_run"]).resolve()
        physical = (root / Path(*relative.parts)).resolve()
        require(physical.is_relative_to(root), "read table escapes snapshot root")
        require(
            Path(reference["table_path"]).resolve() == physical
            and reference["delta_version"] == original["version"],
            "read reference/report mismatch",
        )
        instrument = next(
            i
            for i in self.namespace["instruments"]
            if i["instrument_id"] == query["identity"]["instrument_id"]
        )
        label = (
            "source"
            if query["layer"] == "bars"
            else {"equity": "equity", "reference_index": "index", "future": "futures"}[
                instrument["kind"]
            ]
        )
        contexts = [c for c in report["contexts"] if c["label"] == label]
        require(len(contexts) == 1, "absent/ambiguous layer configuration")
        context = contexts[0]
        config_hash = hashlib.sha256(context["context_json"].encode("utf-8")).hexdigest()
        config = json.loads(context["context_json"])
        require(
            context["dataset_context_id"] == "CTX:" + config_hash
            and config["label"] == label
            and config["source_manifest_sha256"]
            == binding["selector"]["pins"]["source_manifest_sha256"]
            and config["mapping_version"] == binding["selector"]["pins"]["assignment_version"]
            and config["registry_hash"] == report["registry_hash"],
            "layer configuration mismatch",
        )
        parameters = config["calculation_parameters"]
        require(query["timeframe"] in parameters["timeframes"], "timeframe outside accepted output")
        if query["layer"] != "bars":
            require(
                binding["source_descriptor"]["legacy_series_id"] in parameters["targets"],
                "descriptor outside Research configuration",
            )
            require(
                query["research"] == bundle["selections"][label][query["layer"]],
                "Research selection is not pinned",
            )
            profiles = parameters["profiles"]
            if query["layer"] != "L0":
                require(
                    query["research"]["profile_version"]
                    == profiles[
                        "derived_profile_version"
                        if query["layer"] == "L2"
                        else "indicator_profile_version"
                    ],
                    "profile/context mismatch",
                )
            if query["layer"] == "L2":
                require(
                    query["research"]["source_indicator_profile_version"]
                    == profiles["indicator_profile_version"],
                    "source indicator profile mismatch",
                )
        table = open_pinned_table(reference)
        dataset = table.to_pyarrow_dataset()
        require(
            hashlib.sha256(dataset.schema.serialize().to_pybytes()).hexdigest()
            == read["arrow_schema_sha256"],
            "source Arrow schema changed",
        )
        columns = [
            {"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in dataset.schema
        ]
        require(columns == original["columns"], "source schema/report mismatch")
        constants = _native_constants(
            binding,
            query,
            context["dataset_context_id"],
            bars=query["layer"] == "bars",
            schema_names=dataset.schema.names,
        )
        timestamp = "bar_start_ts" if query["layer"] == "bars" else "ts"
        require(
            set(constants) | {timestamp} <= set(dataset.schema.names),
            "source predicate columns missing",
        )
        dataset = delta_arrow_dataset(table, predicate_columns=[*constants, timestamp])
        recipes = {
            "carrier": "snapshot_read_projection.v1",
            "source_quality": parameters["quality"],
        }
        if label == "source":
            recipes["normalization"] = parameters["normalization"]
        else:
            recipes.update(
                {
                    key: value
                    for key, value in parameters["profiles"].items()
                    if key.endswith("_version")
                }
            )
        metadata = {
            "original_reference": original,
            "read_pin": {
                "table_id": reference["table_id"],
                "delta_version": reference["delta_version"],
                "arrow_schema_sha256": read["arrow_schema_sha256"],
                "source_schema_ipc_base64": base64.b64encode(
                    dataset.schema.serialize().to_pybytes()
                ).decode("ascii"),
                "reference_sha256": identity_input_fingerprint(reference),
            },
            "configuration": {
                "dataset_context_id": context["dataset_context_id"],
                "sha256": config_hash,
                "original_json": context["context_json"],
            },
            "recipes": [
                {"name": name, "version": value} for name, value in sorted(recipes.items())
            ],
            "report_sha256": binding["selector"]["pins"]["report_sha256"],
            "manifest_sha256": binding["selector"]["pins"]["source_manifest_sha256"],
            "mapping_version": binding["selector"]["pins"]["assignment_version"],
            "namespace_sha256": binding["selector"]["pins"]["namespace_sha256"],
            "pin_bundle_sha256": self.bundle_sha256,
        }
        return NativeReadSource(dataset, binding, metadata, context["dataset_context_id"])


def native_predicate(
    source: NativeReadSource, query: dict[str, Any], *, timestamp: str, bars: bool
) -> ds.Expression:
    """Exact legacy boundary predicates; active contracts never identify native bars."""
    constants = _native_constants(
        source.binding,
        query,
        source.context_id,
        bars=bars,
        schema_names=source.dataset.schema.names,
    )
    require(
        set(constants) | {timestamp} <= set(source.dataset.schema.names),
        "source predicate columns missing",
    )
    predicate = ds.scalar(True)
    for column, value in constants.items():
        predicate &= ds.field(column).is_null() if value is None else ds.field(column) == value
    time_type = source.dataset.schema.field(timestamp).type
    require(
        (pa.types.is_timestamp(time_type) and time_type.tz == "UTC")
        or pa.types.is_string(time_type),
        "source timestamp schema must be UTC or explicit UTC strings",
    )
    time = ds.field(timestamp).cast(pa.timestamp("ns", tz="UTC"))
    for key, operation in (("start", "ge"), ("end", "lt")):
        instant = pa.scalar(datetime.fromisoformat(query[key].replace("Z", "+00:00"))).cast(
            pa.timestamp("ns", tz="UTC")
        )
        predicate &= time >= instant if operation == "ge" else time < instant
    return predicate


def _native_constants(binding, query, context_id, *, bars, schema_names):
    descriptor = binding["source_descriptor"]
    constants = {
        "legacy_series_id": descriptor["legacy_series_id"],
        "series_type": "native",
        "contract_id": descriptor["legacy_contract_id"],
        "cf_scope_key": None,
        "dataset_context_id": context_id,
        "timeframe": query["timeframe"],
    }
    if bars:
        constants["security_id"] = descriptor["security_id"]
    else:
        constants.update({k: v for k, v in query["research"].items() if v is not None})
        if "price_space" in schema_names:
            constants["price_space"] = query["identity"]["price_basis"]
    return constants


def read_native_bars(
    snapshot: PinnedNativeSnapshot,
    request: NativeReadRequest,
    *,
    cache: SnapshotReadCache | None = None,
) -> ArrowReadResult:
    from trading_advisor_3000.product_plane.common.snapshot_projection import read_projection

    query, binding = snapshot.resolve(request)
    require(query["layer"] == "bars", "Market Data reader accepts bars only")
    source = snapshot.source(query, binding, table_name="analytical_bars")
    return read_projection(
        query,
        binding=binding,
        source=source,
        namespace=snapshot.namespace,
        predicate=None
        if source is None
        else native_predicate(source, query, timestamp="bar_start_ts", bars=True),
        cache=cache,
    )
