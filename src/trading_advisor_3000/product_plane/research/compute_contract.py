"""Persisted Research compatibility authority; application provenance is separate."""

from __future__ import annotations

import hashlib
import platform
import re
from contextvars import ContextVar
from dataclasses import asdict
from functools import lru_cache, wraps
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from .semantic_fingerprint import ContractError, SymbolGraph, digest, engine_identity

VERSION = "research-compute.v1"
_BASE = "trading_advisor_3000."
_RESEARCH = _BASE + "product_plane.research."
_SPARK = _BASE + "spark_jobs."
_ROOTS = {
    "cf": tuple(
        _SPARK + "continuous_front_job." + name
        for name in ("_build_spark_native_tables", "_cast_dataframe_to_contract")
    ),
    "l0": tuple(
        _SPARK + "research_bar_views_job." + name
        for name in (
            "_native_bar_views",
            "_pit_active_front_bar_views",
            "_with_l0_metrics",
            "_with_bar_usage_contract",
            "_with_execution_economics",
            "_instrument_tree_from_bar_views",
            "_cast_to_contract",
        )
    ),
    "l1": tuple(
        _RESEARCH + "indicators.materialize." + name
        for name in (
            "_compute_partition_frame",
            "_compute_profile_frame_for_series",
            "_compute_cached_profile_frame",
            "_source_rows_frame",
            "_load_volume_profile_source_rows",
            "_indicator_value",
            "_null_warmup_span",
            "_apply_volume_profile_columns",
            "_merge_indicator_values",
        )
    )
    + (_RESEARCH + "datasets.views.ResearchBarView",),
    "l2": tuple(
        _RESEARCH + "derived_indicators.materialize." + name
        for name in (
            "_prepare_base_frame",
            "_compute_derived_frame",
            "_derived_value",
            "_source_frame_arrow_to_inputs",
            "_source_indicator_row",
            "_merge_derived_values",
            "_derived_output_row",
        )
    )
    + tuple(
        _SPARK + "research_derived_source_frames_job." + name
        for name in (
            "_join_source_frames",
            "_select_source_frame",
            "_cast_to_source_contract",
            "_filter_to_refresh_windows",
        )
    ),
    "sidecar": tuple(
        _SPARK + "continuous_front_indicator_sidecar_job." + name
        for name in (
            "_build_input_frame",
            "_build_base_sidecar_frame",
            "_build_derived_sidecar_frame",
            "_cast_to_sidecar_contract",
            "_derived_sidecar_scope_keys",
            "_filter_to_sidecar_key_scope",
        )
    ),
    "proof": tuple(
        _SPARK + "continuous_front_indicator_sidecar_job." + name
        for name in (
            "_input_row_hash_from_payload",
            "_sidecar_row_hash_from_payload",
            "_normalize_hash_value",
            "_normalize_numeric_hash_value",
            "ROW_HASH_VERSION",
        )
    ),
    "quality": (_SPARK + "continuous_front_job._build_native_qc",)
    + tuple(
        _RESEARCH + "continuous_front_indicators.pandas_job." + name
        for name in (
            "_verify_required_qc_groups",
            "_verify_runtime_evidence",
            "_verify_input_projection_identity_delta",
            "_verify_materialized_prefix_window_coverage_delta",
            "_verify_formula_samples_delta",
            "_verify_pandas_ta_parity_delta",
            "_verify_lineage_delta",
        )
    )
    + (_RESEARCH + "continuous_front_indicators.publish.publish_status_from_qc",),
}

_ROOTS["proof"] += (
    tuple(
        _RESEARCH + "indicators.materialize." + name
        for name in (
            "_bars_hash",
            "_indicator_input_row_hashes",
            "_volume_profile_source_hash",
            "_refresh_context_hash",
            "_native_indicator_source_hash",
        )
    )
    + tuple(
        _RESEARCH + "derived_indicators.materialize." + name
        for name in (
            "_source_inputs_hash",
            "_fast_source_hashes",
            "_derived_source_snapshot_hash",
        )
    )
    + (_RESEARCH + "derived_indicators.fingerprints.columnar_fingerprint",)
)

# Exact stable native APIs only. Filters/casts/selection stay in the symbol closure.
_LEAVES = (
    tuple(
        _BASE + "product_plane.data_plane.delta_runtime." + name
        for name in (
            "read_delta_table_rows",
            "read_delta_table_arrow",
            "iter_delta_table_row_batches",
            "delta_table_columns",
            "delta_table_identity",
            "delta_table_version",
            "count_delta_table_rows",
            "has_delta_log",
        )
    )
    + (
        _BASE + "product_plane.common.data_snapshots.read_spark_delta",
        _SPARK + "continuous_front_job._policy_timestamp",
        _SPARK + "continuous_front_job._build_native_qc",
        _SPARK + "research_bar_views_job._utc_now_iso",
        _RESEARCH + "continuous_front_indicators.qc.utc_now_iso",
        _RESEARCH + "continuous_front_indicators.input_projection._utc_now_iso",
        _RESEARCH + "continuous_front_indicators.rules._utc_now_iso",
        _RESEARCH + "continuous_front.continuous_front_store_contract",
        _RESEARCH + "datasets.store.research_dataset_store_contract",
        _RESEARCH
        + "continuous_front_indicators.contracts.continuous_front_indicator_store_contract",
        _RESEARCH + "derived_indicators.source_frames.research_derived_source_frame_store_contract",
        _RESEARCH + "indicators.store.indicator_store_contract",
        _RESEARCH + "derived_indicators.store.research_derived_indicator_store_contract",
        _RESEARCH + "indicators.registry.build_indicator_profile_registry",
        _RESEARCH + "indicators.registry.default_indicator_profile",
        _RESEARCH + "derived_indicators.registry.build_derived_indicator_profile_registry",
        _RESEARCH + "derived_indicators.registry.current_derived_indicator_profile",
        _RESEARCH + "indicators.materialize._indicator_compute_contract_hash",
    )
    + _ROOTS["proof"]
)


def kernel_contract(*, package: Path | None = None) -> dict[str, str]:
    package = package or Path(__file__).resolve().parents[2]
    # Cache admission uses exact bytes of ALL current Python sources. This is
    # only an invalidation key, never a persisted numerical/release identity.
    signature = tuple(
        (path.relative_to(package).as_posix(), hashlib.sha256(path.read_bytes()).hexdigest())
        for path in sorted(package.rglob("*.py"))
    )
    return dict(
        _cached_kernels(str(package), signature, digest({"roots": _ROOTS, "leaves": _LEAVES}))
    )


@lru_cache(maxsize=8)
def _cached_kernels(package: str, source_signature: tuple, boundaries: str) -> tuple:
    return tuple(
        {
            name: SymbolGraph(
                Path(package), tuple(item for item in _LEAVES if item not in roots)
            ).fingerprint(roots)
            for name, roots in _ROOTS.items()
        }.items()
    )


def numeric_dependencies() -> dict:
    import pandas_ta_classic as ta

    dependencies = {"python": platform.python_version()}
    for name in ("numpy", "pandas", "pandas-ta-classic", "pyspark", "pyarrow"):
        try:
            dependencies[name] = version(name)
        except PackageNotFoundError:
            dependencies[name] = None
    dependencies["talib_available"] = bool(ta.Imports.get("talib"))
    dependencies["talib_version"] = version("TA-Lib") if dependencies["talib_available"] else None
    return dependencies


def _profile_payload(profile):
    payload = asdict(profile)
    payload.pop("description", None)
    return payload


def semantic_compute_contract(configuration=None, *, package: Path | None = None) -> dict:
    from .continuous_front import continuous_front_store_contract
    from .continuous_front_indicators.contracts import continuous_front_indicator_store_contract
    from .datasets import research_dataset_store_contract
    from .derived_indicators.registry import (
        build_derived_indicator_profile_registry,
        current_derived_indicator_profile,
    )
    from .derived_indicators.source_frames import research_derived_source_frame_store_contract
    from .derived_indicators.store import research_derived_indicator_store_contract
    from .indicators.registry import build_indicator_profile_registry, default_indicator_profile
    from .indicators.store import indicator_store_contract

    configuration = configuration or {}
    l1 = (
        build_indicator_profile_registry().get(configuration["indicator_profile_version"])
        if configuration.get("indicator_profile_version")
        else default_indicator_profile()
    )
    l2 = (
        build_derived_indicator_profile_registry().get(
            configuration["derived_indicator_profile_version"]
        )
        if configuration.get("derived_indicator_profile_version")
        else current_derived_indicator_profile()
    )
    stores = [
        continuous_front_store_contract(),
        research_dataset_store_contract(),
        indicator_store_contract(l1),
        research_derived_indicator_store_contract(l2),
        continuous_front_indicator_store_contract(indicator_profile=l1, derived_profile=l2),
        research_derived_source_frame_store_contract(
            source_indicator_columns=l1.expected_output_columns()
        ),
    ]
    columns = {name: table["columns"] for store in stores for name, table in store.items()}
    kernels = kernel_contract(package=package)
    contract = {
        "version": VERSION,
        "engine": engine_identity(),
        "kernels": {
            key: value for key, value in kernels.items() if key not in {"quality", "proof"}
        },
        "quality": kernels["quality"],
        "proof": kernels["proof"],
        "profiles": {"l1": _profile_payload(l1), "l2": _profile_payload(l2)},
        "schemas": columns,
        "numeric_dependencies": numeric_dependencies(),
    }
    # Normalize tuples and other producer values to the persisted JSON representation.
    import json

    contract = json.loads(json.dumps(contract, allow_nan=False))
    return {**contract, "digest": digest(contract)}


def validate_contract(contract: object) -> dict:
    expected = {
        "version",
        "engine",
        "kernels",
        "quality",
        "proof",
        "profiles",
        "schemas",
        "numeric_dependencies",
        "digest",
    }
    if (
        not isinstance(contract, dict)
        or set(contract) != expected
        or contract["version"] != VERSION
    ):
        raise ContractError("unsupported Research compute contract")
    if (
        not isinstance(contract["kernels"], dict)
        or set(contract["kernels"]) != set(_ROOTS) - {"quality", "proof"}
        or contract["digest"]
        != digest({key: value for key, value in contract.items() if key != "digest"})
    ):
        raise ContractError("invalid Research compute contract digest or components")
    identities = [contract[key] for key in ("engine", "quality", "proof", "digest")] + list(
        contract["kernels"].values()
    )
    if any(
        not isinstance(value, str) or not re.fullmatch("[0-9a-f]{64}", value)
        for value in identities
    ):
        raise ContractError("invalid Research compute fingerprint")
    if (
        not isinstance(contract["profiles"], dict)
        or set(contract["profiles"]) != {"l1", "l2"}
        or any(not isinstance(value, dict) for value in contract["profiles"].values())
    ):
        raise ContractError("invalid active Research profiles")
    if not isinstance(contract["schemas"], dict) or any(
        not isinstance(value, dict) for value in contract["schemas"].values()
    ):
        raise ContractError("invalid Research schema descriptors")
    if not isinstance(contract["numeric_dependencies"], dict) or not {
        "python",
        "numpy",
        "pandas",
        "pandas-ta-classic",
        "pyspark",
        "pyarrow",
        "talib_available",
        "talib_version",
    } == set(contract["numeric_dependencies"]):
        raise ContractError("invalid numeric dependency descriptors")
    return contract


def compare_contracts(old: object, current: dict) -> bool:
    old, current = validate_contract(old), validate_contract(current)
    if old["engine"] != current["engine"]:
        raise ContractError("Research fingerprint authority changed; supported migration required")
    if any(old[key] != current[key] for key in ("kernels", "profiles", "numeric_dependencies")):
        return False
    if old["quality"] != current["quality"]:
        raise ContractError(
            "Research quality rules changed; current pinned-output revalidation required"
        )
    if old["schemas"] != current["schemas"]:
        raise ContractError("Research schema or proof codec changed; native adaptation required")
    if old["proof"] != current["proof"]:
        raise ContractError("Research proof codec changed; native adaptation required")
    return True


# One frozen identity per native operation, never a process-wide release pin.
# Entry/exit source checks bound the cache to the operation and catch drift.

_active_numeric_identity: ContextVar[dict | None] = ContextVar(
    "research_numeric_identity", default=None
)


def _current_numeric_identity() -> dict:
    return {
        "engine": engine_identity(),
        "kernels": kernel_contract(),
        "dependencies": numeric_dependencies(),
    }


def numerical_identity() -> dict:
    identity = _active_numeric_identity.get() or _current_numeric_identity()
    return {
        "engine": identity["engine"],
        "kernels": dict(identity["kernels"]),
        "dependencies": dict(identity["dependencies"]),
    }


def native_compute_scope(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        if _active_numeric_identity.get() is not None:
            return function(*args, **kwargs)
        identity = _current_numeric_identity()
        token = _active_numeric_identity.set(identity)
        try:
            result = function(*args, **kwargs)
        finally:
            _active_numeric_identity.reset(token)
        if _current_numeric_identity() != identity:
            raise ContractError(
                "Research numerical code/dependencies changed during native operation"
            )
        return result

    return wrapped
