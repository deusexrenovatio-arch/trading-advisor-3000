"""Core projection shapes preserve old definitions and complete catalog identity."""

import copy
import hashlib
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.identity_v3 import (
    SCHEMAS,
    validate_identity_contract,
)
from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
        ).encode()
    ).hexdigest()


def core_example(layer):
    from trading_advisor_3000.product_plane.research.native_core_policy import (
        core_policy,
        core_policy_ref,
    )

    source = json.loads((FIXTURES / "native_research_core_inputs.v1.json").read_bytes())["source"]
    source["layer"] = "L0" if layer == "L1" else "L1"
    source["table_name"] = "research_bar_views" if layer == "L1" else "research_indicator_frames"
    if layer == "L2":
        source["context_id"] = "NFCTX:" + "b" * 64
    namespace = source["namespace"]
    policy = core_policy(layer)
    authority = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"]
    quality = copy.deepcopy(
        authority["native_research_core_context"]["properties"]["quality"]["properties"]["policy"][
            "const"
        ]
    )
    numerical = {
        "version": "native_research_core_numerical.v1",
        "layer": layer,
        "basis": policy["source_basis"],
        "policy_sha256": digest(policy),
        "profile_sha256": policy["resolved_profile_sha256"],
        "library_source_sha256": policy["runtime_profile"]["ta_source_sha256"],
        "kernels": {"legacy": "a" * 64, "adapter": "b" * 64},
        "engines": {
            "python": "3.11.15",
            "numpy": "2.4.6",
            "pandas": "2.3.3",
            "pandas_ta_classic": "0.4.47",
            "spark": "3.5.8",
            "delta": "3.3.2",
            "arrow": "24.0.0",
            "talib_absent": True,
            "numba": "0.65.1",
            "scipy": "1.17.1",
            "ta_numba_enabled": True,
            "ta_scipy_enabled": True,
        },
    }
    numerical["digest"] = digest(numerical)
    intent = {
        "schema_version": 1,
        "contract_version": "native_research_core_intent.v1",
        "input_mode": source["input_mode"],
        "layer": layer,
        "profile_version": policy["profile_version"],
        "command_id": "synthetic-shape-only",
        "source_root": "/research-trial",
        "parent_completion": source["completion"],
        "policy_sha256": digest(policy),
        "numerical_identity": numerical,
        "code_sha256": "c" * 64,
    }
    capabilities = []
    for row in policy["catalog"]:
        entry = {
            "sequence": copy.deepcopy(source["sequences"][0]),
            "feature": row["output"],
            "materialized": row["materialized"],
            "analysis_rows": 1,
            "reason": "synthetic-shape-only; no source/compute proof",
        }
        if row["materialized"]:
            ready = row["admission_minimum"] <= 2
            entry.update(
                state="ready" if ready else "insufficient_history",
                required_observations=row["admission_minimum"],
                finite_count=1 if ready else 0,
                analysis_finite_count=1 if ready else 0,
                first_finite_bar_index=1 if ready else None,
                first_finite_source_begin="2026-09-15 09:01:00" if ready else None,
            )
        else:
            entry["state"] = (
                "unverified_policy" if row["required_source_facts"] else "not_applicable"
            )
        capabilities.append(entry)
    context = {
        "schema_version": 1,
        "contract_version": "native_research_core_context.v1",
        "input_mode": source["input_mode"],
        "layer": layer,
        "research_context_id": source["research_context_id"],
        "source": source,
        "policy": core_policy_ref(layer),
        "policy_sha256": digest(policy),
        "layout_sha256": "d" * 64,
        "numerical_identity": numerical,
        "runtime_code_sha256": intent["code_sha256"],
        "quality": {"policy": quality, "sha256": digest(quality)},
        "capabilities": capabilities,
    }
    context["feature_context_id"] = "NFCTX:" + digest(context)
    completion = {
        "schema_version": 1,
        "contract_version": "native_research_core_completion.v1",
        "status": "complete_unpublished",
        "layer": layer,
        "command_id": intent["command_id"],
        "input_sha256": digest(intent),
        "attempt_id": "f" * 32,
        "snapshot": source["snapshot"],
        "context": source["context"],
        "intent": source["context"],
        "source_root": "/research-trial",
        "parent_completion": source["completion"],
        "created_at_utc": "2026-11-10T00:00:00Z",
    }
    return namespace, {
        "policy": policy,
        "context": context,
        "intent": intent,
        "completion": completion,
    }


@pytest.mark.parametrize("layer,slots,retained", [("L1", 103, 79), ("L2", 94, 72)])
def test_closed_core_policy_catalog(layer, slots, retained):
    from trading_advisor_3000.product_plane.research.native_core_policy import core_policy

    policy = core_policy(layer)
    namespace = json.loads(
        (
            Path(__file__).parents[1] / "fixtures/contracts/native_research_compute.v1.json"
        ).read_bytes()
    )["namespace"]
    validate_identity_contract("native_research_core_policy.v1.json", policy, namespace)
    assert len(policy["catalog"]) == slots
    assert sum(row["materialized"] for row in policy["catalog"]) == retained
    assert len({row["output"] for row in policy["catalog"]}) == slots


def test_all_pre_core_shared_definitions_are_immutable():
    previous = json.loads(
        (FIXTURES / "native_research_pre_core_definition_hashes.v1.json").read_bytes()
    )["definitions"]
    actual = json.loads((SCHEMAS / "market_identity.v3.json").read_bytes())["$defs"]
    assert len(previous) == 58
    assert previous.keys() <= actual.keys()
    assert {name: digest(actual[name]) for name in previous} == previous


@pytest.mark.parametrize("layer", ["L1", "L2"])
@pytest.mark.parametrize("kind", ["policy", "context", "intent", "completion"])
def test_complete_synthetic_core_shapes(layer, kind):
    namespace, values = core_example(layer)
    validate_identity_contract(f"native_research_core_{kind}.v1.json", values[kind], namespace)


@pytest.mark.parametrize(
    "mutation",
    [
        "parent",
        "policy",
        "extra",
        "count",
        "ready_without_analysis",
        "null_first",
        "unavailable_stats",
        "backend",
        "basis",
        "ns",
        "source_sequences",
    ],
)
def test_core_context_rejects_shape_claim_drift(mutation):
    namespace, values = core_example("L1")
    context = values["context"]
    if mutation == "parent":
        context["source"]["layer"] = "L1"
    elif mutation == "policy":
        context["policy"]["profile_version"] = "core_v1"
    elif mutation == "extra":
        context["price"] = 123.0
    elif mutation == "count":
        context["capabilities"][0]["finite_count"] = 3
    elif mutation == "ready_without_analysis":
        entry = next(c for c in context["capabilities"] if c["state"] == "ready")
        entry["analysis_finite_count"] = 0
    elif mutation == "null_first":
        entry = next(c for c in context["capabilities"] if c["state"] == "insufficient_history")
        entry["first_finite_bar_index"] = 0
    elif mutation == "unavailable_stats":
        entry = next(c for c in context["capabilities"] if not c["materialized"])
        entry["finite_count"] = 0
    elif mutation == "backend":
        context["numerical_identity"]["engines"]["talib_absent"] = False
    elif mutation == "basis":
        context["numerical_identity"]["basis"] = "canonical"
    elif mutation == "ns":
        context["capabilities"][0]["sequence"]["asset_id"] = "ASSET:foreign"
    else:
        context["source"]["sequences"] *= 9
    with pytest.raises(SchemaValidationError):
        validate_identity_contract("native_research_core_context.v1.json", context, namespace)
