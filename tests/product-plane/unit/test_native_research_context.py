"""Owner cross-collection checks beyond closed per-object schema assertions."""

import copy
import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research.native_observations import (
    l0_layouts,
    sha,
    validate_l0_context,
)


def valid_context():
    path = Path(__file__).parents[1] / "fixtures/contracts/native_research_l0_context.v1.json"
    context = json.loads(path.read_bytes())
    context["layout_sha256"] = sha(l0_layouts())
    bind_context(context)
    return context


def bind_context(context):
    context["research_context_id"] = "NCTX:" + sha(
        {k: v for k, v in context.items() if k != "research_context_id"}
    )


def test_valid_closed_context_with_complete_capability_matrix():
    context = valid_context()
    validate_l0_context(context, context["source"])


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "extra_sequence", "wrong_rows"])
def test_owner_rejects_incomplete_or_foreign_matrix_even_with_valid_nctx(mutation):
    context = valid_context()
    if mutation == "missing":
        context["capabilities"].pop()
    elif mutation == "duplicate":
        context["capabilities"].append(copy.deepcopy(context["capabilities"][0]))
    elif mutation == "extra_sequence":
        context["capabilities"][0]["sequence"]["source_symbol"] = "FOREIGN"
    else:
        context["capabilities"][0]["sequence"]["rows"] = 3
    bind_context(context)
    with pytest.raises(SchemaValidationError, match="capability matrix"):
        validate_l0_context(context, context["source"])


@pytest.mark.parametrize(
    "field",
    ["policy_sha256", "layout_sha256", "quality", "numerical_identity", "research_context_id"],
)
def test_owner_rejects_self_consistent_shape_with_invalid_hash(field):
    context = valid_context()
    if field == "quality":
        context[field]["sha256"] = "f" * 64
    elif field == "numerical_identity":
        context[field]["digest"] = "f" * 64
    elif field == "research_context_id":
        context[field] = "NCTX:" + "f" * 64
    else:
        context[field] = "f" * 64
    with pytest.raises(SchemaValidationError):
        validate_l0_context(context, context["source"])


def test_reader_checks_actual_admission_not_only_embedded_claim():
    context = valid_context()
    actual = copy.deepcopy(context["source"])
    actual["metadata"]["sha256"] = "f" * 64
    with pytest.raises(SchemaValidationError, match="source admission"):
        validate_l0_context(context, actual)
