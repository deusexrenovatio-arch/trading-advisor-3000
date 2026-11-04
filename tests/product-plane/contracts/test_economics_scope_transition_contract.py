import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import (
    SchemaValidationError,
    load_schema,
    validate_schema,
)


def test_economics_transition_has_one_strict_zero_effect_schema():
    name = "data_economics_scope_transition.v1.json"
    schema = load_schema(
        Path(__file__).parents[3]
        / "src/trading_advisor_3000/product_plane/contracts/schemas"
        / name
    )
    fixture = json.loads((Path(__file__).parents[1] / "fixtures/contracts" / name).read_text())
    validate_schema(schema, fixture)
    validate_schema(schema, {**fixture, "cutover": None})
    for invalid in (
        {**fixture, "allow_unknown": True},
        {**fixture, "differences": 1},
        {**fixture, "target_tables_sha256": "unknown"},
        {**fixture, "schema_version": True},
    ):
        with pytest.raises(SchemaValidationError):
            validate_schema(schema, invalid)
