import json
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import (
    SchemaValidationError,
    load_schema,
    validate_schema,
)


def test_recovery_report_has_one_schema_and_rejects_unknown_permission():
    root = Path(__file__).parents[3]
    schema = load_schema(
        root
        / "src/trading_advisor_3000/product_plane/contracts/schemas/data_scope_recovery.v1.json"
    )
    fixture = json.loads(
        (Path(__file__).parents[1] / "fixtures/contracts/data_scope_recovery.v1.json").read_text()
    )
    validate_schema(schema, fixture)
    with pytest.raises(SchemaValidationError):
        validate_schema(schema, {**fixture, "allow_unknown": True})
    with pytest.raises(SchemaValidationError):
        validate_schema(schema, {**fixture, "preserved_prefix_rows": 0})
