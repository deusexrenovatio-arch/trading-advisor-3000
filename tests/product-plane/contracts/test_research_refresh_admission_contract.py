from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.research import admission as owner


def test_admission_schema_fixture_and_unknown_fields():
    import json

    from trading_advisor_3000.product_plane.contracts.schema_validation import (
        SchemaValidationError,
        load_schema,
        validate_schema,
    )

    root = Path(__file__).resolve().parents[3]
    schema = load_schema(
        root
        / "src/trading_advisor_3000/product_plane/contracts/schemas"
        / "research_refresh_admission.v1.json"
    )
    fixture = json.loads(
        (
            Path(__file__).parents[1] / "fixtures/contracts/research_refresh_admission.v1.json"
        ).read_text(encoding="utf-8")
    )
    validate_schema(schema, fixture)
    with pytest.raises(SchemaValidationError, match="unexpected"):
        validate_schema(schema, {**fixture, "force_full_refresh": True})
    with pytest.raises(ValueError, match="BLOCKED"):
        owner.validate_admission(
            owner._seal({k: v for k, v in fixture.items() if k != "admission_id"})
        )
