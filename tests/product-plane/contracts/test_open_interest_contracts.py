import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from trading_advisor_3000.product_plane.contracts import CanonicalBar

ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src/trading_advisor_3000/product_plane/contracts/schemas"
FIXTURES = ROOT / "tests/product-plane/fixtures/contracts"


@pytest.mark.parametrize(
    "name,field",
    [
        ("canonical_bar", "open_interest"),
        ("continuous_front_bars", "native_open_interest"),
    ],
)
def test_open_interest_v2_preserves_null_and_observed_zero(name, field):
    schema = json.loads((SCHEMAS / f"{name}.v2.json").read_text())
    payload = json.loads((FIXTURES / f"{name}.v2.json").read_text())
    validator = Draft202012Validator(schema)
    for value in [None, 0, 123]:
        payload[field] = value
        validator.validate(payload)
        if name == "canonical_bar":
            assert CanonicalBar.from_dict(payload).to_dict()[field] == value
    for invalid in [-1, True, "123"]:
        payload[field] = invalid
        assert list(validator.iter_errors(payload))
        if name == "canonical_bar":
            with pytest.raises(ValueError):
                CanonicalBar.from_dict(payload)
    legacy = json.loads((SCHEMAS / f"{name}.v1.json").read_text())
    payload[field] = None
    assert list(Draft202012Validator(legacy).iter_errors(payload))
