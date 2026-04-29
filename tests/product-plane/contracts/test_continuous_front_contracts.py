from __future__ import annotations

import json
from pathlib import Path

from trading_advisor_3000.product_plane.contracts.schema_validation import load_schema, validate_schema


ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "src" / "trading_advisor_3000" / "product_plane" / "contracts" / "schemas"
FIXTURES = ROOT / "tests" / "product-plane" / "fixtures" / "contracts"
CONTINUOUS_FRONT_TABLES = (
    "continuous_front_bars",
    "continuous_front_roll_events",
    "continuous_front_adjustment_ladder",
    "continuous_front_qc_report",
)


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict), path
    return payload


def test_continuous_front_json_contracts_and_fixtures_validate() -> None:
    for table_name in CONTINUOUS_FRONT_TABLES:
        schema = load_schema(SCHEMAS / f"{table_name}.v1.json")
        fixture = _load_json(FIXTURES / f"{table_name}.v1.json")
        assert schema["$id"] == f"contracts/{table_name}.v1.json"
        validate_schema(schema, fixture)


def test_continuous_front_delta_contracts_have_plan_keys_and_qc() -> None:
    bar_columns = set(load_schema(SCHEMAS / "continuous_front_bars.v1.json")["properties"])
    assert {
        "dataset_version",
        "roll_policy_version",
        "adjustment_policy_version",
        "active_contract_id",
        "candidate_contract_id",
        "roll_event_id",
        "continuous_close",
        "cumulative_additive_offset",
        "causality_watermark_ts",
    } <= bar_columns
    qc_columns = set(load_schema(SCHEMAS / "continuous_front_qc_report.v1.json")["properties"])
    assert {"blocked_reason", "status", "future_causality_violation_count"} <= qc_columns
