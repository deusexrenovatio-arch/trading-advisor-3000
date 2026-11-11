"""Admission must distinguish unavailable facts, history and a failed calculation."""

import json

import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research import native_features as owner


def measured_source(layer="L1", rows=300):
    policy = owner.feature_policy(layer, "native_core_" + layer.lower() + ".v1")
    sequence = {"rows": rows}
    features = {}
    for name, minimum in owner.feature_requirements(policy).items():
        ready = rows >= minimum
        features[name] = {
            "finite_count": int(ready),
            "analysis_finite_count": int(ready),
            "first_finite_bar_index": rows - 1 if ready else None,
            "first_finite_source_begin": "2026-09-15 09:01:00" if ready else None,
        }
    counts = [
        {
            "sequence": sequence,
            "analysis_rows": 1,
            "features": features,
            "quantities": {
                "volume": {"known": rows, "not_applicable": 0},
                "open_interest": {"known": 0, "not_applicable": 0},
            },
        }
    ]
    return {"sequences": [sequence]}, policy, counts


def test_complete_catalog_reports_missing_facts_without_fake_statistics():
    source, policy, counts = measured_source()
    result = owner.feature_capabilities(source, policy, counts)
    by_name = {row["feature"]: row for row in result}
    assert len(result) == 103
    assert by_name["macd_hist_12_26_9"]["state"] == "ready"
    assert by_name["oi_change_1"]["state"] == "missing_source"
    assert by_name["vp_vwap"]["state"] == "unverified_policy"
    assert "finite_count" not in by_name["oi_change_1"]


def test_quantity_not_applicable_precedes_insufficient_history():
    source, policy, counts = measured_source(rows=2)
    counts[0]["quantities"]["volume"] = {"known": 0, "not_applicable": 2}
    for item in policy["catalog"]:
        if item["materialized"] and "volume" in item["quantity_requirements"]:
            counts[0]["features"][item["output"]] = {
                "finite_count": 0,
                "analysis_finite_count": 0,
                "first_finite_bar_index": None,
                "first_finite_source_begin": None,
            }
    by_name = {row["feature"]: row for row in owner.feature_capabilities(source, policy, counts)}
    assert by_name["rvol_20"]["state"] == "not_applicable"
    assert by_name["macd_12_26_9"]["state"] == "insufficient_history"
    assert by_name["true_range"]["state"] == "ready"


@pytest.mark.parametrize(
    "rows,finite,message", [(33, 1, "without source/history"), (34, 0, "all-NULL")]
)
def test_macd_group_admission_and_post_admission_failure_are_distinct(rows, finite, message):
    source, policy, counts = measured_source(rows=rows)
    counts[0]["features"]["macd_hist_12_26_9"] = {
        "finite_count": finite,
        "analysis_finite_count": finite,
        "first_finite_bar_index": rows - 1 if finite else None,
        "first_finite_source_begin": "2026-09-15 09:01:00" if finite else None,
    }
    with pytest.raises(SchemaValidationError, match=message):
        owner.feature_capabilities(source, policy, counts)


def test_newly_available_oi_cannot_reuse_a_missing_source_catalog_policy():
    source, policy, counts = measured_source()
    counts[0]["quantities"]["open_interest"]["known"] = 300
    with pytest.raises(SchemaValidationError, match="newly applicable"):
        owner.feature_capabilities(source, policy, counts)


def test_core_layout_retains_each_parent_passthrough_once():
    l0 = owner.l0_layouts()["research_bar_views"]
    l1 = owner.feature_layout("L1", "native_core_l1.v1")
    l2 = owner.feature_layout("L2", "native_core_l2.v1")
    assert l1["fields"][: len(l0["fields"])] == l0["fields"]
    assert l2["fields"][: len(l1["fields"])] == l1["fields"]
    names = [row["name"] for row in l2["fields"]]
    assert len(names) == len(set(names))
    assert all(names.count(name) == 1 for name in ["true_range", "close_slope_20", "rvol_20"])
    assert len(l2["fields"]) - len(l0["fields"]) == 148 + 4


def test_core_l2_rejects_first_slice_parent_before_recursive_read(monkeypatch):
    receipt = {"layer": "L1", "context": {}}
    context = {
        "layer": "L1",
        "contract_version": "native_research_feature_context.v1",
        "policy": owner.feature_policy("L1"),
    }
    values = iter([receipt, context])
    monkeypatch.setattr(owner, "pinned_bytes", lambda *args: json.dumps(next(values)).encode())

    def forbidden(*args):
        pytest.fail("reached recursive reader before parent admission")

    monkeypatch.setattr(owner, "NativeFeatureSnapshot", forbidden)
    with pytest.raises(SchemaValidationError, match="requires completed core L1"):
        owner.parent_reader(None, {}, "L2", "native_core_l2.v1")


def test_unknown_profile_and_context_family_fail_closed():
    with pytest.raises(SchemaValidationError, match="unsupported feature profile"):
        owner.feature_policy("L1", "core_v1")
    with pytest.raises(SchemaValidationError, match="unsupported feature context family"):
        owner.feature_policy_from_context({"contract_version": "unknown"})
