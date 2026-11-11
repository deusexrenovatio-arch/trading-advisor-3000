"""Observed adapters retain native seeds without admitting time or execution."""

import copy
import json
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research import native_feature_materialization as owner
from trading_advisor_3000.product_plane.research import native_features as reader_owner
from trading_advisor_3000.product_plane.research.native_features import (
    feature_capabilities,
    feature_layout,
    feature_policy,
    validate_feature_context,
)
from trading_advisor_3000.product_plane.research.native_observations import sha


def observed_frame(rows):
    return pd.DataFrame(
        {
            "source_begin": [f"2026-09-15 09:{i:02}:00" for i in range(rows)],
            **{
                name: [Decimal(100 + i) for i in range(rows)]
                for name in ["open", "high", "low", "close"]
            },
            "atr_14": np.nan,
        }
    )


@pytest.mark.parametrize(
    "change", [{"talib_absent": False}, {"numpy": "other"}, {"pandas_ta_classic": "other"}]
)
def test_worker_rejects_backend_or_engine_drift(monkeypatch, change):
    expected = owner.worker_engines()
    monkeypatch.setattr(owner, "worker_engines", lambda: {**expected, **change})
    with pytest.raises(ValueError, match="unadmitted feature worker"):
        owner._check_worker_engines(expected)


def test_daily_insufficient_history_does_not_call_l1_library(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("insufficient daily history called native indicator kernel")

    monkeypatch.setattr(owner, "_compute_profile_frame_unmasked", forbidden)
    frame = observed_frame(2)
    before = frame.copy(deep=True)
    policy = feature_policy("L1")
    result = owner._compute_l1_group(
        frame,
        owner.profile_for_policy(policy),
        policy["requirements"],
        owner.worker_engines(),
        ["source_begin"],
    )
    assert result[list(policy["requirements"])].isna().all().all()
    pd.testing.assert_frame_equal(frame, before)


def test_native_sma_seed_and_daily_diff_preserve_parent_decimal():
    frame = observed_frame(21)
    before = frame.copy(deep=True)
    l1 = feature_policy("L1")
    result = owner._compute_l1_group(
        frame,
        owner.profile_for_policy(l1),
        l1["requirements"],
        owner.worker_engines(),
        ["source_begin"],
    )
    assert result["sma_20"].first_valid_index() == 19
    assert result.loc[19, "sma_20"] == 109.5
    assert result.loc[20, "sma_20"] == 110.5
    l2 = feature_policy("L2")
    daily = owner._compute_l2_group(
        frame.iloc[:2],
        owner.profile_for_policy(l2),
        l2["requirements"],
        owner.worker_engines(),
        ["source_begin"],
    )
    assert pd.isna(daily.loc[0, "close_change_1"])
    assert daily.loc[1, "close_change_1"] == 1.0
    assert daily[["rolling_high_20", "rolling_low_20"]].isna().all().all()
    pd.testing.assert_frame_equal(frame, before)


def test_unknown_layer_cannot_fall_through_to_l2():
    with pytest.raises(SchemaValidationError, match="unsupported feature layer"):
        feature_layout("L3")


def test_feature_capabilities_reject_finite_values_below_required_history():
    sequence = {"rows": 2}
    policy = feature_policy("L1")
    counts = [
        {
            "sequence": sequence,
            "analysis_rows": 1,
            "features": {
                name: {
                    "finite_count": 1,
                    "analysis_finite_count": 1,
                    "first_finite_bar_index": 0,
                    "first_finite_source_begin": "2026-09-14 00:00:00",
                }
                for name in policy["requirements"]
            },
        }
    ]
    with pytest.raises(SchemaValidationError, match="insufficient history"):
        feature_capabilities({"sequences": [sequence]}, policy, counts)


def test_capability_order_survives_canonical_json_map_sorting():
    sequence = {"rows": 2}
    policy = feature_policy("L1")
    values = {
        "finite_count": 0,
        "analysis_finite_count": 0,
        "first_finite_bar_index": None,
        "first_finite_source_begin": None,
    }
    counts = [
        {
            "sequence": sequence,
            "analysis_rows": 1,
            "features": {name: values for name in policy["requirements"]},
        }
    ]
    serialized_policy = json.loads(json.dumps(policy, sort_keys=True))
    assert feature_capabilities({"sequences": [sequence]}, policy, counts) == feature_capabilities(
        {"sequences": [sequence]}, serialized_policy, counts
    )


@pytest.mark.parametrize("layer", ["l1", "l2"])
def test_stored_context_rejects_unadmitted_library_without_current_factory(layer):
    path = (
        Path(__file__).parents[1]
        / "fixtures"
        / "contracts"
        / f"native_research_feature_context.{layer}.v1.json"
    )
    context = json.loads(path.read_bytes())
    source = copy.deepcopy(context["source"])
    context["policy_sha256"] = sha(context["policy"])
    context["numerical_identity"]["policy_sha256"] = context["policy_sha256"]
    context["layout_sha256"] = sha({context["policy"]["table"]: feature_layout(context["layer"])})
    context["quality"]["sha256"] = sha(context["quality"]["policy"])
    context["numerical_identity"]["engines"]["pandas_ta_classic"] = "99.0.0"
    context["numerical_identity"]["digest"] = sha(
        {k: v for k, v in context["numerical_identity"].items() if k != "digest"}
    )
    context["feature_context_id"] = "NFCTX:" + sha(
        {k: v for k, v in context.items() if k != "feature_context_id"}
    )
    with pytest.raises(SchemaValidationError, match="stored feature.*profile"):
        validate_feature_context(context, source)


@pytest.mark.parametrize("layer,parent_layer", [("L1", "L1"), ("L2", "L2"), ("L2", "L0")])
def test_parent_layer_checked_before_recursive_reader(monkeypatch, layer, parent_layer):
    def forbidden(*args, **kwargs):
        pytest.fail("wrong parent layer reached recursive reader")

    monkeypatch.setattr(
        reader_owner, "pinned_bytes", lambda *args: json.dumps({"layer": parent_layer}).encode()
    )
    monkeypatch.setattr(reader_owner, "NativeResearchSnapshot", forbidden)
    monkeypatch.setattr(reader_owner, "NativeFeatureSnapshot", forbidden)
    with pytest.raises(SchemaValidationError, match="parent layer mismatch"):
        reader_owner.parent_reader(Path("/research-trial"), {}, layer)
