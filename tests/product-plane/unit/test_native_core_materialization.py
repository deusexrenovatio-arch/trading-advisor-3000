"""Core dispatch keeps atomic history gates and exact inherited buffers."""

from dataclasses import replace
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research import native_feature_materialization as owner
from trading_advisor_3000.product_plane.research.native_core_policy import core_policy
from trading_advisor_3000.product_plane.research.native_features import feature_requirements


def observed_frame(rows, *, index=False):
    close = [Decimal(100 + i) for i in range(rows)]
    return pd.DataFrame(
        {
            "source_begin": [f"2026-09-15 {i:04}" for i in range(rows)],
            "open": close,
            "high": [c + 2 for c in close],
            "low": [c - 2 for c in close],
            "close": close,
            "volume": [None if index else Decimal(10 + i) for i in range(rows)],
            "volume_state": "not_applicable" if index else "known",
            "open_interest": None,
            "open_interest_state": "not_applicable",
            "true_range": 4.0,
            "log_ret_1": [np.nan]
            + [float(np.log(float(close[i] / close[i - 1]))) for i in range(1, rows)],
            "atr_14": np.nan,
            "close_slope_20": np.nan,
            "rvol_20": np.nan,
        }
    )


def test_core_profile_seals_origin_and_resolved_factory(monkeypatch):
    policy = core_policy("L1")
    profile = owner.profile_for_policy(policy)
    assert len(profile.indicators) == 52
    assert len(profile.expected_output_columns()) == 79
    original = owner.core_v1_indicator_profile()
    changed = replace(
        original,
        indicators=(replace(original.indicators[0], warmup_bars=999), *original.indicators[1:]),
    )
    monkeypatch.setattr(owner, "core_v1_indicator_profile", lambda: changed)
    with pytest.raises(SchemaValidationError, match="factory"):
        owner.profile_for_policy(policy)


def test_macd_admission_is_atomic_and_keeps_native_early_seed(monkeypatch):
    monkeypatch.setattr(owner, "_check_worker_engines", lambda expected: None)
    policy = core_policy("L1")
    profile = owner.profile_for_policy(policy)
    short = owner._compute_l1_group(
        observed_frame(33), profile, feature_requirements(policy), {}, ["source_begin"]
    )
    names = ["macd_12_26_9", "macd_signal_12_26_9", "macd_hist_12_26_9"]
    assert short[names].isna().all().all()
    result = owner._compute_l1_group(
        observed_frame(34), profile, feature_requirements(policy), {}, ["source_begin"]
    )
    # Linear prices seed both EMAs exactly; their lag difference is (26 - 12) / 2.
    assert result.loc[25, names[0]] == pytest.approx(7.0)
    assert pd.isna(result.loc[25, names[1]])
    assert result.loc[33, names[1]] == pytest.approx(7.0)
    assert result.loc[33, names[2]] == pytest.approx(0.0, abs=1e-12)


def test_daily_numeric_volume_and_passthroughs_preserve_decimal(monkeypatch):
    monkeypatch.setattr(owner, "_check_worker_engines", lambda expected: None)
    frame = observed_frame(2)
    before = frame.copy(deep=True)
    p1, p2 = core_policy("L1"), core_policy("L2")
    l1 = owner._compute_l1_group(
        frame, owner.profile_for_policy(p1), feature_requirements(p1), {}, ["source_begin"]
    )
    assert "true_range" not in l1
    assert l1["obv"].tolist() == [10.0, 21.0]
    assert l1["ad"].tolist() == [0.0, 0.0]
    assert l1.loc[1, "pvt"] == pytest.approx(11.0)
    parent = frame.assign(**{name: l1[name] for name in l1.columns if name != "source_begin"})
    parent_before = parent.copy(deep=True)
    l2 = owner._compute_l2_group(
        parent, owner.profile_for_policy(p2), feature_requirements(p2), {}, ["source_begin"]
    )
    pd.testing.assert_frame_equal(parent, parent_before)
    assert "close_slope_20" not in l2 and "rvol_20" not in l2
    assert pd.isna(l2.loc[0, "close_change_1"])
    assert l2.loc[1, "close_change_1"] == 1.0
    assert l2.loc[1, "volume_change_1"] == 1.0
    assert l2["rolling_high_20"].isna().all()
    pd.testing.assert_frame_equal(frame, before)
    assert isinstance(frame.loc[1, "volume"], Decimal)


def test_index_volume_does_not_call_quantity_library(monkeypatch):
    monkeypatch.setattr(owner, "_check_worker_engines", lambda expected: None)
    real_compute = owner._compute_profile_frame_unmasked

    def forbid_quantity(frame, profile):
        assert all("volume" not in s.required_input_columns for s in profile.indicators)
        return real_compute(frame, profile)

    monkeypatch.setattr(owner, "_compute_profile_frame_unmasked", forbid_quantity)
    policy = core_policy("L1")
    result = owner._compute_l1_group(
        observed_frame(2, index=True),
        owner.profile_for_policy(policy),
        feature_requirements(policy),
        {},
        ["source_begin"],
    )
    assert result[["obv", "ad", "pvt"]].isna().all().all()


def test_core_missing_dependency_rejects_before_numerical_work(monkeypatch):
    policy = core_policy("L2")
    frame = observed_frame(2).drop(columns="atr_14")
    with pytest.raises(SchemaValidationError, match="missing.*input"):
        owner._require_core_inputs(frame.columns, policy)


@pytest.mark.parametrize(
    "change",
    [
        {"numba": "other"},
        {"scipy": "other"},
        {"ta_numba_enabled": False},
        {"ta_scipy_enabled": False},
    ],
)
def test_core_worker_rejects_optional_backend_drift(monkeypatch, change):
    expected = {
        **owner.worker_engines(),
        "numba": "0.65.1",
        "scipy": "1.17.1",
        "ta_numba_enabled": True,
        "ta_scipy_enabled": True,
    }
    monkeypatch.setattr(owner, "core_worker_engines", lambda: {**expected, **change})
    monkeypatch.setattr(
        owner, "_ta_source_sha256", lambda: core_policy("L1")["runtime_profile"]["ta_source_sha256"]
    )
    with pytest.raises(ValueError, match="unadmitted feature worker"):
        owner._check_worker_engines(expected)


def test_core_worker_rejects_changed_library_bytes(monkeypatch):
    expected = {
        **owner.worker_engines(),
        "numba": "0.65.1",
        "scipy": "1.17.1",
        "ta_numba_enabled": True,
        "ta_scipy_enabled": True,
    }
    monkeypatch.setattr(owner, "core_worker_engines", lambda: expected)
    monkeypatch.setattr(owner, "_ta_source_sha256", lambda: "a" * 64)
    with pytest.raises(ValueError, match="unadmitted feature worker"):
        owner._check_worker_engines(expected)


@pytest.mark.parametrize("layer", ["L1", "L2"])
def test_adapter_identity_resolves_actual_core_symbol_closure(layer):
    # The default and core profiles share this exact adapter AST; host engine is not core-admitted.
    identity = owner.feature_numerical_identity(layer)
    assert identity["engines"] == owner.worker_engines()
    assert identity["policy_sha256"] == owner.sha(owner.feature_policy(layer))
    assert identity["kernels"]["adapter"]
