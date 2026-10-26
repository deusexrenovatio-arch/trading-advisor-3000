import sys
from types import ModuleType

import pytest

from trading_advisor_3000.product_plane.common import staging_execution as execution


def test_cold_fixture_uses_small_only_after_protected_validation(monkeypatch):
    calls = []
    module = ModuleType("trading_advisor_3000.product_plane.common.staging_test_fixture")
    module.validate_branch_fixture = lambda plan: calls.append(plan) or {"version": 1}
    monkeypatch.setitem(sys.modules, module.__name__, module)
    plan = {
        "stage": "branch",
        "attempt_kind": "small",
        "root_job": "research_data_prep_job",
        "source": {"fixture": {"manifest_sha256": "a" * 64}},
        "limits": {"rebuild": 0},
    }
    execution._check_operation(plan, "rebuild")
    assert calls == [plan]


@pytest.mark.parametrize(
    "stage,kind,job,fixture",
    [
        ("acceptance", "small", "research_data_prep_job", True),
        ("branch", "small", "moex_baseline_update_job", True),
        ("branch", "small", "research_data_prep_job", False),
    ],
)
def test_fixture_exception_never_authorizes_ordinary_rebuild(stage, kind, job, fixture):
    plan = {
        "stage": stage,
        "attempt_kind": kind,
        "root_job": job,
        "source": {"fixture": {"manifest_sha256": "a" * 64}} if fixture else {},
        "limits": {"rebuild": 0},
    }
    with pytest.raises(execution.StagingExecutionError, match="rebuild"):
        execution._check_operation(plan, "rebuild")


def test_fixture_validation_failure_cannot_fall_back_to_rebuild(monkeypatch):
    module = ModuleType("trading_advisor_3000.product_plane.common.staging_test_fixture")

    def reject(plan):
        raise ValueError("fixture changed")

    module.validate_branch_fixture = reject
    monkeypatch.setitem(sys.modules, module.__name__, module)
    plan = {
        "stage": "branch",
        "attempt_kind": "small",
        "root_job": "research_data_prep_job",
        "source": {"fixture": {"manifest_sha256": "a" * 64}},
        "limits": {"rebuild": 0},
    }
    with pytest.raises(execution.StagingExecutionError, match="fixture changed"):
        execution._check_operation(plan, "rebuild")


@pytest.mark.parametrize("canonical,passes", [("fixture", True), ("foreign", False)])
def test_fixture_asset_keeps_canonical_input_readonly(tmp_path, monkeypatch, canonical, passes):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.common import staging_test_fixture

    output = tmp_path / "outputs"
    fixture = tmp_path / "fixture"
    plan = {"data_root": str(output), "source": {"fixture": {"manifest_sha256": "a" * 64}}}
    monkeypatch.setattr(execution, "_plan", lambda *a: plan)
    monkeypatch.setattr(
        staging_test_fixture,
        "validate_branch_fixture",
        lambda *a: {"canonical_dir": str(fixture), "raw_dir": str(fixture / "raw")},
    )
    observed = []

    def scoped(context, *, writable_paths, **kwargs):
        execution._paths(plan, writable_paths)
        observed.extend(writable_paths)
        return nullcontext()

    monkeypatch.setattr(execution, "staging_execution_scope", scoped)

    @execution.staging_asset
    def native_asset(context):
        return "executed"

    context = SimpleNamespace(
        op_config={
            "canonical_output_dir": str(tmp_path / canonical),
            "materialized_output_dir": str(output),
        }
    )
    if passes:
        assert native_asset(context) == "executed"
        assert observed == [output]
    else:
        with pytest.raises(execution.StagingExecutionError, match="outside protected"):
            native_asset(context)
