from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import reattest_staging_acceptance as reuse
import run_staging_control as control


@pytest.fixture
def dispatch(tmp_path, monkeypatch):
    candidate = dict(
        base_sha="base",
        head_sha="head",
        executable_sha256="exec",
        config_sha256="config",
        image_digest="image",
    )
    plan = dict(
        stage="acceptance",
        candidate=candidate.copy(),
        limits=dict(small=0, acceptance=1, rebuild=0),
    )
    args = SimpleNamespace(
        stage="acceptance",
        candidate=tmp_path / "candidate.json",
        repo=tmp_path,
        base_ref="base",
        plan=tmp_path / "plan.json",
        original_receipt=None,
        prepare_only=False,
        initialize_catch_up=False,
    )
    args.candidate.write_text(json.dumps({"candidate": candidate}))
    monkeypatch.setattr(reuse, "resolve", lambda *a: "base")
    monkeypatch.setattr(
        reuse, "git", lambda *a: '{"keys": {"test": "public"}}' if a[1] == "show" else ""
    )
    monkeypatch.setattr(reuse, "checked_candidate", lambda *a: candidate)
    monkeypatch.setattr(control, "load_plan", lambda *a: plan)
    return args, candidate, plan


def with_proof(dispatch, monkeypatch):
    args, candidate, plan = dispatch
    original = {"payload": {"issued_at_utc": "2026-10-25T00:00:00Z"}}
    args.original_receipt = args.repo / "original-receipt.json"
    args.original_receipt.write_text(json.dumps(original))
    native = dict(candidate=candidate.copy(), plan_sha256=control.digest(plan))
    monkeypatch.setattr(reuse, "verify_acceptance_route", lambda *a, **kw: native)
    return native


def test_no_proof_selects_one_acceptance(dispatch):
    assert control.candidate_action(dispatch[0]) == "RUN"


def test_same_artifact_and_plan_reuses_despite_new_sha(dispatch, monkeypatch):
    native = with_proof(dispatch, monkeypatch)
    native["candidate"]["head_sha"] = "old-sha"
    assert control.candidate_action(dispatch[0]) == "REUSE"


@pytest.mark.parametrize("changed", ["executable_sha256", "config_sha256", "image_digest"])
def test_changed_artifact_selects_one_acceptance(dispatch, monkeypatch, changed):
    native = with_proof(dispatch, monkeypatch)
    native["candidate"][changed] = "old"
    assert control.candidate_action(dispatch[0]) == "RUN"


def test_plan_metadata_change_cannot_trigger_recomputation(dispatch, monkeypatch):
    native = with_proof(dispatch, monkeypatch)
    native["plan_sha256"] = "old-inputs-or-environment"
    with pytest.raises(ValueError, match="original plan"):
        control.candidate_action(dispatch[0])


def test_stale_plan_cannot_run_new_candidate(dispatch):
    dispatch[2]["candidate"]["head_sha"] = "old"
    with pytest.raises(ValueError, match="new candidate"):
        control.candidate_action(dispatch[0])


def test_bad_signature_stops_before_compute(dispatch, monkeypatch):
    with_proof(dispatch, monkeypatch)

    def reject(*a, **kw):
        raise ValueError("invalid signature")

    monkeypatch.setattr(reuse, "verify_acceptance_route", reject)
    monkeypatch.setattr(control, "prepare", lambda *a: pytest.fail("compute preparation"))
    with pytest.raises(ValueError, match="invalid signature"):
        control.dispatch_candidate(dispatch[0])


def test_failed_observer_never_falls_back_to_compute(dispatch, monkeypatch):
    with_proof(dispatch, monkeypatch)

    def reject(*a):
        raise ValueError("observer failed")

    monkeypatch.setattr(reuse, "reattest", reject)
    monkeypatch.setattr(control, "supervise", lambda *a: pytest.fail("compute fallback"))
    with pytest.raises(ValueError, match="observer failed"):
        control.dispatch_candidate(dispatch[0])


def test_compute_failure_is_not_retried(dispatch, monkeypatch):
    calls = []
    monkeypatch.setattr(control, "prepare", lambda *a: {})

    def fail(*a):
        calls.append(1)
        raise ValueError("native failure")

    monkeypatch.setattr(control, "supervise", fail)
    with pytest.raises(ValueError, match="native failure"):
        control.dispatch_candidate(dispatch[0])
    assert calls == [1]


def test_no_initialization_during_reuse(dispatch, monkeypatch):
    with_proof(dispatch, monkeypatch)
    dispatch[0].initialize_catch_up = True
    with pytest.raises(ValueError, match="initializ"):
        control.dispatch_candidate(dispatch[0])


def test_new_plan_disables_small_and_rebuild_by_default():
    from prepare_staging_plan import initial_limits

    assert initial_limits("acceptance") == dict(small=0, acceptance=1, rebuild=0)
    with pytest.raises(ValueError, match="explicit"):
        initial_limits("branch")
    assert initial_limits("branch", 1)["small"] == 1


@pytest.mark.parametrize(
    "limits", [dict(small=0, acceptance=2, rebuild=0), dict(small=0, acceptance=1, rebuild=1)]
)
def test_automatic_route_cannot_expand_compute_budget(dispatch, limits):
    dispatch[2]["limits"] = limits
    with pytest.raises(ValueError, match="one acceptance"):
        control.candidate_action(dispatch[0])
