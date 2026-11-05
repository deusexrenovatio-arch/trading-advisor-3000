from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from staging_support import LEGACY_TASK, initialize_budget, reserve, worker_lock
from validate_pr_evidence import check_pr_evidence, validate_result

JOBS = ("update", "features", "publish")


def proof():
    return dict(
        version=1,
        stage="acceptance",
        evidence_level="native",
        status="PASS",
        base_sha="a" * 40,
        head_sha="b" * 40,
        tree_sha="c" * 40,
        ci_run_id="42",
    )


def validate(value):
    return validate_result(value, base="a" * 40, head="b" * 40, tree="c" * 40, ci_run="42")


def test_complete_artifact_contract():
    assert validate(proof())["status"] == "PASS"


@pytest.mark.parametrize(
    "key,value",
    [
        ("stage", "branch"),
        ("status", "FAILED"),
        ("base_sha", "old"),
        ("head_sha", "old"),
        ("tree_sha", "old"),
        ("ci_run_id", "41"),
        ("evidence_level", "short"),
    ],
)
def test_missing_failed_short_or_stale_proof_blocks(key, value):
    result = proof()
    result[key] = value
    with pytest.raises(ValueError):
        validate(result)


@pytest.fixture
def instance(tmp_path):
    DagsterInstance = pytest.importorskip("dagster").DagsterInstance

    (tmp_path / "native-metadata").mkdir()
    with DagsterInstance.local_temp(str(tmp_path / "native-metadata")) as value:
        yield value


def test_uninitialized_task_never_gets_a_free_attempt(instance):
    with pytest.raises(ValueError, match="uninitialized"):
        reserve(instance, "task", "branch", "one", {})


def test_native_reservations_survive_failure_and_sha_changes(instance):
    initialize_budget(instance, "task", dict(branch=2, acceptance=1), dict(branch=0, acceptance=0))
    assert reserve(instance, "task", "branch", "one", {"head": "old"})["used"] == 1
    assert reserve(instance, "task", "branch", "two", {"head": "new"})["used"] == 2
    with pytest.raises(ValueError, match="exhausted"):
        reserve(instance, "task", "branch", "three", {"head": "another"})
    with pytest.raises(ValueError, match="no reset"):
        initialize_budget(
            instance, "task", dict(branch=2, acceptance=1), dict(branch=0, acceptance=0)
        )


def test_old_real_spend_cannot_be_reset_in_new_metadata(instance):
    with pytest.raises(ValueError, match="previous"):
        initialize_budget(
            instance, LEGACY_TASK, dict(branch=3, acceptance=1), dict(branch=0, acceptance=0)
        )
    initialize_budget(
        instance, LEGACY_TASK, dict(branch=3, acceptance=1), dict(branch=3, acceptance=0)
    )
    with pytest.raises(ValueError, match="exhausted"):
        reserve(instance, LEGACY_TASK, "branch", "four", {})


def test_worker_lock_serializes_tasks_and_does_not_erase_another_lock(tmp_path):
    with worker_lock(tmp_path):
        with pytest.raises(FileExistsError):
            with worker_lock(tmp_path):
                pytest.fail("concurrent worker")
        assert (tmp_path / "staging-worker.lock").exists()
    assert not (tmp_path / "staging-worker.lock").exists()


def test_native_launcher_requires_real_successful_parent_once(instance):
    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus

    from trading_advisor_3000.product_plane.common.staging_execution import bind_run

    plan = dict(jobs=list(JOBS), root_run_id="root", run_config={"ops": {}})
    root = DagsterRun(
        job_name=JOBS[0], run_id="root", run_config={"ops": {}}, status=DagsterRunStatus.STARTING
    )
    instance.add_run(root)
    bind_run(instance, root, plan)
    with pytest.raises(ValueError, match="already spent"):
        bind_run(instance, root, plan)
    child = DagsterRun(
        job_name=JOBS[1],
        run_id="child",
        tags={"ta3000/upstream_run_id": "root"},
        status=DagsterRunStatus.STARTING,
    )
    instance.add_run(child)
    with pytest.raises(ValueError, match="successful"):
        bind_run(instance, child, plan)
    # Only native terminal metadata is changed; no producer or quota service is used.

    from dagster._core.events import DagsterEvent, DagsterEventType

    instance.report_dagster_event(
        DagsterEvent(DagsterEventType.RUN_SUCCESS.value, job_name=JOBS[0]), "root"
    )
    bind_run(instance, child, plan)
    foreign = copy.copy(plan)
    foreign["root_run_id"] = "other"
    with pytest.raises(ValueError, match="Foreign root"):
        bind_run(instance, child, foreign)


def test_pr_gate_stops_on_missing_or_failed_native_evidence(monkeypatch):
    import run_pr_gate as gate

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pr_gate.py",
            "--changed-files",
            "src/trading_advisor_3000/product_plane/research/io/types.py",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
        ],
    )
    monkeypatch.setattr(gate, "check_architecture_review", lambda *a, **k: 0)
    monkeypatch.setattr(gate, "check_pr_evidence", lambda *a, **k: 2)
    monkeypatch.setattr(gate, "_run_pr_checks", lambda *a: pytest.fail("admission bypass"))
    assert gate.main() == 2


def test_launch_claim_is_atomic_inside_native_store(instance):
    from concurrent.futures import ThreadPoolExecutor

    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus

    from trading_advisor_3000.product_plane.common.staging_execution import bind_run

    plan = dict(jobs=[JOBS[0]], root_run_id="root", run_config={})
    run = DagsterRun(job_name=JOBS[0], run_id="root", status=DagsterRunStatus.STARTING)
    instance.add_run(run)

    def attempt(_):
        try:
            bind_run(instance, run, plan)
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, range(2))) == [False, True]


def test_collector_reads_normal_baseline_effective_changed_windows():
    from collect_staging_acceptance import baseline_changed_windows

    AssetMaterialization = pytest.importorskip("dagster").AssetMaterialization

    actual = AssetMaterialization("moex_baseline_update", metadata={"effective_changed_windows": 2})
    assert baseline_changed_windows(actual) == 2
    other = AssetMaterialization("moex_canonical_refresh", metadata={"changed_windows_count": 9})
    assert baseline_changed_windows(other) == 0


def test_code_level_continues_ordinary_pr_checks(monkeypatch):
    import run_pr_gate as gate

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pr_gate.py",
            "--changed-files",
            "configs/change_surface_mapping.yaml",
            "--snapshot-mode",
            "changed-files",
            "--profile",
            "none",
        ],
    )
    monkeypatch.setattr(gate, "check_architecture_review", lambda *a, **k: 0)
    monkeypatch.setattr(gate, "_run_pr_checks", lambda *a: 77)
    assert gate.main() == 77


def test_native_level_stops_without_evidence():
    assert check_pr_evidence(["src/trading_advisor_3000/product_plane/research/io.py"]) == 2


def test_shell_result_contract_is_product_independent():
    value = proof()
    value["producer_payload"] = {"opaque": "validated by its producer"}
    assert validate(value) is value


def test_git_diff_cannot_be_replaced_with_caller_code_only_scope(monkeypatch):
    import validate_pr_evidence as gate

    monkeypatch.setattr(
        gate, "git", lambda *a: "src/trading_advisor_3000/product_plane/research/io.py"
    )
    assert check_pr_evidence(["docs/README.md"], base_ref="base", head_ref="head") == 2


@pytest.mark.parametrize("value", [None, [], "PASS"])
def test_malformed_common_envelope_is_rejected(value):
    with pytest.raises(ValueError):
        validate(value)
