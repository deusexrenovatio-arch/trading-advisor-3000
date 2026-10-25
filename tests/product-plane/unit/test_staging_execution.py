from __future__ import annotations

import base64
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from dagster import DagsterInstance
from dagster._core.events import DagsterEvent
from dagster._core.remote_origin import (
    RegisteredCodeLocationOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
)
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus

from trading_advisor_3000.product_plane.common import staging_execution as guard


@pytest.fixture
def staging(tmp_path, monkeypatch):
    data, quota, home = (tmp_path / name for name in ("data", "quota", "runtime"))
    for root in (data, quota, home):
        root.mkdir()
    plan = {
        "schema_version": "staging_plan.v1",
        "task_id": "quota-test",
        "stage": "branch",
        "attempt_kind": "small",
        "data_root": str(data),
        "quota_home": str(quota),
        "deadline_utc": (datetime.now(UTC) + timedelta(days=1)).isoformat(),
        "max_runtime_seconds": 120,
        "limits": {"small": 2, "acceptance": 1, "rebuild": 0},
        "candidate": {
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "tree_sha": "c" * 40,
            "executable_sha256": "d" * 64,
            "config_sha256": "e" * 64,
            "image_digest": "sha256:" + "f" * 64,
        },
        "root_job": "source",
        "jobs": {
            "source": {"upstream_job": None, "max_runs": 1},
            "research": {"upstream_job": "source", "max_runs": 1},
        },
        "native_steps": {"calculate": 1},
        "require_scheduled": False,
        "consumer": {},
        "scope": {
            "instrument_ids": ["BR"],
            "timeframes": ["1d"],
            "start_utc": "2026-01-01T00:00:00Z",
            "end_utc": "2026-02-01T00:00:00Z",
            "max_changed_window_days": 2,
        },
    }
    path = tmp_path / "plan.json"
    permit_path, lock_path = tmp_path / "permit.json", tmp_path / "host-lock.json"
    monkeypatch.setattr(guard, "IMMUTABLE_PERMIT_PATH", permit_path)
    monkeypatch.setattr(guard, "IMMUTABLE_HOST_LOCK_PATH", lock_path)

    def save():
        path.write_text(json.dumps(plan), encoding="utf-8")
        (data / guard.ROOT_MARKER).write_text(
            json.dumps(
                {
                    "schema_version": "staging_root.v1",
                    "plan_path": str(path),
                    "plan_digest": guard.digest(plan),
                }
            ),
            encoding="utf-8",
        )
        now = datetime.now(UTC)
        permit_path.write_text(
            json.dumps(
                {
                    "schema_version": "staging_execution_permit.v1",
                    "plan_sha256": guard.digest(plan),
                    "lease_nonce": "a" * 32,
                    "issued_at_utc": now.isoformat(),
                    "deadline_utc": (now + timedelta(seconds=30)).isoformat(),
                }
            )
        )
        lock_path.write_text(
            json.dumps(
                {
                    "schema_version": "staging_host_lock.v1",
                    "plan_sha256": guard.digest(plan),
                    "lease_nonce": "a" * 32,
                    "project": "unit-staging",
                    "pid": os.getpid(),
                }
            )
        )

    save()
    monkeypatch.setenv(guard.PLAN_ENV, str(path))
    monkeypatch.setenv("DAGSTER_HOME", str(home))
    monkeypatch.delenv(guard.GRANT_ENV, raising=False)
    monkeypatch.setattr(guard, "IMMUTABLE_PLAN_PATH", tmp_path / "no-immutable-plan.json")
    token = guard._ACTIVE.set(None)
    protected_quota = guard._quota

    @contextmanager
    def unit_quota(plan, *, host_quota_home=None):
        with DagsterInstance.local_temp(str(host_quota_home or plan["quota_home"])) as store:
            yield store

    monkeypatch.setattr(guard, "_quota", unit_quota)
    # Initialize schema before concurrent reservation tests; the durable directory survives runs.
    with DagsterInstance.local_temp(str(quota)):
        pass
    with DagsterInstance.local_temp(str(home)) as instance:

        def context(run_id, job=None, tags=None, status=DagsterRunStatus.STARTED, run_config=None):
            job = job or plan["root_job"]
            previous = instance.get_run_by_id(run_id)
            run = previous or instance.add_run(
                DagsterRun(
                    job_name=job,
                    run_id=run_id,
                    status=status,
                    tags=tags or {},
                    run_config=run_config or {},
                    remote_job_origin=RemoteJobOrigin(
                        RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("staging"), "repo"), job
                    ),
                )
            )
            if previous is None and status == DagsterRunStatus.STARTED:
                instance.report_dagster_event(
                    DagsterEvent(event_type_value="PIPELINE_START", job_name=job), run_id=run_id
                )
            return SimpleNamespace(
                run_id=run.run_id,
                op_execution_context=SimpleNamespace(
                    instance=instance,
                    op_config={"output_dir": str(data)},
                    retry_number=0,
                ),
            )

        yield SimpleNamespace(
            plan=plan,
            path=path,
            data=data,
            quota=quota,
            save=save,
            context=context,
            instance=instance,
            protected_quota=protected_quota,
            permit_path=permit_path,
            lock_path=lock_path,
        )
    guard._ACTIVE.reset(token)


def scope(staging, run_id, **kwargs):
    return guard.staging_execution_scope(
        staging.context(run_id, **kwargs), stage="asset", writable_paths=[staging.data]
    )


def test_failed_run_and_recreated_runtime_still_spend_task_budget(staging):
    for run in ("failed-1", "cancelled-2"):
        with pytest.raises(RuntimeError, match="actual failure"):
            with scope(staging, run):
                raise RuntimeError("actual failure")
    staging.plan["candidate"]["head_sha"] = "1" * 40
    staging.save()
    with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
        with scope(staging, "new-sha-new-run"):
            pass


def test_atomic_reservation_prevents_concurrent_overspend(staging):
    contexts = [staging.context(f"race-{n}") for n in range(5)]

    def reserve(context):
        try:
            with guard.staging_execution_scope(
                context, stage="asset", writable_paths=[staging.data]
            ):
                return True
        except guard.StagingExecutionError:
            return False

    with ThreadPoolExecutor(max_workers=5) as pool:
        assert sum(pool.map(reserve, contexts)) == 2


def test_step_retry_direct_rebuild_and_unknown_entry_cannot_bypass(staging):
    context = staging.context("retry")
    context.op_execution_context.retry_number = 1
    with pytest.raises(guard.StagingExecutionError, match="step retry"):
        with guard.staging_execution_scope(context, stage="asset", writable_paths=[staging.data]):
            pass
    with scope(staging, "root"):
        with pytest.raises(guard.StagingExecutionError, match="full rebuild"):
            guard.require_staging_compute(
                stage="calculate", output_path=staging.data, operation="rebuild"
            )
        with pytest.raises(guard.StagingExecutionError, match="outside plan"):
            guard.require_staging_compute(stage="unknown", output_path=staging.data)
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
        with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
            guard.protected_subprocess_env({}, native_entry="calculate", output_path=staging.data)
        with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
            guard.require_staging_compute(stage="calculate", output_path=staging.data)


def test_marker_keeps_direct_compute_closed_after_environment_removed(staging, monkeypatch):
    monkeypatch.delenv(guard.PLAN_ENV)
    with pytest.raises(guard.StagingExecutionError, match="bound Dagster owner"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
    guard.require_staging_compute(stage="calculate", output_path=staging.data.parent / "unmarked")


def test_child_requires_completed_real_parent_and_does_not_reset_quota(staging):
    with scope(staging, "root"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
    staging.instance.report_dagster_event(
        DagsterEvent(event_type_value="PIPELINE_SUCCESS", job_name="source"), run_id="root"
    )
    with scope(staging, "child", job="research", tags={"ta3000/upstream_run_id": "root"}):
        with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
            guard.require_staging_compute(stage="calculate", output_path=staging.data)
    with pytest.raises(guard.StagingExecutionError, match="completed allowed upstream"):
        with scope(staging, "forged", job="research", tags={"ta3000/upstream_run_id": "absent"}):
            pass


def test_window_path_and_publication_deadline_are_enforced(staging):
    with scope(staging, "root") as active:
        with pytest.raises(guard.StagingExecutionError, match="escapes"):
            guard.require_staging_compute(
                stage="calculate", output_path=staging.data.parent / "prod"
            )
        with pytest.raises(guard.StagingExecutionError, match="window exceeds"):
            guard.require_staging_compute(
                stage="calculate",
                output_path=staging.data,
                windows=[
                    {
                        "instrument_id": "OTHER",
                        "timeframe": "1d",
                        "start_ts": "2026-01-02T00:00:00Z",
                        "end_ts": "2026-01-03T00:00:00Z",
                    }
                ],
            )
        guard.assert_staging_publication(staging.data)
        staging.instance.report_run_canceled(staging.instance.get_run_by_id("root"))
        with pytest.raises(guard.StagingExecutionError, match="stopped"):
            guard.assert_staging_publication(staging.data)
        active.reservation = SimpleNamespace(run_config={"started_at": time.time() - 121})
        with pytest.raises(guard.StagingExecutionError, match="deadline"):
            guard.assert_staging_publication(staging.data)


def test_subprocess_grant_binds_entry_output_and_is_single_use(staging, monkeypatch):
    with scope(staging, "root"):
        env = guard.protected_subprocess_env({}, native_entry="calculate", output_path=staging.data)
    original = json.loads(env[guard.GRANT_ENV])
    changed = deepcopy(original)
    changed["native_entry"] = "other"
    monkeypatch.setenv(guard.GRANT_ENV, json.dumps(changed))
    with pytest.raises(guard.StagingExecutionError, match="binding changed"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
    monkeypatch.setenv(guard.GRANT_ENV, env[guard.GRANT_ENV])
    guard.require_staging_compute(stage="calculate", output_path=staging.data)
    guard._ACTIVE.set(None)  # A second native worker gets the same environment.
    with pytest.raises(guard.StagingExecutionError, match="already consumed"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)


def test_ephemeral_instance_policy_change_and_marker_tamper_are_blocked(staging):
    context = staging.context("root")
    with scope(staging, "root"):
        pass
    staging.plan["limits"]["small"] += 1
    staging.save()
    with pytest.raises(guard.StagingExecutionError, match="policy changed"):
        with scope(staging, "new-budget"):
            pass
    staging.plan["limits"]["small"] -= 1
    staging.save()
    with DagsterInstance.ephemeral() as ephemeral:
        context.op_execution_context.instance = ephemeral
        with pytest.raises(guard.StagingExecutionError, match="persistent"):
            with guard.staging_execution_scope(
                context, stage="asset", writable_paths=[staging.data]
            ):
                pass
    (staging.data / guard.ROOT_MARKER).write_text("{}")
    with pytest.raises(guard.StagingExecutionError, match="marker/plan"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)


def test_raw_scope_aliases_are_bounded_and_unknown_aliases_fail_closed(staging):
    raw = {
        "internal_id": "BR",
        "timeframe": "1d",
        "source_interval": 24,
        "window_start_utc": "2026-01-02T00:00:00Z",
        "window_end_utc": "2026-01-03T00:00:00Z",
    }
    with scope(staging, "root"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data, windows=[raw])
        raw["window_end_utc"] = "2026-01-10T00:00:00Z"
        with pytest.raises(guard.StagingExecutionError, match="window exceeds"):
            guard.require_staging_compute(
                stage="calculate", output_path=staging.data, windows=[raw]
            )
        with pytest.raises(guard.StagingExecutionError, match="lacks bounded"):
            guard.require_staging_compute(
                stage="calculate", output_path=staging.data, windows=[{"contract_id": "BR"}]
            )


def test_coordinator_checks_budget_before_queue_and_rejects_unknown_job(staging):
    coordinator = guard.StagingRunCoordinator()
    coordinator.register_instance(staging.instance)
    context = staging.context("queued", status=DagsterRunStatus.NOT_STARTED)
    run = staging.instance.get_run_by_id(context.run_id)
    queued = coordinator.submit_run(SimpleNamespace(dagster_run=run))
    assert queued.status == DagsterRunStatus.QUEUED
    assert queued.tags["ta3000/staging_plan_sha256"] == guard.digest(staging.plan)
    assert queued.tags["ta3000/staging_root_run_id"] == run.run_id
    unknown = staging.context("unknown", job="unapproved", status=DagsterRunStatus.NOT_STARTED)
    with pytest.raises(guard.StagingExecutionError, match="outside protected"):
        coordinator.submit_run(
            SimpleNamespace(dagster_run=staging.instance.get_run_by_id(unknown.run_id))
        )
    assert staging.instance.get_run_by_id(unknown.run_id).status == DagsterRunStatus.NOT_STARTED
    with pytest.raises(guard.StagingExecutionError, match="conflicting staging"):
        with scope(staging, "spoofed", tags={"ta3000/staging_plan_sha256": "wrong"}):
            pass


def operator_keys(staging, monkeypatch):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key = Ed25519PrivateKey.generate()
    path = staging.path.parent / "operator-private.key"
    path.write_bytes(key.private_bytes_raw())
    (staging.path.parent / "operator-keyring.json").write_text(
        json.dumps(
            {
                "keys": {
                    "operator": base64.b64encode(key.public_key().public_bytes_raw()).decode(
                        "ascii"
                    ),
                }
            }
        )
    )
    monkeypatch.setenv("TA3000_STAGING_OPERATOR_PRIVATE_KEY", str(path))
    monkeypatch.setenv("TA3000_STAGING_OPERATOR_KEY_ID", "operator")
    return path


def test_signed_extension_preserves_spent_slots_and_is_replay_safe(staging, monkeypatch):
    operator_keys(staging, monkeypatch)
    for run in ("spent-1", "spent-2"):
        with scope(staging, run):
            pass
    limits = {**staging.plan["limits"], "small": 3}
    approval = guard.authorize_budget_extension(
        staging.path,
        authorization_id="human-approval-1",
        reason="diagnosed failure",
        new_limits=limits,
    )
    assert (
        guard.authorize_budget_extension(
            staging.path,
            authorization_id="human-approval-1",
            reason="diagnosed failure",
            new_limits=limits,
        )
        == approval
    )
    staging.plan["limits"] = limits
    staging.save()
    with scope(staging, "third"):
        pass
    with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
        with scope(staging, "fourth"):
            pass
    with pytest.raises(guard.StagingExecutionError, match="already used differently"):
        guard.authorize_budget_extension(
            staging.path, authorization_id="human-approval-1", reason="different", new_limits=limits
        )
    with pytest.raises(guard.StagingExecutionError, match="monotonically"):
        guard.authorize_budget_extension(
            staging.path,
            authorization_id="reduce",
            reason="reduce",
            new_limits={**limits, "small": 2},
        )


def test_untrusted_operator_key_cannot_extend_and_never_reaches_native_env(staging, monkeypatch):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key_path = operator_keys(staging, monkeypatch)
    with scope(staging, "root"):
        env = guard.protected_subprocess_env(
            dict(os.environ), native_entry="calculate", output_path=staging.data
        )
        assert "TA3000_STAGING_OPERATOR_PRIVATE_KEY" not in env
    key_path.write_bytes(Ed25519PrivateKey.generate().private_bytes_raw())
    with pytest.raises(ValueError, match="signature"):
        guard.authorize_budget_extension(
            staging.path,
            authorization_id="forged",
            reason="self-extension",
            new_limits={**staging.plan["limits"], "small": 3},
        )
    with scope(staging, "second"):
        pass
    with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
        with scope(staging, "third"):
            pass


def test_immutable_control_plan_blocks_env_removed_output_escape(staging, monkeypatch):
    monkeypatch.setattr(guard, "IMMUTABLE_PLAN_PATH", staging.path)
    monkeypatch.delenv(guard.PLAN_ENV)
    with pytest.raises(guard.StagingExecutionError, match="escapes"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data.parent / "other")
    monkeypatch.setenv(guard.PLAN_ENV, str(staging.path.parent / "fake-plan.json"))
    with pytest.raises(guard.StagingExecutionError, match="cannot be overridden"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)


def test_protected_runtime_has_no_sqlite_backend_switch(staging):
    (staging.quota / "storage.json").write_text(
        json.dumps(
            {
                "schema_version": "staging_quota_storage.v1",
                "backend": "sqlite",
                "postgres_url": "sqlite:///fake",
                "expected_database": "fake",
                "expected_role": "fake",
            }
        )
    )
    with pytest.raises(guard.StagingExecutionError, match="append-only PostgreSQL"):
        with staging.protected_quota(staging.plan):
            pass


def test_windows_explicit_host_transport_changes_only_docker_hostname(monkeypatch):
    from sqlalchemy.engine import make_url

    config = {
        "postgres_url": "postgresql+psycopg2://writer:p%40ss%3Aword@host.docker.internal:55432/quota?connect_timeout=3",
        "expected_role": "writer",
        "expected_database": "quota",
    }
    original = deepcopy(config)
    monkeypatch.setattr(guard, "os", SimpleNamespace(name="nt"))
    assert guard._quota_url(config) == config["postgres_url"]
    old = make_url(config["postgres_url"])
    mapped = make_url(guard._quota_url(config, host_transport=True))
    assert mapped.host == "127.0.0.1" and mapped.set(host=old.host) == old
    assert config == original
    monkeypatch.setattr(guard, "os", SimpleNamespace(name="posix"))
    assert guard._quota_url(config, host_transport=True) == config["postgres_url"]


@pytest.mark.parametrize("part", ["endpoint", "role", "database"])
def test_windows_host_transport_rejects_unknown_authority(part, monkeypatch):
    config = {
        "postgres_url": "postgresql+psycopg2://writer@host.docker.internal:55432/quota",
        "expected_role": "writer",
        "expected_database": "quota",
    }
    if part == "endpoint":
        config["postgres_url"] = config["postgres_url"].replace("host.docker.internal", "other")
    else:
        config[f"expected_{part}"] = "other"
    monkeypatch.setattr(guard, "os", SimpleNamespace(name="nt"))
    with pytest.raises(guard.StagingExecutionError, match="binding changed"):
        guard._quota_url(config, host_transport=True)


def test_host_quota_transport_requires_explicit_absolute_path():
    with pytest.raises(guard.StagingExecutionError, match="absolute path"):
        with guard._quota({}, host_quota_home="relative-quota"):
            pytest.fail("relative host authority was admitted")


def test_insert_only_attacker_cannot_reuse_claim_with_another_nonce(staging, monkeypatch):
    with scope(staging, "root"):
        env = guard.protected_subprocess_env({}, native_entry="calculate", output_path=staging.data)
    original = json.loads(env[guard.GRANT_ENV])
    forged = {**original, "nonce": uuid4().hex}
    with guard._quota(staging.plan) as quota:
        issued = quota.get_run_by_id(guard._key("staging-grant-v1", original["nonce"]))
        guard._add(
            quota,
            guard._key("staging-grant-v1", forged["nonce"]),
            {"grant": forged, "claim_id": issued.run_config["claim_id"]},
        )
    monkeypatch.setenv(guard.GRANT_ENV, env[guard.GRANT_ENV])
    guard.require_staging_compute(stage="calculate", output_path=staging.data)
    guard._ACTIVE.set(None)
    monkeypatch.setenv(guard.GRANT_ENV, json.dumps(forged))
    with pytest.raises(guard.StagingExecutionError, match="claim already consumed"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)


def test_grant_rejects_unbounded_claim_and_finished_computing_owner(staging, monkeypatch):
    with scope(staging, "root"):
        env = guard.protected_subprocess_env({}, native_entry="calculate", output_path=staging.data)
    forged = json.loads(env[guard.GRANT_ENV])
    forged["nonce"] = uuid4().hex
    with guard._quota(staging.plan) as quota:
        claim_id = uuid4().hex
        guard._add(
            quota,
            claim_id,
            {
                "run_id": "root",
                "plan_sha256": guard.digest(staging.plan),
                "native_entry": "calculate",
                "output_path": str(staging.data),
            },
        )
        guard._add(
            quota,
            guard._key("staging-grant-v1", forged["nonce"]),
            {"grant": forged, "claim_id": claim_id},
        )
    monkeypatch.setenv(guard.GRANT_ENV, json.dumps(forged))
    with pytest.raises(guard.StagingExecutionError, match="root/claim binding"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
    staging.instance.report_dagster_event(
        DagsterEvent(event_type_value="PIPELINE_SUCCESS", job_name="source"), run_id="root"
    )
    monkeypatch.setenv(guard.GRANT_ENV, env[guard.GRANT_ENV])
    with pytest.raises(guard.StagingExecutionError, match="stopped"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)


def test_append_only_forged_root_cannot_mint_another_native_budget(staging, monkeypatch):
    with scope(staging, "root"):
        env = guard.protected_subprocess_env({}, native_entry="calculate", output_path=staging.data)
    guard._ACTIVE.set(None)
    monkeypatch.setenv(guard.GRANT_ENV, env[guard.GRANT_ENV])
    guard.require_staging_compute(stage="calculate", output_path=staging.data)
    original = json.loads(env[guard.GRANT_ENV])
    forged = {**original, "nonce": uuid4().hex, "root_reservation_id": uuid4().hex}
    claim = guard._key("staging-native-v1", forged["root_reservation_id"], "calculate", 0)
    with guard._quota(staging.plan) as quota:
        root = quota.get_run_by_id(original["root_reservation_id"])
        guard._add(quota, forged["root_reservation_id"], dict(root.run_config))
        guard._add(
            quota,
            claim,
            {
                "plan_sha256": guard.digest(staging.plan),
                "run_id": "root",
                "native_entry": "calculate",
                "output_path": str(staging.data.resolve()),
            },
        )
        guard._add(
            quota,
            guard._key("staging-grant-v1", forged["nonce"]),
            {
                "grant": forged,
                "claim_id": claim,
            },
        )
    staging.instance.add_run_tags(
        "root",
        {
            "ta3000/staging_root_reservation_id": forged["root_reservation_id"],
        },
    )
    guard._ACTIVE.set(None)
    monkeypatch.setenv(guard.GRANT_ENV, json.dumps(forged))
    with pytest.raises(guard.StagingExecutionError, match="conflicting staging|canonical root"):
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
    with guard._quota(staging.plan) as quota:
        assert quota.get_run_by_id(guard._key("staging-grant-consumed-v1", forged["nonce"])) is None


def test_no_host_permit_holds_queue_and_cannot_spend_a_compute_slot(staging):
    coordinator = guard.StagingRunCoordinator()
    coordinator.register_instance(staging.instance)
    staging.permit_path.unlink()
    assert coordinator.get_run_queue_config().max_concurrent_runs == 0
    context = staging.context("held", status=DagsterRunStatus.NOT_STARTED)
    coordinator.submit_run(
        SimpleNamespace(dagster_run=staging.instance.get_run_by_id(context.run_id))
    )
    with pytest.raises(guard.StagingExecutionError, match="execution permit"):
        with scope(staging, "in-process"):
            pass
    with guard._quota(staging.plan) as quota:
        assert quota.get_runs() == []
    staging.save()
    assert coordinator.get_run_queue_config().max_concurrent_runs == 1


def test_permit_nonce_and_expiry_fence_native_and_publication(staging):
    with scope(staging, "root"):
        lock = json.loads(staging.lock_path.read_text())
        lock["lease_nonce"] = "b" * 32
        staging.lock_path.write_text(json.dumps(lock))
        with pytest.raises(guard.StagingExecutionError, match="execution permit"):
            guard.require_staging_compute(stage="calculate", output_path=staging.data)
        staging.save()
        permit = json.loads(staging.permit_path.read_text())
        permit["deadline_utc"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        staging.permit_path.write_text(json.dumps(permit))
        with pytest.raises(guard.StagingExecutionError, match="execution permit"):
            guard.assert_staging_publication(staging.data)


def reference_plan(staging, monkeypatch):
    from dagster._core.remote_origin import RemoteInstigatorOrigin
    from dagster._core.scheduler.instigation import (
        InstigatorState,
        InstigatorStatus,
        InstigatorType,
        ScheduleInstigatorData,
        TickData,
        TickStatus,
    )

    from trading_advisor_3000.product_plane.common.staging_contracts import sign_receipt
    from trading_advisor_3000.product_plane.common.staging_route_proof import JOBS, STAGES

    private = operator_keys(staging, monkeypatch)
    repo = staging.path.parent / "reference-repository"
    repo.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "-b", "main")
    (repo / "pyproject.toml").write_text("# unit fixture\n")
    git("add", "pyproject.toml")
    git("-c", "user.name=Unit", "-c", "user.email=unit@example.invalid", "commit", "-m", "fixture")
    main, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
    staging.plan["candidate"].update(head_sha=main, tree_sha=tree)
    staging.plan.update(stage="acceptance", attempt_kind="acceptance", require_scheduled=True)
    staging.plan["root_job"] = JOBS[0]
    staging.plan["jobs"] = {
        job: {"upstream_job": JOBS[index - 1] if index else None, "max_runs": 1}
        for index, job in enumerate(JOBS)
    }
    now = datetime.now(UTC)
    accepted = {
        "schema_version": "staging_receipt.v1",
        "task_id": staging.plan["task_id"],
        "stage": "acceptance",
        "candidate": deepcopy(staging.plan["candidate"]),
        "plan_sha256": guard.digest(staging.plan),
        "issued_at_utc": now.isoformat(),
        "expires_at_utc": (now + timedelta(hours=1)).isoformat(),
        "verifier_revision": "a" * 40,
        "source": {"snapshot_id": "unit-source", "manifest_sha256": "7" * 64},
        "outputs": {
            "snapshot_id": "unit-output",
            "manifest_sha256": "8" * 64,
            "data_snapshot_id": "unit-data",
        },
        "runs": {
            job: {
                "status": "SUCCESS",
                "run_id": f"accepted-{index}",
                "candidate_head_sha": main,
                "upstream_run_id": f"accepted-{index - 1}" if index else None,
            }
            for index, job in enumerate(JOBS)
        },
        "schedule": {
            "status": "SUCCESS",
            "name": "moex_baseline_daily_update_schedule",
            "tick_id": "unit-tick",
            "run_id": "accepted-0",
            "timestamp": now.timestamp(),
        },
        "changed_windows": 1,
        "qc": {"status": "PASS"},
        "consumer": {
            "status": "PASS",
            "rows": 1,
            "snapshot_id": "unit-output",
            "entrypoint": "research.io.load_backtest_frames",
            "cache_hit": False,
            "process_id": "unit-consumer",
            "digest": "9" * 64,
            "read_at_utc": now.isoformat(),
        },
        "coverage": {
            "status": "PASS",
            "required_stages": list(STAGES),
            "executed_stages": list(STAGES),
            "required_scenarios": [],
            "executed_scenarios": [],
        },
        "runtime": {
            "mode": "candidate",
            "code_sha256": staging.plan["candidate"]["executable_sha256"],
            "config_sha256": staging.plan["candidate"]["config_sha256"],
            "image_digest": staging.plan["candidate"]["image_digest"],
            "budget": {
                "status": "PASS",
                "plan_sha256": guard.digest(staging.plan),
                "root_reservation_id": "a" * 32,
                "native_claims": {"calculate": 1},
            },
        },
    }
    staging.plan.update(
        mode="reference",
        reference={
            "main_sha": main,
            "timezone": "Europe/Moscow",
            "receipt": sign_receipt(accepted, private.read_bytes(), "operator"),
        },
    )
    staging.save()
    repository = RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("staging"), "repo")
    origin = RemoteInstigatorOrigin(repository, "moex_baseline_daily_update_schedule")
    state = InstigatorState(
        origin,
        InstigatorType.SCHEDULE,
        InstigatorStatus.RUNNING,
        ScheduleInstigatorData("0 0 * * *"),
    )
    staging.instance.add_instigator_state(state)

    def tick(run_id, stamp, *, store=True, run_config=None):
        context = staging.context(
            run_id,
            tags={
                "dagster/schedule_name": state.name,
                # Caller tags never supply the quota clock.
                "dagster/scheduled_execution_time": "2099-01-01T00:00:00Z",
            },
            run_config=run_config,
        )
        if store:
            staging.instance.create_tick(
                TickData(
                    instigator_origin_id=state.instigator_origin_id,
                    instigator_name=state.name,
                    instigator_type=InstigatorType.SCHEDULE,
                    status=TickStatus.SUCCESS,
                    timestamp=datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp(),
                    run_ids=[run_id],
                    selector_id=state.selector_id,
                )
            )
        return context

    def activate(authorization_id="reference-1"):
        return guard.authorize_reference_mode(
            staging.path,
            authorization_id=authorization_id,
            reason="approved main / diagnosis",
            repository_root=repo,
            main_ref="refs/heads/main",
            runtime_image_digest=staging.plan["candidate"]["image_digest"],
        )

    return SimpleNamespace(repo=repo, tick=tick, activate=activate)


def test_reference_requires_operator_activation_and_real_tick_then_uses_moscow_day(
    staging, monkeypatch
):
    ref = reference_plan(staging, monkeypatch)
    ref.tick("first", "2026-01-01T20:30:00Z")
    with pytest.raises(guard.StagingExecutionError, match="host activation"):
        with scope(staging, "first"):
            pass
    ref.activate()
    with scope(staging, "first") as current:
        assert current.reservation.run_config["local_day"] == "2026-01-01"
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
    ref.tick("same-day", "2026-01-01T20:45:00Z")
    with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
        with scope(staging, "same-day"):
            pass
    ref.tick("next-moscow-day", "2026-01-01T21:30:00Z")
    with scope(staging, "next-moscow-day") as current:
        assert current.reservation.run_config["local_day"] == "2026-01-02"
        guard.require_staging_compute(stage="calculate", output_path=staging.data)
    ref.tick("tag-only", "2026-01-03T00:00:00Z", store=False)
    with pytest.raises(guard.StagingExecutionError, match="persisted schedule tick"):
        with scope(staging, "tag-only"):
            pass


def test_reference_failure_stops_later_ticks_until_explicit_new_authorization(staging, monkeypatch):
    ref = reference_plan(staging, monkeypatch)
    ref.activate()
    ref.tick("failed", "2026-01-01T20:30:00Z")
    with pytest.raises(RuntimeError, match="failed calculation"):
        with scope(staging, "failed"):
            raise RuntimeError("failed calculation")
    ref.tick("later", "2026-01-02T21:30:00Z")
    with pytest.raises(guard.StagingExecutionError, match="reference halted"):
        with scope(staging, "later"):
            pass
    ref.activate()  # Replaying approval is not reactivation.
    with pytest.raises(guard.StagingExecutionError, match="reference halted"):
        with scope(staging, "later"):
            pass
    ref.activate("reference-after-diagnosis")
    with scope(staging, "later"):
        pass


def test_reference_activation_rejects_changed_main_and_wrong_artifact(staging, monkeypatch):
    ref = reference_plan(staging, monkeypatch)
    with pytest.raises(guard.StagingExecutionError, match="artifact differs"):
        guard.authorize_reference_mode(
            staging.path,
            authorization_id="bad-image",
            reason="test",
            repository_root=ref.repo,
            main_ref="refs/heads/main",
            runtime_image_digest="sha256:" + "0" * 64,
        )
    (ref.repo / "pyproject.toml").write_text("# changed after acceptance\n")
    with pytest.raises(guard.StagingExecutionError, match="artifact differs"):
        ref.activate()


def test_reference_activation_rejects_branch_reference_and_incomplete_receipts(
    staging, monkeypatch
):
    from trading_advisor_3000.product_plane.common.staging_contracts import sign_receipt

    ref = reference_plan(staging, monkeypatch)
    original = deepcopy(staging.plan["reference"]["receipt"]["payload"])
    private = (staging.path.parent / "operator-private.key").read_bytes()
    for mutation in ("branch", "reference", "incomplete"):
        value = deepcopy(original)
        if mutation == "branch":
            value["stage"] = "branch"
        elif mutation == "reference":
            value["runtime"]["mode"] = "reference"
        else:
            value["outputs"] = {}
        staging.plan["reference"]["receipt"] = sign_receipt(value, private, "operator")
        staging.save()
        with pytest.raises(ValueError):
            ref.activate(f"invalid-{mutation}")


def test_native_monitor_tag_is_bounded_by_graph_deadline_and_caller_cannot_broaden(staging):
    with scope(staging, "root", tags={guard.RUNTIME_TAG: "999999"}) as active:
        run = staging.instance.get_run_by_id("root")
        assert 0 < int(run.tags[guard.RUNTIME_TAG]) <= 120
        active.reservation = SimpleNamespace(
            run_id=active.reservation.run_id, run_config={"started_at": time.time() - 60}
        )
        assert (
            0 < guard._runtime_cap(staging.plan, active.reservation, staging.instance, "root") <= 60
        )
        staging.instance.add_run_tags("root", {guard.RUNTIME_TAG: "999999"})
        with pytest.raises(guard.StagingExecutionError, match="changed bindings"):
            guard.require_staging_compute(stage="calculate", output_path=staging.data)


def test_native_launcher_charges_before_decorator_free_candidate_and_reuses_on_start(
    staging, monkeypatch
):
    from dagster._core.launcher.default_run_launcher import DefaultRunLauncher

    invoked = []

    def candidate_without_guards(launcher, context):
        with guard._quota(staging.plan) as quota:
            roots = [row for row in quota.get_runs() if "root_run_id" in row.run_config]
            assert any(row.run_config["root_run_id"] == context.dagster_run.run_id for row in roots)
        invoked.append(context.dagster_run.run_id)

    monkeypatch.setattr(DefaultRunLauncher, "launch_run", candidate_without_guards)
    launcher = guard.StagingRunLauncher()
    launcher.register_instance(staging.instance)
    monkeypatch.setattr(staging.instance, "_run_launcher", launcher)
    for name in ("old-artifact-1", "old-artifact-2", "over-budget"):
        staging.context(name, status=DagsterRunStatus.NOT_STARTED)
    staging.instance.launch_run("old-artifact-1", workspace=None)
    first = staging.instance.get_run_by_id("old-artifact-1")
    assert 0 < int(first.tags[guard.RUNTIME_TAG]) <= 120
    with pytest.raises(guard.StagingExecutionError, match="launch already spent"):
        launcher.launch_run(SimpleNamespace(dagster_run=first, workspace=None))
    staging.instance.report_dagster_event(
        DagsterEvent(event_type_value="PIPELINE_START", job_name="source"),
        run_id=first.run_id,
    )
    with scope(staging, first.run_id) as active:
        assert active.reservation.run_config["root_run_id"] == first.run_id
    staging.instance.launch_run("old-artifact-2", workspace=None)
    with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
        staging.instance.launch_run("over-budget", workspace=None)
    assert invoked == ["old-artifact-1", "old-artifact-2"]


def test_native_launcher_concurrent_decorator_free_roots_remain_bounded(staging, monkeypatch):
    from dagster._core.launcher.default_run_launcher import DefaultRunLauncher

    monkeypatch.setattr(DefaultRunLauncher, "launch_run", lambda *args: None)
    launcher = guard.StagingRunLauncher()
    launcher.register_instance(staging.instance)
    runs = []
    for index in range(5):
        context = staging.context(f"launch-race-{index}", status=DagsterRunStatus.NOT_STARTED)
        staging.instance.report_dagster_event(
            DagsterEvent(event_type_value="PIPELINE_STARTING", job_name="source"),
            run_id=context.run_id,
        )
        runs.append(staging.instance.get_run_by_id(context.run_id))

    def launch(run):
        try:
            launcher.launch_run(SimpleNamespace(dagster_run=run, workspace=None))
            return True
        except guard.StagingExecutionError:
            return False

    with ThreadPoolExecutor(max_workers=5) as pool:
        assert sum(pool.map(launch, runs)) == 2


def test_branch_direct_coordinator_needs_no_daemon_and_uses_protected_launcher(
    staging, monkeypatch
):
    from dagster._core.launcher.default_run_launcher import DefaultRunLauncher

    invoked = []
    monkeypatch.setattr(
        DefaultRunLauncher,
        "launch_run",
        lambda _, context: invoked.append(context.dagster_run.run_id),
    )
    launcher = guard.StagingRunLauncher()
    launcher.register_instance(staging.instance)
    monkeypatch.setattr(staging.instance, "_run_launcher", launcher)
    coordinator = guard.StagingDirectRunCoordinator()
    coordinator.register_instance(staging.instance)
    for name in ("held", "direct-1", "direct-2", "over-budget"):
        staging.context(name, status=DagsterRunStatus.NOT_STARTED)

    def submit(name):
        return coordinator.submit_run(
            SimpleNamespace(dagster_run=staging.instance.get_run_by_id(name), workspace=None)
        )

    staging.permit_path.unlink()
    with pytest.raises(guard.StagingExecutionError, match="execution permit"):
        submit("held")
    assert staging.instance.get_run_by_id("held").status == DagsterRunStatus.NOT_STARTED
    staging.save()
    assert submit("direct-1").status == DagsterRunStatus.STARTING
    assert submit("direct-2").status == DagsterRunStatus.STARTING
    with pytest.raises(guard.StagingExecutionError, match="budget exhausted"):
        submit("over-budget")
    assert invoked == ["direct-1", "direct-2"]
    staging.plan.update(stage="acceptance", attempt_kind="acceptance", require_scheduled=True)
    staging.save()
    with pytest.raises(guard.StagingExecutionError, match="requires a branch plan"):
        submit("held")


def reference_oi(staging, monkeypatch):
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from trading_advisor_3000.product_plane.common.data_snapshots import (
        capture_table,
        create_snapshot,
        save_snapshot,
    )
    from trading_advisor_3000.product_plane.common.staging_source_scope import parameter_value
    from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh

    oi = staging.data / "raw/economics/raw_moex_daily_open_interest.delta"
    write_deltalake(str(oi), pa.table({"open_interest": [1]}), engine="pyarrow")
    params = {
        "daily_open_interest_path": oi,
        "daily_open_interest_version": 0,
        "canonical_bars_path": staging.data / "canonical/bars.delta",
        "lookback_days": 7,
    }
    staging.plan["producer_parameters"] = {"run_moex_baseline_update": parameter_value(params)}
    # Mock the owner metadata adapter's fixed production table IDs, not native Delta pins.
    monkeypatch.setattr(
        native_history_refresh,
        "registered_history_refresh_kwargs",
        lambda root: {
            "daily_open_interest_path": oi,
            "daily_open_interest_version": DeltaTable(str(oi)).version(),
        },
    )
    ref = reference_plan(staging, monkeypatch)
    ref.activate()

    def publish():
        manifest = create_snapshot(
            tables={oi.stem: capture_table(oi)},
            required_tables=[oi.stem],
            dataset_version="test",
            run_id="native-owner",
            code_version="accepted",
            quality_checks={"native-input": "PASS"},
        )
        root = params["canonical_bars_path"].parent / ".data_snapshots"
        save_snapshot(root, manifest)
        (root / "published_current.json").write_text(
            json.dumps(
                {
                    "schema_version": "data_snapshot_publication.v1",
                    "snapshot_id": manifest["snapshot_id"],
                    "manifest_sha256": manifest["manifest_sha256"],
                }
            )
        )

    def tick(name, stamp, version):
        return ref.tick(
            name,
            stamp,
            run_config={
                "ops": {
                    "moex_baseline_update": {
                        "config": {
                            "daily_open_interest_path": str(oi),
                            "daily_open_interest_version": version,
                        }
                    }
                }
            },
        )

    publish()
    return SimpleNamespace(params=params, path=oi, publish=publish, tick=tick)


def test_reference_day_two_renews_only_accepted_oi_version_and_freezes_cycle_pin(
    staging, monkeypatch
):
    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common.staging_source_scope import (
        require_staging_parameters,
    )

    oi = reference_oi(staging, monkeypatch)

    def check(parameters):
        require_staging_parameters(
            stage="run_moex_baseline_update", output_path=staging.data, parameters=parameters
        )

    oi.tick("day-1", "2026-01-01T20:30:00Z", 0)
    with scope(staging, "day-1"):
        check(oi.params)
    write_deltalake(
        str(oi.path), pa.table({"open_interest": [2]}), mode="overwrite", engine="pyarrow"
    )
    oi.publish()
    oi.tick("day-2", "2026-01-02T20:30:00Z", 1)
    with scope(staging, "day-2"):
        current = {**oi.params, "daily_open_interest_version": 1}
        check(current)
        with pytest.raises(guard.StagingExecutionError, match="behavior"):
            check({**current, "lookback_days": 365})
        with pytest.raises(guard.StagingExecutionError, match="behavior"):
            check({**current, "daily_open_interest_path": staging.data / "caller.delta"})
        from trading_advisor_3000.product_plane.data_plane.moex import native_history_refresh

        with monkeypatch.context() as owner_changed:
            owner_changed.setattr(
                native_history_refresh,
                "registered_history_refresh_kwargs",
                lambda root: {"lookback_days": 365},
            )
            with pytest.raises(guard.StagingExecutionError, match="native owner metadata"):
                check(current)
        oi.publish()  # Even a valid new publication cannot replace this cycle's captured input.
        with pytest.raises(guard.StagingExecutionError, match="inside an admitted daily cycle"):
            check(current)


def test_reference_oi_caller_flag_or_unpublished_version_cannot_renew(staging, monkeypatch):
    import pyarrow as pa
    from deltalake import write_deltalake

    from trading_advisor_3000.product_plane.common.staging_source_scope import (
        require_staging_parameters,
    )

    oi = reference_oi(staging, monkeypatch)
    write_deltalake(
        str(oi.path), pa.table({"open_interest": [2]}), mode="overwrite", engine="pyarrow"
    )
    oi.tick("caller", "2026-01-01T20:30:00Z", 0)
    oi.tick("unpublished", "2026-01-02T20:30:00Z", 1)
    for name, message in (
        ("caller", "recorded schedule config"),
        ("unpublished", "currently accepted"),
    ):
        with scope(staging, name), pytest.raises(guard.StagingExecutionError, match=message):
            require_staging_parameters(
                stage="run_moex_baseline_update",
                output_path=staging.data,
                parameters={**oi.params, "daily_open_interest_version": 1},
            )
