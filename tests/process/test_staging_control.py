from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


def prepared_inputs(tmp_path, monkeypatch):
    paths = {
        name: tmp_path / name
        for name in (
            "DATA_ROOT",
            "CONTROL_ROOT",
            "QUOTA_ROOT",
            "CANDIDATE_WORKTREE",
            "VERIFIER_WORKTREE",
            "HOST_STATE",
        )
    }
    for path in paths.values():
        path.mkdir()
    monkeypatch.setattr(control, "ROOT", paths["VERIFIER_WORKTREE"])
    plan = {
        "stage": "branch",
        "data_root": "/ta3000-data/moex-historical",
        "candidate": {"executable_sha256": "a" * 64, "image_digest": "sha256:" + "b" * 64},
        "deadline_utc": "2030-01-01T00:00:00Z",
        "max_runtime_seconds": 20,
        "source": {
            "native_schedule": {"run_config": {"ops": {"fixture": {"config": {"bounded": True}}}}}
        },
    }
    root = paths["CONTROL_ROOT"]
    (root / "plan.json").write_text("{}")
    (root / "execution-manifest.json").write_text("{}")
    (root / "root-marker.json").write_text(
        json.dumps(
            {
                "schema_version": "staging_root.v1",
                "plan_path": "/staging-control/plan.json",
                "plan_digest": control.digest(plan),
            }
        )
    )
    values = {control.PREFIX + key: str(path) for key, path in paths.items()}
    values.update(
        {
            control.PREFIX + key: value
            for key, value in {
                "MAX_GROWTH_BYTES": "1024",
                "MIN_FREE_BYTES": "1024",
                "SCRATCH_BYTES": "1024",
                "TASK_SLUG": "fixture",
                "IMAGE": plan["candidate"]["image_digest"],
            }.items()
        }
    )
    env = tmp_path / "stage.env"
    env.write_text("\n".join(f"{key}={value}" for key, value in values.items()))
    args = SimpleNamespace(
        stage="branch",
        env_file=env,
        plan=root / "plan.json",
        execution_manifest=root / "execution-manifest.json",
        private_key=None,
        prepare_only=True,
    )
    monkeypatch.setattr(control, "load_plan", lambda path: plan)
    monkeypatch.setattr(control, "verify_deployed_files", lambda *a: "a" * 64)
    monkeypatch.setattr(control, "source_preflight", lambda *a, **kw: {})
    return args, paths, plan


def test_host_controller_rejects_actual_production_mount_overlap(tmp_path, monkeypatch):
    args, paths, _ = prepared_inputs(tmp_path, monkeypatch)
    monkeypatch.setattr(
        control,
        "inspect_container",
        lambda _: {"Mounts": [{"Type": "bind", "Source": str(paths["DATA_ROOT"])}]},
    )
    with pytest.raises(ValueError, match="production mount"):
        control.prepare(args)


@pytest.mark.parametrize("stage", ["branch", "acceptance"])
@pytest.mark.parametrize("recipe", [None, {}, {"ops": {}}])
def test_missing_native_recipe_fails_before_docker_or_http(tmp_path, monkeypatch, stage, recipe):
    args, _, plan = prepared_inputs(tmp_path, monkeypatch)
    args.stage = plan["stage"] = stage
    plan["source"]["native_schedule"]["run_config"] = recipe
    monkeypatch.setattr(control, "inspect_container", lambda *a: pytest.fail("Docker inspection"))
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: pytest.fail("HTTP query"))
    with pytest.raises(ValueError, match="captured native run config"):
        control.prepare(args)


def test_branch_checks_accepted_source_before_inspecting_any_runtime(tmp_path, monkeypatch):
    args, _, _ = prepared_inputs(tmp_path, monkeypatch)

    def reject(*a, **kw):
        raise ValueError("source changed")

    monkeypatch.setattr(control, "source_preflight", reject)
    monkeypatch.setattr(control, "inspect_container", lambda *a: pytest.fail("Docker inspection"))
    with pytest.raises(ValueError, match="source changed"):
        control.prepare(args)


def test_host_controller_refuses_hardware_overcommit_before_start(tmp_path, monkeypatch):
    args, _, _ = prepared_inputs(tmp_path, monkeypatch)
    monkeypatch.setattr(
        control,
        "inspect_container",
        lambda _: {
            "Mounts": [],
            "NetworkSettings": {"Ports": {"3000/tcp": [{"HostPort": "3000"}]}},
        },
    )
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **k: [])
    monkeypatch.setattr(control, "validate_config", lambda *a: None)

    def checked(*cmd, **kwargs):
        assert "up" not in cmd
        return json.dumps(
            {"MemTotal": 1, "NCPU": 1}
            if cmd[1] == "info"
            else {"services": {"webserver": {"mem_limit": 2, "cpus": 1}}}
        )

    monkeypatch.setattr(control, "checked", checked)
    with pytest.raises(ValueError, match="hardware"):
        control.prepare(args)


def test_host_controller_never_stops_foreign_container(monkeypatch):
    calls = []
    monkeypatch.setattr(
        control,
        "inspect_container",
        lambda _: {"Config": {"Labels": {"com.docker.compose.project": "production"}}},
    )
    monkeypatch.setattr(control, "checked", lambda *cmd, **kw: calls.append(cmd))
    with pytest.raises(ValueError, match="foreign"):
        control.stop_owned("ta3000-acceptance", [{"Id": "foreign"}])
    assert calls == []


def test_timeout_stops_only_its_owned_project(tmp_path, monkeypatch):
    args = SimpleNamespace(stage="branch")
    own = {
        "Id": "own",
        "State": {"Running": True, "Health": {"Status": "healthy"}},
        "Config": {
            "Labels": {
                "com.docker.compose.project": "fixture",
                "com.docker.compose.service": "webserver",
            }
        },
    }
    observations = iter([[], [own], [own], [own]])
    monkeypatch.setattr(control, "owned", lambda _: next(observations))
    monkeypatch.setattr(control, "inspect_container", lambda _: own)
    monkeypatch.setattr(control, "verify_runtime", lambda *a: None)
    calls = []
    monkeypatch.setattr(control, "checked", lambda *cmd, **kw: calls.append(cmd))
    prepared = {
        "project": "fixture",
        "command": ["docker", "compose"],
        "env": {},
        "plan": {"max_runtime_seconds": 1},
        "paths": {"HOST_STATE": tmp_path / "state"},
        "deadline": datetime.now(UTC) - timedelta(seconds=1),
    }
    with pytest.raises(ValueError, match="deadline"):
        control.supervise(args, prepared)
    assert ("docker", "stop", "--time", "10", "own") in calls
    assert not (tmp_path / "state" / "heavy-compute.lock").exists()


def test_private_key_cannot_be_mounted_and_env_has_no_secret_keys(tmp_path, monkeypatch):
    args, paths, _ = prepared_inputs(tmp_path, monkeypatch)
    args.private_key = paths["CONTROL_ROOT"] / "private.key"
    with pytest.raises(ValueError, match="outside every"):
        control.prepare(args)
    args.private_key = None
    monkeypatch.setenv(
        "TA3000_STAGING_OPERATOR_PRIVATE_KEY", str(paths["CONTROL_ROOT"] / "operator.key")
    )
    with pytest.raises(ValueError, match="outside every"):
        control.prepare(args)
    args.env_file.write_text("TA3000_STAGING_OPERATOR_PRIVATE_KEY=secret")
    with pytest.raises(ValueError, match="literal staging"):
        control.read_environment(args.env_file)


def test_reattest_keeps_original_plan_but_has_only_a_short_read_transport(tmp_path, monkeypatch):
    args, paths, plan = prepared_inputs(tmp_path, monkeypatch)
    args.stage = plan["stage"] = "acceptance"
    plan["deadline_utc"] = "2000-01-01T00:00:00Z"
    original_digest = control.digest(plan)
    (paths["CONTROL_ROOT"] / "root-marker.json").write_text(
        json.dumps(
            {
                "schema_version": "staging_root.v1",
                "plan_path": "/staging-control/plan.json",
                "plan_digest": original_digest,
            }
        )
    )
    (paths["CONTROL_ROOT"] / "original-receipt.json").write_text("{}")
    monkeypatch.setattr(
        control,
        "inspect_container",
        lambda _: {
            "Mounts": [],
            "NetworkSettings": {"Ports": {"3000/tcp": [{"HostPort": "3000"}]}},
        },
    )
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **k: [])
    monkeypatch.setattr(control, "validate_config", lambda *a: None)
    seen = []
    monkeypatch.setattr(control, "source_preflight", lambda *a, **k: seen.append(k) or {})
    monkeypatch.setattr(control, "upstream_change_preflight", lambda *a: pytest.fail("new compute"))
    monkeypatch.setattr(
        control,
        "checked",
        lambda *cmd, **kw: (
            "c" * 40
            if cmd[0] == "git"
            else json.dumps(
                {"MemTotal": 10, "NCPU": 1}
                if cmd[1] == "info"
                else {"services": {"webserver": {"mem_limit": 2, "cpus": 1}}}
            )
        ),
    )
    now = datetime.now(UTC)
    result = control.prepare_reattest(args)
    assert seen == [{"historical_observation": True}]
    assert control.digest(result["plan"]) == original_digest
    assert now < result["deadline"] <= datetime.now(UTC) + timedelta(seconds=180)
    assert result["observer_verifier_revision"] == "c" * 40
    assert result["historical_observation"] and result["upstream"] is None
    with pytest.raises(ValueError, match="cannot activate"):
        control.supervise(args, result)
    with pytest.raises(ValueError, match="reattestation only"):
        control.observe_protected(result, {}, timeout=180, mode="observe")


def test_stopped_retained_runtime_is_readable_only_with_explicit_opt_in(monkeypatch):
    container = {
        "Config": {
            "Labels": {
                "com.docker.compose.project": "own",
                "com.docker.compose.service": "webserver",
            }
        },
        "Image": "sha256:actual",
        "State": {"Running": False},
        "Mounts": [
            {"Type": "volume", "Destination": "/opt/dagster/dagster_home", "Name": "own-home"}
        ],
        "HostConfig": {
            "Memory": 2,
            "NanoCpus": 1_000_000_000,
            "Tmpfs": {"/tmp": "size=1,mode=1777"},
        },
    }
    prepared = {
        "project": "own",
        "plan": {"candidate": {"image_digest": "sha256:actual"}},
        "config": {
            "volumes": {"staging-home": {"name": "own-home"}},
            "services": {
                "webserver": {
                    "mem_limit": 2,
                    "cpus": 1,
                    "tmpfs": ["/tmp:size=1,mode=1777"],
                    "volumes": [],
                }
            },
        },
    }
    monkeypatch.setattr(control, "owned", lambda _: [container])
    with pytest.raises(ValueError, match="no longer running"):
        control.verify_runtime(container, prepared)
    assert control.runtime_services(prepared, include_stopped=True)[0] is container
    container["Image"] = "sha256:foreign"
    with pytest.raises(ValueError, match="image differs"):
        control.runtime_services(prepared, include_stopped=True)
