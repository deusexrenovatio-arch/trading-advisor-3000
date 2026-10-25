import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


@pytest.fixture
def metadata(tmp_path, monkeypatch):
    production = {
        "Id": "actual-prod",
        "Image": "sha256:prod",
        "State": {"Running": True},
        "Config": {"Labels": {"com.docker.compose.project": "production"}},
        "Mounts": [
            {
                "Type": "volume",
                "Destination": "/opt/dagster/dagster_home",
                "Name": "actual-prod-home",
                "Source": "/var/lib/docker/volumes/actual-prod-home/_data",
            }
        ],
    }
    root = {"runId": "actual-root", "pipelineName": "moex_baseline_update_job", "status": "SUCCESS"}
    tick = {
        "id": "origin:100.0",
        "timestamp": 100.0,
        "status": "SUCCESS",
        "runIds": [root["runId"]],
    }
    value = {
        "project": "ta3000-acceptance",
        "production_url": "production",
        "production_runtime_identity": control._production_identity(production),
        "plan": {
            "candidate": {"image_digest": "sha256:fixed"},
            "source": {"native_schedule": {"cron": "0 2 * * *"}},
        },
        "paths": {"VERIFIER_WORKTREE": tmp_path / "trusted"},
    }
    monkeypatch.setattr(control, "inspect_container", lambda *a: production)
    monkeypatch.setattr(
        control, "graphql_runs", lambda *a, **kw: [root] if kw.get("job_name") else []
    )
    monkeypatch.setattr(control, "graphql_schedule", lambda *a: {"ticks": [tick]})
    monkeypatch.setattr(control, "_public_noop_prefilter", lambda *a: True)
    return value, production, root, tick


def test_noop_observer_has_only_readonly_home_and_trusted_code(metadata, monkeypatch):
    value, _, root, tick = metadata
    calls = []

    def run(*cmd, **kw):
        calls.append((cmd, kw))
        return SimpleNamespace(
            returncode=1 if cmd[1] == "inspect" else 0,
            stdout='["history/runs.db","history/runs/index.db","schedules/schedules.db"]'
            if "-c" in cmd
            else "{}",
        )

    monkeypatch.setattr(control, "run", run)
    control.observe_production_noop(value, value["production_runtime_identity"], root, tick)
    command, kwargs = next(
        call for call in calls if "/staging-verifier/scripts/observe_production_noop.py" in call[0]
    )
    assert command[:2] == ("docker", "run") and kwargs["timeout"] == 30
    assert "--read-only" in command and "none" in command and "no-new-privileges" in command
    assert command[command.index("--memory") + 1] == str(2 * 1024**3)
    mounts = [command[i + 1] for i, part in enumerate(command) if part == "--mount"]
    assert len(mounts) == 5 and all(item.endswith(",readonly") for item in mounts)
    assert all(
        text not in " ".join(command)
        for text in ("/ta3000-data", "docker.sock", "PRIVATE_KEY", "/staging-control")
    )


def test_production_identity_uses_mount_contents_independent_of_native_order(metadata):
    _, production, _, _ = metadata
    production["Mounts"].append(
        {"Type": "bind", "Destination": "/workspace", "Source": "actual-code"}
    )
    first = control._production_identity(production)
    production["Mounts"].reverse()
    assert control._production_identity(production) == first


def test_exact_noop_witness_is_throttled_and_unstable_or_blocked_never_allows(
    metadata, monkeypatch
):
    value, production, root, tick = metadata
    calls = []
    proof = {
        "status": "PASS-NOOP",
        "root_run_id": root["runId"],
        "schedule": {"id": tick["id"]},
        "metadata_observation": {"observation_complete": True},
        "compute_attempts": 0,
        "sensor": {
            "upstream_run_id": root["runId"],
            "status": "SKIPPED",
            "cursor": "native-cursor",
            "skip_reason": "NOOP: proven; admission=" + "a" * 64,
        },
    }
    monkeypatch.setattr(control, "observe_production_noop", lambda *a: calls.append(1) or proof)
    assert control.production_noop_witness(value, root, tick)
    assert control.production_noop_witness(value, root, tick) and calls == [1]
    production["Mounts"][0]["Name"] = "foreign-home"
    with pytest.raises(ValueError, match="identity changed"):
        control.production_noop_witness(value, root, tick)


@pytest.mark.parametrize("damage", ["blocked", "unstable", "wrong-root", "active-after"])
def test_noop_denies_nonproof_or_changed_production(metadata, monkeypatch, damage):
    value, _, root, tick = metadata
    proof = {
        "status": "PASS-NOOP",
        "root_run_id": root["runId"],
        "schedule": {"id": tick["id"]},
        "metadata_observation": {"observation_complete": True},
        "compute_attempts": 0,
        "sensor": {
            "upstream_run_id": root["runId"],
            "status": "SKIPPED",
            "cursor": "native-cursor",
            "skip_reason": "NOOP: proven; admission=" + "a" * 64,
        },
    }
    if damage == "blocked":
        proof["status"] = "BLOCKED"
    elif damage == "unstable":
        proof["metadata_observation"]["observation_complete"] = False
    elif damage == "wrong-root":
        proof["root_run_id"] = "foreign-root"

    def observe(*a):
        if damage == "active-after":
            monkeypatch.setattr(
                control,
                "graphql_runs",
                lambda *a, **kw: [root] if kw.get("job_name") else [{"status": "STARTED"}],
            )
        return copy.deepcopy(proof)

    monkeypatch.setattr(control, "observe_production_noop", observe)
    assert not control.production_noop_witness(value, root, tick)


def test_success_cache_reuses_immutable_witness_after_throttle_with_fresh_bindings(
    metadata, monkeypatch
):
    value, _, root, tick = metadata
    binding = control.digest(
        {"root": root, "tick": tick, "production": value["production_runtime_identity"]}
    )
    value["production_noop_cache"] = {
        root["runId"]: {"binding": binding, "proof": {"actual": True}, "next_attempt": 0}
    }
    monkeypatch.setattr(
        control, "observe_production_noop", lambda *a: pytest.fail("rehashed immutable witness")
    )
    assert control.production_noop_witness(value, root, tick)
    monkeypatch.setattr(control, "graphql_schedule", lambda *a: {"ticks": []})
    assert not control.production_noop_witness(value, root, tick)


def test_failed_metadata_probes_are_throttled_then_block_after_three(metadata, monkeypatch):
    value, _, root, tick = metadata
    calls = []
    monkeypatch.setattr(
        control, "observe_production_noop", lambda *a: calls.append(1) or {"status": "BLOCKED"}
    )
    clock = [0]
    monkeypatch.setattr(control.time, "monotonic", lambda: clock[0])
    for instant in (0, 31, 62):
        clock[0] = instant
        assert not control.production_noop_witness(value, root, tick)
        assert not control.production_noop_witness(value, root, tick)
    assert len(calls) == 3
    clock[0] = 93
    with pytest.raises(ValueError, match="three times"):
        control.production_noop_witness(value, root, tick)


def test_generic_empty_sensor_skip_does_not_spawn_metadata_reader(metadata, monkeypatch):
    value, _, root, tick = metadata
    monkeypatch.undo()
    monkeypatch.setattr(control, "inspect_container", lambda *a: {"Id": "unused"})
    # Exercise the actual public prefilter directly: it is not a NOOP authority.
    monkeypatch.setattr(
        control,
        "graphql_read",
        lambda *a: {
            "sensorOrError": {
                "__typename": "Sensor",
                "sensorState": {
                    "status": "RUNNING",
                    "ticks": [
                        {
                            "status": "SKIPPED",
                            "timestamp": 1000,
                            "runIds": [],
                            "skipReason": "Sensor function returned an empty result",
                        }
                    ],
                },
            }
        },
    )
    assert not control._public_noop_prefilter(value, {**root, "endTime": 1})
