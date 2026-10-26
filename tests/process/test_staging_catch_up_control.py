import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


def prepared(tmp_path, monkeypatch):
    paths = {
        key: tmp_path / key
        for key in ("CONTROL_ROOT", "HOST_STATE", "QUOTA_ROOT", "VERIFIER_WORKTREE")
    }
    for path in paths.values():
        path.mkdir()
    value = {
        "plan": {"stage": "acceptance", "candidate": {"image_digest": "sha256:" + "b" * 64}},
        "paths": paths,
        "project": "ta3000-acceptance",
        "deadline": datetime.now(UTC) + timedelta(minutes=5),
        "config": {"services": {"webserver": {"mem_limit": 3 * 1024**3}}},
        "values": {control.PREFIX + "SCRATCH_BYTES": str(256 * 1024**2)},
    }
    web = {
        "Id": "own-web",
        "Image": value["plan"]["candidate"]["image_digest"],
        "Mounts": [
            {"Destination": "/opt/dagster/dagster_home", "Name": "ta3000-acceptance_staging-home"},
        ],
    }
    daemon = {
        **web,
        "Id": "own-daemon",
        "State": {"Running": False},
        "Config": {
            "Labels": {"com.docker.compose.service": "daemon"},
        },
    }
    monkeypatch.setattr(control, "runtime_url", lambda *a: "own-url")
    monkeypatch.setattr(control, "runtime_services", lambda *a, **kw: (web, [daemon]))
    monkeypatch.setattr(control, "verify_runtime", lambda *a, **kw: None)
    monkeypatch.setattr(
        control, "inspect_container", lambda cid: daemon if cid == "own-daemon" else web
    )
    monkeypatch.setattr(control, "watch_limits", lambda *a: None)
    monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [])
    monkeypatch.setattr(
        control, "_owned_lease", lambda *a: paths["CONTROL_ROOT"] / "execution-permit.json"
    )
    monkeypatch.setattr(control, "daemon_state", lambda *a: None)
    return value, web, SimpleNamespace(nonce="a" * 32)


@pytest.mark.parametrize("stage,mode", [("branch", "candidate"), ("acceptance", "reference")])
def test_catch_up_never_initializes_branch_or_reference(tmp_path, monkeypatch, stage, mode):
    value, web, lease = prepared(tmp_path, monkeypatch)
    value["plan"].update(stage=stage, mode=mode)
    monkeypatch.setattr(control, "daemon_state", lambda *a: pytest.fail("started daemon"))
    with pytest.raises(ValueError, match="candidate acceptance"):
        control.initialize_catch_up(value, web, lease, registration_seconds=0)


@pytest.mark.parametrize("active,permit", [(True, False), (False, True)])
def test_active_work_or_existing_permit_forbids_initialization(
    tmp_path, monkeypatch, active, permit
):
    value, web, lease = prepared(tmp_path, monkeypatch)
    if active:
        monkeypatch.setattr(control, "graphql_runs", lambda *a, **kw: [{"status": "STARTED"}])
    if permit:
        (value["paths"]["CONTROL_ROOT"] / "execution-permit.json").write_text("{}")
    monkeypatch.setattr(control, "daemon_state", lambda *a: pytest.fail("started daemon"))
    with pytest.raises(ValueError, match="permit|active"):
        control.initialize_catch_up(value, web, lease, registration_seconds=0)


def test_initializer_is_capped_no_dataset_and_only_own_home_is_writable(tmp_path, monkeypatch):
    value, web, lease = prepared(tmp_path, monkeypatch)
    calls, states = [], []
    monkeypatch.setattr(control, "daemon_state", lambda p, enabled: states.append(enabled))

    def run(*cmd, **kw):
        calls.append((cmd, kw))
        return SimpleNamespace(
            returncode=1 if cmd[1] == "inspect" else 0,
            stdout=json.dumps(
                {
                    "status": "INITIALIZED_NATIVE_CHECKPOINT",
                    "compute_attempts": 0,
                    "plan_sha256": control.digest(value["plan"]),
                }
            ),
        )

    monkeypatch.setattr(control, "run", run)
    control.initialize_catch_up(value, web, lease, registration_seconds=0)
    command, options = calls[0]
    assert states == [True, False]
    assert command[:2] == ("docker", "run") and options["timeout"] <= 25
    mounts = [command[i + 1] for i, part in enumerate(command) if part == "--mount"]
    assert not any(
        "/workspace" in mount or "/ta3000-data" in mount or "docker.sock" in mount
        for mount in mounts
    )
    writable = [mount for mount in mounts if not mount.endswith(",readonly")]
    assert writable == [
        "type=volume,source=ta3000-acceptance_staging-home,target=/opt/dagster/dagster_home"
    ]
    assert "--apply" in command and "--read-only" in command and "no-new-privileges" in command
    context = json.loads(
        (value["paths"]["CONTROL_ROOT"] / "schedule-initialization.json").read_text()
    )
    assert context["daemon_stopped"] is True and context["lease_nonce"] == lease.nonce
    assert (
        datetime.fromisoformat(context["expires_at_utc"])
        - datetime.fromisoformat(context["verified_at_utc"])
    ).total_seconds() <= 30
    assert (value["paths"]["CONTROL_ROOT"] / "schedule-initialization-result.json").is_file()
    with pytest.raises(ValueError, match="recovery"):
        control.initialize_catch_up(value, web, lease, registration_seconds=0)


def test_foreign_runtime_or_expired_lease_cannot_reach_initializer(tmp_path, monkeypatch):
    value, web, lease = prepared(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(control, "run", lambda *a, **kw: calls.append(a))
    monkeypatch.setattr(
        control,
        "verify_runtime",
        lambda *a, **kw: (_ for _ in ()).throw(ValueError("foreign runtime")),
    )
    with pytest.raises(ValueError, match="foreign"):
        control.initialize_catch_up(value, web, lease, registration_seconds=0)
    assert not calls
    monkeypatch.setattr(control, "verify_runtime", lambda *a, **kw: None)
    value["deadline"] = datetime.now(UTC) - timedelta(seconds=1)
    with pytest.raises(ValueError, match="expired"):
        control.initialize_catch_up(value, web, lease, registration_seconds=0)
    assert not calls
