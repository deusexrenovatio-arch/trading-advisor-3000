"""Guards for source-only host-control changes; tests use temporary files and mock Docker."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


def prepared(tmp_path):
    paths = {key: tmp_path / key for key in ("HOST_STATE", "CONTROL_ROOT")}
    return {
        "project": "ta3000-branch-fixture",
        "plan": {"stage": "branch", "task_id": "fixture"},
        "paths": paths,
        "production_mounts": [tmp_path / "production"],
        "production_url": "http://127.0.0.1:3000/graphql",
        "deadline": control.datetime(2030, 1, 1, tzinfo=control.UTC),
        "host_identity_checked": True,
    }


def test_permit_requires_the_live_exclusive_host_lease(tmp_path):
    value = prepared(tmp_path)
    with pytest.raises(ValueError, match="owned host lease"):
        control.renew_permit(value, None)
    assert not (value["paths"]["CONTROL_ROOT"] / "execution-permit.json").exists()
    lease = control.acquire_heavy(value)
    try:
        control.renew_permit(value, lease)
        permit = control.json.loads(
            (value["paths"]["CONTROL_ROOT"] / "execution-permit.json").read_text()
        )
        assert permit["lease_nonce"] == lease.nonce
        assert control.datetime.fromisoformat(
            permit["deadline_utc"]
        ) - control.datetime.fromisoformat(permit["issued_at_utc"]) <= control.timedelta(seconds=30)
    finally:
        control.release_heavy(value, lease)
    assert not (value["paths"]["CONTROL_ROOT"] / "execution-permit.json").exists()


def test_permit_cannot_write_under_an_actual_production_mount(tmp_path):
    value = prepared(tmp_path)
    value["paths"]["CONTROL_ROOT"] = value["production_mounts"][0] / "nested"
    with pytest.raises(ValueError, match="production"):
        control.acquire_heavy(value)
    assert not value["production_mounts"][0].exists()


def test_branch_control_target_denies_production_roles_and_foreign_projects(tmp_path, monkeypatch):
    value = prepared(tmp_path)
    runtime = {
        "Id": "own",
        "Config": {
            "Labels": {
                "com.docker.compose.project": "production",
                "com.docker.compose.service": "webserver",
            }
        },
    }
    monkeypatch.setattr(control, "inspect_container", lambda _: runtime)
    monkeypatch.setattr(control, "verify_runtime", lambda *args: None)
    with pytest.raises(ValueError, match="own branch"):
        control.branch_control_target(value, {"Id": "own"})
    value["plan"]["stage"] = "acceptance"
    with pytest.raises(ValueError, match="branch"):
        control.branch_control_target(value, {"Id": "own"})


def test_branch_control_target_denies_the_actual_production_endpoint(tmp_path, monkeypatch):
    value = prepared(tmp_path)
    runtime = {
        "Id": "own",
        "Config": {
            "Labels": {
                "com.docker.compose.project": value["project"],
                "com.docker.compose.service": "webserver",
            }
        },
        "NetworkSettings": {"Ports": {"3000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "3000"}]}},
    }
    monkeypatch.setattr(control, "inspect_container", lambda _: runtime)
    monkeypatch.setattr(control, "verify_runtime", lambda *args: None)
    with pytest.raises(ValueError, match="production endpoint"):
        control.branch_control_target(value, runtime)


def test_runtime_readback_rejects_actual_graphql_mutation():
    with pytest.raises(ValueError, match="read-only"):
        control.graphql_read(
            "http://127.0.0.1:1", 'mutation Forbidden { stopRunningSchedule(id:"x") {__typename} }'
        )
