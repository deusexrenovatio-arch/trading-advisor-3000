import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


@pytest.mark.parametrize("mode", ["observe", "reattest", "branch"])
def test_collector_is_a_separate_capped_read_only_container(tmp_path, monkeypatch, mode):
    calls = []

    def run(*command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=1 if command[1] == "inspect" else 0, stdout="{}")

    monkeypatch.setattr(control, "run", run)
    monkeypatch.setenv("TA3000_STAGING_OPERATOR_PRIVATE_KEY", "never-pass-private-key")
    paths = {
        key: tmp_path / key
        for key in (
            "CANDIDATE_WORKTREE",
            "VERIFIER_WORKTREE",
            "CONTROL_ROOT",
            "QUOTA_ROOT",
            "DATA_ROOT",
        )
    }
    prepared = {
        "plan": {
            "data_root": "/ta3000-data/moex-historical",
            "candidate": {"image_digest": "sha256:" + "b" * 64},
        },
        "paths": paths,
        "project": "ta3000-acceptance",
        "config": {"services": {"webserver": {"mem_limit": str(4 * 1024**3)}}},
        "values": {control.PREFIX + "SCRATCH_BYTES": str(1024**3)},
    }
    web = {"Mounts": [{"Destination": "/opt/dagster/dagster_home", "Name": "own-home"}]}
    control.observe_protected(prepared, web, timeout=30, mode=mode)
    command = calls[0]
    assert command[:2] == ("docker", "run")
    assert "--read-only" in command and "--cap-drop" in command
    assert "no-new-privileges" in command and "-I" in command
    assert "never-pass-private-key" not in command and "docker.sock" not in " ".join(command)
    mounts = [command[index + 1] for index, value in enumerate(command) if value == "--mount"]
    assert all(mount.endswith(",readonly") for mount in mounts)
    assert any("source=own-home,target=/opt/dagster/dagster_home" in mount for mount in mounts)
    assert command[command.index("--memory") + 1] == str(2 * 1024**3)
    assert "/staging-verifier/scripts/collect_staging_acceptance.py" in command
    if mode == "reattest":
        assert command[-2:] == ("--original-receipt", "/staging-control/original-receipt.json")
