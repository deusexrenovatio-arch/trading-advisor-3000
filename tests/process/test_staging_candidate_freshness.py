import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


def context(tmp_path):
    return {
        "plan": {"stage": "acceptance", "candidate": {"base_sha": "a" * 40}},
        "paths": {"CANDIDATE_WORKTREE": tmp_path},
    }


@pytest.mark.parametrize("remote", ["b" * 40 + "\trefs/heads/main\n", ""])
def test_stale_or_unknown_remote_main_cannot_receive_first_permit(tmp_path, monkeypatch, remote):
    prepared, permits = context(tmp_path), []
    monkeypatch.setattr(control, "checked", lambda *a, **kw: remote)
    monkeypatch.setattr(control, "renew_permit", lambda *a: permits.append(1))
    with pytest.raises(ValueError, match="recompose"):
        control.renew_candidate_permit(prepared, object(), {"runId": "queued", "status": "QUEUED"})
    assert not permits and not prepared["fresh_main_roots"]


def test_remote_pass_cached_per_root_and_started_branch_reference_are_unchanged(
    tmp_path, monkeypatch
):
    prepared, queries, permits = context(tmp_path), [], []

    def remote(*args, **kwargs):
        assert args[-3:] == ("ls-remote", "origin", "refs/heads/main") and kwargs["timeout"] == 30
        queries.append(1)
        return "a" * 40 + "\trefs/heads/main\n"

    monkeypatch.setattr(control, "checked", remote)
    monkeypatch.setattr(control, "renew_permit", lambda *a: permits.append(1))
    root = {"runId": "queued", "status": "QUEUED"}
    control.renew_candidate_permit(prepared, object(), root)
    control.renew_candidate_permit(prepared, object(), root)
    control.renew_candidate_permit(
        prepared, object(), {"runId": "already-charged", "status": "STARTED"}
    )
    prepared["plan"]["mode"] = "reference"
    control.renew_candidate_permit(prepared, object(), root)
    prepared["plan"].update(stage="branch", mode="candidate")
    control.renew_candidate_permit(prepared, object(), root)
    assert len(queries) == 1 and len(permits) == 5
