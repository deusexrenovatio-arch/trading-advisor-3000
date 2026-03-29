from __future__ import annotations

import os

from scripts.run_phase2a_spark_proof import _docker_user_args


def test_docker_user_args_use_host_uid_gid_when_available(monkeypatch) -> None:
    monkeypatch.setattr(os, "getuid", lambda: 1000, raising=False)
    monkeypatch.setattr(os, "getgid", lambda: 1001, raising=False)

    assert _docker_user_args() == ["--user", "1000:1001"]


def test_docker_user_args_are_empty_without_posix_ids(monkeypatch) -> None:
    monkeypatch.delattr(os, "getuid", raising=False)
    monkeypatch.delattr(os, "getgid", raising=False)

    assert _docker_user_args() == []
