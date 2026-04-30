from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts import run_historical_data_spark_proof as proof_runner
from scripts.run_historical_data_spark_proof import (
    _docker_exec_args,
    _docker_host_owner,
    _docker_runtime_root,
    _hostify_container_path,
)


def test_docker_host_owner_uses_host_uid_gid_when_available(monkeypatch) -> None:
    monkeypatch.setattr(os, "getuid", lambda: 1000, raising=False)
    monkeypatch.setattr(os, "getgid", lambda: 1001, raising=False)

    assert _docker_host_owner() == "1000:1001"


def test_docker_host_owner_is_empty_without_posix_ids(monkeypatch) -> None:
    monkeypatch.delattr(os, "getuid", raising=False)
    monkeypatch.delattr(os, "getgid", raising=False)

    assert _docker_host_owner() == ""


def test_docker_exec_args_wrap_python_command_with_post_run_chown(monkeypatch) -> None:
    monkeypatch.setattr(os, "getuid", lambda: 1000, raising=False)
    monkeypatch.setattr(os, "getgid", lambda: 1001, raising=False)

    args = _docker_exec_args(
        source=Path("tests/product-plane/fixtures/data_plane/raw_backfill_sample.jsonl"),
        output_dir=Path(".tmp/test-historical-data-proof"),
        contracts="BR-6.26,Si-6.26",
        spark_master="local[2]",
        output_json=Path(".tmp/test-historical-data-proof.json"),
    )

    assert args[:2] == ["/bin/sh", "-lc"]
    assert "python scripts/run_historical_data_spark_proof.py" in args[2]
    assert "chown -R 1000:1001" in args[2]


def test_docker_exec_args_fall_back_to_plain_python_command(monkeypatch) -> None:
    monkeypatch.delattr(os, "getuid", raising=False)
    monkeypatch.delattr(os, "getgid", raising=False)

    args = _docker_exec_args(
        source=Path("tests/product-plane/fixtures/data_plane/raw_backfill_sample.jsonl"),
        output_dir=Path(".tmp/test-historical-data-proof"),
        contracts="BR-6.26,Si-6.26",
        spark_master="local[2]",
        output_json=Path(".tmp/test-historical-data-proof.json"),
    )

    assert args[:2] == ["python", "scripts/run_historical_data_spark_proof.py"]


def test_docker_image_healthcheck_requires_pandas(monkeypatch) -> None:
    calls: list[list[str]] = []

    class Completed:
        returncode = 0

    def fake_run(args, **kwargs):
        calls.append(args)
        return Completed()

    monkeypatch.setattr(proof_runner.subprocess, "run", fake_run)

    assert proof_runner._docker_image_healthcheck("phase-proof")
    assert calls == [["docker", "run", "--rm", "phase-proof", "python", "-c", "import yaml; import pandas"]]


def test_hostify_container_path_maps_workspace_path_back_into_repo() -> None:
    host_path = Path(_hostify_container_path("/workspace/.tmp/test-historical-data-proof/canonical_bars.delta"))

    assert host_path.parts[-3:] == (".tmp", "test-historical-data-proof", "canonical_bars.delta")


def test_hostify_container_path_accepts_windows_like_separator_form() -> None:
    host_path = Path(_hostify_container_path("\\workspace\\.tmp\\test-historical-data-proof\\canonical_bars.delta"))

    assert host_path.parts[-3:] == (".tmp", "test-historical-data-proof", "canonical_bars.delta")


def test_docker_runtime_root_fails_closed_when_missing() -> None:
    with pytest.raises(RuntimeError, match="required"):
        _docker_runtime_root("   ")
