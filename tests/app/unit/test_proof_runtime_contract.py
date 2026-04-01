from __future__ import annotations

from pathlib import Path

import pytest

from scripts.proof_runtime_contract import (
    container_to_host_path,
    ensure_output_directory_writable,
    ensure_output_file_writable,
    host_to_container_path,
    normalize_runtime_root,
)


def test_runtime_root_rejects_missing_value() -> None:
    with pytest.raises(RuntimeError, match="required"):
        normalize_runtime_root("", field_name="docker runtime root")


def test_runtime_root_rejects_relative_path() -> None:
    with pytest.raises(RuntimeError, match="absolute container path"):
        normalize_runtime_root("tmp/ta3000", field_name="docker runtime root")


def test_output_directory_rejects_file_target(tmp_path: Path) -> None:
    blocked_path = tmp_path / "not-a-directory"
    blocked_path.write_text("x\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="expected directory"):
        ensure_output_directory_writable(blocked_path)


def test_output_file_rejects_directory_target(tmp_path: Path) -> None:
    blocked_path = tmp_path / "report.json"
    blocked_path.mkdir(parents=True, exist_ok=True)

    with pytest.raises(RuntimeError, match="file target"):
        ensure_output_file_writable(blocked_path)


def test_path_round_trip_normalizes_slashes_for_host_and_container(tmp_path: Path) -> None:
    repo_root = (tmp_path / "repo").resolve()
    artifact_path = repo_root / "nested" / "proof.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("{}\n", encoding="utf-8")

    container_path = host_to_container_path(artifact_path, repo_root=repo_root)
    assert container_path.endswith("/nested/proof.json")
    assert container_path.startswith("/workspace/")

    host_path = Path(container_to_host_path(container_path, repo_root=repo_root))
    assert host_path == artifact_path.resolve()

    windows_like_container = container_path.replace("/", "\\")
    host_path_windows_like = Path(container_to_host_path(windows_like_container, repo_root=repo_root))
    assert host_path_windows_like == artifact_path.resolve()
