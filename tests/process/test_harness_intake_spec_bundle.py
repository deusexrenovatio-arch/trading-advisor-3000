from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from scripts.harness.intake_spec_bundle import IntakeError, UnsupportedArtifactError, run_bundle_intake


def _build_zip(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)


def test_bundle_intake_happy_path_generates_manifest_and_text_cache(tmp_path: Path) -> None:
    zip_path = tmp_path / "sample_bundle.zip"
    members = {
        "requirements/spec.md": b"# Spec\n\nImplement secure intake.\n",
        "notes/context.txt": b"Context line 1\nContext line 2\n",
    }
    _build_zip(zip_path, members)

    registry_root = tmp_path / "registry"
    result = run_bundle_intake(
        input_zip=zip_path,
        registry_root=registry_root,
        run_id="RUN-WP02-HAPPY",
    )

    assert result.manifest_path.exists()
    assert result.text_cache_path.exists()
    assert result.workspace_root.exists()
    assert result.artifact_count == 2

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["run_id"] == "RUN-WP02-HAPPY"
    assert len(manifest["artifacts"]) == 2

    artifacts_by_relpath = {item["path"]: item for item in manifest["artifacts"]}
    for rel_path, raw in members.items():
        workspace_relpath = f"workspace/{rel_path}"
        assert workspace_relpath in artifacts_by_relpath
        artifact = artifacts_by_relpath[workspace_relpath]
        assert artifact["sha256"] == hashlib.sha256(raw).hexdigest()
        assert artifact["size_bytes"] == len(raw)
        assert artifact["extraction_status"] == "extracted"
        assert artifact["source_kind"] == "zip"
        workspace_file = result.run_root / workspace_relpath
        assert workspace_file.read_bytes() == raw

    cache_payload = json.loads(result.text_cache_path.read_text(encoding="utf-8"))
    assert cache_payload["run_id"] == "RUN-WP02-HAPPY"
    assert len(cache_payload["entries"]) == 2
    for entry in cache_payload["entries"]:
        text_path = result.run_root / entry["text_path"]
        assert text_path.exists()
        assert text_path.read_text(encoding="utf-8")


def test_bundle_intake_fail_closed_on_unsupported_member_type(tmp_path: Path) -> None:
    zip_path = tmp_path / "unsupported_bundle.zip"
    _build_zip(
        zip_path,
        {
            "payload.exe": b"MZ...",
            "requirements.md": b"# spec",
        },
    )

    registry_root = tmp_path / "registry"
    with pytest.raises(UnsupportedArtifactError):
        run_bundle_intake(input_zip=zip_path, registry_root=registry_root, run_id="RUN-WP02-UNSUPPORTED")

    assert not (registry_root / "intake" / "RUN-WP02-UNSUPPORTED").exists()


def test_bundle_intake_fail_closed_on_corrupted_zip(tmp_path: Path) -> None:
    broken_zip = tmp_path / "broken_bundle.zip"
    broken_zip.write_bytes(b"this is not a zip archive")

    registry_root = tmp_path / "registry"
    with pytest.raises(IntakeError):
        run_bundle_intake(input_zip=broken_zip, registry_root=registry_root, run_id="RUN-WP02-BROKEN")

    assert not (registry_root / "intake" / "RUN-WP02-BROKEN").exists()


def test_bundle_intake_fail_closed_on_path_traversal_member(tmp_path: Path) -> None:
    zip_path = tmp_path / "unsafe_bundle.zip"
    _build_zip(
        zip_path,
        {
            "../escape.md": b"escape",
        },
    )

    registry_root = tmp_path / "registry"
    with pytest.raises(IntakeError):
        run_bundle_intake(input_zip=zip_path, registry_root=registry_root, run_id="RUN-WP02-TRAVERSAL")

    assert not (registry_root / "intake" / "RUN-WP02-TRAVERSAL").exists()
