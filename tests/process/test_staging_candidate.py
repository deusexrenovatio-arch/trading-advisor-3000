from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from build_staging_candidate import (  # noqa: E402
    build_candidate,
    executable_fingerprint,
    execution_manifest,
    git,
    verify_deployed_files,
)


@pytest.fixture
def repository(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    git(root, "config", "user.name", "Staging test")
    git(root, "config", "user.email", "staging-test@localhost")
    (root / "src").mkdir()
    (root / "src/app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "pyproject.toml").write_text("[project]\nname='test'\n", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-qm", "base")
    return root, git(root, "rev-parse", "HEAD")


def commit(root, name, content):
    (root / name).write_text(content, encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-qm", "change " + name)
    return git(root, "rev-parse", "HEAD")


def test_real_batch_preserves_feature_checkout_and_binds_result(repository):
    root, base = repository
    first = commit(root, "src/app.py", "VALUE = 2\n")
    git(root, "checkout", "--detach", base)
    second = commit(root, "src/second.py", "OTHER = 3\n")
    before = git(root, "rev-parse", "HEAD")
    result = build_candidate(
        root,
        base_ref=base,
        branch_refs=[first, second],
        candidate_ref="refs/heads/integration",
        image_digest="sha256:" + "1" * 64,
    )["candidate"]
    assert git(root, "rev-parse", "HEAD") == before
    assert git(root, "rev-parse", "refs/heads/integration^{tree}") == result["tree_sha"]
    assert git(root, "show", result["head_sha"] + ":src/app.py") == "VALUE = 2"
    for head in (base, first, second):
        git(root, "merge-base", "--is-ancestor", head, result["head_sha"])
    with pytest.raises(ValueError):
        build_candidate(
            root,
            base_ref=base,
            branch_refs=[first],
            candidate_ref="refs/heads/integration",
            image_digest="sha256:" + "1" * 64,
        )


def test_conflict_and_dirty_checkout_refuse_before_landing(repository):
    root, base = repository
    first = commit(root, "src/app.py", "VALUE = 2\n")
    git(root, "checkout", "--detach", base)
    second = commit(root, "src/app.py", "VALUE = 3\n")
    with pytest.raises(ValueError, match="merge-tree"):
        build_candidate(
            root,
            base_ref=base,
            branch_refs=[first, second],
            candidate_ref="refs/heads/conflict",
            image_digest="sha256:" + "1" * 64,
        )
    assert (
        subprocess.run(
            ["git", "show-ref", "--verify", "refs/heads/conflict"], cwd=root, capture_output=True
        ).returncode
        != 0
    )
    (root / "src/app.py").write_text("DIRTY = True\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clean"):
        build_candidate(
            root,
            base_ref=base,
            branch_refs=[first],
            candidate_ref="refs/heads/dirty",
            image_digest="sha256:" + "1" * 64,
        )


def test_docs_reuse_and_actual_mounted_code_comparison(repository):
    root, base = repository
    docs = commit(root, "README.md", "Documentation\n")
    assert executable_fingerprint(root, base) == executable_fingerprint(root, docs)
    manifest = execution_manifest(root, docs)
    (root / "src/app.py").write_bytes(b"VALUE = 1\r\n")
    assert verify_deployed_files(root, manifest) == executable_fingerprint(root, docs)
    (root / "src/rogue.py").write_text("BAD = True\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Unpinned"):
        verify_deployed_files(root, manifest)
