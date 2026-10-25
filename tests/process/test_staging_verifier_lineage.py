"""Git-only equivalence checks; no runtime operations or producer compute."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402
from build_staging_candidate import git  # noqa: E402


@pytest.fixture
def equivalent_merge(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-qb", "main")
    git(root, "config", "user.name", "Verifier fixture")
    git(root, "config", "user.email", "fixture@localhost")
    (root / "pyproject.toml").write_text("[project]\nname='fixture'\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "base")
    base = git(root, "rev-parse", "HEAD")
    git(root, "switch", "-qc", "foundation")
    (root / "scripts").mkdir()
    (root / "scripts/verifier.py").write_text("VALUE = 1\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "foundation")
    raw = git(root, "rev-parse", "HEAD")
    git(root, "switch", "-q", "main")
    git(root, "merge", "--no-ff", "-qm", "foundation merge", "foundation")
    merged = git(root, "rev-parse", "HEAD")
    assert raw != merged and git(root, "rev-parse", raw + "^{tree}") == git(
        root, "rev-parse", merged + "^{tree}"
    )
    monkeypatch.setattr(control, "ROOT", root)
    return root, {"verifier_revision": raw}, base, merged


def test_identical_merge_is_historical_only_and_original_plan_stays_pinned(equivalent_merge):
    _, plan, _, merged = equivalent_merge
    assert control._verifier_preflight(plan, historical_observation=True) == merged
    with pytest.raises(ValueError, match="pinned revision"):
        control._verifier_preflight(plan)


@pytest.mark.parametrize(
    "name",
    [
        "scripts/verifier.py",
        "configs/settings.json",
        "pyproject.toml",
        "uv.lock",
        "requirements-extra.txt",
    ],
)
def test_historical_merge_rejects_any_executable_config_or_dependency_change(
    equivalent_merge, name
):
    root, plan, _, _ = equivalent_merge
    path = root / name
    path.parent.mkdir(exist_ok=True)
    path.write_text("CHANGED = 2\n")
    git(root, "add", ".")
    git(root, "commit", "-qm", "changed closure")
    with pytest.raises(ValueError, match="closure changed"):
        control._verifier_preflight(plan, historical_observation=True)


def test_historical_rejects_dirty_dependencies_and_non_descendant_verifier(equivalent_merge):
    root, plan, base, _ = equivalent_merge
    (root / "requirements-untracked.txt").write_text("changed\n")
    with pytest.raises(ValueError, match="unpinned"):
        control._verifier_preflight(plan, historical_observation=True)
    (root / "requirements-untracked.txt").unlink()
    (root / "pyproject.toml").write_text("changed\n")
    with pytest.raises(ValueError, match="unpinned"):
        control._verifier_preflight(plan, historical_observation=True)
    git(root, "reset", "--hard", base)
    with pytest.raises(ValueError):
        control._verifier_preflight(plan, historical_observation=True)


@pytest.mark.parametrize("committed", [False, True])
def test_rename_out_of_executable_closure_cannot_hide_deleted_verifier(equivalent_merge, committed):
    root, plan, _, _ = equivalent_merge
    (root / "docs").mkdir()
    git(root, "mv", "scripts/verifier.py", "docs/verifier.md")
    if committed:
        git(root, "commit", "-qm", "renamed to docs")
    with pytest.raises(ValueError, match="closure changed|unpinned"):
        control._verifier_preflight(plan, historical_observation=True)
