from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.serena_worktree_bootstrap import bootstrap, derive_project_name  # noqa: E402

GIT_LOCAL_ENV_VARS = (
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CONFIG",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_OBJECT_DIRECTORY",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_IMPLICIT_WORK_TREE",
    "GIT_GRAFT_FILE",
    "GIT_INDEX_FILE",
    "GIT_NO_REPLACE_OBJECTS",
    "GIT_REPLACE_REF_BASE",
    "GIT_PREFIX",
    "GIT_SHALLOW_FILE",
    "GIT_COMMON_DIR",
)


def _make_worktree(tmp_path: Path, name: str = "6146") -> Path:
    worktree = tmp_path / "worktrees" / name / "trading advisor 3000"
    worktree.mkdir(parents=True)
    return worktree


def _without_git_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in GIT_LOCAL_ENV_VARS:
        env.pop(name, None)
    return env


def test_bootstrap_creates_ta3000_serena_metadata(tmp_path: Path) -> None:
    worktree = _make_worktree(tmp_path, "6146")
    serena_config = tmp_path / "home" / ".serena" / "serena_config.yml"
    serena_config.parent.mkdir(parents=True)
    serena_config.write_text("language_backend: LSP\n\nprojects:\n", encoding="utf-8")

    result = bootstrap(worktree=worktree, serena_config=serena_config)

    assert result.ok, result.errors
    project_yml = (worktree / ".serena" / "project.yml").read_text(encoding="utf-8")
    assert 'project_name: "trading-advisor-3000"' in project_yml
    assert "codex_ai_delivery_shell_package/**" in project_yml
    assert "Respect the dual-surface boundary" in project_yml
    assert (worktree / ".serena" / "project.local.yml").read_text(encoding="utf-8") == (
        '# Local Serena overrides for this worktree. Do not commit.\nproject_name: "ta3000-6146"\n'
    )
    assert str(worktree.resolve()) in serena_config.read_text(encoding="utf-8")


def test_bootstrap_repairs_generated_default_project_config(tmp_path: Path) -> None:
    worktree = _make_worktree(tmp_path, "49a3")
    serena_dir = worktree / ".serena"
    serena_dir.mkdir()
    bad_config = (
        'project_name: "trading advisor 3000"\n'
        "languages:\n"
        "- python\n"
        "ignored_paths: []\n"
        'initial_prompt: ""\n'
    )
    (serena_dir / "project.yml").write_text(bad_config, encoding="utf-8")

    result = bootstrap(worktree=worktree, register=False)

    assert result.ok, result.errors
    assert (serena_dir / "project.yml.bak").read_text(encoding="utf-8") == bad_config
    repaired = (serena_dir / "project.yml").read_text(encoding="utf-8")
    assert 'project_name: "trading-advisor-3000"' in repaired
    assert "graphify-out/**" in repaired
    assert 'project_name: "ta3000-49a3"' in (serena_dir / "project.local.yml").read_text(
        encoding="utf-8"
    )


def test_check_mode_reports_missing_metadata_without_writing(tmp_path: Path) -> None:
    worktree = _make_worktree(tmp_path, "work3")

    result = bootstrap(worktree=worktree, register=False, check_only=True)

    assert not result.ok
    assert any("missing Serena folder" in error for error in result.errors)
    assert not (worktree / ".serena").exists()


def test_derive_project_name_uses_worktree_id_for_repo_leaf(tmp_path: Path) -> None:
    worktree = _make_worktree(tmp_path, "work3")

    assert derive_project_name(worktree) == "ta3000-work3"


def test_post_checkout_hook_runs_bootstrap_from_worktree_root(tmp_path: Path) -> None:
    worktree = _make_worktree(tmp_path, "auto")
    clean_git_env = _without_git_env()
    subprocess.run(["git", "init", "-q"], cwd=worktree, env=clean_git_env, check=True)
    (worktree / "scripts").mkdir()
    (worktree / ".githooks").mkdir()
    shutil.copy2(ROOT / "scripts" / "serena_worktree_bootstrap.py", worktree / "scripts")
    shutil.copy2(ROOT / ".githooks" / "post-checkout", worktree / ".githooks" / "post-checkout")
    fake_home = tmp_path / "home"
    fake_serena_config = fake_home / ".serena" / "serena_config.yml"
    fake_serena_config.parent.mkdir(parents=True)
    fake_serena_config.write_text("language_backend: LSP\n\nprojects:\n", encoding="utf-8")
    env = _without_git_env()
    env["HOME"] = str(fake_home)
    env["USERPROFILE"] = str(fake_home)
    env["GIT_DIR"] = subprocess.run(
        ["git", "rev-parse", "--git-dir"],
        cwd=ROOT,
        env=clean_git_env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    env["GIT_WORK_TREE"] = str(ROOT)

    result = subprocess.run(
        [
            sys.executable,
            str(worktree / ".githooks" / "post-checkout"),
            "0" * 40,
            "1" * 40,
            "1",
        ],
        cwd=worktree,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    project_local = (worktree / ".serena" / "project.local.yml").read_text(encoding="utf-8")
    assert 'project_name: "ta3000-auto"' in project_local
    assert str(worktree.resolve()) in fake_serena_config.read_text(encoding="utf-8")


def test_post_checkout_hook_skips_regular_branch_checkout(tmp_path: Path) -> None:
    worktree = _make_worktree(tmp_path, "regular")
    subprocess.run(["git", "init", "-q"], cwd=worktree, env=_without_git_env(), check=True)
    (worktree / "scripts").mkdir()
    (worktree / ".githooks").mkdir()
    shutil.copy2(ROOT / "scripts" / "serena_worktree_bootstrap.py", worktree / "scripts")
    shutil.copy2(ROOT / ".githooks" / "post-checkout", worktree / ".githooks" / "post-checkout")

    result = subprocess.run(
        [
            sys.executable,
            str(worktree / ".githooks" / "post-checkout"),
            "1" * 40,
            "2" * 40,
            "1",
        ],
        cwd=worktree,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert not (worktree / ".serena").exists()
