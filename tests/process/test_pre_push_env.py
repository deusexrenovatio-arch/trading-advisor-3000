from __future__ import annotations

import os
import runpy
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_pre_push_cleans_git_context_and_preserves_gate_failure(monkeypatch):
    hook = runpy.run_path(str(ROOT / ".githooks" / "pre-push"))
    monkeypatch.setenv("GIT_DIR", "outer-repository.git")
    monkeypatch.setenv("GIT_WORK_TREE", "outer-worktree")
    monkeypatch.setenv("GIT_INDEX_FILE", "outer-index")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.bare")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "true")
    monkeypatch.setenv("AI_SHELL_TEST_CONTEXT", "retained")
    gate_call = {}

    def run(cmd, **kwargs):
        if cmd == ["git", "rev-parse", "--local-env-vars"]:
            return subprocess.CompletedProcess(
                cmd, 0, "GIT_DIR\nGIT_WORK_TREE\nGIT_INDEX_FILE\nGIT_CONFIG_COUNT\n"
            )
        gate_call.update(kwargs)
        assert "run_loop_gate.py" in cmd[1]
        return subprocess.CompletedProcess(cmd, 7)

    monkeypatch.setattr(hook["subprocess"], "run", run)
    assert hook["run_scoped_loop_gate"](ROOT) == 7
    env = gate_call.get("env", os.environ)
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_CONFIG_COUNT"):
        assert name not in env
    assert env["AI_SHELL_TEST_CONTEXT"] == "retained"
    assert gate_call["cwd"] == str(ROOT)
