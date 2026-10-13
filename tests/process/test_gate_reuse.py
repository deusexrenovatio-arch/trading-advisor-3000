from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
import gate_reuse as reuse  # noqa: E402

FAST = "python scripts/run_boring_checks.py --profile quick --scope changed"


@pytest.fixture
def runner(tmp_path, monkeypatch):
    calls = []
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setattr(reuse, "run_command", lambda command: calls.append(command) or 0)
    monkeypatch.setattr(
        reuse,
        "_fingerprint",
        lambda root, scope, command: hashlib.sha256(
            json.dumps(scope, sort_keys=True).encode()
        ).hexdigest(),
    )
    return tmp_path, calls


def test_success_reused_but_new_scope_executes(runner):
    root, calls = runner
    for profile in ["none", "none", "ops"]:
        assert reuse.run_loop_commands([FAST], scope={"profile": profile}, root=root) == (0, None)
    assert len(calls) == 2


@pytest.mark.parametrize("mode", ["ci", "force", "non_fast"])
def test_ci_force_and_non_fast_checks_always_run(runner, monkeypatch, mode):
    root, calls = runner
    command = FAST
    enabled = True
    if mode == "ci":
        monkeypatch.setenv("CI", "true")
    if mode == "force":
        enabled = False
    if mode == "non_fast":
        command = "python scripts/validate_stack_conformance.py"
    for _ in range(2):
        reuse.run_loop_commands([command], scope={}, root=root, reuse=enabled)
    assert len(calls) == 2


def test_failed_check_is_not_cached(runner, monkeypatch):
    root, calls = runner
    monkeypatch.setattr(reuse, "run_command", lambda command: calls.append(command) or 1)
    for _ in range(2):
        assert reuse.run_loop_commands([FAST], scope={}, root=root)[0] == 1
    assert len(calls) == 2
    assert not (root / ".tmp" / "gate-reuse").exists()


@pytest.mark.parametrize("corruption", ["invalid", "expired", "wrong_key"])
def test_bad_receipt_reexecutes(runner, corruption):
    root, calls = runner
    reuse.run_loop_commands([FAST], scope={}, root=root)
    path = next((root / ".tmp" / "gate-reuse").glob("*.json"))
    if corruption == "invalid":
        path.write_text("broken")
    else:
        saved = json.loads(path.read_text())
        saved["created_at" if corruption == "expired" else "key"] = 0
        path.write_text(json.dumps(saved))
    reuse.run_loop_commands([FAST], scope={}, root=root)
    assert len(calls) == 2


def test_inputs_changed_during_check_are_not_cached(runner, monkeypatch):
    root, calls = runner
    monkeypatch.setattr(reuse, "_fingerprint", lambda *args: str(len(calls)))
    reuse.run_loop_commands([FAST], scope={}, root=root)
    assert not (root / ".tmp" / "gate-reuse").exists()


def test_real_key_changes_with_source_scope_command_env_and_dependencies(tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    source = tmp_path / "check.py"
    source.write_text("initial")
    subprocess.run(["git", "-C", str(tmp_path), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "initial",
        ],
        check=True,
    )
    original = reuse._fingerprint(tmp_path, {}, FAST)
    subprocess.run(["git", "-C", str(tmp_path), "branch", "proof-base"], check=True)
    selected = reuse._fingerprint(tmp_path, {"base_ref": "proof-base"}, FAST)
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "next",
        ],
        check=True,
    )
    with_old_ref = reuse._fingerprint(tmp_path, {"base_ref": "proof-base"}, FAST)
    subprocess.run(["git", "-C", str(tmp_path), "branch", "-f", "proof-base", "HEAD"], check=True)
    assert reuse._fingerprint(tmp_path, {"base_ref": "proof-base"}, FAST) != with_old_ref
    assert selected != with_old_ref
    original = reuse._fingerprint(tmp_path, {}, FAST)
    source.write_text("changed")
    assert reuse._fingerprint(tmp_path, {}, FAST) != original
    source.write_text("initial")
    assert reuse._fingerprint(tmp_path, {"profile": "ops"}, FAST) != original
    assert reuse._fingerprint(tmp_path, {}, FAST + " --stdin") != original
    monkeypatch.setenv("GATE_REGRESSION_SENTINEL", "changed")
    assert reuse._fingerprint(tmp_path, {}, FAST) != original
    monkeypatch.delenv("GATE_REGRESSION_SENTINEL")
    monkeypatch.setattr(reuse.importlib.metadata, "distributions", lambda: [])
    assert reuse._fingerprint(tmp_path, {}, FAST) != original


def test_pr_gate_forwards_force_fresh_flag():
    from run_pr_gate import _build_loop_gate_command

    command = _build_loop_gate_command(
        mapping="mapping.yaml",
        from_git=True,
        git_ref="HEAD",
        base_ref=None,
        head_ref=None,
        explicit_changed_files=None,
        snapshot_mode="changed-files",
        profile="none",
        no_reuse=True,
    )
    assert "--no-reuse" in command


def test_pr_ci_cancellation_is_bound_to_workflow_and_pr():
    workflow = (SCRIPTS.parent / ".github/workflows/ci.yml").read_text()
    assert "group: ${{ github.workflow }}-pr-${{ github.event.pull_request.number }}" in workflow
    assert "cancel-in-progress: true" in workflow
    assert "scripts/run_pr_gate.py" in workflow


@pytest.mark.parametrize(
    "command",
    [
        FAST + " --fix",
        FAST + " && spark-submit proof.py",
        "python other.py " + FAST,
        FAST + " --scope",
    ],
)
def test_non_pure_or_invalid_commands_cannot_be_reused(command):
    assert not reuse._eligible(command)
