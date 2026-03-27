from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from codex_phase_orchestrator import orchestrate_current_phase, run_codex_prompt  # noqa: E402
from codex_phase_policy import RoleLaunchConfig  # noqa: E402


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _launch(model: str) -> RoleLaunchConfig:
    return RoleLaunchConfig(profile="", model=model, config_overrides=())


def _module_fixture(repo: Path) -> tuple[Path, Path]:
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)

    contract = repo / "docs/codex/contracts/demo.execution-contract.md"
    parent = repo / "docs/codex/modules/demo.parent.md"
    phase1 = repo / "docs/codex/modules/demo.phase-01.md"
    phase2 = repo / "docs/codex/modules/demo.phase-02.md"
    worker = repo / "docs/codex/prompts/phases/worker.md"
    acceptor = repo / "docs/codex/prompts/phases/acceptor.md"
    remediation = repo / "docs/codex/prompts/phases/remediation.md"

    _write(
        contract,
        """# Execution Contract

## Next Allowed Unit Of Work
- Execute phase 01 only: foundation work.
""",
    )
    _write(
        parent,
        """# Module Parent Brief

## Next Phase To Execute
- docs/codex/modules/demo.phase-01.md
""",
    )
    _write(
        phase1,
        """# Module Phase Brief

## Phase
- Name: Phase 01
- Status: planned

## Objective
- Build the foundation.
""",
    )
    _write(
        phase2,
        """# Module Phase Brief

## Phase
- Name: Phase 02
- Status: planned

## Objective
- Add enforcement.
""",
    )
    _write(worker, "worker prompt\n")
    _write(acceptor, "acceptor prompt\n")
    _write(remediation, "remediation prompt\n")
    return contract, parent


def test_orchestrator_pass_advances_next_phase(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass",
        max_remediation_cycles=2,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 0
    assert "Status: completed" in (tmp_path / "docs/codex/modules/demo.phase-01.md").read_text(encoding="utf-8")
    assert "- docs/codex/modules/demo.phase-02.md" in parent.read_text(encoding="utf-8")
    assert "Execute Phase 02 only: Add enforcement." in contract.read_text(encoding="utf-8")
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "accepted"
    assert payload["attempts_total"] == 1
    assert payload["route_mode"] == "governed-phase-orchestration"
    assert payload["attempts"][0]["acceptor_used_skills"][0] == "phase-acceptance-governor"


def test_orchestrator_block_then_pass_uses_remediation_loop(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="block-then-pass",
        max_remediation_cycles=2,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 0
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "accepted"
    assert payload["attempts_total"] == 2
    assert payload["attempts"][0]["verdict"] == "BLOCKED"
    assert payload["attempts"][1]["verdict"] == "PASS"
    assert any("unlock next phase" in item for item in payload["route_trace"])


def test_orchestrator_blocked_keeps_same_phase_locked(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="blocked",
        max_remediation_cycles=1,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 3
    assert "Status: blocked" in (tmp_path / "docs/codex/modules/demo.phase-01.md").read_text(encoding="utf-8")
    assert "- docs/codex/modules/demo.phase-01.md" in parent.read_text(encoding="utf-8")
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "blocked"
    assert payload["attempts_total"] == 2
    assert payload["route_trace"][-1] == "phase remains locked"


def test_orchestrator_auto_blocks_worker_shortcuts_even_if_acceptor_passes(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass-with-shortcut",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["final_status"] == "blocked"
    assert payload["attempts"][0]["policy_blockers_total"] >= 1
    acceptance_md = (
        tmp_path
        / "artifacts/codex/orchestration"
        / payload["run_id"]
        / "attempt-01"
        / "acceptance.md"
    ).read_text(encoding="utf-8")
    assert "Silent or unapproved fallback path" in acceptance_md


def test_orchestrator_auto_blocks_acceptance_evidence_gaps(tmp_path: Path) -> None:
    contract, parent = _module_fixture(tmp_path)
    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass-with-evidence-gap",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )
    assert code == 3
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["attempts"][0]["policy_blockers_total"] >= 1
    route_report = (
        tmp_path
        / "artifacts/codex/orchestration"
        / payload["run_id"]
        / "route-report.md"
    ).read_text(encoding="utf-8")
    assert "Route Mode: governed-phase-orchestration" in route_report
    assert "acceptor: model=gpt-5.4" in route_report


def test_skip_clean_check_still_uses_independent_git_snapshot_for_changed_files(
    monkeypatch, tmp_path: Path
) -> None:
    contract, parent = _module_fixture(tmp_path)
    independent_file = tmp_path / "docs/independent-proof.md"
    _write(independent_file, "independent diff source\n")

    def fake_run_role(**kwargs: object) -> dict[str, object]:
        role = kwargs["role"]
        if role in {"worker", "remediation"}:
            return {
                "status": "DONE",
                "summary": "Synthetic worker output.",
                "route_signal": f"{role}:phase-only",
                "files_touched": ["docs/example.md", "scripts/example.py"],
                "checks_run": [],
                "remaining_risks": [],
                "assumptions": [],
                "skips": [],
                "fallbacks": [],
                "deferred_work": [],
            }
        return {
            "verdict": "PASS",
            "summary": "Synthetic acceptance pass.",
            "route_signal": "acceptance:governed-phase-route",
            "used_skills": [
                "phase-acceptance-governor",
                "architecture-review",
                "testing-suite",
                "docs-sync",
            ],
            "blockers": [],
            "rerun_checks": [],
            "evidence_gaps": [],
            "prohibited_findings": [],
        }

    monkeypatch.setattr("codex_phase_orchestrator.run_role", fake_run_role)

    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="simulate",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin=None,
        simulate_scenario="pass",
        max_remediation_cycles=0,
        ignore_globs=(),
        skip_clean_check=True,
    )

    assert code == 0
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    changed_files_payload = json.loads(
        (
            tmp_path
            / "artifacts/codex/orchestration"
            / payload["run_id"]
            / "attempt-01"
            / "changed-files.json"
        ).read_text(encoding="utf-8")
    )
    assert "docs/independent-proof.md" in changed_files_payload["changed_files"]
    assert "docs/example.md" not in changed_files_payload["changed_files"]
    assert "scripts/example.py" not in changed_files_payload["changed_files"]


def test_codex_cli_route_owned_runtime_artifacts_do_not_break_worker_contract(
    monkeypatch, tmp_path: Path
) -> None:
    contract, parent = _module_fixture(tmp_path)
    _write(tmp_path / ".runlogs/codex-governed-entry/last-route.json", "{}\n")
    _write(tmp_path / "artifacts/codex/orchestration/route-owned.tmp", "orchestrator owned\n")

    def fake_run_codex_prompt(**kwargs: object) -> int:
        output_path = Path(str(kwargs["output_path"]))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.name.startswith("acceptor"):
            output_path.write_text(
                "\n".join(
                    [
                        "Acceptor summary",
                        "BEGIN_PHASE_ACCEPTANCE_JSON",
                        json.dumps(
                            {
                                "verdict": "PASS",
                                "summary": "Codex backend marker contract satisfied.",
                                "route_signal": "acceptance:governed-phase-route",
                                "used_skills": [
                                    "phase-acceptance-governor",
                                    "architecture-review",
                                    "testing-suite",
                                    "docs-sync",
                                ],
                                "blockers": [],
                                "rerun_checks": [],
                                "evidence_gaps": [],
                                "prohibited_findings": [],
                            }
                        ),
                        "END_PHASE_ACCEPTANCE_JSON",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            return 0

        output_path.write_text(
            "\n".join(
                [
                    "Worker summary",
                    "BEGIN_PHASE_WORKER_JSON",
                    json.dumps(
                        {
                            "status": "DONE",
                            "summary": "Codex backend marker contract satisfied.",
                            "route_signal": "worker:phase-only",
                            "files_touched": ["docs/codex/modules/demo.phase-01.md"],
                            "checks_run": ["tests/process/test_codex_phase_orchestrator.py"],
                            "remaining_risks": [],
                            "assumptions": [],
                            "skips": [],
                            "fallbacks": [],
                            "deferred_work": [],
                        }
                    ),
                    "END_PHASE_WORKER_JSON",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr("codex_phase_orchestrator.run_codex_prompt", fake_run_codex_prompt)

    code, state_path = orchestrate_current_phase(
        repo_root=tmp_path,
        execution_contract_path=contract,
        parent_path=parent,
        worker_prompt_path=tmp_path / "docs/codex/prompts/phases/worker.md",
        acceptor_prompt_path=tmp_path / "docs/codex/prompts/phases/acceptor.md",
        remediation_prompt_path=tmp_path / "docs/codex/prompts/phases/remediation.md",
        artifact_root=tmp_path / "artifacts/codex/orchestration",
        backend="codex-cli",
        worker_launch=_launch("gpt-5.3-codex"),
        acceptor_launch=_launch("gpt-5.4"),
        remediation_launch=_launch("gpt-5.3-codex"),
        codex_bin="codex",
        simulate_scenario="pass",
        max_remediation_cycles=0,
        ignore_globs=(".runlogs/**", "artifacts/codex/**", "docs/codex/packages/inbox/*.zip"),
        skip_clean_check=True,
    )

    assert code == 0
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    changed_files_payload = json.loads(
        (
            tmp_path
            / "artifacts/codex/orchestration"
            / payload["run_id"]
            / "attempt-01"
            / "changed-files.json"
        ).read_text(encoding="utf-8")
    )
    assert "artifacts/codex/orchestration/route-owned.tmp" not in changed_files_payload["changed_files"]
    assert ".runlogs/codex-governed-entry/last-route.json" not in changed_files_payload["changed_files"]


def test_run_codex_prompt_forces_utf8_encoding(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_subprocess_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured.update(kwargs)
        return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="", stderr="")

    monkeypatch.setattr("codex_phase_orchestrator.subprocess.run", fake_subprocess_run)

    exit_code = run_codex_prompt(
        repo_root=tmp_path,
        prompt="worker prompt",
        launch=_launch("gpt-5.3-codex"),
        output_path=tmp_path / "last-message.txt",
        codex_bin="codex",
    )

    assert exit_code == 0
    assert captured["text"] is True
    assert captured["encoding"] == "utf-8"
