from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_contracts(root: Path) -> tuple[Path, Path]:
    execution_contract = root / "docs/codex/contracts/demo.execution-contract.md"
    phase_brief = root / "docs/codex/modules/demo.phase-05.md"
    _write(
        execution_contract,
        """# Execution Contract

## Release Target Contract
- Target Decision: ALLOW_RELEASE_READINESS
- Target Environment: governed pipeline
- Forbidden Proof Substitutes: docs-only
- Release-Ready Proof Class: live-real
""",
    )
    _write(
        phase_brief,
        """# Module Phase Brief

## Release Gate Impact
- Surface Transition: enforcement contour
- Minimum Proof Class: live-real
- Accepted State Label: release_decision

## Release Surface Ownership
- Owned Surfaces: enforcement_serialization_contour
- Delivered Proof Class: live-real
- Required Real Bindings: route markers, lock evidence
- Target Downgrade Is Forbidden: yes
""",
    )
    return execution_contract, phase_brief


def _write_summary(path: Path, *, snapshot_mode: str, profile: str) -> None:
    _write(
        path,
        f"""## gate

- Snapshot mode: `{snapshot_mode}`
- Profile: `{profile}`
""",
    )


def _write_acceptance(path: Path, *, verdict: str = "PASS") -> None:
    _write(
        path,
        json.dumps(
            {
                "verdict": verdict,
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
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
    )


def _write_mutation_events(path: Path) -> None:
    payloads = [
        {"event": "acquire_started"},
        {"event": "acquired"},
        {"event": "released"},
    ]
    _write(path, "\n".join(json.dumps(item, ensure_ascii=False) for item in payloads) + "\n")


def _run_build(tmp_path: Path, *, route_profile: str, pr_profile: str) -> dict[str, object]:
    execution_contract, phase_brief = _write_contracts(tmp_path)
    route_state = tmp_path / ".runlogs/codex-governed-entry/last-route.json"
    _write(
        route_state,
        json.dumps(
            {
                "snapshot_mode": "changed-files",
                "profile": route_profile,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
    )
    loop_summary = tmp_path / "artifacts/ci/loop-summary.md"
    pr_summary = tmp_path / "artifacts/ci/pr-summary.md"
    acceptance_json = tmp_path / "artifacts/codex/orchestration/demo/attempt-01/acceptance.json"
    _write_summary(loop_summary, snapshot_mode="changed-files", profile=route_profile)
    _write_summary(pr_summary, snapshot_mode="changed-files", profile=pr_profile)
    _write_acceptance(acceptance_json, verdict="PASS")
    mutation_events = tmp_path / ".runlogs/codex-governed-entry/repo-mutation-events.jsonl"
    _write_mutation_events(mutation_events)
    output = tmp_path / "artifacts/codex/h4/release-decision.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/build_governed_release_decision.py",
            "--execution-contract",
            str(execution_contract),
            "--phase-brief",
            str(phase_brief),
            "--acceptance-json",
            str(acceptance_json),
            "--route-state",
            str(route_state),
            "--loop-summary",
            str(loop_summary),
            "--pr-summary",
            str(pr_summary),
            "--mutation-events",
            str(mutation_events),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    return json.loads(output.read_text(encoding="utf-8"))


def test_build_release_decision_emits_allow_when_evidence_is_consistent(tmp_path: Path) -> None:
    payload = _run_build(tmp_path, route_profile="ops", pr_profile="ops")
    assert payload["verdict"] == "ALLOW_RELEASE_READINESS"
    assert payload["blockers"] == []


def test_build_release_decision_emits_deny_on_profile_marker_mismatch(tmp_path: Path) -> None:
    payload = _run_build(tmp_path, route_profile="ops", pr_profile="none")
    assert payload["verdict"] == "DENY_RELEASE_READINESS"
    assert any("profile marker mismatch" in item for item in payload["blockers"])


def test_build_release_decision_emits_deny_when_acceptance_is_blocked(tmp_path: Path) -> None:
    execution_contract, phase_brief = _write_contracts(tmp_path)
    route_state = tmp_path / ".runlogs/codex-governed-entry/last-route.json"
    _write(
        route_state,
        json.dumps({"snapshot_mode": "changed-files", "profile": "none"}, ensure_ascii=False, indent=2) + "\n",
    )
    loop_summary = tmp_path / "artifacts/ci/loop-summary.md"
    pr_summary = tmp_path / "artifacts/ci/pr-summary.md"
    acceptance_json = tmp_path / "artifacts/codex/orchestration/demo/attempt-01/acceptance.json"
    _write_summary(loop_summary, snapshot_mode="changed-files", profile="none")
    _write_summary(pr_summary, snapshot_mode="changed-files", profile="none")
    _write_acceptance(acceptance_json, verdict="BLOCKED")
    mutation_events = tmp_path / ".runlogs/codex-governed-entry/repo-mutation-events.jsonl"
    _write_mutation_events(mutation_events)
    output = tmp_path / "artifacts/codex/h4/release-decision.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/build_governed_release_decision.py",
            "--execution-contract",
            str(execution_contract),
            "--phase-brief",
            str(phase_brief),
            "--acceptance-json",
            str(acceptance_json),
            "--route-state",
            str(route_state),
            "--loop-summary",
            str(loop_summary),
            "--pr-summary",
            str(pr_summary),
            "--mutation-events",
            str(mutation_events),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["verdict"] == "DENY_RELEASE_READINESS"
    assert any("acceptance verdict is BLOCKED" in item for item in payload["blockers"])
