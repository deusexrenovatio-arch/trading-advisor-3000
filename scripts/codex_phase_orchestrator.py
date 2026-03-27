#!/usr/bin/env python3
"""Run worker -> acceptance -> remediation -> unlock for one module phase."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from codex_phase_policy import (
    ACCEPTANCE_ROUTE_SIGNAL,
    ACCEPTANCE_BEGIN,
    ACCEPTANCE_END,
    DEFAULT_ACCEPTOR_MODEL,
    DEFAULT_WORKER_MODEL,
    EXPECTED_WORKER_ROUTE_SIGNALS,
    ROUTE_GUARDRAILS,
    ROUTE_MODE,
    WORKER_BEGIN,
    WORKER_END,
    AcceptanceResult,
    AttemptRecord,
    RoleLaunchConfig,
    WorkerReport,
    apply_acceptance_policy,
    normalize_acceptance_payload,
    normalize_worker_payload,
    render_acceptance_markdown,
    render_route_report,
    route_guardrail_text,
    state_payload,
)


DEFAULT_ARTIFACT_ROOT = Path("artifacts/codex/orchestration")
DEFAULT_WORKER_PROMPT = Path("docs/codex/prompts/phases/worker.md")
DEFAULT_ACCEPTOR_PROMPT = Path("docs/codex/prompts/phases/acceptor.md")
DEFAULT_REMEDIATION_PROMPT = Path("docs/codex/prompts/phases/remediation.md")
DEFAULT_IGNORE_GLOBS = (
    ".runlogs/**",
    "artifacts/**",
    "docs/codex/packages/inbox/*.zip",
)


class OrchestratorError(RuntimeError):
    """Domain error for phase orchestration."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def updated_stamp() -> str:
    return utc_now().strftime("%Y-%m-%d %H:%M UTC")


def utc_slug() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def slugify(text: str, fallback: str = "run") -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in text).strip("-")
    squashed = "-".join(part for part in cleaned.split("-") if part)
    return squashed[:48] if squashed else fallback


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_path(repo_root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def find_heading(lines: list[str], heading: str) -> int:
    for idx, raw in enumerate(lines):
        if raw.strip() == heading:
            return idx
    return -1


def section_bounds(lines: list[str], heading: str) -> tuple[int, int]:
    start = find_heading(lines, heading)
    if start < 0:
        raise OrchestratorError(f"missing heading `{heading}`")
    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end = idx
            break
    return start, end


def section_lines(lines: list[str], heading: str) -> list[str]:
    start, end = section_bounds(lines, heading)
    return lines[start + 1 : end]


def parse_bullet_map(lines: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in lines:
        stripped = raw.strip()
        if not stripped.startswith("- ") or ":" not in stripped:
            continue
        key, value = stripped[2:].split(":", 1)
        result[key.strip()] = value.strip()
    return result


def set_bullet_value(path: Path, heading: str, key: str, value: str) -> None:
    lines = read_lines(path)
    start, end = section_bounds(lines, heading)
    replacement = f"- {key}: {value}"
    inserted = False
    for idx in range(start + 1, end):
        stripped = lines[idx].strip()
        if stripped.startswith(f"- {key}:"):
            lines[idx] = replacement
            inserted = True
            break
    if not inserted:
        lines.insert(end, replacement)
    write_lines(path, lines)


def set_single_bullet_section(path: Path, heading: str, value: str) -> None:
    lines = read_lines(path)
    start, end = section_bounds(lines, heading)
    new_lines = lines[: start + 1] + [f"- {value}"] + lines[end:]
    write_lines(path, new_lines)


def phase_name_from_brief(path: Path) -> str:
    bullets = parse_bullet_map(section_lines(read_lines(path), "## Phase"))
    value = bullets.get("Name")
    if not value:
        raise OrchestratorError(f"{path.as_posix()} missing phase name")
    return value


def phase_status_from_brief(path: Path) -> str:
    bullets = parse_bullet_map(section_lines(read_lines(path), "## Phase"))
    value = bullets.get("Status")
    if not value:
        raise OrchestratorError(f"{path.as_posix()} missing phase status")
    return value


def phase_objective_from_brief(path: Path) -> str:
    lines = section_lines(read_lines(path), "## Objective")
    for raw in lines:
        stripped = raw.strip()
        if stripped.startswith("- "):
            return stripped[2:].strip()
    raise OrchestratorError(f"{path.as_posix()} missing objective bullet")


def parent_next_phase(parent_path: Path) -> str:
    lines = section_lines(read_lines(parent_path), "## Next Phase To Execute")
    for raw in lines:
        stripped = raw.strip()
        if stripped.startswith("- "):
            return stripped[2:].strip()
    raise OrchestratorError(f"{parent_path.as_posix()} missing next phase pointer")


def phase_sequence(parent_path: Path) -> list[Path]:
    filename = parent_path.name
    if not filename.endswith(".parent.md"):
        raise OrchestratorError(f"unexpected parent brief name: {filename}")
    slug = filename[: -len(".parent.md")]
    phase_paths = sorted(parent_path.parent.glob(f"{slug}.phase-*.md"))
    if not phase_paths:
        raise OrchestratorError(f"no phase briefs found for {slug}")
    return phase_paths


def resolve_current_phase(repo_root: Path, parent_path: Path) -> Path:
    target = parent_next_phase(parent_path)
    if target.lower() == "none":
        raise OrchestratorError("module has no next phase to execute")
    return resolve_path(repo_root, target)


def next_phase_after(parent_path: Path, current_phase_path: Path) -> Path | None:
    phases = phase_sequence(parent_path)
    try:
        index = [path.resolve() for path in phases].index(current_phase_path.resolve())
    except ValueError as exc:
        raise OrchestratorError(f"{current_phase_path.as_posix()} is not in the parent phase sequence") from exc
    if index + 1 >= len(phases):
        return None
    return phases[index + 1]


def relative_to_repo(path: Path, repo_root: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def update_parent_and_contract_after_pass(
    *,
    repo_root: Path,
    parent_path: Path,
    execution_contract_path: Path,
    current_phase_path: Path,
) -> None:
    next_phase = next_phase_after(parent_path, current_phase_path)
    if next_phase is None:
        set_single_bullet_section(parent_path, "## Next Phase To Execute", "none")
        set_single_bullet_section(
            execution_contract_path,
            "## Next Allowed Unit Of Work",
            "All planned phases are accepted. Prepare closeout or a new module run.",
        )
        return

    next_rel = relative_to_repo(next_phase, repo_root)
    next_name = phase_name_from_brief(next_phase)
    next_objective = phase_objective_from_brief(next_phase)
    set_single_bullet_section(parent_path, "## Next Phase To Execute", next_rel)
    set_single_bullet_section(
        execution_contract_path,
        "## Next Allowed Unit Of Work",
        f"Execute {next_name} only: {next_objective}",
    )


def update_phase_status(path: Path, status: str) -> None:
    set_bullet_value(path, "## Phase", "Status", status)


def git_changed_paths(repo_root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise OrchestratorError(completed.stdout + completed.stderr)
    paths: list[str] = []
    for raw in completed.stdout.splitlines():
        text = raw.rstrip()
        path_text = text[3:] if len(text) > 3 else ""
        if " -> " in path_text:
            path_text = path_text.split(" -> ", 1)[1]
        if path_text:
            normalized = path_text.replace("\\", "/").strip()
            if normalized not in paths:
                paths.append(normalized)
    return paths


def is_ignored(path_text: str, ignore_globs: tuple[str, ...]) -> bool:
    normalized = path_text.replace("\\", "/").strip()
    return any(fnmatch(normalized, pattern) for pattern in ignore_globs)


def visible_changes(repo_root: Path, ignore_globs: tuple[str, ...]) -> list[str]:
    return [path for path in git_changed_paths(repo_root) if not is_ignored(path, ignore_globs)]


def ensure_clean_worktree(repo_root: Path, ignore_globs: tuple[str, ...]) -> None:
    changed = visible_changes(repo_root, ignore_globs)
    if changed:
        joined = "\n".join(f"- {item}" for item in changed)
        raise OrchestratorError(
            "worktree must be clean before orchestration starts outside ignored paths:\n" + joined
        )


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def tagged_json(text: str, begin: str, end: str) -> dict[str, Any]:
    start = text.rfind(begin)
    stop = text.rfind(end)
    if start < 0 or stop < 0 or stop <= start:
        raise OrchestratorError(f"missing tagged json block {begin} ... {end}")
    payload = text[start + len(begin) : stop].strip()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise OrchestratorError(f"failed to parse tagged json: {exc}") from exc
    if not isinstance(data, dict):
        raise OrchestratorError("tagged json payload must be an object")
    return data


def build_worker_prompt(
    *,
    template_path: Path,
    execution_contract_path: Path,
    parent_path: Path,
    phase_path: Path,
    attempt: int,
    remediation_blockers_path: Path | None,
) -> str:
    base = read_text(template_path).rstrip()
    blockers = remediation_blockers_path.as_posix() if remediation_blockers_path else "NONE"
    return (
        f"{base}\n\n"
        "Route Guardrails:\n"
        f"{route_guardrail_text()}\n\n"
        f"Execution Contract: {execution_contract_path.as_posix()}\n"
        f"Module Parent Brief: {parent_path.as_posix()}\n"
        f"Phase Brief: {phase_path.as_posix()}\n"
        f"Attempt Number: {attempt}\n"
        f"Remediation Blockers: {blockers}\n"
    )


def build_acceptor_prompt(
    *,
    template_path: Path,
    execution_contract_path: Path,
    parent_path: Path,
    phase_path: Path,
    worker_report_path: Path,
    changed_files_path: Path,
    attempt: int,
) -> str:
    base = read_text(template_path).rstrip()
    return (
        f"{base}\n\n"
        "Route Guardrails:\n"
        f"{route_guardrail_text()}\n\n"
        f"Execution Contract: {execution_contract_path.as_posix()}\n"
        f"Module Parent Brief: {parent_path.as_posix()}\n"
        f"Phase Brief: {phase_path.as_posix()}\n"
        f"Worker Report: {worker_report_path.as_posix()}\n"
        f"Changed Files Snapshot: {changed_files_path.as_posix()}\n"
        f"Attempt Number: {attempt}\n"
    )


def codex_backend_available(codex_bin: str | None = None) -> tuple[bool, str]:
    binary = codex_bin or shutil.which("codex")
    if not binary:
        return False, "codex executable not found in PATH"
    try:
        completed = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:  # pragma: no cover - environment-specific
        return False, f"failed to launch codex: {type(exc).__name__}: {exc}"
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout).strip()
        return False, f"codex returned {completed.returncode}: {stderr}"
    return True, completed.stdout.strip() or binary


def run_codex_prompt(
    *,
    repo_root: Path,
    prompt: str,
    launch: RoleLaunchConfig,
    output_path: Path,
    codex_bin: str | None,
) -> int:
    binary = codex_bin or shutil.which("codex")
    if not binary:
        raise OrchestratorError("codex executable not found in PATH")
    cmd = [binary, "exec", "-C", str(repo_root), "--output-last-message", str(output_path)]
    if launch.profile:
        cmd.extend(["-p", launch.profile])
    if launch.model:
        cmd.extend(["-m", launch.model])
    for override in launch.config_overrides:
        cmd.extend(["-c", override])
    cmd.append("-")
    completed = subprocess.run(
        cmd,
        input=prompt,
        text=True,
        cwd=repo_root,
        check=False,
    )
    return int(completed.returncode)


def simulate_worker_payload(scenario: str, attempt: int, kind: str) -> dict[str, Any]:
    route_signal = EXPECTED_WORKER_ROUTE_SIGNALS.get(kind)
    if not route_signal:
        raise OrchestratorError(f"unsupported simulated worker kind: {kind}")
    payload = {
        "status": "DONE",
        "summary": f"Simulated {kind} attempt {attempt}.",
        "route_signal": route_signal,
        "files_touched": [],
        "checks_run": [],
        "remaining_risks": [],
        "assumptions": [],
        "skips": [],
        "fallbacks": [],
        "deferred_work": [],
    }
    if scenario == "pass-with-shortcut":
        payload["fallbacks"] = ["Used a weaker fallback path without explicit phase-contract approval."]
    return payload


def simulate_acceptance_payload(scenario: str, attempt: int) -> dict[str, Any]:
    payload = {
        "route_signal": ACCEPTANCE_ROUTE_SIGNAL,
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
    if scenario == "pass":
        payload.update({"verdict": "PASS", "summary": "Simulated acceptance pass."})
        return payload
    if scenario == "blocked":
        payload.update(
            {
                "verdict": "BLOCKED",
                "summary": "Simulated acceptance blocker set.",
                "blockers": [
                    {
                        "id": "B1",
                        "title": "Phase evidence incomplete",
                        "why": "The simulated acceptor requires remediation before unlock.",
                        "remediation": "Apply the requested remediation and rerun acceptance.",
                    }
                ],
                "rerun_checks": ["python scripts/run_loop_gate.py --from-git --git-ref HEAD"],
            }
        )
        return payload
    if scenario == "block-then-pass":
        if attempt == 1:
            return simulate_acceptance_payload("blocked", attempt)
        return simulate_acceptance_payload("pass", attempt)
    if scenario == "pass-with-shortcut":
        payload.update({"verdict": "PASS", "summary": "Simulated acceptor would pass without policy enforcement."})
        return payload
    if scenario == "pass-with-evidence-gap":
        payload.update(
            {
                "verdict": "PASS",
                "summary": "Simulated acceptor found the phase acceptable.",
                "evidence_gaps": ["No executed-check evidence was provided for the required negative path."],
            }
        )
        return payload
    raise OrchestratorError(f"unsupported simulate scenario: {scenario}")


def run_role(
    *,
    role: str,
    backend: str,
    scenario: str,
    repo_root: Path,
    prompt_text: str,
    prompt_path: Path,
    output_path: Path,
    launch: RoleLaunchConfig,
    codex_bin: str | None,
    attempt: int,
) -> dict[str, Any]:
    prompt_path.write_text(prompt_text, encoding="utf-8")
    if backend == "simulate":
        if role in {"worker", "remediation"}:
            payload = simulate_worker_payload(scenario=scenario, attempt=attempt, kind=role)
            output = (
                f"Simulated {role} run.\n"
                f"{WORKER_BEGIN}\n"
                f"{json.dumps(payload, ensure_ascii=False)}\n"
                f"{WORKER_END}\n"
            )
            output_path.write_text(output, encoding="utf-8")
            return payload
        payload = simulate_acceptance_payload(scenario=scenario, attempt=attempt)
        output = (
            "Simulated acceptance run.\n"
            f"{ACCEPTANCE_BEGIN}\n"
            f"{json.dumps(payload, ensure_ascii=False)}\n"
            f"{ACCEPTANCE_END}\n"
        )
        output_path.write_text(output, encoding="utf-8")
        return payload

    if backend != "codex-cli":
        raise OrchestratorError(f"unsupported backend: {backend}")

    code = run_codex_prompt(
        repo_root=repo_root,
        prompt=prompt_text,
        launch=launch,
        output_path=output_path,
        codex_bin=codex_bin,
    )
    if code != 0:
        raise OrchestratorError(f"codex backend failed for {role} with exit code {code}")
    output_text = read_text(output_path)
    if role in {"worker", "remediation"}:
        return tagged_json(output_text, WORKER_BEGIN, WORKER_END)
    return tagged_json(output_text, ACCEPTANCE_BEGIN, ACCEPTANCE_END)

def write_changed_files(path: Path, changed_files: list[str]) -> None:
    payload = {"changed_files": changed_files}
    write_json(path, payload)


def default_run_dir(artifact_root: Path, phase_path: Path) -> Path:
    name = f"{utc_slug()}-{slugify(phase_path.stem)}"
    return artifact_root / name


def write_state_and_route_report(*, state_path: Path, route_report_path: Path, payload: dict[str, Any]) -> None:
    write_json(state_path, payload)
    route_report_path.write_text(render_route_report(payload), encoding="utf-8")


def orchestrate_current_phase(
    *,
    repo_root: Path,
    execution_contract_path: Path,
    parent_path: Path,
    worker_prompt_path: Path,
    acceptor_prompt_path: Path,
    remediation_prompt_path: Path,
    artifact_root: Path,
    backend: str,
    worker_launch: RoleLaunchConfig,
    acceptor_launch: RoleLaunchConfig,
    remediation_launch: RoleLaunchConfig,
    codex_bin: str | None,
    simulate_scenario: str,
    max_remediation_cycles: int,
    ignore_globs: tuple[str, ...],
    skip_clean_check: bool,
) -> tuple[int, Path]:
    phase_path = resolve_current_phase(repo_root, parent_path)
    if phase_status_from_brief(phase_path) == "completed":
        raise OrchestratorError(f"phase already completed: {phase_path.as_posix()}")

    if not skip_clean_check:
        ensure_clean_worktree(repo_root, ignore_globs)

    run_root = default_run_dir(artifact_root, phase_path)
    run_root.mkdir(parents=True, exist_ok=True)
    state_path = run_root / "state.json"
    route_report_path = run_root / "route-report.md"
    attempt_records: list[AttemptRecord] = []
    role_configs = {
        "worker": worker_launch,
        "acceptor": acceptor_launch,
        "remediation": remediation_launch,
    }

    update_phase_status(phase_path, "in_progress")
    final_code = 3
    final_status = "blocked"
    blockers_path: Path | None = None
    total_attempts = max_remediation_cycles + 1

    for attempt in range(1, total_attempts + 1):
        kind = "worker" if attempt == 1 else "remediation"
        attempt_dir = run_root / f"attempt-{attempt:02d}"
        attempt_dir.mkdir(parents=True, exist_ok=True)

        worker_template = worker_prompt_path if kind == "worker" else remediation_prompt_path
        worker_launch_cfg = worker_launch if kind == "worker" else remediation_launch
        worker_prompt = build_worker_prompt(
            template_path=worker_template,
            execution_contract_path=execution_contract_path,
            parent_path=parent_path,
            phase_path=phase_path,
            attempt=attempt,
            remediation_blockers_path=blockers_path,
        )
        worker_prompt_file = attempt_dir / f"{kind}-prompt.md"
        worker_last_message = attempt_dir / f"{kind}-last-message.txt"
        worker_payload = run_role(
            role=kind,
            backend=backend,
            scenario=simulate_scenario,
            repo_root=repo_root,
            prompt_text=worker_prompt,
            prompt_path=worker_prompt_file,
            output_path=worker_last_message,
            launch=worker_launch_cfg,
            codex_bin=codex_bin,
            attempt=attempt,
        )
        try:
            worker_report = normalize_worker_payload(worker_payload, role=kind)
        except ValueError as exc:
            raise OrchestratorError(str(exc)) from exc
        worker_report_path = attempt_dir / f"{kind}-report.json"
        write_json(worker_report_path, asdict(worker_report))

        # When clean-check is skipped, keep attempt-scoped evidence from the worker report
        # instead of emitting an empty changed-files snapshot that will fail acceptance.
        changed_files = list(worker_report.files_touched) if skip_clean_check else visible_changes(repo_root, ignore_globs)
        changed_files_path = attempt_dir / "changed-files.json"
        write_changed_files(changed_files_path, changed_files)

        acceptor_prompt = build_acceptor_prompt(
            template_path=acceptor_prompt_path,
            execution_contract_path=execution_contract_path,
            parent_path=parent_path,
            phase_path=phase_path,
            worker_report_path=worker_report_path,
            changed_files_path=changed_files_path,
            attempt=attempt,
        )
        acceptor_prompt_file = attempt_dir / "acceptor-prompt.md"
        acceptor_last_message = attempt_dir / "acceptor-last-message.txt"
        acceptance_payload = run_role(
            role="acceptor",
            backend=backend,
            scenario=simulate_scenario,
            repo_root=repo_root,
            prompt_text=acceptor_prompt,
            prompt_path=acceptor_prompt_file,
            output_path=acceptor_last_message,
            launch=acceptor_launch,
            codex_bin=codex_bin,
            attempt=attempt,
        )
        try:
            acceptance = apply_acceptance_policy(
                worker=worker_report,
                acceptance=normalize_acceptance_payload(acceptance_payload),
            )
        except ValueError as exc:
            raise OrchestratorError(str(exc)) from exc
        acceptance_json_path = attempt_dir / "acceptance.json"
        acceptance_md_path = attempt_dir / "acceptance.md"
        write_json(
            acceptance_json_path,
            {
                "verdict": acceptance.verdict,
                "summary": acceptance.summary,
                "route_signal": acceptance.route_signal,
                "used_skills": acceptance.used_skills,
                "blockers": [asdict(item) for item in acceptance.blockers],
                "rerun_checks": acceptance.rerun_checks,
                "evidence_gaps": acceptance.evidence_gaps,
                "prohibited_findings": acceptance.prohibited_findings,
                "policy_blockers": [asdict(item) for item in acceptance.policy_blockers],
            },
        )
        acceptance_md_path.write_text(render_acceptance_markdown(acceptance), encoding="utf-8")
        blockers_path = acceptance_md_path

        attempt_records.append(
            AttemptRecord(
                attempt=attempt,
                kind=kind,
                worker_summary=worker_report.summary,
                worker_route_signal=worker_report.route_signal,
                worker_report_path=worker_report_path.as_posix(),
                changed_files_path=changed_files_path.as_posix(),
                acceptance_json_path=acceptance_json_path.as_posix(),
                acceptance_md_path=acceptance_md_path.as_posix(),
                acceptor_route_signal=acceptance.route_signal,
                acceptor_used_skills=acceptance.used_skills,
                verdict=acceptance.verdict,
                blockers_total=len(acceptance.blockers),
                policy_blockers_total=len(acceptance.policy_blockers),
            )
        )

        interim_state = state_payload(
            run_id=run_root.name,
            updated_at=updated_stamp(),
            backend=backend,
            phase_brief=phase_path.as_posix(),
            phase_name=phase_name_from_brief(phase_path),
            phase_status=phase_status_from_brief(phase_path),
            attempt_records=attempt_records,
            final_status=final_status,
            next_phase=parent_next_phase(parent_path),
            role_configs=role_configs,
        )
        interim_state["current_attempt"] = attempt
        write_state_and_route_report(
            state_path=state_path,
            route_report_path=route_report_path,
            payload=interim_state,
        )

        if acceptance.verdict == "PASS":
            update_phase_status(phase_path, "completed")
            update_parent_and_contract_after_pass(
                repo_root=repo_root,
                parent_path=parent_path,
                execution_contract_path=execution_contract_path,
                current_phase_path=phase_path,
            )
            final_code = 0
            final_status = "accepted"
            break

        update_phase_status(phase_path, "blocked")
        if attempt >= total_attempts:
            final_code = 3
            final_status = "blocked"
            break
        update_phase_status(phase_path, "in_progress")

    final_state = state_payload(
        run_id=run_root.name,
        updated_at=updated_stamp(),
        backend=backend,
        phase_brief=phase_path.as_posix(),
        phase_name=phase_name_from_brief(phase_path),
        phase_status=phase_status_from_brief(phase_path),
        attempt_records=attempt_records,
        final_status=final_status,
        next_phase=parent_next_phase(parent_path),
        role_configs=role_configs,
    )
    write_state_and_route_report(
        state_path=state_path,
        route_report_path=route_report_path,
        payload=final_state,
    )
    return final_code, state_path


def latest_state_file(artifact_root: Path) -> Path | None:
    candidates = sorted(artifact_root.rglob("state.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def print_status(state_path: Path) -> int:
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def print_role_launches(
    worker_launch: RoleLaunchConfig,
    acceptor_launch: RoleLaunchConfig,
    remediation_launch: RoleLaunchConfig,
) -> None:
    for role_name, launch in (
        ("worker", worker_launch),
        ("acceptor", acceptor_launch),
        ("remediation", remediation_launch),
    ):
        print(f"{role_name}_model: {launch.model or 'default'}")
        print(f"{role_name}_profile: {launch.profile or 'none'}")
        print(
            f"{role_name}_config_overrides: "
            f"{', '.join(launch.config_overrides) if launch.config_overrides else 'none'}"
        )


def run_preflight(
    *,
    repo_root: Path,
    execution_contract_path: Path,
    parent_path: Path,
    backend: str,
    worker_launch: RoleLaunchConfig,
    acceptor_launch: RoleLaunchConfig,
    remediation_launch: RoleLaunchConfig,
    codex_bin: str | None,
    ignore_globs: tuple[str, ...],
    skip_clean_check: bool,
) -> int:
    phase_path = resolve_current_phase(repo_root, parent_path)
    phase_name = phase_name_from_brief(phase_path)
    phase_status = phase_status_from_brief(phase_path)
    print(f"execution_contract: {execution_contract_path.as_posix()}")
    print(f"parent_brief: {parent_path.as_posix()}")
    print(f"current_phase: {phase_path.as_posix()}")
    print(f"current_phase_name: {phase_name}")
    print(f"current_phase_status: {phase_status}")
    print(f"route_mode: {ROUTE_MODE}")
    print(f"route_guardrails: {', '.join(ROUTE_GUARDRAILS)}")
    print_role_launches(worker_launch, acceptor_launch, remediation_launch)
    if backend == "codex-cli":
        ok, detail = codex_backend_available(codex_bin=codex_bin)
        print("backend: codex-cli")
        print(f"backend_available: {ok}")
        print(f"backend_detail: {detail}")
        if not ok:
            return 1
    else:
        print(f"backend: {backend}")
        print("backend_available: True")
        print("backend_detail: simulate backend is deterministic and test-only")

    if skip_clean_check:
        print("clean_worktree_check: skipped")
        return 0
    changed = visible_changes(repo_root, ignore_globs)
    if changed:
        print("clean_worktree_check: FAILED")
        for item in changed:
            print(f"- {item}")
        return 1
    print("clean_worktree_check: OK")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase worker/acceptance orchestration runner.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(target: argparse.ArgumentParser) -> None:
        target.add_argument("--execution-contract", required=True)
        target.add_argument("--parent-brief", required=True)
        target.add_argument("--backend", choices=("simulate", "codex-cli"), default="simulate")
        target.add_argument("--profile", default="")
        target.add_argument("--codex-bin", default=None)
        target.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
        target.add_argument("--worker-prompt-file", default=str(DEFAULT_WORKER_PROMPT))
        target.add_argument("--acceptor-prompt-file", default=str(DEFAULT_ACCEPTOR_PROMPT))
        target.add_argument("--remediation-prompt-file", default=str(DEFAULT_REMEDIATION_PROMPT))
        target.add_argument("--worker-profile", default="")
        target.add_argument("--acceptor-profile", default="")
        target.add_argument("--remediation-profile", default="")
        target.add_argument("--worker-model", default=DEFAULT_WORKER_MODEL)
        target.add_argument("--acceptor-model", default=DEFAULT_ACCEPTOR_MODEL)
        target.add_argument("--remediation-model", default="")
        target.add_argument("--worker-config", action="append", default=[])
        target.add_argument("--acceptor-config", action="append", default=[])
        target.add_argument("--remediation-config", action="append", default=[])
        target.add_argument("--ignore-glob", action="append", default=[])
        target.add_argument("--skip-clean-check", action="store_true")

    preflight = subparsers.add_parser("preflight")
    add_common(preflight)

    run_now = subparsers.add_parser("run-current-phase")
    add_common(run_now)
    run_now.add_argument("--max-remediation-cycles", type=int, default=2)
    run_now.add_argument(
        "--simulate-scenario",
        choices=("pass", "blocked", "block-then-pass", "pass-with-shortcut", "pass-with-evidence-gap"),
        default="pass",
        help="Used only by the simulate backend.",
    )

    status = subparsers.add_parser("status")
    status.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    status.add_argument("--state-file", default=None)
    return parser


def build_role_launches(args: argparse.Namespace) -> tuple[RoleLaunchConfig, RoleLaunchConfig, RoleLaunchConfig]:
    shared_profile = str(getattr(args, "profile", "") or "").strip()
    worker_launch = RoleLaunchConfig(
        profile=str(getattr(args, "worker_profile", "") or "").strip() or shared_profile,
        model=str(getattr(args, "worker_model", "") or "").strip(),
        config_overrides=tuple(str(item).strip() for item in getattr(args, "worker_config", []) if str(item).strip()),
    )
    acceptor_launch = RoleLaunchConfig(
        profile=str(getattr(args, "acceptor_profile", "") or "").strip() or shared_profile,
        model=str(getattr(args, "acceptor_model", "") or "").strip(),
        config_overrides=tuple(
            str(item).strip() for item in getattr(args, "acceptor_config", []) if str(item).strip()
        ),
    )
    remediation_launch = RoleLaunchConfig(
        profile=str(getattr(args, "remediation_profile", "") or "").strip() or shared_profile,
        model=str(getattr(args, "remediation_model", "") or "").strip() or worker_launch.model,
        config_overrides=tuple(
            str(item).strip() for item in getattr(args, "remediation_config", []) if str(item).strip()
        ),
    )
    return worker_launch, acceptor_launch, remediation_launch


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv or sys.argv[1:])
    repo_root = resolve_repo_root()

    if args.command == "status":
        artifact_root = resolve_path(repo_root, args.artifact_root)
        state_path = resolve_path(repo_root, args.state_file) if args.state_file else latest_state_file(artifact_root)
        if state_path is None or not state_path.exists():
            print("error: no orchestration state found", file=sys.stderr)
            return 2
        return print_status(state_path)

    execution_contract_path = resolve_path(repo_root, args.execution_contract)
    parent_path = resolve_path(repo_root, args.parent_brief)
    artifact_root = resolve_path(repo_root, args.artifact_root)
    worker_prompt_path = resolve_path(repo_root, args.worker_prompt_file)
    acceptor_prompt_path = resolve_path(repo_root, args.acceptor_prompt_file)
    remediation_prompt_path = resolve_path(repo_root, args.remediation_prompt_file)
    ignore_globs = tuple([*DEFAULT_IGNORE_GLOBS, *list(args.ignore_glob)])
    worker_launch, acceptor_launch, remediation_launch = build_role_launches(args)

    if args.command == "preflight":
        return run_preflight(
            repo_root=repo_root,
            execution_contract_path=execution_contract_path,
            parent_path=parent_path,
            backend=args.backend,
            worker_launch=worker_launch,
            acceptor_launch=acceptor_launch,
            remediation_launch=remediation_launch,
            codex_bin=args.codex_bin,
            ignore_globs=ignore_globs,
            skip_clean_check=bool(args.skip_clean_check),
        )

    try:
        code, state_path = orchestrate_current_phase(
            repo_root=repo_root,
            execution_contract_path=execution_contract_path,
            parent_path=parent_path,
            worker_prompt_path=worker_prompt_path,
            acceptor_prompt_path=acceptor_prompt_path,
            remediation_prompt_path=remediation_prompt_path,
            artifact_root=artifact_root,
            backend=args.backend,
            worker_launch=worker_launch,
            acceptor_launch=acceptor_launch,
            remediation_launch=remediation_launch,
            codex_bin=args.codex_bin,
            simulate_scenario=args.simulate_scenario,
            max_remediation_cycles=max(int(args.max_remediation_cycles), 0),
            ignore_globs=ignore_globs,
            skip_clean_check=bool(args.skip_clean_check),
        )
    except OrchestratorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(f"state_file: {state_path.as_posix()}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
