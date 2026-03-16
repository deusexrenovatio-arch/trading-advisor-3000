from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from gate_common import is_non_trivial_diff
from handoff_resolver import read_task_note_lines


ALLOWED_OUTCOME_STATUSES = {"in_progress", "completed", "partial", "blocked"}
ALLOWED_DECISION_QUALITY = {
    "pending",
    "correct_first_time",
    "correct_after_replan",
    "wrong_path",
    "partial_outcome",
    "environment_blocked",
}
ALLOWED_ROUTE_MATCH = {"pending", "matched", "expanded", "mismatched"}
ALLOWED_PRIMARY_REWORK_CAUSES = {
    "none",
    "wrong_path",
    "context_gap",
    "environment",
    "test_gap",
    "workflow_gap",
    "architecture_gap",
    "requirements_gap",
    "other",
}
ALLOWED_IMPROVEMENT_ACTIONS = {
    "pending",
    "none",
    "docs",
    "validator",
    "skill",
    "env",
    "architecture",
    "workflow",
    "test",
}
REQUIRED_FIELDS = (
    "task_id",
    "branch",
    "goal_class",
    "start_primary_context",
    "route_match",
    "decision_quality",
    "primary_rework_cause",
    "incident_signature",
    "improvement_action",
    "improvement_artifact",
    "outcome_status",
)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"YAML payload must be object: {path}")
    return payload


def _parse_iso_date(value: str, label: str, errors: list[str]) -> None:
    try:
        date.fromisoformat(value)
    except ValueError:
        errors.append(f"{label}: invalid ISO date `{value}`")


def _collect_changed_between_refs(base_sha: str, head_sha: str) -> list[str]:
    completed = subprocess.run(
        ["git", "diff", "--name-only", f"{base_sha}..{head_sha}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return []
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _collect_working_tree_changes() -> list[str]:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return []
    out: list[str] = []
    for raw in completed.stdout.splitlines():
        text = raw.rstrip()
        path_text = text[3:] if len(text) > 3 else ""
        if " -> " in path_text:
            path_text = path_text.split(" -> ", 1)[1]
        if path_text:
            out.append(path_text.strip())
    return out


def _parse_task_outcome(path: Path) -> dict[str, str]:
    _target, lines, _pointer = read_task_note_lines(path)
    start = -1
    for idx, raw in enumerate(lines):
        if raw.strip() == "## Task Outcome":
            start = idx
            break
    if start < 0:
        return {}
    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end = idx
            break
    fields: dict[str, str] = {}
    for raw in lines[start + 1 : end]:
        stripped = raw.strip()
        if not stripped.startswith("- ") or ":" not in stripped:
            continue
        key, value = stripped[2:].split(":", 1)
        norm = key.strip().lower().replace(" ", "_").replace("-", "_")
        fields[norm] = value.strip()
    return fields


def run(
    *,
    session_handoff_path: Path,
    task_outcomes_path: Path,
    base_sha: str | None,
    head_sha: str | None,
    changed_files_override: list[str] | None,
    require_terminal_outcome: bool,
) -> int:
    payload = _load_yaml(task_outcomes_path)
    errors: list[str] = []
    if payload.get("version") != 1:
        errors.append(f"task outcomes unsupported version: {payload.get('version')!r}")
    updated_at = str(payload.get("updated_at", "")).strip()
    if not updated_at:
        errors.append("task outcomes missing top-level field 'updated_at'")
    else:
        _parse_iso_date(updated_at, "task_outcomes.updated_at", errors)

    items = payload.get("items")
    if not isinstance(items, list):
        errors.append("task outcomes field 'items' must be a list")
        items = []

    seen_ids: set[str] = set()
    terminal_count = 0
    for idx, row in enumerate(items):
        label = f"items[{idx}]"
        if not isinstance(row, dict):
            errors.append(f"{label} must be an object")
            continue
        for field in REQUIRED_FIELDS:
            if not str(row.get(field, "")).strip():
                errors.append(f"{label} missing required field '{field}'")
        task_id = str(row.get("task_id", "")).strip()
        if task_id:
            if task_id in seen_ids:
                errors.append(f"duplicate task_id in ledger: {task_id}")
            seen_ids.add(task_id)
        status = str(row.get("outcome_status", "")).strip().lower()
        if status not in ALLOWED_OUTCOME_STATUSES:
            errors.append(f"{label}.outcome_status invalid: {status!r}")
        if status in {"completed", "partial", "blocked"}:
            terminal_count += 1
            if not str(row.get("closed_at", "")).strip():
                errors.append(f"{label}.closed_at is required for terminal outcomes")
        if str(row.get("decision_quality", "")).strip().lower() not in ALLOWED_DECISION_QUALITY:
            errors.append(f"{label}.decision_quality invalid")
        if str(row.get("route_match", "")).strip().lower() not in ALLOWED_ROUTE_MATCH:
            errors.append(f"{label}.route_match invalid")
        if str(row.get("primary_rework_cause", "")).strip().lower() not in ALLOWED_PRIMARY_REWORK_CAUSES:
            errors.append(f"{label}.primary_rework_cause invalid")
        if str(row.get("improvement_action", "")).strip().lower() not in ALLOWED_IMPROVEMENT_ACTIONS:
            errors.append(f"{label}.improvement_action invalid")
        if not isinstance(row.get("start_contexts", []), list):
            errors.append(f"{label}.start_contexts must be a list")
        if not isinstance(row.get("final_contexts", []), list):
            errors.append(f"{label}.final_contexts must be a list")
        try:
            attempts = int(row.get("same_path_attempts", 0))
            if attempts < 1:
                raise ValueError
        except (TypeError, ValueError):
            errors.append(f"{label}.same_path_attempts must be a positive integer")

    changed_files = (
        changed_files_override
        if changed_files_override is not None
        else (_collect_changed_between_refs(base_sha, head_sha) if base_sha and head_sha else _collect_working_tree_changes())
    )
    non_trivial = is_non_trivial_diff(changed_files)
    if require_terminal_outcome and non_trivial and terminal_count == 0:
        errors.append("non-trivial diff requires at least one terminal task outcome record")

    current_task_outcome = _parse_task_outcome(session_handoff_path)
    current_status = current_task_outcome.get("outcome_status", "").strip().lower()
    current_quality = current_task_outcome.get("decision_quality", "").strip().lower()
    if current_status and current_status not in ALLOWED_OUTCOME_STATUSES:
        errors.append(f"Task Outcome outcome_status invalid: {current_status!r}")
    if current_quality and current_quality not in ALLOWED_DECISION_QUALITY:
        errors.append(f"Task Outcome decision_quality invalid: {current_quality!r}")
    if current_status == "in_progress" and current_quality not in {"", "pending"}:
        errors.append("Task Outcome with in_progress status must keep decision_quality=pending")

    if errors:
        print("task outcomes validation failed:")
        for item in errors:
            print(f"- {item}")
        print("remediation: see docs/runbooks/governance-remediation.md")
        return 1

    print(
        "task outcomes validation: OK "
        f"(items={len(items)} terminal_records={terminal_count} non_trivial_diff={non_trivial})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate task outcomes ledger and current task outcome contract.")
    parser.add_argument("--session-handoff-path", default="docs/session_handoff.md")
    parser.add_argument("--task-outcomes-path", default="memory/task_outcomes.yaml")
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--head-sha", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--require-terminal-outcome", action="store_true")
    args = parser.parse_args()

    changed_files_override: list[str] | None = None
    if args.base_sha and args.head_sha:
        changed_files_override = None
    elif args.stdin or args.changed_files:
        stdin_items = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()] if args.stdin else []
        changed_files_override = [*list(args.changed_files), *stdin_items]

    sys.exit(
        run(
            session_handoff_path=Path(args.session_handoff_path),
            task_outcomes_path=Path(args.task_outcomes_path),
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            changed_files_override=changed_files_override,
            require_terminal_outcome=bool(args.require_terminal_outcome),
        )
    )


if __name__ == "__main__":
    main()
