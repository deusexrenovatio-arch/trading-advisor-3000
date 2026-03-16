from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from handoff_resolver import read_task_note_lines


TERMINAL_OUTCOME_STATUSES = {"completed", "partial", "blocked"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _repo_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return Path.cwd().resolve()
    return Path(completed.stdout.strip()).resolve()


def _current_branch(repo_root: Path) -> str:
    completed = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return "<unknown>"
    value = completed.stdout.strip()
    return value or "<detached>"


def _parse_section_fields(lines: list[str], heading: str) -> dict[str, str]:
    start = -1
    for idx, raw in enumerate(lines):
        if raw.strip() == heading:
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
        normalized = (
            key.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        fields[normalized] = value.strip()
    return fields


def _load_ledger(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": _today_iso(), "items": []}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        payload = {"version": 1}
    payload.setdefault("version", 1)
    payload.setdefault("updated_at", _today_iso())
    items = payload.get("items")
    if not isinstance(items, list):
        payload["items"] = []
    return payload


def _write_ledger(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def run(
    *,
    session_handoff_path: Path,
    task_outcomes_path: Path,
    task_id_override: str | None = None,
) -> int:
    repo_root = _repo_root()
    note_path, note_lines, _is_pointer = read_task_note_lines(session_handoff_path)
    task_outcome = _parse_section_fields(note_lines, "## Task Outcome")
    outcome_status = task_outcome.get("outcome_status", "in_progress").strip().lower() or "in_progress"
    final_contexts = [
        part.strip()
        for part in task_outcome.get("final_contexts", "").split(",")
        if part.strip() and part.strip().lower() not in {"pending", "none"}
    ]
    task_id = task_id_override or note_path.stem.replace("_", "-").upper()
    linked_plan_id = task_outcome.get("linked_plan_id", "").strip() or None
    linked_memory_id = task_outcome.get("linked_memory_id", "").strip() or None

    record = {
        "task_id": task_id,
        "closed_at": _now_iso() if outcome_status in TERMINAL_OUTCOME_STATUSES else None,
        "branch": _current_branch(repo_root),
        "goal_class": "process",
        "start_primary_context": "CTX-OPS",
        "start_contexts": ["CTX-OPS"],
        "final_contexts": final_contexts,
        "route_match": task_outcome.get("route_match", "pending").strip().lower() or "pending",
        "time_to_first_patch_sec": None,
        "same_path_attempts": 1,
        "decision_quality": task_outcome.get("decision_quality", "pending").strip().lower() or "pending",
        "primary_rework_cause": task_outcome.get("primary_rework_cause", "none").strip().lower() or "none",
        "incident_signature": task_outcome.get("incident_signature", "none").strip() or "none",
        "improvement_action": task_outcome.get("improvement_action", "pending").strip().lower() or "pending",
        "improvement_artifact": task_outcome.get("improvement_artifact", "pending").strip() or "pending",
        "linked_plan_id": linked_plan_id,
        "linked_memory_id": linked_memory_id,
        "outcome_status": outcome_status,
        "unmapped_files_count": 0,
        "intent_sources": ["request", "session_handoff"],
    }

    ledger = _load_ledger(task_outcomes_path)
    items = [row for row in ledger.get("items", []) if isinstance(row, dict)]
    replaced = False
    for idx, row in enumerate(items):
        if str(row.get("task_id", "")).strip() == task_id:
            items[idx] = record
            replaced = True
            break
    if not replaced:
        items.append(record)
    ledger["items"] = items
    ledger["updated_at"] = _today_iso()
    _write_ledger(task_outcomes_path, ledger)
    print(
        "task outcomes sync: OK "
        f"(task_id={task_id} outcome_status={outcome_status} terminal={outcome_status in TERMINAL_OUTCOME_STATUSES})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync current task outcome to memory/task_outcomes.yaml")
    parser.add_argument("--session-handoff-path", default="docs/session_handoff.md")
    parser.add_argument("--task-outcomes-path", default="memory/task_outcomes.yaml")
    parser.add_argument("--task-id", default=None)
    args = parser.parse_args()
    sys.exit(
        run(
            session_handoff_path=Path(args.session_handoff_path),
            task_outcomes_path=Path(args.task_outcomes_path),
            task_id_override=args.task_id,
        )
    )


if __name__ == "__main__":
    main()
