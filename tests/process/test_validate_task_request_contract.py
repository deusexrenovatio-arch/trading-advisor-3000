from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_task_request_contract import run  # noqa: E402


def _valid_task_note() -> str:
    return """# Task Note
Updated: 2026-03-16 12:00 UTC

## Goal
- Deliver: validate task contract parser.

## Task Request Contract
- Objective: prove validator contract.
- In Scope: temporary task note and pointer shim.
- Out of Scope: domain logic.
- Constraints: deterministic checks only.
- Done Evidence: validator returns zero.
- Priority Rule: quality and safety over speed.

## Current Delta
- Added synthetic task note for validator test.

## First-Time-Right Report
1. Confirmed coverage: all required sections are present.
2. Missing or risky scenarios: none.
3. Resource/time risks and chosen controls: keep scope local.
4. Highest-priority fixes or follow-ups: none.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: repeated failure twice.
- Reset Action: isolate failing command.
- New Search Space: contract parser.
- Next Probe: rerun validator.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: pending
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending
"""


def _pointer_handoff(task_path: Path) -> str:
    return f"""# Session Handoff
Updated: 2026-03-16 12:01 UTC

## Active Task Note
- Path: {task_path.as_posix()}
- Mode: full
- Status: in_progress

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
"""


def test_validate_task_request_contract_accepts_valid_note(tmp_path: Path) -> None:
    note_path = tmp_path / "TASK-VALID.md"
    handoff_path = tmp_path / "session_handoff.md"
    note_path.write_text(_valid_task_note(), encoding="utf-8")
    handoff_path.write_text(_pointer_handoff(note_path), encoding="utf-8")
    code = run(
        handoff_path,
        changed_files_override=[str(handoff_path), str(note_path)],
    )
    assert code == 0


def test_validate_task_request_contract_rejects_missing_contract_items(tmp_path: Path) -> None:
    note_path = tmp_path / "TASK-BAD.md"
    handoff_path = tmp_path / "session_handoff.md"
    note_path.write_text(
        """# Task Note
Updated: 2026-03-16

## Goal
- bad note.

## Task Request Contract
- Objective: missing most required items.
""",
        encoding="utf-8",
    )
    handoff_path.write_text(_pointer_handoff(note_path), encoding="utf-8")
    code = run(
        handoff_path,
        changed_files_override=[str(handoff_path), str(note_path)],
    )
    assert code == 1
