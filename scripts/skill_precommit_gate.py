from __future__ import annotations

import argparse
import shlex
import subprocess
import sys

from gate_common import collect_changed_files
from skill_update_decision import (
    CATALOG_DOC,
    ROUTING_DOC,
    SKILL_PREFIX,
    WORKFLOW_DOC,
    build_decision,
)


def _normalize(path_text: str) -> str:
    return path_text.replace("\\", "/").strip().lower()


def _run(command: str) -> int:
    print(f">>> {command}")
    completed = subprocess.run(command, shell=True, check=False)
    return int(completed.returncode)


def _python_cmd(script_path: str) -> str:
    executable = sys.executable
    if " " in executable:
        executable = shlex.quote(executable)
    return f"{executable} {script_path}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Precommit governance gate for skill updates.")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    args = parser.parse_args()

    changed_files = collect_changed_files(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=args.from_git,
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )
    decision = build_decision(changed_files)
    if decision.get("status") == "update_required":
        print("skill precommit gate: FAILED")
        print("required updates missing for skill changes:")
        for item in decision.get("missing_required_updates", []):
            print(f"- {item}")
        print("remediation: see docs/workflows/skill-governance-sync.md")
        raise SystemExit(1)

    normalized = {_normalize(path_text) for path_text in changed_files}
    should_validate = any(marker.startswith(SKILL_PREFIX) for marker in normalized) or any(
        marker == _normalize(path)
        for path in (CATALOG_DOC, ROUTING_DOC, WORKFLOW_DOC)
        for marker in normalized
    )
    if should_validate:
        code = _run(_python_cmd("scripts/validate_skills.py"))
        if code != 0:
            print("skill precommit gate: FAILED (validate_skills.py)")
            raise SystemExit(code)

    print(
        "skill precommit gate: OK "
        f"(status={decision.get('status')} skill_changes={decision.get('skill_changes_detected')})"
    )
    raise SystemExit(0)


if __name__ == "__main__":
    main()
