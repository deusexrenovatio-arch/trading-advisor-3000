from __future__ import annotations

import argparse
import shlex
import subprocess
import sys

from gate_common import collect_changed_files
from skill_update_decision import (
    CATALOG_FILE,
    ROUTING_DOC,
    SKILL_PREFIX,
    WORKFLOW_DOC,
    _build_decision,
    _collect_name_status,
    _compute_metadata_drift,
    _parse_git_skill_operations,
    _path_exists_in_git,
    _changed_skill_ids,
)


REMEDIATION_DOC = "docs/workflows/skill-governance-sync.md"


def _normalize(path_text: str) -> str:
    return path_text.replace("\\", "/").strip().lower()


def _run(command: str) -> int:
    print(f">>> {command}")
    completed = subprocess.run(command, shell=True, check=False)
    return int(completed.returncode)


def _python_cmd(script_path: str, *args: str) -> str:
    executable = sys.executable
    if " " in executable:
        executable = shlex.quote(executable)
    suffix = " ".join(args).strip()
    if suffix:
        return f"{executable} {script_path} {suffix}"
    return f"{executable} {script_path}"


def _print_remediation_sequence(decision: dict[str, object]) -> None:
    print("skill precommit gate remediation sequence:")
    print("1. regenerate catalog mirror: python scripts/sync_skills_catalog.py")
    if str(CATALOG_FILE) in decision.get("missing_required_updates", []):
        print("2. stage generated catalog: docs/agent/skills-catalog.md")
    if ROUTING_DOC in decision.get("missing_required_updates", []):
        print("3. update routing policy: docs/agent/skills-routing.md")
    if WORKFLOW_DOC in decision.get("missing_required_updates", []):
        print("4. update workflow contract: docs/workflows/skill-governance-sync.md")
    print("5. rerun strict checks:")
    print("   - python scripts/sync_skills_catalog.py --check")
    print("   - python scripts/validate_skills.py --strict")
    print("   - python scripts/skill_update_decision.py --strict --from-git --git-ref HEAD")


def main() -> None:
    parser = argparse.ArgumentParser(description="Precommit governance gate for runtime skill updates.")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    changed_files = collect_changed_files(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=args.from_git,
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )
    strict = bool(args.strict) or True

    git_name_status = _collect_name_status(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=args.from_git,
    )
    operations = _parse_git_skill_operations(git_name_status)
    baseline_ref = args.base_ref or args.git_ref or "HEAD"
    changed_skill_ids = _changed_skill_ids(changed_files)
    if args.from_git and not args.base_ref and not args.head_ref:
        inferred_added: set[str] = set(operations.get("added", []))
        inferred_updated: set[str] = set(operations.get("updated", []))
        for skill_id in changed_skill_ids:
            rel = f".cursor/skills/{skill_id}/SKILL.md"
            if not _path_exists_in_git(baseline_ref, rel):
                inferred_added.add(skill_id)
                inferred_updated.discard(skill_id)
        operations["added"] = sorted(inferred_added)
        operations["updated"] = sorted(inferred_updated)

    metadata_drift, routing_trigger_drift_skills, forbidden_non_baseline = _compute_metadata_drift(
        changed_skill_ids=changed_skill_ids,
        baseline_ref=baseline_ref,
    )
    decision = _build_decision(
        changed_files=changed_files,
        git_operations=operations,
        metadata_drift=metadata_drift,
        routing_trigger_drift_skills=routing_trigger_drift_skills,
        forbidden_non_baseline=forbidden_non_baseline,
        strict=strict,
    )

    if decision.get("status") in {"update_required", "blocked"}:
        print("skill precommit gate: FAILED")
        print(f"status={decision.get('status')}")
        missing = decision.get("missing_required_updates", [])
        if isinstance(missing, list) and missing:
            print("missing_required_updates:")
            for item in missing:
                print(f"- {item}")
        _print_remediation_sequence(decision)
        print(f"remediation: see {REMEDIATION_DOC}")
        raise SystemExit(1)

    normalized = {_normalize(path_text) for path_text in changed_files}
    should_validate = any(marker.startswith(SKILL_PREFIX) for marker in normalized) or any(
        marker in {_normalize(str(CATALOG_FILE)), _normalize(ROUTING_DOC), _normalize(WORKFLOW_DOC)}
        for marker in normalized
    )
    if should_validate:
        commands = [
            _python_cmd("scripts/sync_skills_catalog.py", "--check"),
            _python_cmd("scripts/validate_skills.py", "--strict"),
        ]
        for command in commands:
            code = _run(command)
            if code != 0:
                print("skill precommit gate: FAILED")
                print(f"failing_command={command}")
                print(f"remediation: see {REMEDIATION_DOC}")
                raise SystemExit(code)

    print(
        "skill precommit gate: OK "
        f"(status={decision.get('status')} runtime_change={decision.get('runtime_change_detected')})"
    )
    raise SystemExit(0)


if __name__ == "__main__":
    main()
