from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from gate_common import collect_changed_files


SKILL_PREFIX = ".cursor/skills/"
CATALOG_DOC = "docs/agent/skills-catalog.md"
ROUTING_DOC = "docs/agent/skills-routing.md"
WORKFLOW_DOC = "docs/workflows/skill-governance-sync.md"


def _normalize(path_text: str) -> str:
    return path_text.replace("\\", "/").strip().lower()


def _dedupe(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        marker = _normalize(item)
        if not marker or marker in seen:
            continue
        seen.add(marker)
        out.append(item.replace("\\", "/").strip())
    return out


def _changed_skill_ids(changed_files: list[str]) -> list[str]:
    skill_ids: list[str] = []
    seen: set[str] = set()
    for path_text in changed_files:
        normalized = _normalize(path_text)
        if not normalized.startswith(SKILL_PREFIX):
            continue
        parts = normalized.split("/")
        if len(parts) < 3:
            continue
        skill_id = parts[2]
        if not skill_id or skill_id in seen:
            continue
        seen.add(skill_id)
        skill_ids.append(skill_id)
    return sorted(skill_ids)


def build_decision(changed_files: list[str]) -> dict[str, Any]:
    files = _dedupe(changed_files)
    normalized_set = {_normalize(path_text) for path_text in files}
    skill_ids = _changed_skill_ids(files)
    skill_changes = bool(skill_ids)

    catalog_changed = _normalize(CATALOG_DOC) in normalized_set
    routing_changed = _normalize(ROUTING_DOC) in normalized_set
    workflow_changed = _normalize(WORKFLOW_DOC) in normalized_set

    missing_required: list[str] = []
    if skill_changes and not catalog_changed:
        missing_required.append(CATALOG_DOC)
    if skill_changes and not (routing_changed or workflow_changed):
        missing_required.append(f"{ROUTING_DOC} or {WORKFLOW_DOC}")

    if not skill_changes:
        status = "no_skill_changes"
    elif missing_required:
        status = "update_required"
    else:
        status = "ready"

    return {
        "status": status,
        "changed_files_count": len(files),
        "skill_changes_detected": skill_changes,
        "changed_skill_ids": skill_ids,
        "catalog_changed": catalog_changed,
        "routing_changed": routing_changed,
        "workflow_changed": workflow_changed,
        "missing_required_updates": missing_required,
        "recommendation": (
            "Sync skills docs with runtime skill changes and rerun validator."
            if status == "update_required"
            else "Skill governance update requirements satisfied."
        ),
    }


def _render_text(decision: dict[str, Any]) -> str:
    lines = [
        f"status: {decision.get('status')}",
        f"skill_changes_detected: {decision.get('skill_changes_detected')}",
        f"changed_skill_ids: {', '.join(decision.get('changed_skill_ids', [])) or 'none'}",
        f"catalog_changed: {decision.get('catalog_changed')}",
        f"routing_changed: {decision.get('routing_changed')}",
        f"workflow_changed: {decision.get('workflow_changed')}",
    ]
    missing = decision.get("missing_required_updates", [])
    if missing:
        lines.append("missing_required_updates:")
        for item in missing:
            lines.append(f"- {item}")
    lines.append(f"recommendation: {decision.get('recommendation')}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Decide whether skill governance docs must be updated.")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--format", choices=("json", "text"), default="json")
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
    decision = build_decision(changed_files)

    if args.format == "text":
        print(_render_text(decision))
    else:
        print(json.dumps(decision, ensure_ascii=False, indent=2))

    if args.strict and decision.get("status") == "update_required":
        print("skill update decision: FAILED (required governance docs were not updated)")
        print("remediation: see docs/workflows/skill-governance-sync.md")
        raise SystemExit(1)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
