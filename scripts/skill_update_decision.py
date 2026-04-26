from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from gate_common import collect_changed_files
from sync_skills_catalog import CATALOG_FILE, SKILLS_ROOT, build_catalog_text, load_runtime_skills


SKILL_PREFIX = ".codex/skills/"
LEGACY_SKILL_PREFIX = ".cursor/skills/"
SKILL_PREFIXES = (SKILL_PREFIX, LEGACY_SKILL_PREFIX)
ROUTING_DOC = "docs/agent/skills-routing.md"
WORKFLOW_DOC = "docs/workflows/skill-governance-sync.md"
SKILL_GOVERNANCE_PROCESS_FILES = {
    "scripts/sync_skills_catalog.py",
    "scripts/validate_skills.py",
    "scripts/skill_update_decision.py",
    "scripts/skill_precommit_gate.py",
}
METADATA_FIELDS = ("classification", "wave", "status", "routing_triggers")
KEEP_CORE_CLASS = "KEEP_CORE"


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


def _load_frontmatter_from_text(text: str) -> dict[str, Any]:
    normalized = text.lstrip("\ufeff")
    if not normalized.startswith("---"):
        return {}
    try:
        _, body = normalized.split("---", 1)
        fm_raw, _ = body.split("\n---", 1)
    except ValueError:
        return {}
    payload = yaml.safe_load(fm_raw) or {}
    return payload if isinstance(payload, dict) else {}


def _load_frontmatter(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _load_frontmatter_from_text(path.read_text(encoding="utf-8"))


def _normalize_triggers(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _skill_id_from_path(path_text: str) -> str | None:
    normalized = _normalize(path_text)
    for prefix in SKILL_PREFIXES:
        if not normalized.startswith(prefix):
            continue
        parts = normalized.split("/")
        if len(parts) < 4 or parts[3] != "skill.md":
            return None
        skill_id = parts[2].strip()
        return skill_id or None
    return None


def _changed_skill_ids(changed_files: list[str]) -> list[str]:
    skill_ids: set[str] = set()
    for path_text in changed_files:
        skill_id = _skill_id_from_path(path_text)
        if skill_id:
            skill_ids.add(skill_id)
    return sorted(skill_ids)


def _changed_active_skill_ids(changed_files: list[str]) -> list[str]:
    skill_ids: set[str] = set()
    for path_text in changed_files:
        if not _normalize(path_text).startswith(SKILL_PREFIX):
            continue
        skill_id = _skill_id_from_path(path_text)
        if skill_id:
            skill_ids.add(skill_id)
    return sorted(skill_ids)


def _collect_name_status(
    *,
    base_ref: str | None,
    head_ref: str | None,
    git_ref: str | None,
    from_git: bool,
) -> list[str]:
    if base_ref and head_ref:
        command = ["git", "diff", "--name-status", f"{base_ref}..{head_ref}"]
    elif from_git:
        command = ["git", "diff", "--name-status", git_ref or "HEAD"]
    else:
        return []
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        return []
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _parse_git_skill_operations(lines: list[str]) -> dict[str, Any]:
    added: set[str] = set()
    removed: set[str] = set()
    updated: set[str] = set()
    renamed: list[dict[str, str]] = []

    for line in lines:
        parts = line.split("\t")
        if not parts:
            continue
        status = parts[0].upper()
        if status.startswith("R") and len(parts) >= 3:
            old_id = _skill_id_from_path(parts[1])
            new_id = _skill_id_from_path(parts[2])
            if old_id and new_id:
                renamed.append({"from": old_id, "to": new_id})
                continue
        if len(parts) < 2:
            continue
        skill_id = _skill_id_from_path(parts[1])
        if not skill_id:
            continue
        if status.startswith("A"):
            added.add(skill_id)
        elif status.startswith("D"):
            removed.add(skill_id)
        else:
            updated.add(skill_id)

    # normalize overlaps
    for item in renamed:
        added.discard(item["to"])
        removed.discard(item["from"])
        updated.discard(item["from"])
        updated.discard(item["to"])
    return {
        "added": sorted(added),
        "removed": sorted(removed),
        "updated": sorted(updated),
        "renamed": renamed,
    }


def _path_exists_in_git(ref: str, path_text: str) -> bool:
    command = ["git", "cat-file", "-e", f"{ref}:{path_text}"]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    return completed.returncode == 0


def _read_from_git(ref: str, path_text: str) -> str:
    completed = subprocess.run(
        ["git", "show", f"{ref}:{path_text}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout


def _compute_metadata_drift(
    *,
    changed_skill_ids: list[str],
    baseline_ref: str,
) -> tuple[dict[str, dict[str, dict[str, Any]]], list[str], list[str]]:
    drift: dict[str, dict[str, dict[str, Any]]] = {}
    routing_trigger_drift: list[str] = []
    forbidden_non_baseline: list[str] = []

    for skill_id in changed_skill_ids:
        rel = f"{SKILL_PREFIX}{skill_id}/SKILL.md"
        current_path = Path(rel)
        current_fm = _load_frontmatter(current_path)
        old_text = _read_from_git(baseline_ref, rel)
        old_fm = _load_frontmatter_from_text(old_text) if old_text else {}
        skill_drift: dict[str, dict[str, Any]] = {}

        for field in METADATA_FIELDS:
            old_value = old_fm.get(field)
            current_value = current_fm.get(field)
            if field == "routing_triggers":
                old_value = _normalize_triggers(old_value)
                current_value = _normalize_triggers(current_value)
            if old_value != current_value:
                skill_drift[field] = {"old": old_value, "new": current_value}
        if skill_drift:
            drift[skill_id] = skill_drift
        if "routing_triggers" in skill_drift:
            routing_trigger_drift.append(skill_id)

        classification = str(current_fm.get("classification", "")).strip()
        if current_path.exists() and classification and classification != KEEP_CORE_CLASS:
            forbidden_non_baseline.append(skill_id)

    return drift, sorted(set(routing_trigger_drift)), sorted(set(forbidden_non_baseline))


def _detect_catalog_drift() -> bool:
    records, errors = load_runtime_skills(skills_root=SKILLS_ROOT)
    if errors:
        return True
    expected = build_catalog_text(records=records)
    if not CATALOG_FILE.exists():
        return True
    return CATALOG_FILE.read_text(encoding="utf-8") != expected


def _build_decision(
    *,
    changed_files: list[str],
    git_operations: dict[str, Any],
    metadata_drift: dict[str, dict[str, dict[str, Any]]],
    routing_trigger_drift_skills: list[str],
    forbidden_non_baseline: list[str],
    strict: bool,
) -> dict[str, Any]:
    files = _dedupe(changed_files)
    normalized = {_normalize(path_text) for path_text in files}

    changed_skill_ids = _changed_skill_ids(files)
    added = list(git_operations.get("added", []))
    removed = list(git_operations.get("removed", []))
    updated = list(git_operations.get("updated", []))
    renamed = list(git_operations.get("renamed", []))

    if not (added or removed or renamed or updated):
        updated = changed_skill_ids

    runtime_change = bool(changed_skill_ids or added or removed or renamed)
    catalog_changed = _normalize(str(CATALOG_FILE)) in normalized
    routing_changed = _normalize(ROUTING_DOC) in normalized
    workflow_changed = _normalize(WORKFLOW_DOC) in normalized

    process_change = any(_normalize(item) in normalized for item in SKILL_GOVERNANCE_PROCESS_FILES)
    routing_metadata_change = bool(routing_trigger_drift_skills)
    catalog_drift = _detect_catalog_drift() if strict else False

    missing_required: list[str] = []
    if runtime_change and not catalog_changed:
        missing_required.append(CATALOG_FILE.as_posix())
    if routing_metadata_change and not routing_changed:
        missing_required.append(ROUTING_DOC)
    if process_change and not workflow_changed:
        missing_required.append(WORKFLOW_DOC)
    if strict and catalog_drift:
        missing_required.append("sync_skills_catalog --check")

    if forbidden_non_baseline:
        status = "blocked"
    elif not runtime_change:
        status = "no_skill_changes"
    elif missing_required:
        status = "update_required"
    else:
        status = "ready"

    recommendation = {
        "blocked": "Runtime skill set introduced non-baseline class values. Keep runtime catalog KEEP_CORE-only.",
        "no_skill_changes": "No runtime skill changes detected.",
        "update_required": "Required governance sync documents or generated catalog are out of date.",
        "ready": "Skill governance update requirements satisfied.",
    }[status]

    return {
        "status": status,
        "strict_mode": strict,
        "changed_files_count": len(files),
        "runtime_change_detected": runtime_change,
        "changed_skill_ids": changed_skill_ids,
        "operations": {
            "added": sorted(added),
            "removed": sorted(removed),
            "updated": sorted(updated),
            "renamed": renamed,
        },
        "metadata_drift": metadata_drift,
        "routing_trigger_drift_skills": routing_trigger_drift_skills,
        "forbidden_non_baseline_skills": forbidden_non_baseline,
        "catalog_changed": catalog_changed,
        "routing_changed": routing_changed,
        "workflow_changed": workflow_changed,
        "process_change_detected": process_change,
        "catalog_drift_detected": catalog_drift,
        "missing_required_updates": sorted(set(missing_required)),
        "recommendation": recommendation,
    }


def _render_text(decision: dict[str, Any]) -> str:
    lines = [
        f"status: {decision.get('status')}",
        f"strict_mode: {decision.get('strict_mode')}",
        f"runtime_change_detected: {decision.get('runtime_change_detected')}",
        f"changed_skill_ids: {', '.join(decision.get('changed_skill_ids', [])) or 'none'}",
    ]
    operations = decision.get("operations", {})
    if isinstance(operations, dict):
        lines.append(
            "operations: "
            + ", ".join(
                [
                    f"added={len(operations.get('added', []))}",
                    f"removed={len(operations.get('removed', []))}",
                    f"updated={len(operations.get('updated', []))}",
                    f"renamed={len(operations.get('renamed', []))}",
                ]
            )
        )
    if decision.get("routing_trigger_drift_skills"):
        lines.append(
            "routing_trigger_drift_skills: "
            + ", ".join(decision.get("routing_trigger_drift_skills", []))
        )
    if decision.get("forbidden_non_baseline_skills"):
        lines.append(
            "forbidden_non_baseline_skills: "
            + ", ".join(decision.get("forbidden_non_baseline_skills", []))
        )
    missing = decision.get("missing_required_updates", [])
    if missing:
        lines.append("missing_required_updates:")
        for item in missing:
            lines.append(f"- {item}")
    lines.append(f"recommendation: {decision.get('recommendation')}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Decide required governance sync updates for skill changes.")
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

    git_name_status = _collect_name_status(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        git_ref=args.git_ref,
        from_git=args.from_git,
    )
    operations = _parse_git_skill_operations(git_name_status)

    baseline_ref = args.base_ref or args.git_ref or "HEAD"
    changed_skill_ids = _changed_skill_ids(changed_files)

    # Infer added files for untracked skill folders in working tree.
    if args.from_git and not args.base_ref and not args.head_ref:
        inferred_added: set[str] = set(operations.get("added", []))
        inferred_updated: set[str] = set(operations.get("updated", []))
        for skill_id in _changed_active_skill_ids(changed_files):
            rel = f"{SKILL_PREFIX}{skill_id}/SKILL.md"
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
        strict=bool(args.strict),
    )

    if args.format == "text":
        print(_render_text(decision))
    else:
        print(json.dumps(decision, ensure_ascii=False, indent=2))

    if args.strict and decision.get("status") in {"update_required", "blocked"}:
        print("skill update decision: FAILED (strict mode)")
        print("remediation: see docs/workflows/skill-governance-sync.md")
        raise SystemExit(1)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
