from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml

from sync_skills_catalog import (
    CATALOG_MARKER,
    CATALOG_FILE,
    REQUIRED_FRONTMATTER_FIELDS,
    SKILLS_ROOT,
    SkillRecord,
    build_catalog_text,
    load_runtime_skills,
)


GOVERNANCE_DOC = Path("docs/workflows/skill-governance-sync.md")
ROUTING_DOC = Path("docs/agent/skills-routing.md")
ROADMAP_DOC = Path("docs/planning/skills-roadmap.md")
COLD_CONTEXT_FILE = Path(".cursorignore")
COLD_SKILLS_PATTERN = ".cursor/skills/**"
ALLOWED_CLASSIFICATIONS = {"KEEP_CORE", "KEEP_OPTIONAL", "DEFER_STACK", "EXCLUDE_DOMAIN_INITIAL"}
ALLOWED_WAVES = {"WAVE_1", "WAVE_2", "WAVE_3"}
ALLOWED_STATUSES = {"ACTIVE", "PAUSED", "DEFERRED"}

KEEP_CORE_BASELINE = {
    "ai-agent-architect",
    "ai-change-explainer",
    "archctl-policy-authoring",
    "architecture-review",
    "business-analyst",
    "ci-bootstrap",
    "codeowners-from-registry",
    "commit-and-pr-hygiene",
    "composition-contracts",
    "dependency-and-license-audit",
    "docs-sync",
    "golden-tests-and-fixtures",
    "incident-runbook",
    "layer-diagnostics-debug",
    "module-scaffold",
    "parallel-worktree-flow",
    "patch-series-splitter",
    "phase-acceptance-governor",
    "product-owner",
    "qa-test-engineer",
    "registry-first",
    "repeated-issue-review",
    "risk-profile-gates",
    "secrets-and-config-hardening",
    "skill-creator",
    "skill-installer",
    "source-onboarding",
    "testing-suite",
    "validate-crosslayer",
}
DOMAIN_TOKEN_RE = re.compile(
    r"\b(trading|moex|futures|arbitrage|geopolitics|intraday|spread|commodity-news)\b",
    re.IGNORECASE,
)


def _load_yaml_frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig").lstrip("\ufeff")
    if not text.startswith("---"):
        return {}
    try:
        _, body = text.split("---", 1)
        fm_raw, _ = body.split("\n---", 1)
    except ValueError:
        return {}
    payload = yaml.safe_load(fm_raw) or {}
    return payload if isinstance(payload, dict) else {}


def _normalize_triggers(value: Any) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(str(item).strip() for item in value if str(item).strip())
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    return tuple()


def _load_catalog_rows(catalog_file: Path) -> dict[str, SkillRecord]:
    rows: dict[str, SkillRecord] = {}
    if not catalog_file.exists():
        return rows
    for raw_line in catalog_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("| `"):
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) != 9:
            continue
        skill_id = parts[0].strip("`")
        if not skill_id:
            continue
        triggers = tuple(
            token.strip()
            for token in parts[6].split(";")
            if token.strip()
        )
        rows[skill_id] = SkillRecord(
            skill_id=skill_id,
            classification=parts[1].strip("`"),
            wave=parts[2].strip("`"),
            status=parts[3].strip("`"),
            scope=parts[4],
            owner_surface=parts[5].strip("`"),
            routing_triggers=triggers,
            description=parts[4],
        )
    return rows


def _validate_runtime_frontmatter(
    *,
    skills_root: Path,
    strict: bool,
    errors: list[str],
) -> dict[str, SkillRecord]:
    records, loader_errors = load_runtime_skills(skills_root=skills_root)
    errors.extend(loader_errors)
    by_id: dict[str, SkillRecord] = {record.skill_id: record for record in records}

    for skill_dir in sorted(path for path in skills_root.iterdir() if path.is_dir()):
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            continue
        fm = _load_yaml_frontmatter(skill_md)
        missing = [field for field in REQUIRED_FRONTMATTER_FIELDS if field not in fm]
        if missing:
            errors.append(f"missing frontmatter fields in {skill_md.as_posix()}: {', '.join(missing)}")
        classification = str(fm.get("classification", "")).strip()
        wave = str(fm.get("wave", "")).strip()
        status = str(fm.get("status", "")).strip()
        owner_surface = str(fm.get("owner_surface", "")).strip()
        triggers = _normalize_triggers(fm.get("routing_triggers", []))

        if classification and classification not in ALLOWED_CLASSIFICATIONS:
            errors.append(f"invalid classification `{classification}` in {skill_md.as_posix()}")
        if strict and classification != "KEEP_CORE":
            errors.append(f"non-baseline classification found in runtime skill: {skill_md.as_posix()}")
        if wave and wave not in ALLOWED_WAVES:
            errors.append(f"invalid wave `{wave}` in {skill_md.as_posix()}")
        if status and status not in ALLOWED_STATUSES:
            errors.append(f"invalid status `{status}` in {skill_md.as_posix()}")
        if not owner_surface:
            errors.append(f"missing owner_surface in {skill_md.as_posix()}")
        if not triggers:
            errors.append(f"missing routing_triggers in {skill_md.as_posix()}")
        content = skill_md.read_text(encoding="utf-8")
        if DOMAIN_TOKEN_RE.search(content):
            errors.append(f"domain token found in baseline skill file: {skill_md.as_posix()}")

    return by_id


def run(
    *,
    skills_root: Path,
    catalog_file: Path,
    governance_doc: Path,
    routing_doc: Path,
    roadmap_doc: Path,
    strict: bool,
) -> int:
    errors: list[str] = []

    if not governance_doc.exists():
        errors.append(f"missing workflow doc: {governance_doc.as_posix()}")
    if not routing_doc.exists():
        errors.append(f"missing routing doc: {routing_doc.as_posix()}")
    if strict and not roadmap_doc.exists():
        errors.append(f"missing roadmap doc: {roadmap_doc.as_posix()}")
    if not COLD_CONTEXT_FILE.exists():
        errors.append(f"missing cold-context policy file: {COLD_CONTEXT_FILE.as_posix()}")
    else:
        cold_text = COLD_CONTEXT_FILE.read_text(encoding="utf-8")
        if COLD_SKILLS_PATTERN not in cold_text:
            errors.append(
                f"cold-context policy missing `{COLD_SKILLS_PATTERN}` in {COLD_CONTEXT_FILE.as_posix()}"
            )

    if not skills_root.exists():
        errors.append(f"missing skills root: {skills_root.as_posix()}")
        if errors:
            print("skill validation failed:")
            for item in errors:
                print(f"- {item}")
            print("remediation: see docs/workflows/skill-governance-sync.md")
            return 1

    runtime_records = _validate_runtime_frontmatter(skills_root=skills_root, strict=strict, errors=errors)

    if strict:
        runtime_ids = set(runtime_records)
        missing_core = sorted(KEEP_CORE_BASELINE.difference(runtime_ids))
        extra_core = sorted(runtime_ids.difference(KEEP_CORE_BASELINE))
        if missing_core:
            errors.append(f"missing KEEP_CORE runtime skills: {', '.join(missing_core)}")
        if extra_core:
            errors.append(f"unexpected runtime skills outside KEEP_CORE baseline: {', '.join(extra_core)}")

    if not catalog_file.exists():
        errors.append(f"missing generated catalog: {catalog_file.as_posix()}")
    else:
        catalog_text = catalog_file.read_text(encoding="utf-8")
        if CATALOG_MARKER not in catalog_text:
            errors.append("skills catalog missing generated marker")
        expected_text = build_catalog_text(records=sorted(runtime_records.values(), key=lambda item: (item.wave, item.skill_id)))
        if strict and catalog_text != expected_text:
            errors.append("generated catalog drift detected (runtime metadata != catalog mirror)")

        catalog_rows = _load_catalog_rows(catalog_file)
        if strict:
            runtime_ids = set(runtime_records)
            catalog_ids = set(catalog_rows)
            missing_rows = sorted(runtime_ids.difference(catalog_ids))
            extra_rows = sorted(catalog_ids.difference(runtime_ids))
            if missing_rows:
                errors.append(f"catalog missing runtime rows: {', '.join(missing_rows)}")
            if extra_rows:
                errors.append(f"catalog contains non-runtime rows: {', '.join(extra_rows)}")
            for skill_id in sorted(runtime_ids.intersection(catalog_ids)):
                runtime = runtime_records[skill_id]
                row = catalog_rows[skill_id]
                if runtime.classification != row.classification:
                    errors.append(f"classification mismatch for `{skill_id}` between runtime and catalog")
                if runtime.wave != row.wave:
                    errors.append(f"wave mismatch for `{skill_id}` between runtime and catalog")
                if runtime.status != row.status:
                    errors.append(f"status mismatch for `{skill_id}` between runtime and catalog")
                if runtime.owner_surface != row.owner_surface:
                    errors.append(f"owner_surface mismatch for `{skill_id}` between runtime and catalog")
                if tuple(runtime.routing_triggers) != tuple(row.routing_triggers):
                    errors.append(f"routing_triggers mismatch for `{skill_id}` between runtime and catalog")

    if errors:
        print("skill validation failed:")
        for item in errors:
            print(f"- {item}")
        print("remediation: see docs/workflows/skill-governance-sync.md")
        return 1

    print(f"skill validation: OK ({len(runtime_records)} local skills, strict={strict})")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate runtime skill governance contracts.")
    parser.add_argument("--skills-root", default=str(SKILLS_ROOT))
    parser.add_argument("--catalog-file", default=str(CATALOG_FILE))
    parser.add_argument("--governance-doc", default=str(GOVERNANCE_DOC))
    parser.add_argument("--routing-doc", default=str(ROUTING_DOC))
    parser.add_argument("--roadmap-doc", default=str(ROADMAP_DOC))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    raise SystemExit(
        run(
            skills_root=Path(args.skills_root),
            catalog_file=Path(args.catalog_file),
            governance_doc=Path(args.governance_doc),
            routing_doc=Path(args.routing_doc),
            roadmap_doc=Path(args.roadmap_doc),
            strict=bool(args.strict),
        )
    )


if __name__ == "__main__":
    main()
