from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


SKILLS_ROOT = Path(".cursor/skills")
CATALOG_FILE = Path("docs/agent/skills-catalog.md")
REQUIRED_FRONTMATTER_FIELDS = (
    "name",
    "description",
    "classification",
    "wave",
    "status",
    "owner_surface",
    "routing_triggers",
)
CATALOG_MARKER = "<!-- generated-by: scripts/sync_skills_catalog.py -->"


@dataclass(frozen=True)
class SkillRecord:
    skill_id: str
    description: str
    classification: str
    wave: str
    status: str
    scope: str
    owner_surface: str
    routing_triggers: tuple[str, ...]


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


def _normalize_trigger_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, list):
        out = [str(item).strip() for item in value if str(item).strip()]
        return tuple(out)
    if isinstance(value, str):
        if not value.strip():
            return tuple()
        return tuple(part.strip() for part in value.split(",") if part.strip())
    return tuple()


def _markdown_escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def load_runtime_skills(*, skills_root: Path) -> tuple[list[SkillRecord], list[str]]:
    errors: list[str] = []
    if not skills_root.exists():
        return [], [f"missing skills root: {skills_root.as_posix()}"]

    records: list[SkillRecord] = []
    for skill_dir in sorted(path for path in skills_root.iterdir() if path.is_dir()):
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            errors.append(f"missing SKILL.md: {skill_dir.as_posix()}")
            continue
        fm = _load_yaml_frontmatter(skill_md)
        missing_fields = [field for field in REQUIRED_FRONTMATTER_FIELDS if field not in fm]
        if missing_fields:
            errors.append(
                f"missing frontmatter fields in {skill_md.as_posix()}: {', '.join(missing_fields)}"
            )
        name = str(fm.get("name", "")).strip()
        if not name:
            errors.append(f"empty frontmatter name: {skill_md.as_posix()}")
            continue
        if name != skill_dir.name:
            errors.append(
                f"frontmatter name mismatch: {skill_md.as_posix()} (name={name}, dir={skill_dir.name})"
            )
        description = str(fm.get("description", "")).strip()
        classification = str(fm.get("classification", "")).strip()
        wave = str(fm.get("wave", "")).strip()
        status = str(fm.get("status", "")).strip()
        scope = str(fm.get("scope", "")).strip() or description
        owner_surface = str(fm.get("owner_surface", "")).strip()
        routing_triggers = _normalize_trigger_values(fm.get("routing_triggers", []))
        records.append(
            SkillRecord(
                skill_id=name,
                description=description,
                classification=classification,
                wave=wave,
                status=status,
                scope=scope,
                owner_surface=owner_surface,
                routing_triggers=routing_triggers,
            )
        )

    records.sort(key=lambda item: (item.wave, item.skill_id))
    return records, errors


def build_catalog_text(*, records: list[SkillRecord]) -> str:
    canonical_payload = [
        {
            "skill_id": item.skill_id,
            "classification": item.classification,
            "wave": item.wave,
            "status": item.status,
            "scope": item.scope,
            "owner_surface": item.owner_surface,
            "routing_triggers": list(item.routing_triggers),
            "source": "local_runtime",
            "hot_context_policy": "cold-by-default",
        }
        for item in records
    ]
    digest = hashlib.sha256(
        json.dumps(canonical_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()

    lines = [
        "# Skills Catalog",
        "",
        CATALOG_MARKER,
        "<!-- source-of-truth: .cursor/skills/*/SKILL.md -->",
        "<!-- generated-contract: do not edit manually; run python scripts/sync_skills_catalog.py -->",
        f"<!-- catalog-sha256: {digest} -->",
        "",
        "| skill_id | classification | wave | status | scope | owner_surface | routing_triggers | source | hot_context_policy |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in records:
        trigger_text = "; ".join(item.routing_triggers)
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{_markdown_escape(item.skill_id)}`",
                    f"`{_markdown_escape(item.classification)}`",
                    f"`{_markdown_escape(item.wave)}`",
                    f"`{_markdown_escape(item.status)}`",
                    _markdown_escape(item.scope),
                    f"`{_markdown_escape(item.owner_surface)}`",
                    _markdown_escape(trigger_text),
                    "`local_runtime`",
                    "`cold-by-default`",
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Generation",
            "- Generate: `python scripts/sync_skills_catalog.py`",
            "- Check drift: `python scripts/sync_skills_catalog.py --check`",
        ]
    )
    return "\n".join(lines) + "\n"


def run(*, skills_root: Path, catalog_file: Path, check_only: bool) -> int:
    records, errors = load_runtime_skills(skills_root=skills_root)
    if errors:
        print("skills catalog sync failed:")
        for item in errors:
            print(f"- {item}")
        return 1

    rendered = build_catalog_text(records=records)
    if check_only:
        if not catalog_file.exists():
            print(f"skills catalog drift: missing file {catalog_file.as_posix()}")
            return 1
        current = catalog_file.read_text(encoding="utf-8")
        if current != rendered:
            print("skills catalog drift detected: generated output differs from tracked catalog")
            print("remediation: run `python scripts/sync_skills_catalog.py` and commit updated catalog")
            return 1
        print(f"skills catalog sync: OK ({len(records)} skills)")
        return 0

    catalog_file.parent.mkdir(parents=True, exist_ok=True)
    catalog_file.write_text(rendered, encoding="utf-8")
    print(f"skills catalog written: {catalog_file.as_posix()} ({len(records)} skills)")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate or verify runtime skills catalog mirror.")
    parser.add_argument("--skills-root", default=str(SKILLS_ROOT))
    parser.add_argument("--catalog-file", default=str(CATALOG_FILE))
    parser.add_argument("--check", action="store_true", help="Fail if catalog is out of sync.")
    args = parser.parse_args()
    raise SystemExit(
        run(
            skills_root=Path(args.skills_root),
            catalog_file=Path(args.catalog_file),
            check_only=bool(args.check),
        )
    )


if __name__ == "__main__":
    main()
