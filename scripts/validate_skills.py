from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml


SKILLS_ROOT = Path(".cursor/skills")
CATALOG_FILE = Path("docs/agent/skills-catalog.md")
GOVERNANCE_DOC = Path("docs/workflows/skill-governance-sync.md")
DOMAIN_TOKEN_RE = re.compile(r"\b(trading|moex|signal|news|futures|arbitrage)\b", re.IGNORECASE)


def _load_yaml_frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig").lstrip("\ufeff")
    if not text.startswith("---"):
        return {}
    try:
        _, body = text.split("---", 1)
        fm_raw, _ = body.split("\n---", 1)
    except ValueError:
        return {}
    data = yaml.safe_load(fm_raw) or {}
    return data if isinstance(data, dict) else {}


def run(skills_root: Path, catalog_file: Path, governance_doc: Path) -> int:
    errors: list[str] = []
    if not catalog_file.exists():
        errors.append(f"missing skills catalog: {catalog_file.as_posix()}")
    if not governance_doc.exists():
        errors.append(f"missing workflow: {governance_doc.as_posix()}")
    if not skills_root.exists():
        errors.append(f"missing skills root: {skills_root.as_posix()}")
        skills_dirs: list[Path] = []
    else:
        skills_dirs = sorted(path for path in skills_root.iterdir() if path.is_dir())

    for skill_dir in skills_dirs:
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            errors.append(f"missing SKILL.md: {skill_dir.as_posix()}")
            continue
        fm = _load_yaml_frontmatter(skill_md)
        name = str(fm.get("name", "")).strip()
        description = str(fm.get("description", "")).strip()
        if not name:
            errors.append(f"missing frontmatter name: {skill_md.as_posix()}")
        if not description:
            errors.append(f"missing frontmatter description: {skill_md.as_posix()}")
        if name and name != skill_dir.name:
            errors.append(f"frontmatter name mismatch: {skill_md.as_posix()} (name={name}, dir={skill_dir.name})")
        content = skill_md.read_text(encoding="utf-8")
        if DOMAIN_TOKEN_RE.search(content):
            errors.append(f"domain token found in generic baseline skill: {skill_md.as_posix()}")

    if catalog_file.exists():
        catalog_text = catalog_file.read_text(encoding="utf-8")
        if DOMAIN_TOKEN_RE.search(catalog_text):
            errors.append("skills catalog contains domain-specific terms blocked for baseline")

    if errors:
        print("skill validation failed:")
        for item in errors:
            print(f"- {item}")
        print("remediation: see docs/workflows/skill-governance-sync.md")
        return 1

    print(f"skill validation: OK ({len(skills_dirs)} local skills)")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate local skill governance baseline.")
    parser.add_argument("--skills-root", default=str(SKILLS_ROOT))
    parser.add_argument("--catalog-file", default=str(CATALOG_FILE))
    parser.add_argument("--governance-doc", default=str(GOVERNANCE_DOC))
    args = parser.parse_args()
    sys.exit(run(Path(args.skills_root), Path(args.catalog_file), Path(args.governance_doc)))


if __name__ == "__main__":
    main()
