from __future__ import annotations

import argparse
import sys
from pathlib import Path


REQUIRED_FILES = (
    "docs/architecture/README.md",
    "docs/architecture/layers.md",
    "docs/architecture/modules.md",
    "docs/architecture/architecture-map.md",
    "docs/architecture/glossary.md",
    "docs/architecture/trading-advisor-3000.md",
    "docs/architecture/layers-v2.md",
    "docs/architecture/entities-v2.md",
    "docs/architecture/architecture-map-v2.md",
    "docs/architecture/adr/README.md",
    "docs/architecture/adr/0001-shell-boundaries.md",
    "scripts/sync_architecture_map.py",
)
FORBIDDEN_TOKENS = (
    "moex",
)


def run(repo_root: Path) -> int:
    errors: list[str] = []
    for rel in REQUIRED_FILES:
        candidate = (repo_root / rel).resolve()
        if not candidate.exists():
            errors.append(f"missing required architecture file: {rel}")
            continue
        text = candidate.read_text(encoding="utf-8")
        for token in FORBIDDEN_TOKENS:
            if token.lower() in text.lower():
                errors.append(f"forbidden token `{token}` in {rel}")

    map_file = (repo_root / "docs/architecture/architecture-map.md").resolve()
    if map_file.exists() and "```mermaid" not in map_file.read_text(encoding="utf-8"):
        errors.append("architecture-map.md must include a mermaid diagram")
    map_v2_file = (repo_root / "docs/architecture/architecture-map-v2.md").resolve()
    if map_v2_file.exists():
        map_v2_text = map_v2_file.read_text(encoding="utf-8")
        if "```mermaid" not in map_v2_text:
            errors.append("architecture-map-v2.md must include a mermaid diagram")
        if "<!-- generated-by: scripts/sync_architecture_map.py -->" not in map_v2_text:
            errors.append("architecture-map-v2.md must include sync generator marker")

    adr_dir = (repo_root / "docs/architecture/adr").resolve()
    adr_files = sorted(path.name for path in adr_dir.glob("*.md") if path.name != "README.md")
    if not adr_files:
        errors.append("adr directory must include at least one ADR file")

    if errors:
        print("architecture policy validation failed:")
        for item in errors:
            print(f"- {item}")
        print("remediation: see docs/runbooks/governance-remediation.md")
        return 1

    print(f"architecture policy validation: OK (adr_files={len(adr_files)})")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate architecture-as-docs baseline policy.")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()
    sys.exit(run(Path(args.repo_root).resolve()))


if __name__ == "__main__":
    main()
