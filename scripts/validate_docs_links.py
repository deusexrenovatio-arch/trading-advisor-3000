#!/usr/bin/env python3
"""Validate local markdown references in docs and AGENTS."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

BACKTICK_MD_RE = re.compile(r"`([^\s`]+\.md)`")
MD_LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+\.md)\)")
DEFAULT_COLD_DIRS = (
    "docs/archive",
    "docs/tasks/archive",
    "docs/agent/plugin-eval",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate local markdown references.")
    parser.add_argument(
        "--roots",
        nargs="+",
        default=["AGENTS.md", "docs"],
        help="Files or directories to scan relative to repository root.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root path.",
    )
    parser.add_argument(
        "--include-cold",
        action="store_true",
        help="Also scan archive/generated documentation roots.",
    )
    return parser.parse_args()


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def iter_markdown_files(root: Path, *, excluded_dirs: tuple[Path, ...]) -> list[Path]:
    if root.is_file() and root.suffix == ".md":
        if any(_is_under(root, excluded) for excluded in excluded_dirs):
            return []
        return [root]
    if root.is_dir():
        return sorted(
            p
            for p in root.rglob("*.md")
            if p.is_file() and not any(_is_under(p, excluded) for excluded in excluded_dirs)
        )
    return []


def normalize_ref(ref: str) -> str | None:
    ref = ref.strip()
    if not ref or ref.startswith(("http://", "https://")):
        return None
    if "*" in ref:
        return None
    return ref


def extract_refs(text: str) -> set[str]:
    refs: set[str] = set()
    for pattern in (BACKTICK_MD_RE, MD_LINK_RE):
        for match in pattern.finditer(text):
            candidate = normalize_ref(match.group(1))
            if candidate:
                refs.add(candidate)
    return refs


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    excluded_dirs = (
        tuple((repo_root / relative).resolve() for relative in DEFAULT_COLD_DIRS)
        if not args.include_cold
        else ()
    )
    missing: list[tuple[Path, str]] = []

    scan_files: list[Path] = []
    for root_arg in args.roots:
        root_path = (repo_root / root_arg).resolve()
        scan_files.extend(iter_markdown_files(root_path, excluded_dirs=excluded_dirs))

    for md_file in sorted(set(scan_files)):
        text = md_file.read_text(encoding="utf-8")
        for ref in sorted(extract_refs(text)):
            target = (repo_root / ref).resolve()
            if not target.exists():
                missing.append((md_file, ref))

    if missing:
        print("[validate_docs_links] Missing markdown references:")
        for file_path, ref in missing:
            rel_file = file_path.relative_to(repo_root)
            print(f"  - {rel_file}: `{ref}`")
        return 1

    print("[validate_docs_links] All markdown references resolved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
