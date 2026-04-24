from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"

ACTIVE_PRODUCT_SURFACE_PREFIXES: tuple[str, ...] = (
    "docs/architecture/product-plane/",
    "docs/checklists/app/",
    "docs/runbooks/app/",
    "src/trading_advisor_3000/product_plane/",
    "tests/product-plane/",
    "deployment/",
)

TECHNICAL_DELIVERY_LABEL_RE = re.compile(
    r"(?<![A-Za-z0-9])phase\s*[-_ ]?\s*\d+(?:\s*[-_ ]?\s*[a-z])?(?![A-Za-z0-9])",
    re.IGNORECASE,
)
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(?P<start>\d+)(?:,\d+)? @@")
PYTHON_DECLARATION_RE = re.compile(
    r"^\s*(?:async\s+def|def|class)\s+[A-Za-z_][A-Za-z0-9_]*"
)


@dataclass(frozen=True)
class Violation:
    file: str
    line: int
    surface: str
    label: str
    excerpt: str


def _normalize_path(path: str) -> str:
    return path.replace("\\", "/").strip()


def _run_git(repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _collect_changed_files(
    *,
    repo_root: Path,
    base_sha: str | None,
    head_sha: str | None,
    changed_files_override: list[str] | None,
) -> list[str]:
    if changed_files_override is not None:
        candidates = [_normalize_path(item) for item in changed_files_override]
    elif base_sha and head_sha:
        completed = _run_git(repo_root, ["diff", "--name-only", f"{base_sha}..{head_sha}"])
        candidates = [_normalize_path(line) for line in completed.stdout.splitlines() if line.strip()]
    else:
        tracked = _run_git(repo_root, ["diff", "--name-only", "HEAD"])
        untracked = _run_git(repo_root, ["ls-files", "--others", "--exclude-standard"])
        candidates = [_normalize_path(line) for line in tracked.stdout.splitlines() if line.strip()]
        candidates.extend(_normalize_path(line) for line in untracked.stdout.splitlines() if line.strip())

    unique: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        marker = item.lower()
        if not item or marker in seen:
            continue
        seen.add(marker)
        unique.append(item)
    return unique


def extract_added_lines_from_patch(patch_text: str) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    current_line: int | None = None
    for raw in patch_text.splitlines():
        if raw.startswith("@@"):
            match = HUNK_RE.match(raw)
            current_line = int(match.group("start")) if match else None
            continue
        if raw.startswith("+++ ") or raw.startswith("--- "):
            continue
        if current_line is None:
            continue
        if raw.startswith("+"):
            rows.append((current_line, raw[1:]))
            current_line += 1
            continue
        if raw.startswith("-"):
            continue
        current_line += 1
    return rows


def _is_active_product_surface(path: str) -> bool:
    normalized = _normalize_path(path)
    return any(normalized.startswith(prefix) for prefix in ACTIVE_PRODUCT_SURFACE_PREFIXES)


def _first_delivery_label(text: str) -> str | None:
    match = TECHNICAL_DELIVERY_LABEL_RE.search(text)
    return match.group(0) if match else None


def _is_markdown_name_line(rel_path: str, line: str) -> bool:
    return rel_path.lower().endswith(".md") and line.lstrip().startswith("#")


def _is_python_name_line(rel_path: str, line: str) -> bool:
    return rel_path.lower().endswith(".py") and bool(PYTHON_DECLARATION_RE.match(line))


def _is_name_line(rel_path: str, line: str) -> bool:
    return _is_markdown_name_line(rel_path, line) or _is_python_name_line(rel_path, line)


def _collect_added_lines_for_file(
    *,
    repo_root: Path,
    rel_path: str,
    base_sha: str | None,
    head_sha: str | None,
) -> list[tuple[int, str]]:
    normalized = _normalize_path(rel_path)
    patch_args = ["diff", "--unified=0", "--no-color"]
    if base_sha and head_sha:
        patch_args.append(f"{base_sha}..{head_sha}")
    else:
        patch_args.append("HEAD")
    patch_args.extend(["--", normalized])
    completed = _run_git(repo_root, patch_args)
    patch_stdout = completed.stdout or ""
    added_rows = extract_added_lines_from_patch(patch_stdout if completed.returncode == 0 else "")
    if added_rows:
        return added_rows

    tracked_check = _run_git(repo_root, ["ls-files", "--error-unmatch", "--", normalized])
    if tracked_check.returncode == 0:
        return []

    candidate = (repo_root / normalized).resolve()
    if not candidate.exists() or not candidate.is_file():
        return []
    try:
        text = candidate.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return []
    return [(index, line) for index, line in enumerate(text.splitlines(), start=1)]


def detect_violations(
    *,
    repo_root: Path,
    changed_files: list[str],
    base_sha: str | None,
    head_sha: str | None,
) -> list[Violation]:
    violations: list[Violation] = []
    for rel_path in changed_files:
        normalized = _normalize_path(rel_path)
        if not _is_active_product_surface(normalized):
            continue

        candidate = (repo_root / normalized).resolve()
        if candidate.exists() and candidate.is_file():
            path_label = _first_delivery_label(normalized)
            if path_label:
                violations.append(
                    Violation(
                        file=normalized,
                        line=0,
                        surface="path",
                        label=path_label,
                        excerpt=normalized,
                    )
                )

        added_rows = _collect_added_lines_for_file(
            repo_root=repo_root,
            rel_path=normalized,
            base_sha=base_sha,
            head_sha=head_sha,
        )
        for line_number, line in added_rows:
            if not _is_name_line(normalized, line):
                continue
            line_label = _first_delivery_label(line)
            if not line_label:
                continue
            violations.append(
                Violation(
                    file=normalized,
                    line=line_number,
                    surface="name-line",
                    label=line_label,
                    excerpt=line.strip()[:240],
                )
            )

    violations.sort(key=lambda item: (item.file, item.line, item.surface, item.label))
    return violations


def run(
    repo_root: Path,
    *,
    base_sha: str | None = None,
    head_sha: str | None = None,
    changed_files_override: list[str] | None = None,
) -> int:
    changed_files = _collect_changed_files(
        repo_root=repo_root,
        base_sha=base_sha,
        head_sha=head_sha,
        changed_files_override=changed_files_override,
    )
    if not changed_files:
        print("product surface naming validation: OK (no changed files)")
        return 0

    violations = detect_violations(
        repo_root=repo_root,
        changed_files=changed_files,
        base_sha=base_sha,
        head_sha=head_sha,
    )
    if not violations:
        print(
            "product surface naming validation: OK "
            f"(changed_files={len(changed_files)} active_prefixes={len(ACTIVE_PRODUCT_SURFACE_PREFIXES)})"
        )
        return 0

    print("product surface naming validation failed:")
    for item in violations:
        location = item.file if item.line == 0 else f"{item.file}:{item.line}"
        print(
            "- "
            + f"{location} added technical delivery label `{item.label}` "
            + f"on {item.surface} `{item.excerpt}`"
        )
    print(
        "remediation: use capability or outcome names for active product surfaces; "
        "keep numbered delivery labels only in immutable provenance/evidence context"
    )
    print(f"remediation doc: {REMEDIATION_DOC}")
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fail closed when active product-facing names reintroduce numbered delivery labels."
        )
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--head-sha", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    args = parser.parse_args()

    changed_files_override: list[str] | None = None
    if args.base_sha and args.head_sha:
        changed_files_override = None
    elif args.stdin or args.changed_files:
        stdin_items = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()] if args.stdin else []
        changed_files_override = [*list(args.changed_files), *stdin_items]

    sys.exit(
        run(
            Path(args.repo_root).resolve(),
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            changed_files_override=changed_files_override,
        )
    )


if __name__ == "__main__":
    main()
