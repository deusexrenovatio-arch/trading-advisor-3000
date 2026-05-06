from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"
LEGACY_TOKENS: tuple[str, ...] = (
    "docs/architecture/app/",
    "tests/app/",
    "src/trading_advisor_3000/app/",
    "trading_advisor_3000.app",
)
ALLOWED_PATH_PREFIXES: tuple[str, ...] = (
    "artifacts/rename-migration/",
    "docs/archive/",
    "docs/codex/contracts/dual-surface-safe-rename.execution-contract.md",
    "docs/codex/modules/dual-surface-safe-rename.",
    "docs/architecture/dual-surface-safe-rename-migration-technical-specification.md",
    "docs/architecture/product-plane/",
    "docs/project-map/state/candidates/",
    "src/trading_advisor_3000/product_plane/",
    "scripts/build_dual_surface_rename_inventory.py",
    "scripts/validate_legacy_namespace_growth.py",
    "tests/process/test_build_dual_surface_rename_inventory.py",
    "tests/process/test_validate_legacy_namespace_growth.py",
)
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(?P<start>\d+)(?:,\d+)? @@")


@dataclass(frozen=True)
class Violation:
    file: str
    line: int
    token: str
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


def _is_allowlisted(path: str) -> bool:
    normalized = _normalize_path(path)
    return any(normalized.startswith(prefix) for prefix in ALLOWED_PATH_PREFIXES)


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
        if _is_allowlisted(rel_path):
            continue
        added_rows = _collect_added_lines_for_file(
            repo_root=repo_root,
            rel_path=rel_path,
            base_sha=base_sha,
            head_sha=head_sha,
        )
        if not added_rows:
            continue
        for line_number, line in added_rows:
            for token in LEGACY_TOKENS:
                if token not in line:
                    continue
                violations.append(
                    Violation(
                        file=_normalize_path(rel_path),
                        line=line_number,
                        token=token,
                        excerpt=line.strip()[:240],
                    )
                )
    violations.sort(key=lambda item: (item.file, item.line, item.token))
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
        print("legacy namespace growth validation: OK (no changed files)")
        return 0

    violations = detect_violations(
        repo_root=repo_root,
        changed_files=changed_files,
        base_sha=base_sha,
        head_sha=head_sha,
    )
    if not violations:
        print(
            "legacy namespace growth validation: OK "
            f"(changed_files={len(changed_files)} tokens={len(LEGACY_TOKENS)})"
        )
        return 0

    print("legacy namespace growth validation failed:")
    for item in violations:
        print(
            "- "
            + f"{item.file}:{item.line} added legacy token `{item.token}` "
            + f"in `{item.excerpt}`"
        )
    print(
        "remediation: move the new reference to target namespace or update the migration allowlist only with explicit rationale"
    )
    print(f"remediation doc: {REMEDIATION_DOC}")
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fail closed when new legacy namespace references are introduced in changed files."
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
