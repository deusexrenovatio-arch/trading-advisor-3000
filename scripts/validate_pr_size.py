from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from typing import Any

from gate_common import collect_changed_files

MAX_REVIEWABLE_FILES = 100
MAX_REVIEWABLE_LINE_CHANGES = 3000

COLD_GENERATED_DELETE_PREFIXES = (
    "artifacts/codex/orchestration/",
    "codex_ai_delivery_shell_package/",
    "docs/archive/",
    "docs/codex/",
    "docs/tasks/active/",
    "docs/tasks/archive/",
)


@dataclass(frozen=True)
class DiffEntry:
    path: str
    additions: int
    deletions: int
    status: str | None = None

    @property
    def line_changes(self) -> int:
        return self.additions + self.deletions


def _normalize_path(path: str) -> str:
    return path.replace("\\", "/").strip().lower()


def _diff_range_args(
    *,
    base_ref: str | None,
    head_ref: str | None,
    from_git: bool,
    git_ref: str | None,
) -> list[str] | None:
    if base_ref and head_ref:
        return [f"{base_ref}..{head_ref}"]
    if from_git:
        return [git_ref or "HEAD"]
    return None


def _run_git_diff(args: list[str], option: str) -> list[str]:
    completed = subprocess.run(
        ["git", "diff", option, *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"git diff {option} failed: {detail}")
    return completed.stdout.splitlines()


def _status_by_path(diff_args: list[str]) -> dict[str, str]:
    statuses: dict[str, str] = {}
    for line in _run_git_diff(diff_args, "--name-status"):
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        status = parts[0].strip()
        path = parts[-1]
        statuses[_normalize_path(path)] = status[:1] if status else ""
    return statuses


def _numstat_entries(diff_args: list[str]) -> list[DiffEntry]:
    statuses = _status_by_path(diff_args)
    entries: list[DiffEntry] = []
    for line in _run_git_diff(diff_args, "--numstat"):
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        raw_additions, raw_deletions, path = parts[0], parts[1], parts[-1]
        normalized = _normalize_path(path)
        additions = int(raw_additions) if raw_additions.isdigit() else 0
        deletions = int(raw_deletions) if raw_deletions.isdigit() else 0
        entries.append(
            DiffEntry(
                path=normalized,
                additions=additions,
                deletions=deletions,
                status=statuses.get(normalized),
            )
        )
    return entries


def _entries_for_args(
    *,
    base_ref: str | None,
    head_ref: str | None,
    from_git: bool,
    git_ref: str | None,
    changed_files: list[str],
) -> list[DiffEntry]:
    diff_args = _diff_range_args(
        base_ref=base_ref,
        head_ref=head_ref,
        from_git=from_git,
        git_ref=git_ref,
    )
    entries = _numstat_entries(diff_args) if diff_args is not None else []
    seen = {_normalize_path(entry.path) for entry in entries}
    for path in changed_files:
        normalized = _normalize_path(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        entries.append(DiffEntry(path=normalized, additions=0, deletions=0))
    return entries


def _is_excluded_cold_generated_delete(entry: DiffEntry) -> bool:
    if entry.status != "D":
        return False
    return any(entry.path.startswith(prefix) for prefix in COLD_GENERATED_DELETE_PREFIXES)


def build_report(entries: list[DiffEntry]) -> dict[str, Any]:
    excluded = [entry for entry in entries if _is_excluded_cold_generated_delete(entry)]
    reviewable = [entry for entry in entries if entry not in excluded]
    reviewable_line_changes = sum(entry.line_changes for entry in reviewable)
    return {
        "reviewable_files": len(reviewable),
        "reviewable_line_changes": reviewable_line_changes,
        "excluded_cold_generated_deletes": len(excluded),
        "limits": {
            "max_reviewable_files": MAX_REVIEWABLE_FILES,
            "max_reviewable_line_changes": MAX_REVIEWABLE_LINE_CHANGES,
        },
        "status": "pass"
        if len(reviewable) <= MAX_REVIEWABLE_FILES
        and reviewable_line_changes <= MAX_REVIEWABLE_LINE_CHANGES
        else "fail",
    }


def _print_text_report(report: dict[str, Any]) -> None:
    limits = report["limits"]
    print("PR size gate:")
    print(f"- reviewable files: {report['reviewable_files']} / {limits['max_reviewable_files']}")
    print(
        f"- reviewable line changes: {report['reviewable_line_changes']} / "
        f"{limits['max_reviewable_line_changes']}"
    )
    print(f"- excluded cold/generated deletes: {report['excluded_cold_generated_deletes']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate reviewable PR size stays bounded.")
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", "--base-sha", dest="base_ref", default=None)
    parser.add_argument("--head-ref", "--head", "--head-sha", dest="head_ref", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    changed_files = collect_changed_files(
        base_ref=None,
        head_ref=None,
        git_ref=args.git_ref,
        from_git=False,
        changed_files=list(args.changed_files),
        from_stdin=args.stdin,
    )

    try:
        entries = _entries_for_args(
            base_ref=args.base_ref,
            head_ref=args.head_ref,
            from_git=args.from_git,
            git_ref=args.git_ref,
            changed_files=changed_files,
        )
    except RuntimeError as exc:
        print(f"PR size gate: FAILED ({exc})")
        return 2

    report = build_report(entries)
    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_text_report(report)

    if report["status"] == "fail":
        print(
            "PR size gate: FAILED "
            "(split the change into a PR stack or remove non-reviewable generated/cold artifacts)"
        )
        return 1

    print("PR size gate: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
