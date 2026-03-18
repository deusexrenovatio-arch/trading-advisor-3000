from __future__ import annotations

import argparse
import fnmatch
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"


@dataclass(frozen=True)
class CodeownersEntry:
    pattern: str
    owners: tuple[str, ...]


def _normalize(path_text: str) -> str:
    return path_text.replace("\\", "/").strip()


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"failed to parse policy YAML: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("CODEOWNERS policy must be a YAML object")
    return payload


def _parse_codeowners(path: Path, owner_token: re.Pattern[str]) -> tuple[list[CodeownersEntry], list[str]]:
    entries: list[CodeownersEntry] = []
    errors: list[str] = []
    seen_patterns: set[str] = set()

    for index, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            errors.append(f"line {index}: expected '<pattern> <owner...>'")
            continue
        pattern = parts[0]
        owners = tuple(parts[1:])
        if pattern in seen_patterns:
            errors.append(f"line {index}: duplicate CODEOWNERS pattern `{pattern}`")
            continue
        seen_patterns.add(pattern)
        for owner in owners:
            if not owner_token.match(owner):
                errors.append(f"line {index}: invalid owner token `{owner}`")
        entries.append(CodeownersEntry(pattern=pattern, owners=owners))
    return entries, errors


def _entry_matches_path(entry: CodeownersEntry, path_text: str) -> bool:
    normalized_path = _normalize(path_text).lstrip("/")
    pattern = _normalize(entry.pattern)
    if pattern == "*":
        return True
    if pattern.startswith("/"):
        pattern = pattern[1:]
    if pattern.endswith("/"):
        return normalized_path == pattern[:-1] or normalized_path.startswith(pattern)
    if any(token in pattern for token in ("*", "?", "[")):
        return fnmatch.fnmatch(normalized_path, pattern)
    return normalized_path == pattern


def _expand_significant_paths(repo_root: Path, specs: list[str]) -> list[str]:
    out: list[str] = []
    for spec in specs:
        normalized_spec = _normalize(spec)
        target = (repo_root / normalized_spec).resolve()
        if not target.exists():
            out.append(normalized_spec)
            continue
        if target.is_file():
            out.append(target.relative_to(repo_root).as_posix())
            continue
        for candidate in sorted(path for path in target.rglob("*") if path.is_file()):
            out.append(candidate.relative_to(repo_root).as_posix())
    deduped: list[str] = []
    seen: set[str] = set()
    for item in out:
        marker = _normalize(item).lower()
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(item)
    return deduped


def run(*, codeowners_path: Path, policy_path: Path, repo_root: Path) -> int:
    errors: list[str] = []

    if not codeowners_path.exists():
        print(f"codeowners validation failed: missing {codeowners_path.as_posix()}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1
    if not policy_path.exists():
        print(f"codeowners validation failed: missing policy {policy_path.as_posix()}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    try:
        policy = _load_yaml(policy_path)
    except ValueError as exc:
        print(f"codeowners validation failed: {exc}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1
    if policy.get("version") != 1:
        print(f"codeowners validation failed: unsupported policy version {policy.get('version')!r}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    owner_regex = str(policy.get("owner_token_regex") or r"^(@[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)?|[^@\s]+@[^@\s]+)$")
    owner_token = re.compile(owner_regex)
    entries, parse_errors = _parse_codeowners(codeowners_path, owner_token)
    errors.extend(parse_errors)

    if not entries:
        errors.append("CODEOWNERS file has no routing entries")

    required_patterns = [str(item).strip() for item in policy.get("required_patterns", []) if str(item).strip()]
    entry_patterns = {entry.pattern for entry in entries}
    for pattern in required_patterns:
        if pattern not in entry_patterns:
            errors.append(f"missing required CODEOWNERS pattern: {pattern}")

    significant_specs = [str(item).strip() for item in policy.get("significant_paths", []) if str(item).strip()]
    significant_files = _expand_significant_paths(repo_root, significant_specs)
    for path_text in significant_files:
        matched = any(_entry_matches_path(entry, path_text) for entry in entries)
        if not matched:
            errors.append(f"unmapped governance surface: {path_text}")

    for entry in entries:
        if not entry.owners:
            errors.append(f"pattern `{entry.pattern}` has no owners")

    if errors:
        print("codeowners validation failed:")
        for item in errors:
            print(f"- {item}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    print(
        "codeowners validation: OK "
        f"(entries={len(entries)} significant_files={len(significant_files)})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate CODEOWNERS coverage and routing contract.")
    parser.add_argument("--path", default="CODEOWNERS")
    parser.add_argument("--policy", default="configs/codeowners_policy.yaml")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()
    raise SystemExit(
        run(
            codeowners_path=Path(args.path),
            policy_path=Path(args.policy),
            repo_root=Path(args.repo_root).resolve(),
        )
    )


if __name__ == "__main__":
    main()
