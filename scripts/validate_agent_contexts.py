from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from context_router import CONTEXTS, route_files


def _normalize(path_text: str) -> str:
    return path_text.replace("\\", "/").strip().lower()


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _should_skip(path: Path, *, repo_root: Path, exclude_globs: list[str]) -> bool:
    rel = path.relative_to(repo_root).as_posix()
    return any(path.match(pattern) or Path(rel).match(pattern) for pattern in exclude_globs)


def _expand_path_spec(
    *,
    repo_root: Path,
    spec: str,
    exclude_globs: list[str],
    errors: list[str],
) -> list[str]:
    target = (repo_root / spec).resolve()
    if not target.exists():
        errors.append(f"missing configured path: {spec}")
        return []
    collected: list[str] = []
    if target.is_file():
        rel = target.relative_to(repo_root).as_posix()
        if not _should_skip(target, repo_root=repo_root, exclude_globs=exclude_globs):
            collected.append(rel)
        return collected

    for candidate in sorted(path for path in target.rglob("*") if path.is_file()):
        if _should_skip(candidate, repo_root=repo_root, exclude_globs=exclude_globs):
            continue
        collected.append(candidate.relative_to(repo_root).as_posix())
    return collected


def _dedupe(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        marker = _normalize(item)
        if not marker or marker in seen:
            continue
        seen.add(marker)
        out.append(item)
    return out


def run(*, repo_root: Path, coverage_config_path: Path) -> int:
    if not coverage_config_path.exists():
        print(f"agent context coverage: ERROR (missing config {coverage_config_path.as_posix()})")
        return 2

    config = _load_yaml(coverage_config_path)
    required_context_cards = [
        str(item).strip()
        for item in config.get("required_context_cards", [])
        if str(item).strip()
    ]
    significant_specs = [
        str(item).strip()
        for item in config.get("significant_paths", [])
        if str(item).strip()
    ]
    high_risk_specs = [
        str(item).strip()
        for item in config.get("high_risk_paths", [])
        if str(item).strip()
    ]
    exclude_globs = [
        str(item).strip()
        for item in config.get("exclude_globs", [])
        if str(item).strip()
    ]
    required_high_risk_context = str(config.get("required_high_risk_context", "")).strip()

    errors: list[str] = []
    declared_contexts = {spec.context_id for spec in CONTEXTS}
    for context_id in required_context_cards:
        card_path = repo_root / "docs" / "agent-contexts" / f"{context_id}.md"
        if not card_path.exists():
            errors.append(f"missing context card: {context_id}")
        if context_id not in declared_contexts:
            errors.append(f"required context not declared in router: {context_id}")

    significant_files: list[str] = []
    for spec in significant_specs:
        significant_files.extend(
            _expand_path_spec(
                repo_root=repo_root,
                spec=spec,
                exclude_globs=exclude_globs,
                errors=errors,
            )
        )
    significant_files = _dedupe(significant_files)
    if not significant_files:
        errors.append("significant path expansion returned no files")

    high_risk_files: list[str] = []
    for spec in high_risk_specs:
        high_risk_files.extend(
            _expand_path_spec(
                repo_root=repo_root,
                spec=spec,
                exclude_globs=exclude_globs,
                errors=errors,
            )
        )
    high_risk_files = _dedupe(high_risk_files)

    coverage_files = _dedupe([*significant_files, *high_risk_files])
    route = route_files(coverage_files, include_cold_paths=True)

    blocking_unmapped = {
        _normalize(path_text)
        for path_text in route.get("unmapped_files", [])
        if str(path_text).strip()
    }
    for path_text in significant_files:
        if _normalize(path_text) in blocking_unmapped:
            errors.append(f"unmapped significant file: {path_text}")

    context_by_file: dict[str, set[str]] = {}
    for entry in route.get("contexts", []):
        if not isinstance(entry, dict):
            continue
        context_id = str(entry.get("id", "")).strip()
        for path_text in entry.get("matched_files", []):
            marker = _normalize(str(path_text))
            if not marker:
                continue
            context_by_file.setdefault(marker, set()).add(context_id)

    if required_high_risk_context:
        for path_text in high_risk_files:
            marker = _normalize(path_text)
            owners = context_by_file.get(marker, set())
            if required_high_risk_context not in owners:
                errors.append(
                    "high-risk routing mismatch: "
                    f"{path_text} is not owned by required context {required_high_risk_context}"
                )

    if errors:
        print("agent context coverage: FAILED")
        for item in errors:
            print(f"- {item}")
        print("remediation: add ownership coverage in scripts/context_router.py and docs/agent-contexts/*.md")
        return 1

    contexts_touched = [
        entry["id"]
        for entry in route.get("contexts", [])
        if isinstance(entry, dict) and entry.get("matched_files_count", 0) > 0
    ]
    print(
        "agent context coverage: OK "
        f"(significant_files={len(significant_files)} high_risk_files={len(high_risk_files)} "
        f"contexts={len(contexts_touched)})"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate significant source files are mapped to a context.")
    parser.add_argument("--repo-root", default=".", help="Repository root path.")
    parser.add_argument(
        "--coverage-config",
        default="configs/context_coverage.yaml",
        help="Coverage config with significant and high-risk path specs.",
    )
    args = parser.parse_args()
    return run(
        repo_root=Path(args.repo_root).resolve(),
        coverage_config_path=Path(args.coverage_config).resolve(),
    )


if __name__ == "__main__":
    raise SystemExit(main())
