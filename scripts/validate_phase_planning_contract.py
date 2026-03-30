from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"
EXECUTION_CONTRACT_HEADING = "## Release Target Contract"
PHASE_IMPACT_HEADING = "## Release Gate Impact"
PHASE_LIMITS_HEADING = "## What This Phase Does Not Prove"
EXECUTION_CONTRACT_PREFIXES = (
    "- target decision:",
    "- target environment:",
    "- mandatory real contours:",
    "- forbidden proof substitutes:",
    "- release-blocking surfaces:",
    "- release-ready proof class:",
)
PHASE_IMPACT_PREFIXES = (
    "- surface transition:",
    "- minimum proof class:",
    "- accepted state label:",
)
ALLOWED_PROOF_CLASSES = {"doc", "schema", "unit", "integration", "staging-real", "live-real"}
ALLOWED_ACCEPTED_STATE_LABELS = {"prep_closed", "real_contour_closed", "release_decision"}


def _normalize_changed_files(paths: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        normalized = str(raw).replace("\\", "/").strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _collect_changed_files(
    *,
    base_sha: str | None,
    head_sha: str | None,
    changed_files_override: list[str] | None,
) -> list[str]:
    if changed_files_override is not None:
        return _normalize_changed_files(changed_files_override)
    if not base_sha or not head_sha:
        return []
    completed = subprocess.run(
        ["git", "diff", "--name-only", f"{base_sha}..{head_sha}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return []
    rows = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    return _normalize_changed_files(rows)


def _find_heading_line(lines: list[str], heading: str) -> int:
    for idx, raw in enumerate(lines):
        if raw.strip() == heading:
            return idx
    return -1


def _section_lines(lines: list[str], heading: str) -> list[str]:
    start = _find_heading_line(lines, heading)
    if start < 0:
        return []
    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].strip().startswith("## "):
            end = idx
            break
    return lines[start + 1 : end]


def _normalize(lines: list[str]) -> list[str]:
    return [line.strip().lower() for line in lines if line.strip()]


def _missing_prefixes(section_lines: list[str], required_prefixes: tuple[str, ...]) -> list[str]:
    normalized = _normalize(section_lines)
    missing: list[str] = []
    for prefix in required_prefixes:
        if not any(line.startswith(prefix) for line in normalized):
            missing.append(prefix)
    return missing


def _extract_prefixed_value(section_lines: list[str], prefix: str) -> str | None:
    normalized_prefix = prefix.strip().lower()
    for raw in section_lines:
        normalized = raw.strip().lower()
        if normalized.startswith(normalized_prefix):
            return raw.strip()[len(prefix) :].strip()
    return None


def _relevant_planning_files(repo_root: Path, changed_files: list[str], validate_all: bool) -> list[Path]:
    candidates: list[Path] = []
    if validate_all:
        candidates.extend(sorted((repo_root / "docs" / "codex" / "contracts").glob("*.execution-contract.md")))
        candidates.extend(sorted((repo_root / "docs" / "codex" / "modules").glob("*.phase-*.md")))
        return [path.resolve() for path in candidates]

    for rel_path in changed_files:
        if not (
            rel_path.startswith("docs/codex/contracts/")
            or rel_path.startswith("docs/codex/modules/")
        ):
            continue
        if not (rel_path.endswith(".execution-contract.md") or ".phase-" in rel_path):
            continue
        path = (repo_root / rel_path).resolve()
        if path.exists() and path not in candidates:
            candidates.append(path)
    return candidates


def _validate_execution_contract(path: Path, errors: list[str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    section = _section_lines(lines, EXECUTION_CONTRACT_HEADING)
    if not section:
        errors.append(f"{path.as_posix()}: missing section `{EXECUTION_CONTRACT_HEADING}`")
        return
    for prefix in _missing_prefixes(section, EXECUTION_CONTRACT_PREFIXES):
        errors.append(f"{path.as_posix()}: missing execution-contract item `{prefix}`")

    target_decision = (_extract_prefixed_value(section, "- target decision:") or "").strip().lower()
    proof_class = (_extract_prefixed_value(section, "- release-ready proof class:") or "").strip().lower()
    if target_decision == "allow_release_readiness" and proof_class != "live-real":
        errors.append(
            f"{path.as_posix()}: `ALLOW_RELEASE_READINESS` target must use `live-real` release-ready proof class"
        )


def _validate_phase_brief(path: Path, errors: list[str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    impact_section = _section_lines(lines, PHASE_IMPACT_HEADING)
    limits_section = _section_lines(lines, PHASE_LIMITS_HEADING)

    if not impact_section:
        errors.append(f"{path.as_posix()}: missing section `{PHASE_IMPACT_HEADING}`")
        return
    if not limits_section:
        errors.append(f"{path.as_posix()}: missing section `{PHASE_LIMITS_HEADING}`")
        return

    for prefix in _missing_prefixes(impact_section, PHASE_IMPACT_PREFIXES):
        errors.append(f"{path.as_posix()}: missing phase-planning item `{prefix}`")

    minimum_proof_class = (_extract_prefixed_value(impact_section, "- minimum proof class:") or "").strip().lower()
    accepted_state_label = (_extract_prefixed_value(impact_section, "- accepted state label:") or "").strip().lower()

    if minimum_proof_class and minimum_proof_class not in ALLOWED_PROOF_CLASSES:
        errors.append(
            f"{path.as_posix()}: invalid minimum proof class `{minimum_proof_class}` "
            f"(allowed: {', '.join(sorted(ALLOWED_PROOF_CLASSES))})"
        )
    if accepted_state_label and accepted_state_label not in ALLOWED_ACCEPTED_STATE_LABELS:
        errors.append(
            f"{path.as_posix()}: invalid accepted state label `{accepted_state_label}` "
            f"(allowed: {', '.join(sorted(ALLOWED_ACCEPTED_STATE_LABELS))})"
        )
    if accepted_state_label == "release_decision" and minimum_proof_class and minimum_proof_class != "live-real":
        errors.append(f"{path.as_posix()}: `release_decision` phases must require `live-real` proof")

    normalized_limits = _normalize(limits_section)
    bullet_lines = [line for line in normalized_limits if line.startswith("- ")]
    if not bullet_lines:
        errors.append(f"{path.as_posix()}: `{PHASE_LIMITS_HEADING}` must contain at least one bullet")
    if accepted_state_label != "release_decision" and not any("release readiness" in line for line in bullet_lines):
        errors.append(
            f"{path.as_posix()}: non-release phases must say explicitly in `{PHASE_LIMITS_HEADING}` "
            "that they do not prove release readiness"
        )


def run(
    repo_root: Path,
    *,
    base_sha: str | None = None,
    head_sha: str | None = None,
    changed_files_override: list[str] | None = None,
    validate_all: bool = False,
) -> int:
    changed_files = _collect_changed_files(
        base_sha=base_sha,
        head_sha=head_sha,
        changed_files_override=changed_files_override,
    )
    planning_files = _relevant_planning_files(repo_root, changed_files, validate_all)

    if not planning_files:
        print("phase planning contract validation: OK (no planning artifacts changed)")
        return 0

    errors: list[str] = []
    for path in planning_files:
        if path.name.endswith(".execution-contract.md"):
            _validate_execution_contract(path, errors)
        elif ".phase-" in path.name:
            _validate_phase_brief(path, errors)

    if errors:
        print("phase planning contract validation failed:")
        for item in errors:
            print(f"- {item}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    print(
        "phase planning contract validation: OK "
        f"(planning_files={len(planning_files)})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate planning-gate contract for execution contracts and phase briefs."
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--head-sha", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--all", action="store_true")
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
            validate_all=bool(args.all),
        )
    )


if __name__ == "__main__":
    main()
