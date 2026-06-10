from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

REMEDIATION_DOC = "docs/runbooks/governance-remediation.md"
INCIDENT_POLICY_PATH = Path("configs/agent_incident_policy.yaml")
DEFAULT_TASK_NOTE_PATH = Path("docs/agent/task-request-contract.md")
REQUIRED_CONTRACT_ITEMS = (
    "- objective:",
    "- in scope:",
    "- out of scope:",
    "- constraints:",
    "- done evidence:",
    "- priority rule:",
)
REQUIRED_REPORT_ITEMS = (
    "1. confirmed coverage:",
    "2. missing or risky scenarios:",
    "3. resource/time risks and chosen controls:",
    "4. highest-priority fixes or follow-ups:",
)
REQUIRED_REPETITION_ITEMS = (
    "- max same-path attempts:",
    "- stop trigger:",
    "- reset action:",
    "- new search space:",
    "- next probe:",
)
MAX_ATTEMPTS_RE = re.compile(r"^- max same-path attempts:\s*(\d+)\s*$")


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


def _load_policy_max_same_path_attempts(errors: list[str]) -> int | None:
    if not INCIDENT_POLICY_PATH.exists():
        errors.append(f"incident policy missing: {INCIDENT_POLICY_PATH.as_posix()}")
        return None
    payload = yaml.safe_load(INCIDENT_POLICY_PATH.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        errors.append("incident policy is not a YAML object")
        return None
    incident = payload.get("incident") or {}
    if not isinstance(incident, dict):
        errors.append("incident policy invalid: 'incident' must be object")
        return None
    raw = incident.get("max_same_path_attempts")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        errors.append("incident policy invalid: max_same_path_attempts must be an integer")
        return None
    if value < 1:
        errors.append("incident policy invalid: max_same_path_attempts must be positive")
        return None
    return value


def _extract_max_same_path_attempts(repetition_section: list[str], errors: list[str]) -> int | None:
    for raw in repetition_section:
        normalized = raw.strip().lower()
        match = MAX_ATTEMPTS_RE.match(normalized)
        if match:
            try:
                value = int(match.group(1))
                if value < 1:
                    raise ValueError
                return value
            except ValueError:
                errors.append("Repetition Control has invalid max same-path attempts value")
                return None
    errors.append("Repetition Control missing numeric '- max same-path attempts: <n>'")
    return None


def run(
    path: Path,
    *,
    base_sha: str | None = None,
    head_sha: str | None = None,
    changed_files_override: list[str] | None = None,
) -> int:
    if not path.exists():
        print(f"task request contract validation failed: missing {path.as_posix()}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    lines = path.read_text(encoding="utf-8").splitlines()
    errors: list[str] = []

    contract_heading = "## Task Request Contract"
    report_heading = "## First-Time-Right Report"
    repetition_heading = "## Repetition Control"

    contract_section = _section_lines(lines, contract_heading)
    report_section = _section_lines(lines, report_heading)
    repetition_section = _section_lines(lines, repetition_heading)

    if not contract_section:
        errors.append(f"missing section: {contract_heading}")
    if not report_section:
        errors.append(f"missing section: {report_heading}")
    if not repetition_section:
        errors.append(f"missing section: {repetition_heading}")

    if contract_section:
        for prefix in _missing_prefixes(contract_section, REQUIRED_CONTRACT_ITEMS):
            errors.append(f"missing contract item in {contract_heading}: {prefix}")
    if report_section:
        for prefix in _missing_prefixes(report_section, REQUIRED_REPORT_ITEMS):
            errors.append(f"missing report item in {report_heading}: {prefix}")
    if repetition_section:
        for prefix in _missing_prefixes(repetition_section, REQUIRED_REPETITION_ITEMS):
            errors.append(f"missing repetition item in {repetition_heading}: {prefix}")
        contract_max = _extract_max_same_path_attempts(repetition_section, errors)
        policy_max = _load_policy_max_same_path_attempts(errors)
        if contract_max is not None and policy_max is not None and contract_max > policy_max:
            errors.append(
                "Repetition Control max same-path attempts "
                f"{contract_max} exceeds policy max_same_path_attempts {policy_max}"
            )

    if errors:
        print("task request contract validation failed:")
        for item in errors:
            print(f"- {item}")
        print(f"remediation: see {REMEDIATION_DOC}")
        return 1

    print(
        "task request contract validation: OK "
        f"(source={path.as_posix()} "
        f"contract_items={len(REQUIRED_CONTRACT_ITEMS)} "
        f"report_items={len(REQUIRED_REPORT_ITEMS)} "
        f"repetition_items={len(REQUIRED_REPETITION_ITEMS)})"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate task contract, first-time-right report, and repetition control."
    )
    parser.add_argument("--path", default=str(DEFAULT_TASK_NOTE_PATH))
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--head-sha", default=None)
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    args = parser.parse_args()

    changed_files_override: list[str] | None = None
    if args.base_sha and args.head_sha:
        changed_files_override = None
    elif args.stdin or args.changed_files:
        stdin_items = (
            [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]
            if args.stdin
            else []
        )
        changed_files_override = [*list(args.changed_files), *stdin_items]

    sys.exit(
        run(
            Path(args.path),
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            changed_files_override=changed_files_override,
        )
    )


if __name__ == "__main__":
    main()
