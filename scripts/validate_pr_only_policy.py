from __future__ import annotations

import argparse
import sys
from pathlib import Path


REQUIRED_TOKENS = (
    "AI_SHELL_EMERGENCY_MAIN_PUSH",
    "AI_SHELL_EMERGENCY_MAIN_PUSH_REASON",
)
FORBIDDEN_TOKENS = (
    "MOEX_CARRY_ALLOW_MAIN_PUSH",
    "MOEX_CARRY_EMERGENCY_MAIN_PUSH",
    "MOEX_CARRY_EMERGENCY_MAIN_PUSH_REASON",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def run(repo_root: Path) -> int:
    files = [
        repo_root / "AGENTS.md",
        repo_root / "README.md",
        repo_root / "docs" / "DEV_WORKFLOW.md",
        repo_root / ".githooks" / "pre-push",
    ]
    errors: list[str] = []
    for path in files:
        if not path.exists():
            errors.append(f"missing required policy file: {path.as_posix()}")
            continue
        text = _read(path)
        for token in REQUIRED_TOKENS:
            if token not in text:
                errors.append(f"{path.as_posix()}: missing required token `{token}`")
        for token in FORBIDDEN_TOKENS:
            if token in text:
                errors.append(f"{path.as_posix()}: forbidden legacy token `{token}`")
    hook_text = _read(repo_root / ".githooks" / "pre-push")
    if "run_loop_gate.py" not in hook_text:
        errors.append(".githooks/pre-push must call run_loop_gate.py")
    if "refs/heads/main" not in hook_text:
        errors.append(".githooks/pre-push must protect refs/heads/main")

    if errors:
        print("pr-only policy validation failed:")
        for item in errors:
            print(f"- {item}")
        print("remediation: see docs/runbooks/governance-remediation.md")
        return 1

    print("pr-only policy validation: OK")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate PR-only main policy surfaces.")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()
    sys.exit(run(Path(args.repo_root).resolve()))


if __name__ == "__main__":
    main()
