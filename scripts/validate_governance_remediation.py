from __future__ import annotations

import argparse
import sys
from pathlib import Path


REQUIRED_COMMAND_MENTIONS = (
    "python scripts/task_session.py begin",
    "python scripts/run_loop_gate.py",
    "python scripts/run_pr_gate.py",
    "python scripts/run_nightly_gate.py",
    "python scripts/validate_task_request_contract.py",
    "python scripts/validate_session_handoff.py",
)


def run(path: Path) -> int:
    if not path.exists():
        print(f"governance remediation validation failed: missing {path.as_posix()}")
        return 1
    text = path.read_text(encoding="utf-8")
    missing = [token for token in REQUIRED_COMMAND_MENTIONS if token not in text]
    if missing:
        print("governance remediation validation failed:")
        for token in missing:
            print(f"- missing command mention: {token}")
        return 1
    print("governance remediation validation: OK")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate governance remediation runbook coverage.")
    parser.add_argument("--path", default="docs/runbooks/governance-remediation.md")
    args = parser.parse_args()
    sys.exit(run(Path(args.path)))


if __name__ == "__main__":
    main()
