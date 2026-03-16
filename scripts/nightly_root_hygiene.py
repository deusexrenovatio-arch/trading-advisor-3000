from __future__ import annotations

import argparse
from pathlib import Path


REQUIRED_ROOT_DIRS = (
    "docs",
    "scripts",
    "configs",
    "plans",
    "memory",
    "src",
    "tests",
    ".github",
)
REQUIRED_FILES = (
    "AGENTS.md",
    "README.md",
    "CODEOWNERS",
    "docs/session_handoff.md",
    "scripts/run_loop_gate.py",
    "scripts/run_pr_gate.py",
    "scripts/run_nightly_gate.py",
)
FORBIDDEN_TOKENS = ("run_" + "lean_gate.py",)
SCAN_EXTENSIONS = {".md", ".py", ".yaml", ".yml"}


def _iter_scan_files(repo_root: Path) -> list[Path]:
    targets: list[Path] = []
    for rel in ("AGENTS.md", "README.md"):
        candidate = repo_root / rel
        if candidate.exists():
            targets.append(candidate)
    for folder in ("docs", "scripts"):
        root = repo_root / folder
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() in SCAN_EXTENSIONS:
                targets.append(path)
    return sorted(set(targets))


def run(repo_root: Path) -> int:
    errors: list[str] = []
    for rel in REQUIRED_ROOT_DIRS:
        candidate = repo_root / rel
        if not candidate.exists() or not candidate.is_dir():
            errors.append(f"missing required root directory: {rel}")

    for rel in REQUIRED_FILES:
        candidate = repo_root / rel
        if not candidate.exists() or not candidate.is_file():
            errors.append(f"missing required file: {rel}")

    for path in _iter_scan_files(repo_root):
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN_TOKENS:
            if token in text:
                rel = path.relative_to(repo_root).as_posix()
                errors.append(f"forbidden token `{token}` in {rel}")

    if errors:
        print("nightly root hygiene: FAILED")
        for item in errors:
            print(f"- {item}")
        print("remediation: see docs/runbooks/governance-remediation.md")
        return 1

    print("nightly root hygiene: OK")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Nightly repository root hygiene validator.")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()
    raise SystemExit(run(Path(args.repo_root).resolve()))


if __name__ == "__main__":
    main()
