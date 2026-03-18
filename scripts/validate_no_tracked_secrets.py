from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path


SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("github_pat", re.compile(r"github_pat_[A-Za-z0-9_]{20,}")),
    ("github_classic_pat", re.compile(r"gh[pousr]_[A-Za-z0-9_]{20,}")),
    ("openai_key", re.compile(r"sk-[A-Za-z0-9]{20,}")),
    ("aws_access_key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("private_key_block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
)

TEXT_SUFFIXES = {
    ".md",
    ".txt",
    ".rst",
    ".py",
    ".yml",
    ".yaml",
    ".toml",
    ".json",
    ".jsonl",
    ".ini",
    ".cfg",
    ".env",
    ".sh",
    ".ps1",
}


def _tracked_files_from_git(*, root: Path) -> list[Path]:
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"git ls-files failed: {completed.stderr.strip()}")
    rows = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    return [root / row for row in rows]


def _is_probably_text(path: Path) -> bool:
    if path.suffix.lower() in TEXT_SUFFIXES:
        return True
    if path.name.lower() in {".env", ".env.example"}:
        return True
    return False


def _scan_file(path: Path) -> list[dict[str, object]]:
    findings: list[dict[str, object]] = []
    if not path.exists() or not path.is_file() or not _is_probably_text(path):
        return findings

    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return findings

    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern_name, pattern in SECRET_PATTERNS:
            if not pattern.search(line):
                continue
            findings.append(
                {
                    "path": path.as_posix(),
                    "line": line_number,
                    "pattern": pattern_name,
                    "snippet": line.strip()[:240],
                }
            )
    return findings


def validate(*, root: Path, paths: list[str] | None = None) -> tuple[list[dict[str, object]], dict[str, object]]:
    target_paths = [root / item for item in (paths or [])] if paths else _tracked_files_from_git(root=root)
    findings: list[dict[str, object]] = []
    scanned = 0
    for path in target_paths:
        if not path.exists() or not path.is_file():
            continue
        scanned += 1
        findings.extend(_scan_file(path))
    report = {
        "status": "ok" if not findings else "failed",
        "scanned_files": scanned,
        "findings_total": len(findings),
    }
    return findings, report


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate that tracked files do not contain hardcoded credentials.")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--paths", nargs="*", default=[])
    args = parser.parse_args()

    root = Path.cwd()
    try:
        findings, report = validate(root=root, paths=list(args.paths))
    except RuntimeError as exc:
        if args.format == "json":
            print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        else:
            print(f"tracked secrets validation: FAILED ({exc})")
        return 2

    payload = {**report, "findings": findings}
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        if not findings:
            print(f"tracked secrets validation: OK (scanned_files={report['scanned_files']})")
        else:
            print("tracked secrets validation: FAILED")
            for item in findings:
                print(
                    f"- {item['path']}:{item['line']} "
                    f"pattern={item['pattern']} snippet={item['snippet']}"
                )
    return 0 if not findings else 1


if __name__ == "__main__":
    raise SystemExit(main())
