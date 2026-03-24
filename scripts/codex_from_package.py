#!/usr/bin/env python3
"""Run Codex from a zip package that contains a full task document set."""

from __future__ import annotations

import argparse
import html
import json
import re
import shutil
import subprocess
import sys
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_INBOX = Path("docs/codex/packages/inbox")
DEFAULT_PROMPT = Path("docs/codex/prompts/entry/from_package.md")
DEFAULT_ARTIFACT_ROOT = Path("artifacts/codex/package-intake")
DEFAULT_OUTPUT = Path("artifacts/codex/from-package-last-message.txt")
ALLOWED_MODES = {"auto", "plan-only", "implement-only", "continue", "repair"}
SUPPORTED_DOC_EXTENSIONS = {".md", ".txt", ".rst", ".docx", ".pdf"}
POSITIVE_HINTS = (
    ("technical_requirements", 140, "filename looks like technical requirements"),
    ("requirements", 120, "filename looks like requirements"),
    ("specification", 120, "filename looks like specification"),
    ("spec", 110, "filename looks like specification"),
    ("тз", 130, "filename looks like TZ"),
    ("техническ", 110, "filename looks like technical doc"),
    ("task", 70, "filename looks task-oriented"),
    ("brief", 60, "filename looks like brief"),
    ("architecture", 50, "filename looks architectural"),
)
NEGATIVE_HINTS = (
    ("readme", -40, "generic README is usually supporting material"),
    ("notes", -20, "notes are usually supporting material"),
    ("appendix", -15, "appendix is usually supporting material"),
    ("draft", -10, "draft is less stable than a finalized spec"),
)
EXTENSION_WEIGHTS = {
    ".md": 40,
    ".txt": 30,
    ".rst": 25,
    ".docx": 20,
    ".pdf": 10,
}


@dataclass(frozen=True)
class DocumentCandidate:
    rel_path: str
    score: int
    reasons: tuple[str, ...]
    title: str | None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def slugify(text: str, fallback: str = "package") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:48] if slug else fallback


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def choose_latest_package(inbox: Path) -> Path | None:
    if not inbox.exists():
        return None
    packages = [path for path in inbox.iterdir() if path.is_file() and path.suffix.lower() == ".zip"]
    if not packages:
        return None
    return max(packages, key=lambda path: (path.stat().st_mtime, path.name.lower()))


def ensure_zip_package(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"package file not found: {path}")
    if not path.is_file():
        raise ValueError(f"package path is not a file: {path}")
    if path.suffix.lower() != ".zip":
        raise ValueError(f"package must be a .zip archive: {path}")


def safe_extract_zip(package_path: Path, extracted_root: Path) -> None:
    extracted_root.mkdir(parents=True, exist_ok=True)
    root = extracted_root.resolve()
    with zipfile.ZipFile(package_path) as archive:
        for member in archive.infolist():
            destination = (extracted_root / member.filename).resolve()
            if destination != root and root not in destination.parents:
                raise ValueError(f"zip contains unsafe path: {member.filename}")
        archive.extractall(extracted_root)


def extract_docx_title(path: Path) -> str | None:
    try:
        with zipfile.ZipFile(path) as archive:
            raw = archive.read("word/document.xml").decode("utf-8", errors="ignore")
    except Exception:
        return None
    texts = [html.unescape(token).strip() for token in re.findall(r"<w:t[^>]*>(.*?)</w:t>", raw)]
    joined = " ".join(token for token in texts if token)
    return first_line(joined)


def first_line(text: str | None) -> str | None:
    if not text:
        return None
    for raw in text.splitlines():
        cleaned = raw.strip().lstrip("#").strip()
        if cleaned:
            return cleaned[:160]
    return None


def extract_title(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix in {".md", ".txt", ".rst"}:
        try:
            return first_line(path.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            return None
    if suffix == ".docx":
        return extract_docx_title(path)
    return None


def normalized_hint_text(path: Path) -> str:
    return path.as_posix().lower().replace(" ", "_").replace("-", "_")


def score_candidate(path: Path, *, extracted_root: Path) -> DocumentCandidate:
    rel_path = path.relative_to(extracted_root).as_posix()
    normalized = normalized_hint_text(Path(rel_path))
    reasons: list[str] = []
    score = EXTENSION_WEIGHTS.get(path.suffix.lower(), 0)
    reasons.append(f"extension weight: {path.suffix.lower() or '<none>'}")

    depth = max(len(Path(rel_path).parts) - 1, 0)
    depth_score = max(0, 18 - depth * 4)
    score += depth_score
    reasons.append(f"path depth bias: +{depth_score}")

    size = path.stat().st_size
    if size >= 2048:
        score += 5
        reasons.append("non-trivial file size")

    for token, weight, reason in POSITIVE_HINTS:
        if token in normalized:
            score += weight
            reasons.append(reason)
    for token, weight, reason in NEGATIVE_HINTS:
        if token in normalized:
            score += weight
            reasons.append(reason)

    title = extract_title(path)
    title_normalized = (title or "").lower()
    if title_normalized:
        if "requirements" in title_normalized or "требован" in title_normalized:
            score += 30
            reasons.append("title looks like requirements")
        if "spec" in title_normalized or "specification" in title_normalized or "тз" in title_normalized:
            score += 25
            reasons.append("title looks like specification")

    return DocumentCandidate(
        rel_path=rel_path,
        score=score,
        reasons=tuple(reasons),
        title=title,
    )


def collect_document_candidates(extracted_root: Path) -> list[DocumentCandidate]:
    candidates: list[DocumentCandidate] = []
    for path in sorted(extracted_root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in SUPPORTED_DOC_EXTENSIONS:
            continue
        candidates.append(score_candidate(path, extracted_root=extracted_root))
    candidates.sort(key=lambda item: (-item.score, item.rel_path))
    return candidates


def summarize_extensions(extracted_root: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in extracted_root.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower() or "<none>"
        counts[suffix] = counts.get(suffix, 0) + 1
    return dict(sorted(counts.items()))


def render_manifest(
    *,
    package_path: Path,
    extracted_root: Path,
    candidates: list[DocumentCandidate],
) -> str:
    top_candidate = candidates[0] if candidates else None
    lines = [
        "# Package Intake Manifest",
        "",
        f"Updated: {utc_now().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## Source",
        f"- Package Zip: {package_path.as_posix()}",
        f"- Extracted Root: {extracted_root.as_posix()}",
        "",
        "## Package Summary",
    ]
    for suffix, count in summarize_extensions(extracted_root).items():
        lines.append(f"- {suffix}: {count}")

    lines.extend(["", "## Suggested Primary Document"])
    if top_candidate is None:
        lines.append("- None")
    else:
        lines.append(f"- Path: {top_candidate.rel_path}")
        lines.append(f"- Score: {top_candidate.score}")
        if top_candidate.title:
            lines.append(f"- Title: {top_candidate.title}")

    lines.extend(["", "## Candidate Ranking"])
    if not candidates:
        lines.append("- No supported candidate documents were found.")
    else:
        for candidate in candidates[:8]:
            lines.append(f"- {candidate.rel_path} | score={candidate.score}")
            if candidate.title:
                lines.append(f"  title: {candidate.title}")
            lines.append(f"  reasons: {', '.join(candidate.reasons[:4])}")

    lines.extend(["", "## Clarification Policy"])
    lines.append("- Ask at most one compact clarification block only when safe progress is impossible.")

    lines.extend(["", "## Risks"])
    if top_candidate is None:
        lines.append("- No trusted primary document was detected from supported file types.")
    elif len(candidates) > 1 and candidates[1].score == top_candidate.score:
        lines.append("- Top candidate tie detected; confirm the primary document if the content conflicts.")
    else:
        lines.append("- No immediate intake blocker detected from filename heuristics.")

    lines.append("")
    return "\n".join(lines)


def write_manifest_json(
    *,
    path: Path,
    package_path: Path,
    extracted_root: Path,
    candidates: list[DocumentCandidate],
) -> None:
    payload = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "package_zip": package_path.as_posix(),
        "extracted_root": extracted_root.as_posix(),
        "suggested_primary_document": candidates[0].rel_path if candidates else None,
        "candidates": [asdict(candidate) for candidate in candidates],
        "extension_counts": summarize_extensions(extracted_root),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_prompt(
    *,
    prompt_path: Path,
    package_path: Path,
    extracted_root: Path,
    manifest_path: Path,
    suggested_primary: str | None,
    mode: str,
) -> str:
    base = prompt_path.read_text(encoding="utf-8").rstrip()
    primary_line = suggested_primary if suggested_primary else "NONE"
    return (
        f"{base}\n\n"
        f"Package zip path: {package_path.as_posix()}\n"
        f"Extracted package root: {extracted_root.as_posix()}\n"
        f"Package manifest path: {manifest_path.as_posix()}\n"
        f"Suggested primary document: {primary_line}\n"
        f"Mode hint: {mode}\n"
    )


def run_codex(*, repo_root: Path, prompt: str, profile: str, output_path: Path) -> int:
    codex_bin = shutil.which("codex")
    if codex_bin is None:
        print(
            "error: `codex` executable was not found in PATH. "
            "Install Codex CLI first and authenticate with your ChatGPT account.",
            file=sys.stderr,
        )
        return 127

    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        codex_bin,
        "exec",
        "-C",
        str(repo_root),
        "-p",
        profile,
        "--output-last-message",
        str(output_path),
        "-",
    ]
    completed = subprocess.run(
        cmd,
        input=prompt,
        text=True,
        cwd=repo_root,
        check=False,
    )
    return int(completed.returncode)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch Codex from a zip package.")
    parser.add_argument("package_path", nargs="?", help="Path to the zip package.")
    parser.add_argument(
        "--mode",
        default="auto",
        choices=sorted(ALLOWED_MODES),
        help="Autopilot mode hint passed to Codex.",
    )
    parser.add_argument(
        "--profile",
        default="deep",
        help="Codex profile name.",
    )
    parser.add_argument(
        "--prompt-file",
        default=str(DEFAULT_PROMPT),
        help="Repo-relative path to the package entry prompt file.",
    )
    parser.add_argument(
        "--inbox",
        default=str(DEFAULT_INBOX),
        help="Repo-relative inbox used when package_path is omitted.",
    )
    parser.add_argument(
        "--artifact-root",
        default=str(DEFAULT_ARTIFACT_ROOT),
        help="Repo-relative artifact root for unpacked packages and manifests.",
    )
    parser.add_argument(
        "--output-last-message",
        default=str(DEFAULT_OUTPUT),
        help="Repo-relative path for Codex's last-message capture.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare the package intake artifacts but do not invoke Codex.",
    )
    return parser.parse_args(argv)


def resolve_path(repo_root: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    repo_root = resolve_repo_root()

    if args.package_path:
        package_path = resolve_path(repo_root, args.package_path)
    else:
        inbox = resolve_path(repo_root, args.inbox)
        latest = choose_latest_package(inbox)
        if latest is None:
            print(f"error: no zip packages found in inbox: {inbox}", file=sys.stderr)
            return 2
        package_path = latest.resolve()

    try:
        ensure_zip_package(package_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    prompt_path = resolve_path(repo_root, args.prompt_file)
    if not prompt_path.exists():
        print(f"error: prompt file not found: {prompt_path}", file=sys.stderr)
        return 2

    artifact_root = resolve_path(repo_root, args.artifact_root)
    run_root = artifact_root / f"{utc_stamp()}-{slugify(package_path.stem)}"
    extracted_root = run_root / "extracted"
    manifest_path = run_root / "manifest.md"
    manifest_json_path = run_root / "manifest.json"

    try:
        safe_extract_zip(package_path, extracted_root)
    except (ValueError, zipfile.BadZipFile) as exc:
        print(f"error: failed to extract package: {exc}", file=sys.stderr)
        return 2

    candidates = collect_document_candidates(extracted_root)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        render_manifest(
            package_path=package_path,
            extracted_root=extracted_root,
            candidates=candidates,
        ),
        encoding="utf-8",
    )
    write_manifest_json(
        path=manifest_json_path,
        package_path=package_path,
        extracted_root=extracted_root,
        candidates=candidates,
    )

    suggested_primary = None
    if candidates:
        suggested_primary = (extracted_root / candidates[0].rel_path).resolve().as_posix()

    print(f"package zip: {package_path.as_posix()}")
    print(f"extracted root: {extracted_root.as_posix()}")
    print(f"manifest: {manifest_path.as_posix()}")
    print(f"suggested primary document: {suggested_primary or 'NONE'}")

    if args.dry_run:
        print("dry run: Codex invocation skipped")
        return 0

    output_path = resolve_path(repo_root, args.output_last_message)
    prompt = build_prompt(
        prompt_path=prompt_path,
        package_path=package_path,
        extracted_root=extracted_root,
        manifest_path=manifest_path,
        suggested_primary=suggested_primary,
        mode=args.mode,
    )
    return run_codex(
        repo_root=repo_root,
        prompt=prompt,
        profile=args.profile,
        output_path=output_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
