"""Validate architecture admission evidence from an existing task or PR body."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping

from gate_common import collect_changed_files

BLOCK = re.compile(r"^```architecture-review[ \t]*\r?\n(.*?)^```[ \t]*\r?$", re.M | re.S)
STANDARD_PATH = "docs/architecture/product-plane/architecture-standard.md"
CRITERION_HEADING = re.compile(r"^### ([A-Z][A-Z0-9]*-\d+)(?:[ \t]|$)", re.M)
DECISION_FIELDS = (
    "owner",
    "accepted_contract",
    "change",
    "reuse",
    "cost",
    "compatibility",
    "validation",
)


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def decision_digest(decision: dict[str, Any]) -> str:
    return _digest(decision)


@dataclass(frozen=True)
class ArchitectureStandard:
    digest: str
    criteria: tuple[str, ...]

    @property
    def pin(self) -> dict[str, str]:
        return {"path": STANDARD_PATH, "digest": self.digest}


def read_standard(repo_root: Path, *, head_ref: str | None = None) -> ArchitectureStandard:
    """The owning Markdown is both normative text and the criterion-ID source."""
    if head_ref:
        result = subprocess.run(
            ["git", "show", f"{head_ref}:{STANDARD_PATH}"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if result.returncode:
            raise ValueError("architecture standard is missing from reviewed head")
        text = result.stdout
    else:
        text = (repo_root / STANDARD_PATH).read_text(encoding="utf-8-sig")
    text = text.removeprefix("\ufeff").replace("\r\n", "\n")
    criteria = tuple(CRITERION_HEADING.findall(text))
    if not criteria or len(criteria) != len(set(criteria)):
        raise ValueError("architecture standard requires nonempty, unique criterion headings")
    return ArchitectureStandard(hashlib.sha256(text.encode("utf-8")).hexdigest(), criteria)


def _git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo_root, capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise ValueError(f"Git operation failed: {args[0]}")
    return result.stdout.strip()


def read_decision_record(
    repo_root: Path, path: str, *, head_ref: str | None = None
) -> dict[str, str]:
    """Pin an owning architecture document from the worktree or exact PR head."""
    root = repo_root.resolve()
    normalized = path.replace("\\", "/")
    parts = PurePosixPath(normalized)
    if (
        parts.is_absolute()
        or PureWindowsPath(normalized).drive
        or ":" in normalized
        or ".." in parts.parts
        or parts.parts[:2] != ("docs", "architecture")
        or parts.suffix != ".md"
    ):
        raise ValueError("record must name repository-relative docs/architecture Markdown")
    normalized = parts.as_posix()
    if head_ref:
        head = _git(root, "rev-parse", "--verify", f"{head_ref}^{{commit}}")
        entry = _git(root, "ls-tree", head, "--", f":(literal){normalized}")
        metadata = entry.split("\t", 1)[0].split()
        if len(metadata) != 3 or metadata[0] not in {"100644", "100755"}:
            raise ValueError("record must exist as a regular file in reviewed head")
        result = subprocess.run(
            ["git", "show", f"{head}:{normalized}"],
            cwd=root,
            capture_output=True,
            text=True,
            encoding="utf-8-sig",
            check=False,
        )
        if result.returncode:
            raise ValueError("record is unavailable from reviewed head")
        text = result.stdout
    else:
        target = root / normalized
        for candidate in (target, *target.parents):
            if candidate == root:
                break
            attributes = getattr(candidate.lstat(), "st_file_attributes", 0)
            if candidate.is_symlink() or attributes & 0x400:  # Windows reparse points
                raise ValueError("record and its ancestors must not be symlinks/reparse points")
        if not target.resolve().is_relative_to(root) or not target.is_file():
            raise ValueError("record must be a regular file inside the repository")
        text = target.read_text(encoding="utf-8-sig")
    text = text.removeprefix("\ufeff").replace("\r\n", "\n")
    if not text.strip():
        raise ValueError("record must not be empty")
    return {"path": normalized, "digest": hashlib.sha256(text.encode("utf-8")).hexdigest()}


def scope_digest(
    repo_root: Path,
    changed_files: list[str],
    *,
    base_ref: str | None = None,
    head_ref: str | None = None,
    git_ref: str = "HEAD",
) -> str:
    """Bind selected paths to immutable PR head or Git-normalized working content."""
    root = repo_root.resolve()
    base, head = _resolve_refs(root, base_ref, head_ref, git_ref)
    files = []
    for raw in sorted(set(changed_files)):
        path = raw.replace("\\", "/")
        parts = PurePosixPath(path)
        if not path or parts.is_absolute() or ".." in parts.parts or PureWindowsPath(path).drive:
            raise ValueError(f"scope path must be repository-relative: {raw}")
        path = parts.as_posix()
        if head_ref:
            entry = _git(root, "ls-tree", head, "--", f":(literal){path}")
            if entry:
                metadata = entry.split("\t", 1)[0].split()
                if metadata[1] != "blob":
                    raise ValueError(f"scope path must name a file: {path}")
                blob = metadata[2]
            else:
                blob = "absent"
        else:
            target = root / path
            if not target.resolve().is_relative_to(root):
                raise ValueError(f"scope path resolves outside repository: {path}")
            if target.is_file():
                blob = _git(root, "hash-object", f"--path={path}", "--", path)
            elif target.exists():
                raise ValueError(f"scope path must name a file: {path}")
            else:
                blob = "absent"
        files.append([path, blob])
    return _digest({"base": base, "head": head, "files": files})


def _resolve_refs(root: Path, base_ref: str | None, head_ref: str | None, git_ref: str):
    if bool(base_ref) != bool(head_ref):
        raise ValueError("base-ref and head-ref must be supplied together")
    return (
        _git(root, "rev-parse", "--verify", f"{base_ref or git_ref}^{{commit}}"),
        _git(root, "rev-parse", "--verify", f"{head_ref or git_ref}^{{commit}}"),
    )


def load_review_body(
    source: str | None = None, *, environ: Mapping[str, str] | None = None
) -> str | None:
    env = os.environ if environ is None else environ
    path = source or env.get("AI_SHELL_ARCHITECTURE_REVIEW_SOURCE")
    if path:
        return Path(path).read_text(encoding="utf-8-sig")
    event_path = env.get("GITHUB_EVENT_PATH")
    if not event_path:
        return None
    event = json.loads(Path(event_path).read_text(encoding="utf-8"))
    if not isinstance(event, dict):
        raise ValueError("GitHub event must be an object")
    pr = event.get("pull_request")
    if pr is None:
        return None
    if not isinstance(pr, dict) or not isinstance(pr.get("body", ""), (str, type(None))):
        raise ValueError("pull_request.body must be text")
    return pr.get("body") or ""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate review field: {key}")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def parse_review(body: str | None) -> dict[str, Any]:
    matches = BLOCK.findall(body or "")
    if len(matches) != 1:
        raise ValueError("classification requires exactly one architecture-review JSON block")
    value = json.loads(
        matches[0], object_pairs_hook=_unique_object, parse_constant=_invalid_constant
    )
    if not isinstance(value, dict):
        raise ValueError("architecture-review must be an object")
    return value


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _criterion_errors(value: Any, *, phase: str, standard: ArchitectureStandard) -> list[str]:
    if not isinstance(value, list):
        return [f"{phase}.criteria must assess the architecture standard"]
    errors = []
    seen: set[str] = set()
    for assessment in value:
        if not isinstance(assessment, dict):
            errors.append(f"{phase}.criteria contains an invalid assessment")
            continue
        identifier = assessment.get("id")
        if not isinstance(identifier, str) or identifier not in standard.criteria:
            errors.append(f"{phase}.criteria contains an unknown criterion id")
            continue
        if identifier in seen:
            errors.append(f"{phase}.criteria duplicates {identifier}")
        seen.add(identifier)
        status = assessment.get("status")
        if status == "satisfied":
            if not _text(assessment.get("evidence")):
                errors.append(f"{phase}.criteria {identifier} requires evidence")
        elif status == "not-applicable":
            if not _text(assessment.get("reason")):
                errors.append(f"{phase}.criteria {identifier} requires a scope reason")
        elif status == "gap":
            errors.append(f"{phase}.criteria {identifier} has an unresolved architectural gap")
        else:
            errors.append(f"{phase}.criteria {identifier} has an invalid status")
    missing = set(standard.criteria) - seen
    if missing:
        errors.append(f"{phase}.criteria missing: {', '.join(sorted(missing))}")
    return errors


def _review_errors(
    value: Any,
    *,
    phase: str,
    author: str,
    expected_decision: str,
    standard: ArchitectureStandard,
) -> list[str]:
    if not isinstance(value, dict):
        return [f"{phase} independent review is required"]
    errors = []
    reviewer = value.get("reviewer")
    if not _text(reviewer) or reviewer.strip().casefold() == author.strip().casefold():
        errors.append(f"{phase}.reviewer must identify an independent reviewer")
    if value.get("verdict") != "PASS":
        errors.append(f"{phase}.verdict must be PASS; dependent acceptance is blocked")
    if value.get("decision_digest") != expected_decision:
        errors.append(f"{phase}.decision_digest is stale or missing")
    evidence = value.get("evidence")
    if not isinstance(evidence, list) or not evidence or not all(map(_text, evidence)):
        errors.append(f"{phase}.evidence must name focused review evidence")
    findings = value.get("findings")
    if not isinstance(findings, list):
        errors.append(f"{phase}.findings must be a list")
    else:
        for finding in findings:
            if not isinstance(finding, dict):
                errors.append(f"{phase}.findings contains an invalid finding")
                continue
            if finding.get("status") != "closed":
                errors.append(f"{phase}.findings contains an open or unclassified finding")
            if not _text(finding.get("id")) or not _text(finding.get("evidence")):
                errors.append(f"{phase}.findings requires id and closure evidence")
    errors.extend(_criterion_errors(value.get("criteria"), phase=phase, standard=standard))
    return errors


def validate_review(
    body: str | None,
    *,
    phase: str,
    expected_scope: str | None,
    expected_standard: ArchitectureStandard | None = None,
    repo_root: Path | None = None,
    head_ref: str | None = None,
) -> list[str]:
    if phase not in {"design", "implementation"}:
        return ["unknown architecture review phase"]
    try:
        value = parse_review(body)
    except (ValueError, TypeError) as exc:
        return [str(exc)]
    errors = []
    impact = value.get("impact")
    if impact not in {"none", "within-contract", "contract-change"}:
        return ["impact must be none, within-contract, or contract-change"]
    for field in ("author", "reason"):
        if not _text(value.get(field)):
            errors.append(f"{field} must be nonempty")
    if phase == "implementation" and value.get("scope_digest") != expected_scope:
        errors.append("scope_digest is stale or missing; refresh independent implementation review")
    if impact == "none":
        return errors
    root = repo_root if repo_root is not None else Path(__file__).resolve().parents[1]
    decision = value.get("decision")
    record = decision.get("record") if isinstance(decision, dict) else None
    if not isinstance(record, dict) or not _text(record.get("path")):
        errors.append("decision.record requires an owning document path and digest")
    else:
        try:
            current = read_decision_record(root, record["path"], head_ref=head_ref)
            if record.get("digest") != current["digest"]:
                errors.append("decision.record.digest is stale or missing; refresh review bindings")
        except (OSError, ValueError) as exc:
            errors.append(f"decision.record: {exc}")
    if not isinstance(decision, dict):
        return errors + ["decision object is required for triggered architecture review"]
    for field in DECISION_FIELDS:
        if not _text(decision.get(field)):
            errors.append(f"decision.{field} must name the reviewed design")
    try:
        standard = expected_standard or read_standard(root, head_ref=head_ref)
    except (OSError, ValueError) as exc:
        return errors + [f"architecture standard unavailable: {exc}"]
    pin = decision.get("standard")
    if not isinstance(pin, dict) or pin.get("path") != STANDARD_PATH:
        errors.append("decision.standard.path must identify the canonical architecture standard")
    if not isinstance(pin, dict) or pin.get("digest") != standard.digest:
        errors.append("decision.standard.digest is stale or missing; refresh both review phases")
    digest = decision_digest(decision)
    author = value.get("author") if isinstance(value.get("author"), str) else ""
    phases = ("design",) if phase == "design" else ("design", "implementation")
    for checkpoint in phases:
        errors.extend(
            _review_errors(
                value.get(checkpoint),
                phase=checkpoint,
                author=author,
                expected_decision=digest,
                standard=standard,
            )
        )
    if phase == "implementation" and impact == "contract-change":
        implementation = value.get("implementation")
        proof = implementation.get("producer_proof") if isinstance(implementation, dict) else None
        if not isinstance(proof, dict) or proof.get("kind") != "real":
            errors.append(
                "implementation.producer_proof requires real producer-to-consumer evidence"
            )
        else:
            for field in ("producer", "consumer", "evidence"):
                if not _text(proof.get(field)):
                    errors.append(f"implementation.producer_proof.{field} is required")
    return errors


def check_architecture_review(
    changed_files: list[str],
    *,
    phase: str,
    source: str | None = None,
    base_ref: str | None = None,
    head_ref: str | None = None,
    git_ref: str = "HEAD",
    diagnostic: bool = False,
) -> int:
    try:
        _resolve_refs(Path.cwd(), base_ref, head_ref, git_ref)
        if not changed_files:
            print("architecture review: no selected changes (Git refs verified)")
            return 0
        body = load_review_body(source)
        if body is None and diagnostic:
            print("architecture review: classification pending; diagnostics only, not admission")
            return 0
        binding = None
        if phase == "implementation":
            binding = scope_digest(
                Path.cwd(), changed_files, base_ref=base_ref, head_ref=head_ref, git_ref=git_ref
            )
        standard = None
        if body is not None and parse_review(body).get("impact") in (
            "within-contract",
            "contract-change",
        ):
            standard = read_standard(Path.cwd(), head_ref=head_ref)
        errors = validate_review(
            body,
            phase=phase,
            expected_scope=binding,
            expected_standard=standard,
            repo_root=Path.cwd(),
            head_ref=head_ref,
        )
    except (OSError, ValueError, TypeError) as exc:
        errors = [str(exc)]
    if errors:
        print("architecture review: FAILED")
        for error in errors:
            print(f"- {error}")
        print("remediation: see docs/agent/architecture-review.md")
        return 2
    print(f"architecture review: OK (phase={phase} scope_digest={binding or 'design-only'})")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--git-ref", default="HEAD")
    parser.add_argument("--base-ref", "--base", dest="base_ref")
    parser.add_argument("--head-ref", "--head", dest="head_ref")
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--changed-files", nargs="*", default=[])
    parser.add_argument("--architecture-review-source")
    parser.add_argument("--phase", choices=("design", "implementation"), default="implementation")
    parser.add_argument("--print-binding", action="store_true")
    parser.add_argument("--record-path", help="Owning architecture Markdown for --print-binding")
    args = parser.parse_args()
    try:
        files = collect_changed_files(
            base_ref=args.base_ref,
            head_ref=args.head_ref,
            git_ref=args.git_ref,
            from_git=args.from_git,
            changed_files=args.changed_files,
            from_stdin=args.stdin,
        )
        if args.print_binding:
            result: dict[str, Any] = {
                "scope_digest": scope_digest(
                    Path.cwd(),
                    files,
                    base_ref=args.base_ref,
                    head_ref=args.head_ref,
                    git_ref=args.git_ref,
                )
            }
            standard = read_standard(Path.cwd(), head_ref=args.head_ref)
            result["standard"] = {**standard.pin, "criteria": list(standard.criteria)}
            body = load_review_body(args.architecture_review_source)
            record_path = args.record_path
            if body is not None:
                decision = parse_review(body).get("decision")
                if isinstance(decision, dict):
                    result["decision_digest"] = decision_digest(decision)
                    record = decision.get("record")
                    if record_path is None and isinstance(record, dict):
                        record_path = record.get("path")
            if record_path is not None:
                if not _text(record_path):
                    raise ValueError("decision.record.path must be nonempty text")
                result["decision_record"] = read_decision_record(
                    Path.cwd(), record_path, head_ref=args.head_ref
                )
            print(json.dumps(result))
            return 0
        return check_architecture_review(
            files,
            phase=args.phase,
            source=args.architecture_review_source,
            base_ref=args.base_ref,
            head_ref=args.head_ref,
            git_ref=args.git_ref,
        )
    except (OSError, ValueError, TypeError) as exc:
        print(f"architecture review: FAILED ({exc})")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
