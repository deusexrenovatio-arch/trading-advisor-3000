#!/usr/bin/env python3
"""Run Codex from a zip package that contains a full task document set."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from typing import Any

from phase_tz_compiler import compile_phase_plan, phase_plan_payload


DEFAULT_INBOX = Path("docs/codex/packages/inbox")
DEFAULT_PROMPT = Path("docs/codex/prompts/entry/from_package.md")
DEFAULT_ARTIFACT_ROOT = Path("artifacts/codex/package-intake")
DEFAULT_OUTPUT = Path("artifacts/codex/from-package-last-message.txt")
ALLOWED_MODES = {"auto", "plan-only", "implement-only", "continue", "repair"}
SUPPORTED_DOC_EXTENSIONS = {".md", ".txt", ".rst", ".docx", ".pdf"}
INTAKE_GATE_BEGIN = "BEGIN_INTAKE_GATE_JSON"
INTAKE_GATE_END = "END_INTAKE_GATE_JSON"
TECHNICAL_INTAKE_BEGIN = "BEGIN_TECHNICAL_INTAKE_JSON"
TECHNICAL_INTAKE_END = "END_TECHNICAL_INTAKE_JSON"
PRODUCT_INTAKE_BEGIN = "BEGIN_PRODUCT_INTAKE_JSON"
PRODUCT_INTAKE_END = "END_PRODUCT_INTAKE_JSON"
INTAKE_BLOCKED_EXIT = 3
BLOCKER_SEVERITIES = ("P0", "P1", "P2")
BLOCKER_SCALES = ("S", "M", "L", "XL")
POSITIVE_HINTS = (
    ("technical_requirements", 140, "filename looks like technical requirements"),
    ("requirements", 120, "filename looks like requirements"),
    ("specification", 120, "filename looks like specification"),
    ("spec", 110, "filename looks like specification"),
    ("tz", 130, "filename looks like TZ"),
    ("тз", 130, "filename looks like TZ"),
    ("техническ", 110, "filename looks like technical doc"),
    ("task", 70, "filename looks task-oriented"),
    ("brief", 60, "filename looks like brief"),
    ("architecture", 50, "filename looks architectural"),
)
NEGATIVE_HINTS = (
    ("readme", -40, "generic README is usually supporting material"),
    ("verdict", -80, "verdict docs are usually downstream acceptance outputs"),
    ("findings", -55, "findings docs are usually analytical supporting material"),
    ("manifest", -45, "manifest docs are usually inventory/supporting material"),
    ("evidence", -25, "evidence docs are usually supporting material"),
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


def extract_tagged_json(text: str, *, begin: str, end: str) -> dict[str, Any]:
    start = text.rfind(begin)
    stop = text.rfind(end)
    if start < 0 or stop < 0 or stop <= start:
        raise ValueError(f"missing tagged json block {begin} ... {end}")
    payload = text[start + len(begin) : stop].strip()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"failed to parse tagged json: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("tagged json payload must be an object")
    return data


def _normalize_blockers(raw_blockers: Any, *, lane: str) -> list[dict[str, str]]:
    if raw_blockers is None:
        return []
    if not isinstance(raw_blockers, list):
        raise ValueError(f"{lane}.blockers must be an array")
    normalized: list[dict[str, str]] = []
    for idx, item in enumerate(raw_blockers, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{lane}.blockers[{idx}] must be an object")
        severity = str(item.get("severity", "")).strip().upper()
        if severity not in BLOCKER_SEVERITIES:
            raise ValueError(f"{lane}.blockers[{idx}].severity must be one of {BLOCKER_SEVERITIES}")
        scale = str(item.get("scale", "")).strip().upper()
        if scale not in BLOCKER_SCALES:
            raise ValueError(f"{lane}.blockers[{idx}].scale must be one of {BLOCKER_SCALES}")
        title = str(item.get("title", "")).strip()
        why = str(item.get("why", item.get("reason", ""))).strip()
        if not title:
            raise ValueError(f"{lane}.blockers[{idx}].title is required")
        if not why:
            raise ValueError(f"{lane}.blockers[{idx}].why is required")
        normalized.append(
            {
                "id": str(item.get("id", f"{lane.upper()}-{idx:02d}")).strip(),
                "severity": severity,
                "scale": scale,
                "title": title,
                "why": why,
                "required_action": str(item.get("required_action", item.get("remediation", ""))).strip(),
            }
        )
    return normalized


def _normalize_lane(payload: dict[str, Any], lane: str) -> dict[str, Any]:
    lane_payload = payload.get(lane)
    if not isinstance(lane_payload, dict):
        raise ValueError(f"{lane} object is required")
    created_docs_raw = lane_payload.get("created_docs", [])
    if created_docs_raw is None:
        created_docs_raw = []
    if not isinstance(created_docs_raw, list):
        raise ValueError(f"{lane}.created_docs must be an array")
    created_docs = [str(item).strip() for item in created_docs_raw if str(item).strip()]
    blockers = _normalize_blockers(lane_payload.get("blockers"), lane=lane)
    review_summary = str(lane_payload.get("review_summary", "")).strip()
    return {
        "created_docs": created_docs,
        "review_summary": review_summary,
        "blockers": blockers,
    }


def _auto_blockers_for_lane(*, lane: str, created_docs: list[str], review_summary: str) -> list[dict[str, str]]:
    auto: list[dict[str, str]] = []
    if not created_docs:
        auto.append(
            {
                "id": f"AUTO-{lane.upper()}-DOCS",
                "severity": "P0",
                "scale": "M",
                "title": "Mandatory documentation artifacts are missing",
                "why": "Intake must produce documentation outputs before the phase gate can pass.",
                "required_action": "Create/refresh execution contract, parent brief, phase briefs, and linked intake docs.",
            }
        )
    if not review_summary:
        auto.append(
            {
                "id": f"AUTO-{lane.upper()}-REVIEW",
                "severity": "P1",
                "scale": "S",
                "title": "Review summary is missing",
                "why": "Intake must include explicit review findings and closure status.",
                "required_action": "Add a concise review summary with accepted risks and unresolved findings.",
            }
        )
    return auto


def _severity_counts(blockers: list[dict[str, str]]) -> dict[str, int]:
    counts = {"P0": 0, "P1": 0, "P2": 0}
    for blocker in blockers:
        counts[blocker["severity"]] += 1
    return counts


def _max_scale(blockers: list[dict[str, str]]) -> str:
    if not blockers:
        return "NONE"
    weights = {"S": 1, "M": 2, "L": 3, "XL": 4}
    strongest = max(blockers, key=lambda item: weights.get(item["scale"], 0))
    return strongest["scale"]


def evaluate_intake_gate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    technical = _normalize_lane(payload, "technical_intake")
    product = _normalize_lane(payload, "product_intake")
    technical_blockers = list(technical["blockers"]) + _auto_blockers_for_lane(
        lane="technical_intake",
        created_docs=technical["created_docs"],
        review_summary=technical["review_summary"],
    )
    product_blockers = list(product["blockers"]) + _auto_blockers_for_lane(
        lane="product_intake",
        created_docs=product["created_docs"],
        review_summary=product["review_summary"],
    )
    all_blockers = technical_blockers + product_blockers
    severity = _severity_counts(all_blockers)
    blocking_total = severity["P0"] + severity["P1"]
    decision = "BLOCKED" if blocking_total > 0 else "PASS"
    return {
        "schema_version": 1,
        "gate_mode": "formal-intake-gate",
        "formal_rule": "BLOCK when any P0/P1 blocker exists across technical_intake and product_intake.",
        "technical_intake": {
            "created_docs": technical["created_docs"],
            "review_summary": technical["review_summary"],
            "blockers": technical_blockers,
            "severity_counts": _severity_counts(technical_blockers),
        },
        "product_intake": {
            "created_docs": product["created_docs"],
            "review_summary": product["review_summary"],
            "blockers": product_blockers,
            "severity_counts": _severity_counts(product_blockers),
        },
        "combined_gate": {
            "decision": decision,
            "blocking_total": blocking_total,
            "severity_counts": severity,
            "max_problem_scale": _max_scale([item for item in all_blockers if item["severity"] in {"P0", "P1"}]),
            "reported_decision": str(payload.get("combined_gate", {}).get("decision", "")).strip().upper(),
        },
    }


def evaluate_intake_gate_from_text(text: str) -> dict[str, Any]:
    payload = extract_tagged_json(text, begin=INTAKE_GATE_BEGIN, end=INTAKE_GATE_END)
    return evaluate_intake_gate_payload(payload)


def render_intake_gate_markdown(gate: dict[str, Any]) -> str:
    combined = gate["combined_gate"]
    lines = [
        "# Intake Gate",
        "",
        f"- Decision: {combined['decision']}",
        f"- Blocking Total (P0/P1): {combined['blocking_total']}",
        f"- Severity Counts: P0={combined['severity_counts']['P0']}, P1={combined['severity_counts']['P1']}, P2={combined['severity_counts']['P2']}",
        f"- Max Problem Scale: {combined['max_problem_scale']}",
        "",
        "## Technical Intake",
        f"- Created Docs: {len(gate['technical_intake']['created_docs'])}",
        f"- Review Summary Present: {'yes' if gate['technical_intake']['review_summary'] else 'no'}",
        f"- Blockers: {len(gate['technical_intake']['blockers'])}",
        "",
        "## Product Intake",
        f"- Created Docs: {len(gate['product_intake']['created_docs'])}",
        f"- Review Summary Present: {'yes' if gate['product_intake']['review_summary'] else 'no'}",
        f"- Blockers: {len(gate['product_intake']['blockers'])}",
        "",
    ]
    return "\n".join(lines)


def lane_tags(lane: str) -> tuple[str, str]:
    if lane == "technical_intake":
        return TECHNICAL_INTAKE_BEGIN, TECHNICAL_INTAKE_END
    if lane == "product_intake":
        return PRODUCT_INTAKE_BEGIN, PRODUCT_INTAKE_END
    raise ValueError(f"unsupported intake lane: {lane}")


def build_intake_lane_prompt(*, base_prompt: str, lane: str) -> str:
    begin, end = lane_tags(lane)
    if lane == "technical_intake":
        lane_scope = (
            "- lane: technical_intake\n"
            "- focus: architecture fit, implementation feasibility, delivery risks, technical blockers\n"
            "- required lenses: architecture-review, business-analyst, tz-oss-scout\n"
        )
    else:
        lane_scope = (
            "- lane: product_intake\n"
            "- focus: product value, user impact, business viability, value-risk blockers\n"
            "- required lenses: product-owner, business-analyst, tz-oss-scout\n"
        )
    return (
        f"{base_prompt}\n\n"
        "Intake Runtime Mode:\n"
        "- You are running a lane-only intake pass in parallel with another lane.\n"
        "- Do not modify repository files in this lane pass.\n"
        "- Provide only lane analysis and blocker classification.\n"
        f"{lane_scope}"
        "Return exactly one tagged JSON block in this lane format:\n"
        f"{begin}\n"
        "{\n"
        '  "created_docs": ["docs/codex/contracts/<slug>.execution-contract.md"],\n'
        '  "review_summary": "Concise lane review summary.",\n'
        '  "blockers": [\n'
        "    {\n"
        '      "id": "LANE-001",\n'
        '      "severity": "P0",\n'
        '      "scale": "M",\n'
        '      "title": "Short blocker title",\n'
        '      "why": "Why this blocks/risks intake",\n'
        '      "required_action": "What must be resolved"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        f"{end}\n"
    )


def extract_lane_payload_from_text(text: str, *, lane: str) -> dict[str, Any]:
    begin, end = lane_tags(lane)
    raw = extract_tagged_json(text, begin=begin, end=end)
    if lane in raw and isinstance(raw.get(lane), dict):
        raw = raw[lane]
    if not isinstance(raw, dict):
        raise ValueError(f"{lane} payload must be an object")
    normalized = _normalize_lane({lane: raw}, lane)
    return {
        "created_docs": list(normalized["created_docs"]),
        "review_summary": str(normalized["review_summary"]),
        "blockers": [dict(item) for item in normalized["blockers"]],
    }


def build_materialization_prompt(
    *,
    base_prompt: str,
    technical_lane_payload: dict[str, Any],
    product_lane_payload: dict[str, Any],
    intake_gate: dict[str, Any],
    intake_gate_path: Path,
) -> str:
    return (
        f"{base_prompt}\n\n"
        "Intake Runtime Mode:\n"
        "- You are running materialization after parallel lane gate PASS.\n"
        "- Materialize/refresh canonical intake documentation now.\n"
        "- Preserve deterministic phase mapping from the source phase IR.\n"
        "- Do not invent extra phases or reorder source phases.\n"
        f"- Intake gate artifact (PASS): {intake_gate_path.as_posix()}\n\n"
        "Lane Inputs (technical_intake):\n"
        f"{json.dumps(technical_lane_payload, ensure_ascii=False, indent=2)}\n\n"
        "Lane Inputs (product_intake):\n"
        f"{json.dumps(product_lane_payload, ensure_ascii=False, indent=2)}\n\n"
        "Merged Intake Gate:\n"
        f"{json.dumps(intake_gate, ensure_ascii=False, indent=2)}\n"
    )


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


def read_plain_text(path: Path) -> str | None:
    if path.suffix.lower() not in {".md", ".txt", ".rst"}:
        return None
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
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
        if "spec" in title_normalized or "specification" in title_normalized or "тз" in title_normalized or "tz" in title_normalized:
            score += 25
            reasons.append("title looks like specification")
        if "verdict" in title_normalized:
            score -= 50
            reasons.append("title looks like verdict output")
        if "findings" in title_normalized:
            score -= 30
            reasons.append("title looks like findings output")

    text = read_plain_text(path)
    if text:
        normalized_text = text.lower()
        phase_headings = len(re.findall(r"^###\s+phase\s+[a-z0-9-]+", text, flags=re.IGNORECASE | re.MULTILINE))
        if phase_headings:
            score += 180 + phase_headings * 12
            reasons.append(f"content defines explicit phase rollout ({phase_headings} phases)")
        if "**acceptance gate**" in normalized_text and "**disprover**" in normalized_text:
            score += 90
            reasons.append("content carries acceptance/disprover contract")
        if "allow_release_readiness" in normalized_text:
            score += 45
            reasons.append("content names explicit release decision target")
        if "phase acceptance verdict" in normalized_text or "acceptance verdict" in normalized_text:
            score -= 40
            reasons.append("content reads like downstream verdict output")
        if "detailed phase findings" in normalized_text:
            score -= 25
            reasons.append("content reads like findings output")

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
    compiler_artifact: Path | None,
    compiler_phase_ids: list[str],
) -> None:
    payload = {
        "updated_at": utc_now().isoformat().replace("+00:00", "Z"),
        "package_zip": package_path.as_posix(),
        "extracted_root": extracted_root.as_posix(),
        "suggested_primary_document": candidates[0].rel_path if candidates else None,
        "candidates": [asdict(candidate) for candidate in candidates],
        "extension_counts": summarize_extensions(extracted_root),
        "suggested_phase_compiler_artifact": compiler_artifact.as_posix() if compiler_artifact else None,
        "suggested_phase_ids": compiler_phase_ids,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_prompt(
    *,
    prompt_path: Path,
    package_path: Path,
    extracted_root: Path,
    manifest_path: Path,
    suggested_primary: str | None,
    suggested_phase_compiler_artifact: str | None,
    suggested_phase_ids: list[str],
    mode: str,
) -> str:
    base = prompt_path.read_text(encoding="utf-8").rstrip()
    primary_line = suggested_primary if suggested_primary else "NONE"
    compiler_line = suggested_phase_compiler_artifact if suggested_phase_compiler_artifact else "NONE"
    phase_ids_line = ",".join(suggested_phase_ids) if suggested_phase_ids else "NONE"
    return (
        f"{base}\n\n"
        f"Package zip path: {package_path.as_posix()}\n"
        f"Extracted package root: {extracted_root.as_posix()}\n"
        f"Package manifest path: {manifest_path.as_posix()}\n"
        f"Suggested primary document: {primary_line}\n"
        f"Suggested phase compiler artifact: {compiler_line}\n"
        f"Suggested phase ids: {phase_ids_line}\n"
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
        "--output-last-message",
        str(output_path),
        "-",
    ]
    if profile.strip():
        cmd[4:4] = ["-p", profile]
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
        default="",
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

    suggested_primary = None
    suggested_phase_compiler_artifact: Path | None = None
    suggested_phase_ids: list[str] = []
    if candidates:
        suggested_primary_path = (extracted_root / candidates[0].rel_path).resolve()
        suggested_primary = suggested_primary_path.as_posix()
        compiled_plan = compile_phase_plan(suggested_primary_path)
        if compiled_plan.phases:
            suggested_phase_compiler_artifact = run_root / "suggested-phase-plan.json"
            suggested_phase_ids = [phase.phase_id for phase in compiled_plan.phases]
            suggested_phase_compiler_artifact.write_text(
                json.dumps(phase_plan_payload(compiled_plan), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

    print(f"package zip: {package_path.as_posix()}")
    print(f"extracted root: {extracted_root.as_posix()}")
    print(f"manifest: {manifest_path.as_posix()}")
    print(f"suggested primary document: {suggested_primary or 'NONE'}")
    print(
        "suggested phase compiler artifact: "
        f"{suggested_phase_compiler_artifact.as_posix() if suggested_phase_compiler_artifact else 'NONE'}"
    )
    print(f"suggested phase ids: {','.join(suggested_phase_ids) if suggested_phase_ids else 'NONE'}")

    write_manifest_json(
        path=manifest_json_path,
        package_path=package_path,
        extracted_root=extracted_root,
        candidates=candidates,
        compiler_artifact=suggested_phase_compiler_artifact,
        compiler_phase_ids=suggested_phase_ids,
    )

    if args.dry_run:
        print("dry run: Codex invocation skipped")
        return 0

    output_path = resolve_path(repo_root, args.output_last_message)
    base_prompt = build_prompt(
        prompt_path=prompt_path,
        package_path=package_path,
        extracted_root=extracted_root,
        manifest_path=manifest_path,
        suggested_primary=suggested_primary,
        suggested_phase_compiler_artifact=(
            suggested_phase_compiler_artifact.as_posix() if suggested_phase_compiler_artifact else None
        ),
        suggested_phase_ids=suggested_phase_ids,
        mode=args.mode,
    )
    lane_outputs = {
        "technical_intake": run_root / "technical-intake-last-message.txt",
        "product_intake": run_root / "product-intake-last-message.txt",
    }
    lane_prompts = {
        lane: build_intake_lane_prompt(base_prompt=base_prompt, lane=lane)
        for lane in lane_outputs
    }
    lane_exit_codes: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(
                run_codex,
                repo_root=repo_root,
                prompt=lane_prompts[lane],
                profile=args.profile,
                output_path=lane_outputs[lane],
            ): lane
            for lane in lane_outputs
        }
        for future in as_completed(futures):
            lane = futures[future]
            lane_exit_codes[lane] = int(future.result())

    for lane, code in lane_exit_codes.items():
        print(f"{lane} exit code: {code}")
    failed_lanes = [lane for lane, code in lane_exit_codes.items() if code != 0]
    if failed_lanes:
        print(f"error: intake lane execution failed: {', '.join(sorted(failed_lanes))}", file=sys.stderr)
        return lane_exit_codes[failed_lanes[0]]

    lane_payloads: dict[str, dict[str, Any]] = {}
    for lane, lane_output in lane_outputs.items():
        if not lane_output.exists():
            print(f"error: missing lane output artifact for {lane}: {lane_output.as_posix()}", file=sys.stderr)
            return INTAKE_BLOCKED_EXIT
        try:
            lane_text = lane_output.read_text(encoding="utf-8")
            lane_payloads[lane] = extract_lane_payload_from_text(lane_text, lane=lane)
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            print(f"error: invalid {lane} payload: {exc}", file=sys.stderr)
            return INTAKE_BLOCKED_EXIT
        lane_payload_path = run_root / f"{lane}.json"
        lane_payload_path.write_text(json.dumps(lane_payloads[lane], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"{lane} artifact: {lane_payload_path.as_posix()}")

    intake_gate = evaluate_intake_gate_payload(
        {
            "technical_intake": lane_payloads["technical_intake"],
            "product_intake": lane_payloads["product_intake"],
            "combined_gate": {"decision": "PASS"},
        }
    )
    intake_gate_json = run_root / "intake-gate.json"
    intake_gate_md = run_root / "intake-gate.md"
    intake_gate_json.write_text(json.dumps(intake_gate, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    intake_gate_md.write_text(render_intake_gate_markdown(intake_gate), encoding="utf-8")
    print(f"intake gate artifact: {intake_gate_json.as_posix()}")
    print(f"intake gate report: {intake_gate_md.as_posix()}")
    print(f"intake gate decision: {intake_gate['combined_gate']['decision']}")
    if intake_gate["combined_gate"]["decision"] != "PASS":
        print("intake gate blocked: resolve P0/P1 blockers and rerun package intake")
        return INTAKE_BLOCKED_EXIT

    materialization_prompt = build_materialization_prompt(
        base_prompt=base_prompt,
        technical_lane_payload=lane_payloads["technical_intake"],
        product_lane_payload=lane_payloads["product_intake"],
        intake_gate=intake_gate,
        intake_gate_path=intake_gate_json,
    )
    materialization_exit_code = run_codex(
        repo_root=repo_root,
        prompt=materialization_prompt,
        profile=args.profile,
        output_path=output_path,
    )
    if materialization_exit_code != 0:
        return materialization_exit_code
    if not output_path.exists():
        print(
            f"error: Codex did not produce output-last-message artifact: {output_path.as_posix()}",
            file=sys.stderr,
        )
        return INTAKE_BLOCKED_EXIT
    print(f"materialization output: {output_path.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
