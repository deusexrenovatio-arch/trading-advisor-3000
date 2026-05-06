#!/usr/bin/env python3
"""Collect candidate project-map issues from advisory memory sources."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable


DEFAULT_PALACE = Path("D:/mempalace/palace")
DEFAULT_OUTPUT = Path("docs/project-map/state/candidates/project-map-candidates.md")
DEFAULT_QUERIES = (
    "trading advisor blocked unresolved proof gap",
    "trading advisor missing proof acceptance blocker",
    "trading advisor open question unknown state",
    "trading advisor outdated task worktree not merged",
    "trading advisor project map problem candidate",
)
DEFAULT_WINGS = ("codex_chats",)


@dataclass(frozen=True)
class MemoryHit:
    query: str
    wing: str
    room: str
    source: str
    match: str
    excerpt: str


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    title: str
    item_type: str
    suggested_node: str
    suggested_priority: str
    confidence: str
    source: str
    query: str
    excerpt: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect project-map candidate issues from MemPalace.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--palace", type=Path, default=DEFAULT_PALACE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--results-per-query", type=int, default=6)
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="MemPalace query to run. Can be provided more than once. Defaults to project-map recovery queries.",
    )
    parser.add_argument(
        "--wing",
        action="append",
        default=[],
        help="MemPalace wing to search. Can be provided more than once. Defaults to codex_chats.",
    )
    parser.add_argument("--check", action="store_true", help="Fail if the generated report is out of date.")
    return parser.parse_args()


def _clean_line(value: str) -> str:
    replacements = {
        "п»ї": "",
        "вЂњ": '"',
        "вЂќ": '"',
        "вЂ™": "'",
        "вЂ“": "-",
        "вЂ”": "-",
        "РІР‚Сљ": '"',
        "РІР‚Сњ": '"',
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    value = re.sub(r"\[[^\]]+\]\([^)]+\)", lambda match: match.group(0).split("](")[0].lstrip("["), value)
    value = value.replace("`", "").replace("**", "")
    value = re.sub(r"\s+", " ", value).strip(" -\t")
    return value


def _shorten(value: str, max_len: int = 118) -> str:
    value = _clean_line(value)
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "..."


def _candidate_key(hit: MemoryHit) -> str:
    normalized = re.sub(r"\s+", " ", hit.excerpt.lower()).strip()
    digest = hashlib.sha1(normalized[:900].encode("utf-8")).hexdigest()[:12]
    return f"{hit.source}|{digest}"


def _has_signal_text(excerpt: str) -> bool:
    lower = excerpt.lower()
    signal_terms = (
        "blocked",
        "blocker",
        "cannot unlock",
        "missing proof",
        "evidence gap",
        "evidence_gaps",
        "remaining_risks",
        "deferred_work",
        "stale",
        "drift",
        "unknown",
        "unresolved",
        "not closed",
        "failed",
    )
    return any(term in lower for term in signal_terms)


def _is_noise_hit(hit: MemoryHit) -> bool:
    excerpt = hit.excerpt.strip()
    lower = excerpt.lower()
    if not excerpt:
        return True
    if lower.startswith('{"timestamp"') or '"type":"session_meta"' in lower[:500]:
        return True
    if lower.startswith("> # context from my ide setup"):
        return True
    if not _has_signal_text(excerpt):
        return True
    return False


def parse_mempalace_search(text: str, *, query: str) -> list[MemoryHit]:
    hits: list[MemoryHit] = []
    current: dict[str, str | list[str]] | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        header = re.match(r"\s*\[\d+\]\s+([^/]+?)\s*/\s*(.+?)\s*$", line)
        if header:
            if current:
                hits.append(_hit_from_current(current, query=query))
            current = {
                "wing": header.group(1).strip(),
                "room": header.group(2).strip(),
                "source": "",
                "match": "",
                "excerpt_lines": [],
            }
            continue
        if current is None:
            continue
        stripped = line.strip()
        if stripped.startswith("Source:"):
            current["source"] = stripped.removeprefix("Source:").strip()
            continue
        if stripped.startswith("Match:"):
            current["match"] = stripped.removeprefix("Match:").strip()
            continue
        if stripped.startswith("────") or stripped.startswith("="):
            continue
        if not stripped:
            continue
        excerpt_lines = current["excerpt_lines"]
        assert isinstance(excerpt_lines, list)
        excerpt_lines.append(stripped)

    if current:
        hits.append(_hit_from_current(current, query=query))
    return [hit for hit in hits if hit.excerpt]


def _hit_from_current(current: dict[str, str | list[str]], *, query: str) -> MemoryHit:
    excerpt_lines = current.get("excerpt_lines", [])
    if not isinstance(excerpt_lines, list):
        excerpt_lines = []
    excerpt = "\n".join(str(line) for line in excerpt_lines).strip()
    return MemoryHit(
        query=query,
        wing=str(current.get("wing") or ""),
        room=str(current.get("room") or ""),
        source=str(current.get("source") or ""),
        match=str(current.get("match") or ""),
        excerpt=excerpt,
    )


def _infer_node(text: str) -> str:
    lower = text.lower()
    rules = (
        ("execution-plane", ("broker", "finam", "stocksharp", "order", "fill", "execution")),
        ("runtime-plane", ("runtime", "signal", "api", "publication", "durable signal")),
        ("data-plane", ("moex", "canonical", "data", "baseline", "spark", "delta", "dagster")),
        ("research-plane", ("research", "backtest", "strategy", "indicator", "vectorbt", "optuna")),
        ("contract-surfaces", ("contract", "schema", "fixture", "compatibility", "adr")),
        ("project-map", ("project map", "obsidian", "cockpit", "graph")),
        ("delivery-gates", ("governed", "gate", "acceptance", "phase", "validation", "proof")),
        ("delivery-shell", ("task", "worktree", "codex", "shell", "memory", "mempalace")),
    )
    scores: list[tuple[int, str]] = []
    for node_id, needles in rules:
        score = sum(1 for needle in needles if needle in lower)
        if score:
            scores.append((score, node_id))
    if not scores:
        return "project-map"
    return sorted(scores, reverse=True)[0][1]


def _infer_type(text: str, room: str) -> str:
    lower = f"{room}\n{text}".lower()
    if any(word in lower for word in ("blocked", "blocker", "missing proof", "cannot unlock")):
        return "problem"
    if any(word in lower for word in ("unknown", "risk", "stale", "outdated")):
        return "risk"
    if any(word in lower for word in ("decision", "question", "unclear")):
        return "question"
    return "question"


def _infer_priority(text: str, item_type: str) -> str:
    lower = text.lower()
    if "p0" in lower or "cannot unlock" in lower or "blocks current" in lower:
        return "p0"
    if item_type == "problem" or "blocked" in lower or "missing proof" in lower:
        return "p1"
    if "unknown" in lower or "stale" in lower or "outdated" in lower:
        return "p2"
    return "p3"


def _infer_confidence(hit: MemoryHit) -> str:
    try:
        score = float(hit.match)
    except ValueError:
        return "low"
    if score >= 0.5:
        return "medium"
    return "low"


def _json_title(line: str) -> str:
    try:
        loaded = json.loads(line)
    except json.JSONDecodeError:
        return ""
    if not isinstance(loaded, dict):
        return ""
    blockers = loaded.get("blockers")
    if isinstance(blockers, list) and blockers:
        first = blockers[0]
        if isinstance(first, dict) and isinstance(first.get("title"), str):
            return _shorten(first["title"])
    for key in ("summary", "title", "status"):
        value = loaded.get(key)
        if isinstance(value, str) and value.strip():
            return _shorten(value)
    return ""


def _title_from_excerpt(excerpt: str) -> str:
    for raw_line in excerpt.splitlines():
        line = _clean_line(raw_line)
        if not line or line.lower() in {"findings", "summary"}:
            continue
        if line.startswith("BEGIN_") or line.startswith("END_"):
            continue
        if line.startswith("{") and len(line) > 20:
            title = _json_title(line)
            if title:
                return title
        return _shorten(line)
    return "Memory candidate"


def candidates_from_hits(hits: Iterable[MemoryHit]) -> list[Candidate]:
    by_key: dict[str, MemoryHit] = {}
    for hit in hits:
        if _is_noise_hit(hit):
            continue
        key = _candidate_key(hit)
        by_key.setdefault(key, hit)

    candidates: list[Candidate] = []
    for hit in by_key.values():
        item_type = _infer_type(hit.excerpt, hit.room)
        digest = hashlib.sha1(_candidate_key(hit).encode("utf-8")).hexdigest()[:10]
        candidates.append(
            Candidate(
                candidate_id=f"candidate-{digest}",
                title=_title_from_excerpt(hit.excerpt),
                item_type=item_type,
                suggested_node=_infer_node(hit.excerpt),
                suggested_priority=_infer_priority(hit.excerpt, item_type),
                confidence=_infer_confidence(hit),
                source=hit.source,
                query=hit.query,
                excerpt=hit.excerpt,
            )
        )

    priority_order = {"p0": 0, "p1": 1, "p2": 2, "p3": 3}
    return sorted(candidates, key=lambda c: (priority_order.get(c.suggested_priority, 9), c.suggested_node, c.title))


def collect_from_mempalace(
    *,
    palace: Path,
    queries: tuple[str, ...],
    wings: tuple[str, ...],
    results_per_query: int,
    repo_root: Path,
) -> list[Candidate]:
    all_hits: list[MemoryHit] = []
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    for wing in wings:
        for query in queries:
            command = [
                "mempalace",
                "--palace",
                str(palace),
                "search",
                query,
                "--wing",
                wing,
                "--results",
                str(results_per_query),
            ]
            completed = subprocess.run(
                command,
                cwd=repo_root,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                check=False,
            )
            if completed.returncode != 0:
                message = completed.stderr.strip() or completed.stdout.strip()
                raise RuntimeError(f"MemPalace search failed for wing `{wing}` query `{query}`: {message}")
            all_hits.extend(parse_mempalace_search(completed.stdout, query=query))
    return candidates_from_hits(all_hits)


def render_report(candidates: list[Candidate]) -> str:
    generated_on = date.today().isoformat()
    lines = [
        "---",
        "title: Project Map Candidate Problems",
        "type: project-candidate-report",
        "status: candidate",
        f"generated_on: {generated_on}",
        f"candidate_count: {len(candidates)}",
        "tags:",
        "  - ta3000/project-map",
        "  - ta3000/project-candidates",
        "---",
        "",
        "# Project Map Candidate Problems",
        "",
        "This is an inbox, not current project truth. Promote a candidate to a project item only after live verification.",
        "",
        "## Operating Rule",
        "",
        "- MemPalace is advisory recall, not authoritative state.",
        "- Keep candidates here until the current repo, artifact, or user decision confirms them.",
        "- Do not let this report affect cockpit roll-up status directly.",
        "",
        "## Summary",
        "",
    ]
    if not candidates:
        lines.extend(["No candidates found.", ""])
        return "\n".join(lines).rstrip() + "\n"

    by_priority: dict[str, int] = {}
    by_node: dict[str, int] = {}
    for candidate in candidates:
        by_priority[candidate.suggested_priority] = by_priority.get(candidate.suggested_priority, 0) + 1
        by_node[candidate.suggested_node] = by_node.get(candidate.suggested_node, 0) + 1
    lines.append(
        "- By priority: "
        + ", ".join(f"{priority}={count}" for priority, count in sorted(by_priority.items()))
    )
    lines.append("- By node: " + ", ".join(f"{node}={count}" for node, count in sorted(by_node.items())))
    lines.append("")
    lines.append("## Candidate Queue")
    lines.append("")

    for index, candidate in enumerate(candidates, start=1):
        lines.extend(
            [
                f"### C{index:03d}: {candidate.title}",
                "",
                f"- Candidate id: `{candidate.candidate_id}`",
                f"- Suggested node: `{candidate.suggested_node}`",
                f"- Suggested type: `{candidate.item_type}`",
                f"- Suggested priority: `{candidate.suggested_priority}`",
                f"- Confidence: `{candidate.confidence}`",
                f"- Source: `{candidate.source}`",
                f"- Query: `{candidate.query}`",
                "",
                "Excerpt:",
                "",
            ]
        )
        excerpt_lines = candidate.excerpt.splitlines()[:12]
        for excerpt_line in excerpt_lines:
            lines.append(f"> {_clean_line(excerpt_line)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _resolve_output(repo_root: Path, output: Path) -> Path:
    return output if output.is_absolute() else repo_root / output


def _diff(path: Path, desired: str) -> str:
    current = path.read_text(encoding="utf-8") if path.exists() else ""
    return "\n".join(
        difflib.unified_diff(
            current.splitlines(),
            desired.splitlines(),
            fromfile=path.as_posix(),
            tofile="desired",
            lineterm="",
        )
    )


def run(
    *,
    repo_root: Path,
    palace: Path,
    output: Path,
    queries: tuple[str, ...],
    wings: tuple[str, ...],
    results_per_query: int,
    check: bool = False,
) -> int:
    repo_root = repo_root.resolve()
    output_path = _resolve_output(repo_root, output)
    candidates = collect_from_mempalace(
        palace=palace,
        queries=queries,
        wings=wings,
        results_per_query=results_per_query,
        repo_root=repo_root,
    )
    desired = render_report(candidates)
    current = output_path.read_text(encoding="utf-8") if output_path.exists() else None
    if current == desired:
        print(f"[collect_project_map_candidates] Candidate report is current: {output_path}")
        return 0
    if check:
        print(f"[collect_project_map_candidates] Candidate report is out of date: {output_path}")
        print(_diff(output_path, desired))
        return 1
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(desired, encoding="utf-8")
    print(f"[collect_project_map_candidates] Wrote {len(candidates)} candidate(s) to {output_path}.")
    return 0


def main() -> int:
    args = parse_args()
    queries = tuple(args.query) if args.query else DEFAULT_QUERIES
    wings = tuple(args.wing) if args.wing else DEFAULT_WINGS
    try:
        return run(
            repo_root=args.repo_root,
            palace=args.palace,
            output=args.output,
            queries=queries,
            wings=wings,
            results_per_query=args.results_per_query,
            check=args.check,
        )
    except RuntimeError as exc:
        print(f"[collect_project_map_candidates] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
