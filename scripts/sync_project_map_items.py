#!/usr/bin/env python3
"""Synchronize project-map items from real repo signals."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from unicodedata import normalize

import yaml


NODE_ROOT = Path("docs/project-map/state/nodes")
ITEM_ROOT = Path("docs/project-map/state/items")
TASK_ROOT = Path("docs/tasks/active")
MANAGED_ORIGIN_KINDS = {"sync-node-state", "sync-task-blocker"}
NO_BLOCKER_PATTERNS = (
    "no blocker",
    "no active blocker",
    "active blockers in scope: none",
    "remediation blockers source: none",
    "remediation blockers: none",
)


@dataclass(frozen=True)
class NodeSignal:
    title: str
    node_id: str
    path: Path
    state: str
    needs_user_attention: bool
    state_source: str


@dataclass(frozen=True)
class ItemSignal:
    item_id: str
    title: str
    item_type: str
    linked_node: str
    status: str
    priority: str
    severity: str
    needs_user_attention: bool
    owner: str
    origin_kind: str
    origin_refs: tuple[str, ...]
    body: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync project-map items from current project-map signals.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--check", action="store_true", help="Fail if managed items are out of date.")
    parser.add_argument(
        "--include-legacy-task-notes",
        action="store_true",
        help=(
            "Also import blocker signals from docs/tasks/active. This is off by default because "
            "task notes on main are legacy worktree artifacts, not authoritative current state."
        ),
    )
    return parser.parse_args()


def _read_note(path: Path) -> tuple[dict[str, Any], str]:
    text = path.read_text(encoding="utf-8-sig")
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 3)
    if end == -1:
        return {}, text
    try:
        loaded = yaml.safe_load(text[3:end]) or {}
    except yaml.YAMLError:
        return {}, text[end + 4 :].strip()
    if not isinstance(loaded, dict):
        loaded = {}
    return loaded, text[end + 4 :].strip()


def _slug(value: str, *, max_len: int = 72) -> str:
    ascii_value = normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value).strip("-").lower()
    if not slug:
        slug = hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]
    return slug[:max_len].strip("-") or "item"


def _filename(title: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', " ", title).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) > 96:
        cleaned = cleaned[:96].rstrip()
    return cleaned or "Project Map Item"


def _rel(path: Path, repo_root: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def _node_by_id(repo_root: Path) -> dict[str, NodeSignal]:
    nodes: dict[str, NodeSignal] = {}
    for path in sorted((repo_root / NODE_ROOT).glob("*.md")):
        fm, _body = _read_note(path)
        node_id = fm.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            continue
        nodes[node_id] = NodeSignal(
            title=str(fm.get("title") or path.stem),
            node_id=node_id,
            path=path,
            state=str(fm.get("state") or "unknown"),
            needs_user_attention=fm.get("needs_user_attention") is True,
            state_source=str(fm.get("state_source") or ""),
        )
    return nodes


def _infer_node_id(text: str, known_nodes: set[str]) -> str:
    lower = text.lower()
    scored: list[tuple[int, str]] = []
    rules = {
        "delivery-gates": ("gate", "loop", "pr gate", "validation", "skill", "governed", "acceptance"),
        "contract-surfaces": ("contract", "schema", "fixture", "compatibility"),
        "data-plane": ("moex", "canonical", "raw", "dagster", "spark", "baseline", "data"),
        "research-plane": ("research", "indicator", "backtest", "strategy", "vectorbt"),
        "runtime-plane": ("runtime", "signal", "publication"),
        "execution-plane": ("broker", "sidecar", "execution", "order", "fill"),
        "project-map": ("project map", "obsidian", "cockpit"),
    }
    for node_id, needles in rules.items():
        if node_id not in known_nodes:
            continue
        score = sum(1 for needle in needles if needle in lower)
        if score:
            scored.append((score, node_id))
    if scored:
        return sorted(scored, reverse=True)[0][1]
    return "delivery-shell" if "delivery-shell" in known_nodes else sorted(known_nodes)[0]


def _node_state_signals(repo_root: Path, nodes: dict[str, NodeSignal]) -> list[ItemSignal]:
    signals: list[ItemSignal] = []
    for node in nodes.values():
        if node.state == "blocked":
            item_type, priority, severity, title = "problem", "p0", "high", f"Blocked node: {node.title}"
        elif node.state == "unknown":
            item_type, priority, severity, title = "risk", "p2", "medium", f"Verify unknown node: {node.title}"
        elif node.needs_user_attention:
            item_type, priority, severity, title = "question", "p1", "medium", f"Decision needed: {node.title}"
        else:
            continue
        body = (
            f"This item is generated from `{_rel(node.path, repo_root)}`.\n\n"
            f"- Node state: `{node.state}`\n"
            f"- State source: {node.state_source or 'not declared'}\n\n"
            "Update the node evidence or close this item when the state is no longer current."
        )
        signals.append(
            ItemSignal(
                item_id=f"sync-node-state-{node.node_id}",
                title=title,
                item_type=item_type,
                linked_node=node.node_id,
                status="open",
                priority=priority,
                severity=severity,
                needs_user_attention=node.needs_user_attention,
                owner="agent",
                origin_kind="sync-node-state",
                origin_refs=(_rel(node.path, repo_root),),
                body=body,
            )
        )
    return signals


def _section(text: str, heading: str) -> list[str]:
    lines = text.splitlines()
    start: int | None = None
    for idx, line in enumerate(lines):
        if line.strip().lower() == heading.lower():
            start = idx + 1
            break
    if start is None:
        return []
    collected: list[str] = []
    for line in lines[start:]:
        if line.startswith("## "):
            break
        collected.append(line)
    return [line.rstrip() for line in collected if line.strip()]


def _meaningful_blocker_lines(lines: list[str]) -> list[str]:
    meaningful: list[str] = []
    for line in lines:
        cleaned = line.strip()
        cleaned_lower = cleaned.lower().strip("- ").strip()
        if not cleaned.startswith("-"):
            continue
        if any(pattern in cleaned_lower for pattern in NO_BLOCKER_PATTERNS):
            continue
        meaningful.append(cleaned)
    return meaningful


def _task_title(path: Path, text: str) -> str:
    generic_title = ""
    for line in text.splitlines():
        if line.startswith("# "):
            generic_title = line[2:].strip()
            break
    if generic_title and generic_title.lower() != "task note":
        return generic_title
    for prefix in ("- Deliver:", "- Objective:"):
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith(prefix):
                return stripped[len(prefix) :].strip()
    return path.stem


def _task_blocker_signals(repo_root: Path, nodes: dict[str, NodeSignal]) -> list[ItemSignal]:
    signals: list[ItemSignal] = []
    known_nodes = set(nodes)
    for path in sorted((repo_root / TASK_ROOT).glob("*.md")):
        text = path.read_text(encoding="utf-8-sig")
        blocker_lines = _meaningful_blocker_lines(_section(text, "## Blockers"))
        outcome_blocked = re.search(r"^- Outcome Status:\s*blocked\s*$", text, flags=re.MULTILINE | re.IGNORECASE)
        if not blocker_lines and not outcome_blocked:
            continue
        title = _task_title(path, text)
        linked_node = _infer_node_id(f"{title}\n{text}", known_nodes)
        priority = "p0" if outcome_blocked else "p1"
        body_lines = blocker_lines or ["- Task outcome is marked blocked."]
        body = (
            f"This legacy diagnostic item is generated from `{_rel(path, repo_root)}`.\n\n"
            "Detected blocker signal:\n\n"
            + "\n".join(body_lines)
            + "\n\nTask notes on main are not authoritative current project state; use this "
            "signal only when explicitly reviewing legacy worktree artifacts."
        )
        signals.append(
            ItemSignal(
                item_id=f"sync-task-blocker-{_slug(path.stem)}",
                title=f"Task blocker: {title}",
                item_type="problem",
                linked_node=linked_node,
                status="open",
                priority=priority,
                severity="high" if priority == "p0" else "medium",
                needs_user_attention=priority == "p0",
                owner="agent",
                origin_kind="sync-task-blocker",
                origin_refs=(_rel(path, repo_root),),
                body=body,
            )
        )
    return signals


def collect_signals(repo_root: Path, *, include_legacy_task_notes: bool = False) -> list[ItemSignal]:
    nodes = _node_by_id(repo_root)
    signals = list(_node_state_signals(repo_root, nodes))
    if include_legacy_task_notes:
        signals.extend(_task_blocker_signals(repo_root, nodes))
    return signals


def _existing_created(path: Path) -> str:
    if not path.exists():
        return date.today().isoformat()
    fm, _body = _read_note(path)
    created = fm.get("created")
    return str(created or date.today().isoformat())


def _tags(signal: ItemSignal) -> list[str]:
    return [
        "ta3000/project-item",
        "ta3000/project-graph",
        f"item/{signal.item_type}",
        f"status/{signal.status}",
        f"priority/{signal.priority}",
        f"severity/{signal.severity}",
        f"origin/{signal.origin_kind}",
    ]


def _render_signal(signal: ItemSignal, *, path: Path) -> str:
    created = _existing_created(path)
    frontmatter = {
        "title": signal.title,
        "type": "project-item",
        "item_id": signal.item_id,
        "item_type": signal.item_type,
        "linked_node": signal.linked_node,
        "status": signal.status,
        "aliases": [signal.title],
        "severity": signal.severity,
        "priority": signal.priority,
        "needs_user_attention": signal.needs_user_attention,
        "owner": signal.owner,
        "created": created,
        "last_seen": date.today().isoformat(),
        "sync_managed": True,
        "origin_kind": signal.origin_kind,
        "origin_refs": list(signal.origin_refs),
        "tags": _tags(signal),
    }
    dumped = yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=True).strip()
    return f"""---
{dumped}
---

# {signal.title}

{signal.body}
"""


def _path_for_signal(repo_root: Path, signal: ItemSignal) -> Path:
    return repo_root / ITEM_ROOT / f"{_filename(signal.title)}.md"


def _managed_item_paths(repo_root: Path) -> set[Path]:
    paths: set[Path] = set()
    for path in sorted((repo_root / ITEM_ROOT).glob("*.md")):
        fm, _body = _read_note(path)
        if not fm and path.name.startswith(("Task blocker ", "Verify unknown node ", "Decision needed ")):
            paths.add(path)
            continue
        if fm.get("sync_managed") is True or str(fm.get("origin_kind") or "") in MANAGED_ORIGIN_KINDS:
            paths.add(path)
    return paths


def _mark_stale(path: Path) -> str:
    fm, body = _read_note(path)
    title = str(fm.get("title") or path.stem)
    fm["status"] = "dropped"
    fm["last_seen"] = date.today().isoformat()
    tags = [tag for tag in fm.get("tags", []) if isinstance(tag, str) and not tag.startswith("status/")]
    tags.append("status/dropped")
    fm["tags"] = tags
    dumped = yaml.safe_dump(fm, sort_keys=False, allow_unicode=True).strip()
    note_body = body or f"# {title}\n\nGenerated signal is no longer present."
    return f"---\n{dumped}\n---\n\n{note_body}\n"


def _desired_files(repo_root: Path, *, include_legacy_task_notes: bool = False) -> dict[Path, str]:
    desired: dict[Path, str] = {}
    signals = collect_signals(repo_root, include_legacy_task_notes=include_legacy_task_notes)
    seen_paths: set[Path] = set()
    for signal in signals:
        path = _path_for_signal(repo_root, signal)
        seen_paths.add(path)
        desired[path] = _render_signal(signal, path=path)
    for stale_path in _managed_item_paths(repo_root) - seen_paths:
        desired[stale_path] = _mark_stale(stale_path)
    return desired


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


def run(repo_root: Path, *, check: bool = False, include_legacy_task_notes: bool = False) -> int:
    repo_root = repo_root.resolve()
    item_root = repo_root / ITEM_ROOT
    item_root.mkdir(parents=True, exist_ok=True)
    desired = _desired_files(repo_root, include_legacy_task_notes=include_legacy_task_notes)
    changed: list[Path] = []
    for path, content in desired.items():
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == content:
            continue
        changed.append(path)
        if check:
            print(f"[sync_project_map_items] Out of date: {_rel(path, repo_root)}")
            print(_diff(path, content))
        else:
            path.write_text(content, encoding="utf-8")
    if changed:
        if check:
            return 1
        print(f"[sync_project_map_items] Updated {len(changed)} project-map item(s).")
    else:
        print("[sync_project_map_items] Project-map items are current.")
    return 0


def main() -> int:
    args = parse_args()
    return run(args.repo_root, check=args.check, include_legacy_task_notes=args.include_legacy_task_notes)


if __name__ == "__main__":
    raise SystemExit(main())
