#!/usr/bin/env python3
"""Build the static HTML project-map cockpit."""

from __future__ import annotations

import argparse
import difflib
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml


MAP_ROOT = Path("docs/project-map/state")
NODE_ROOT = MAP_ROOT / "nodes"
ITEM_ROOT = MAP_ROOT / "items"
OUTPUT = Path("docs/project-map/project-cockpit.html")

LEVEL_1_ORDER = (
    "delivery-shell",
    "product-plane",
    "data-plane",
    "research-plane",
    "runtime-plane",
    "execution-plane",
    "contract-surfaces",
    "project-map",
)
STATE_LABELS = {
    "ok": "OK",
    "attention": "Needs attention",
    "unknown": "Unknown",
    "blocked": "Blocked",
}
ACTIVE_ITEM_STATUSES = {"inbox", "open", "watch"}
PRIORITY_LABELS = {
    "p0": "P0 now",
    "p1": "P1 decision",
    "p2": "P2 review",
    "p3": "P3 watch",
}
PRIORITY_ORDER = {value: index for index, value in enumerate(PRIORITY_LABELS)}
ROLLUP_LABELS = {
    "stable": "Stable",
    "watch": "Watch",
    "review": "Review",
    "unknown": "Unknown",
    "decision": "Decision",
    "blocked": "Blocked",
}


@dataclass(frozen=True)
class ProjectNode:
    path: Path
    title: str
    node_id: str
    surface: str
    level: int
    parent_node: str
    state: str
    confidence: str
    needs_user_attention: bool
    last_verified: str
    update_rule: str
    state_source: str
    source_refs: tuple[str, ...]
    dfd_refs: tuple[str, ...]
    proof_refs: tuple[str, ...]
    excerpt: str


@dataclass(frozen=True)
class ProjectItem:
    path: Path
    title: str
    item_id: str
    item_type: str
    linked_node: str
    status: str
    priority: str
    severity: str
    needs_user_attention: bool
    owner: str
    created: str
    origin_kind: str
    origin_refs: tuple[str, ...]
    excerpt: str


@dataclass(frozen=True)
class BlockRollup:
    status: str
    child_count: int
    active_item_count: int
    priority_counts: Counter[str]
    state_counts: Counter[str]
    needs_user_attention: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build docs/project-map/project-cockpit.html.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT,
        help="Output path, relative to repo root unless absolute.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if the existing HTML is missing or out of date.",
    )
    return parser.parse_args()


def _read_note(path: Path) -> tuple[dict[str, Any], str]:
    text = path.read_text(encoding="utf-8-sig")
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 3)
    if end == -1:
        return {}, text
    loaded = yaml.safe_load(text[3:end]) or {}
    if not isinstance(loaded, dict):
        loaded = {}
    return loaded, text[end + 4 :].strip()


def _as_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item.strip())


def _as_bool(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def _priority_from_legacy(severity: str, needs_user_attention: bool) -> str:
    if needs_user_attention:
        return "p1"
    if severity == "high":
        return "p1"
    if severity == "low":
        return "p3"
    return "p2"


def _plain_excerpt(markdown: str, *, max_len: int = 210) -> str:
    lines: list[str] = []
    in_code = False
    for raw_line in markdown.splitlines():
        line = raw_line.strip()
        if line.startswith("```"):
            in_code = not in_code
            continue
        if in_code or not line:
            continue
        if line.startswith("#") or line.startswith("|") or line.startswith("- "):
            continue
        line = re.sub(r"\[\[([^\]|]+)(?:\|([^\]]+))?\]\]", lambda m: m.group(2) or m.group(1), line)
        line = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", line)
        line = line.replace("`", "")
        lines.append(line)
        if len(" ".join(lines)) >= max_len:
            break
    text = " ".join(lines).strip()
    if len(text) > max_len:
        return text[: max_len - 1].rstrip() + "..."
    return text


def _load_nodes(repo_root: Path) -> list[ProjectNode]:
    nodes: list[ProjectNode] = []
    for path in sorted((repo_root / NODE_ROOT).glob("*.md")):
        fm, body = _read_note(path)
        nodes.append(
            ProjectNode(
                path=path,
                title=str(fm.get("title") or path.stem),
                node_id=str(fm.get("node_id") or path.stem),
                surface=str(fm.get("surface") or "unknown"),
                level=int(fm.get("level") or 1),
                parent_node="" if fm.get("parent_node") is None else str(fm.get("parent_node") or ""),
                state=str(fm.get("state") or "unknown"),
                confidence=str(fm.get("confidence") or "unknown"),
                needs_user_attention=_as_bool(fm.get("needs_user_attention")),
                last_verified=str(fm.get("last_verified") or ""),
                update_rule=str(fm.get("update_rule") or ""),
                state_source=str(fm.get("state_source") or ""),
                source_refs=_as_tuple(fm.get("source_refs")),
                dfd_refs=_as_tuple(fm.get("dfd_refs")),
                proof_refs=_as_tuple(fm.get("proof_refs")),
                excerpt=_plain_excerpt(body),
            )
        )
    return nodes


def _load_items(repo_root: Path) -> list[ProjectItem]:
    items: list[ProjectItem] = []
    item_root = repo_root / ITEM_ROOT
    if not item_root.exists():
        return items
    for path in sorted(item_root.glob("*.md")):
        fm, body = _read_note(path)
        severity = str(fm.get("severity") or "medium")
        needs_user_attention = _as_bool(fm.get("needs_user_attention"))
        items.append(
            ProjectItem(
                path=path,
                title=str(fm.get("title") or path.stem),
                item_id=str(fm.get("item_id") or path.stem),
                item_type=str(fm.get("item_type") or "item"),
                linked_node=str(fm.get("linked_node") or ""),
                status=str(fm.get("status") or "inbox"),
                priority=str(fm.get("priority") or _priority_from_legacy(severity, needs_user_attention)),
                severity=severity,
                needs_user_attention=needs_user_attention,
                owner=str(fm.get("owner") or ""),
                created=str(fm.get("created") or ""),
                origin_kind=str(fm.get("origin_kind") or "manual"),
                origin_refs=_as_tuple(fm.get("origin_refs")),
                excerpt=_plain_excerpt(body),
            )
        )
    return items


def _sort_nodes(nodes: list[ProjectNode]) -> list[ProjectNode]:
    order = {node_id: idx for idx, node_id in enumerate(LEVEL_1_ORDER)}
    return sorted(nodes, key=lambda node: (node.level, order.get(node.node_id, 100), node.title.lower()))


def _sort_children(nodes: list[ProjectNode]) -> list[ProjectNode]:
    state_order = {"blocked": 0, "attention": 1, "unknown": 2, "ok": 3}
    return sorted(nodes, key=lambda node: (state_order.get(node.state, 9), node.title.lower()))


def _sort_items(items: list[ProjectItem]) -> list[ProjectItem]:
    status_order = {"open": 0, "inbox": 1, "watch": 2, "done": 3, "dropped": 4}
    return sorted(
        items,
        key=lambda item: (
            PRIORITY_ORDER.get(item.priority, 99),
            status_order.get(item.status, 99),
            0 if item.needs_user_attention else 1,
            item.title.lower(),
        ),
    )


def _children_by_parent(nodes: list[ProjectNode]) -> dict[str, list[ProjectNode]]:
    children: dict[str, list[ProjectNode]] = defaultdict(list)
    for node in nodes:
        if node.parent_node:
            children[node.parent_node].append(node)
    return children


def _collect_descendants(node: ProjectNode, children: dict[str, list[ProjectNode]]) -> list[ProjectNode]:
    descendants: list[ProjectNode] = []
    pending = list(children.get(node.node_id, []))
    while pending:
        child = pending.pop(0)
        descendants.append(child)
        pending.extend(children.get(child.node_id, []))
    return descendants


def _active_items(items: list[ProjectItem]) -> list[ProjectItem]:
    return [item for item in items if item.status in ACTIVE_ITEM_STATUSES]


def _rollup_for(
    node: ProjectNode,
    *,
    children: dict[str, list[ProjectNode]],
    items_by_node: dict[str, list[ProjectItem]],
) -> BlockRollup:
    related_nodes = [node, *_collect_descendants(node, children)]
    related_ids = {related.node_id for related in related_nodes}
    related_items = [
        item for node_id in related_ids for item in items_by_node.get(node_id, []) if item.status in ACTIVE_ITEM_STATUSES
    ]
    state_counts = Counter(related.state for related in related_nodes)
    priority_counts = Counter(item.priority for item in related_items)
    needs_user_attention = any(related.needs_user_attention for related in related_nodes) or any(
        item.needs_user_attention for item in related_items
    )

    if state_counts.get("blocked", 0) or priority_counts.get("p0", 0):
        status = "blocked"
    elif needs_user_attention or priority_counts.get("p1", 0):
        status = "decision"
    elif state_counts.get("unknown", 0):
        status = "unknown"
    elif state_counts.get("attention", 0) or priority_counts.get("p2", 0):
        status = "review"
    elif priority_counts.get("p3", 0):
        status = "watch"
    else:
        status = "stable"

    return BlockRollup(
        status=status,
        child_count=max(0, len(related_nodes) - 1),
        active_item_count=len(related_items),
        priority_counts=priority_counts,
        state_counts=state_counts,
        needs_user_attention=needs_user_attention,
    )


def _output_base(output: Path) -> Path:
    return output.parent


def _href(repo_root: Path, output: Path, target: Path | str) -> str:
    target_path = target if isinstance(target, Path) else repo_root / target
    rel = os.path.relpath(target_path, repo_root / _output_base(output))
    return quote(rel.replace(os.sep, "/"), safe="/#")


def _note_link(repo_root: Path, output: Path, path: Path, label: str) -> str:
    return f'<a href="{_href(repo_root, output, path)}">{escape(label)}</a>'


def _ref_links(repo_root: Path, output: Path, refs: tuple[str, ...]) -> str:
    if not refs:
        return '<span class="muted">No refs</span>'
    links = []
    for ref in refs:
        name = Path(ref).name
        links.append(f'<a href="{_href(repo_root, output, ref)}">{escape(name)}</a>')
    return "".join(f"<li>{link}</li>" for link in links)


def _data_search(*parts: str) -> str:
    return escape(" ".join(part for part in parts if part).lower(), quote=True)


def _class_token(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", value.lower()).strip("-") or "unknown"


def _badge(label: str, value: str) -> str:
    return f'<span class="badge {escape(label)}-{escape(_class_token(value))}">{escape(value)}</span>'


def _priority_summary(priority_counts: Counter[str]) -> str:
    parts = [f"{priority.upper()} {priority_counts[priority]}" for priority in PRIORITY_LABELS if priority_counts[priority]]
    return " / ".join(parts) if parts else "No active items"


def _state_summary(state_counts: Counter[str]) -> str:
    parts = [f"{state} {state_counts[state]}" for state in ("blocked", "unknown", "attention", "ok") if state_counts[state]]
    return " / ".join(parts) if parts else "No child states"


def _node_card(
    repo_root: Path,
    output: Path,
    node: ProjectNode,
    *,
    major: bool = False,
    rollup: BlockRollup | None = None,
) -> str:
    classes = "node-card filterable"
    if major:
        classes += " node-major"
    if node.needs_user_attention:
        classes += " user-attention"
    rollup_state = rollup.status if rollup else node.state
    rollup_label = ROLLUP_LABELS.get(rollup_state, STATE_LABELS.get(rollup_state, rollup_state))
    search = _data_search(
        node.title,
        node.node_id,
        node.surface,
        node.state,
        rollup_state,
        node.excerpt,
        " ".join(node.source_refs),
        " ".join(node.dfd_refs),
        " ".join(node.proof_refs),
    )
    refs = f"{len(node.source_refs)} source / {len(node.dfd_refs)} DFD / {len(node.proof_refs)} proof"
    rollup_meta = ""
    if rollup:
        rollup_meta = f"""
  <div class="rollup-line">
    <span>{escape(_state_summary(rollup.state_counts))}</span>
    <span>{escape(_priority_summary(rollup.priority_counts))}</span>
  </div>
"""
    return f"""
<article class="{classes}" data-kind="node" data-state="{escape(node.state)}" data-rollup="{escape(rollup_state)}" data-surface="{escape(node.surface)}" data-level="{node.level}" data-search="{search}">
  <div class="card-topline">
    <span class="node-level">L{node.level}</span>
    <span class="surface surface-{escape(node.surface)}">{escape(node.surface)}</span>
  </div>
  <h3>{_note_link(repo_root, output, node.path, node.title)}</h3>
  <p>{escape(node.excerpt or node.update_rule or "No note summary yet.")}</p>
  {rollup_meta}
  <div class="meta-row">
    {_badge("rollup", rollup_label)}
    <span>{escape(refs)}</span>
  </div>
</article>
"""


def _item_card(repo_root: Path, output: Path, item: ProjectItem, node_by_id: dict[str, ProjectNode]) -> str:
    linked = node_by_id.get(item.linked_node)
    linked_title = linked.title if linked else item.linked_node
    search = _data_search(
        item.title,
        item.item_id,
        item.item_type,
        item.status,
        item.priority,
        item.severity,
        item.origin_kind,
        " ".join(item.origin_refs),
        linked_title,
        item.excerpt,
    )
    attention_class = " user-attention" if item.needs_user_attention else ""
    return f"""
<article class="item-card filterable{attention_class}" data-kind="item" data-state="{escape(item.status)}" data-priority="{escape(item.priority)}" data-surface="item" data-level="item" data-search="{search}">
  <div class="card-topline">
    <span class="item-type">{escape(item.item_type)}</span>
    <span>{escape(item.origin_kind)}</span>
    <span class="priority priority-{escape(item.priority)}">{escape(PRIORITY_LABELS.get(item.priority, item.priority))}</span>
    <span class="severity severity-{escape(item.severity)}">{escape(item.severity)}</span>
  </div>
  <h3>{_note_link(repo_root, output, item.path, item.title)}</h3>
  <p>{escape(item.excerpt or "No note summary yet.")}</p>
  <div class="meta-row">
    <span>{escape(item.status)}</span>
    <span>node: {escape(linked_title)}</span>
  </div>
</article>
"""


def _render_summary(nodes: list[ProjectNode], items: list[ProjectItem]) -> str:
    state_counts = Counter(node.state for node in nodes)
    attention_nodes = [node for node in nodes if node.state != "ok" or node.needs_user_attention]
    active_items = _active_items(items)
    priority_counts = Counter(item.priority for item in active_items)
    user_attention = sum(1 for node in nodes if node.needs_user_attention) + sum(
        1 for item in items if item.needs_user_attention
    )
    summary = [
        ("Nodes", str(len(nodes))),
        ("State exceptions", str(len(attention_nodes))),
        ("Active items", str(len(active_items))),
        ("P0/P1 items", str(priority_counts.get("p0", 0) + priority_counts.get("p1", 0))),
        ("User decisions", str(user_attention)),
        ("Unknown", str(state_counts.get("unknown", 0))),
    ]
    return "\n".join(
        f'<div class="metric"><span>{escape(label)}</span><strong>{escape(value)}</strong></div>' for label, value in summary
    )


def _render_level_one(
    repo_root: Path,
    output: Path,
    nodes: list[ProjectNode],
    items_by_node: dict[str, list[ProjectItem]],
) -> str:
    level_one = _sort_nodes([node for node in nodes if node.level == 1])
    children = _children_by_parent(nodes)
    return "\n".join(
        _node_card(
            repo_root,
            output,
            node,
            major=True,
            rollup=_rollup_for(node, children=children, items_by_node=items_by_node),
        )
        for node in level_one
    )


def _render_lanes(
    repo_root: Path,
    output: Path,
    nodes: list[ProjectNode],
    items_by_node: dict[str, list[ProjectItem]],
) -> str:
    node_by_id = {node.node_id: node for node in nodes}
    children = _children_by_parent(nodes)

    lane_nodes = [node_by_id[node_id] for node_id in LEVEL_1_ORDER if node_id in node_by_id]
    html: list[str] = []
    for lane in lane_nodes:
        lane_children = _sort_children(children.get(lane.node_id, []))
        if not lane_children:
            continue
        rollup = _rollup_for(lane, children=children, items_by_node=items_by_node)
        html.append(
            f"""
<section class="lane">
  <header>
    <div>
      <p class="eyebrow">Capability lane</p>
      <h2>{escape(lane.title)}</h2>
    </div>
    <span class="lane-state rollup-{escape(rollup.status)}">{escape(ROLLUP_LABELS.get(rollup.status, rollup.status))}</span>
  </header>
  <div class="lane-grid">
    {"".join(_node_card(repo_root, output, child, rollup=_rollup_for(child, children=children, items_by_node=items_by_node)) for child in lane_children)}
  </div>
</section>
"""
        )
    return "\n".join(html)


def _render_attention(
    repo_root: Path,
    output: Path,
    items: list[ProjectItem],
    node_by_id: dict[str, ProjectNode],
) -> str:
    active_items = _sort_items(_active_items(items))
    if not active_items:
        return '<p class="empty">No active project items.</p>'
    return "\n".join(_item_card(repo_root, output, item, node_by_id) for item in active_items)


def _render_evidence(repo_root: Path, output: Path, nodes: list[ProjectNode]) -> str:
    rows: list[str] = []
    for node in _sort_nodes(nodes):
        search = _data_search(node.title, node.node_id, node.state, " ".join(node.source_refs), " ".join(node.dfd_refs))
        rows.append(
            f"""
<details class="evidence-row filterable" data-kind="evidence" data-state="{escape(node.state)}" data-surface="{escape(node.surface)}" data-level="{node.level}" data-search="{search}">
  <summary>
    <span>{escape(node.title)}</span>
    <span>{escape(node.state)} / L{node.level}</span>
  </summary>
  <div class="evidence-grid">
    <div><h4>Source</h4><ul>{_ref_links(repo_root, output, node.source_refs)}</ul></div>
    <div><h4>DFD</h4><ul>{_ref_links(repo_root, output, node.dfd_refs)}</ul></div>
    <div><h4>Proof</h4><ul>{_ref_links(repo_root, output, node.proof_refs)}</ul></div>
  </div>
</details>
"""
        )
    return "\n".join(rows)


def render(repo_root: Path, *, output: Path = OUTPUT) -> str:
    repo_root = repo_root.resolve()
    output = output if output.is_absolute() else output
    nodes = _load_nodes(repo_root)
    items = _load_items(repo_root)
    node_by_id = {node.node_id: node for node in nodes}
    items_by_node: dict[str, list[ProjectItem]] = defaultdict(list)
    for item in items:
        items_by_node[item.linked_node].append(item)
    state_options = sorted({node.state for node in nodes} | {item.status for item in items})
    priority_options = [priority for priority in PRIORITY_LABELS if any(item.priority == priority for item in items)]
    surface_options = sorted({node.surface for node in nodes})
    state_select = "\n".join(f'<option value="{escape(value)}">{escape(value)}</option>' for value in state_options)
    priority_select = "\n".join(
        f'<option value="{escape(value)}">{escape(PRIORITY_LABELS.get(value, value))}</option>'
        for value in priority_options
    )
    surface_select = "\n".join(f'<option value="{escape(value)}">{escape(value)}</option>' for value in surface_options)

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TA3000 Project Cockpit</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #18191c;
      --panel: #23252a;
      --panel-strong: #2d3036;
      --line: #3b414a;
      --text: #eceff4;
      --muted: #aab2bf;
      --ok: #2ec27e;
      --attention: #ffd43b;
      --unknown: #ff8a3d;
      --blocked: #ff5c5c;
      --shell: #8cc8ff;
      --product: #d0b7ff;
      --mixed: #77ddb7;
      --item: #4fc3f7;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    a {{ color: inherit; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .shell {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 28px;
    }}
    .topbar {{
      display: grid;
      grid-template-columns: minmax(280px, 1fr) minmax(520px, 1.4fr);
      gap: 20px;
      align-items: end;
      margin-bottom: 20px;
    }}
    h1, h2, h3, h4, p {{ margin: 0; }}
    h1 {{ font-size: 30px; font-weight: 680; }}
    h2 {{ font-size: 18px; font-weight: 650; }}
    h3 {{ font-size: 15px; font-weight: 650; }}
    h4 {{ font-size: 12px; color: var(--muted); text-transform: uppercase; }}
    .subtitle {{ margin-top: 8px; color: var(--muted); max-width: 860px; }}
    .controls {{
      display: grid;
      grid-template-columns: minmax(190px, 1fr) 150px 150px 150px 120px;
      gap: 10px;
    }}
    input, select, button {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
      color: var(--text);
      padding: 10px 12px;
      font: inherit;
    }}
    button {{ cursor: pointer; }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 26px;
    }}
    .metric {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 12px;
    }}
    .metric span {{ display: block; color: var(--muted); font-size: 12px; }}
    .metric strong {{ display: block; margin-top: 4px; font-size: 24px; }}
    .section {{
      margin-top: 28px;
    }}
    .section > header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 12px;
    }}
    .eyebrow {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }}
    .level-one-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 12px;
    }}
    .lane {{
      border-top: 1px solid var(--line);
      padding-top: 18px;
      margin-top: 18px;
    }}
    .lane > header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }}
    .lane-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(235px, 1fr));
      gap: 10px;
    }}
    .attention-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 10px;
    }}
    .node-card, .item-card {{
      min-height: 150px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 13px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }}
    .node-major {{
      min-height: 170px;
      background: var(--panel-strong);
      border-width: 2px;
    }}
    .node-card[data-state="ok"] {{ border-color: color-mix(in srgb, var(--ok) 56%, var(--line)); }}
    .node-card[data-state="attention"] {{ border-color: color-mix(in srgb, var(--attention) 70%, var(--line)); }}
    .node-card[data-state="unknown"] {{ border-color: color-mix(in srgb, var(--unknown) 70%, var(--line)); }}
    .node-card[data-state="blocked"] {{ border-color: color-mix(in srgb, var(--blocked) 70%, var(--line)); }}
    .item-card {{ border-color: color-mix(in srgb, var(--item) 58%, var(--line)); }}
    .user-attention {{ box-shadow: inset 4px 0 0 var(--blocked); }}
    .card-topline, .meta-row {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
    }}
    .node-card p, .item-card p {{
      color: var(--muted);
      flex: 1;
    }}
    .rollup-line {{
      display: grid;
      gap: 4px;
      color: var(--muted);
      font-size: 12px;
      border-top: 1px solid var(--line);
      padding-top: 8px;
    }}
    .node-level, .surface, .badge, .item-type, .severity, .priority, .lane-state {{
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border-radius: 999px;
      padding: 2px 8px;
      background: #363a42;
      color: var(--text);
      white-space: nowrap;
    }}
    .surface-shell {{ background: color-mix(in srgb, var(--shell) 28%, #29313a); }}
    .surface-product-plane {{ background: color-mix(in srgb, var(--product) 28%, #312c3d); }}
    .surface-mixed {{ background: color-mix(in srgb, var(--mixed) 24%, #2b3934); }}
    .state-ok {{ background: color-mix(in srgb, var(--ok) 32%, #26352d); }}
    .state-attention {{ background: color-mix(in srgb, var(--attention) 32%, #3f3824); color: #fff4bd; }}
    .state-needs-attention {{ background: color-mix(in srgb, var(--attention) 32%, #3f3824); color: #fff4bd; }}
    .state-unknown {{ background: color-mix(in srgb, var(--unknown) 32%, #3c3028); }}
    .rollup-stable {{ background: color-mix(in srgb, var(--ok) 32%, #26352d); }}
    .rollup-watch {{ background: color-mix(in srgb, var(--item) 26%, #29323a); }}
    .rollup-review {{ background: color-mix(in srgb, var(--attention) 32%, #3f3824); color: #fff4bd; }}
    .rollup-unknown {{ background: color-mix(in srgb, var(--unknown) 32%, #3c3028); }}
    .rollup-decision {{ background: color-mix(in srgb, var(--attention) 48%, #443824); color: #fff4bd; }}
    .rollup-blocked {{ background: color-mix(in srgb, var(--blocked) 42%, #3d292c); }}
    .priority-p0 {{ background: color-mix(in srgb, var(--blocked) 54%, #3d292c); }}
    .priority-p1 {{ background: color-mix(in srgb, var(--attention) 48%, #3f3824); color: #fff4bd; }}
    .priority-p2 {{ background: color-mix(in srgb, var(--item) 34%, #29323a); }}
    .priority-p3 {{ background: #363a42; }}
    .severity-high {{ background: color-mix(in srgb, var(--blocked) 34%, #3d292c); }}
    .severity-medium {{ background: color-mix(in srgb, var(--attention) 30%, #3b3627); }}
    .severity-low {{ background: color-mix(in srgb, var(--ok) 24%, #29362f); }}
    .evidence-list {{
      display: grid;
      gap: 8px;
    }}
    .evidence-row {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }}
    .evidence-row summary {{
      cursor: pointer;
      padding: 12px;
      display: flex;
      justify-content: space-between;
      gap: 16px;
      color: var(--text);
    }}
    .evidence-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      border-top: 1px solid var(--line);
      padding: 12px;
    }}
    ul {{ margin: 8px 0 0; padding-left: 18px; color: var(--muted); }}
    li + li {{ margin-top: 4px; }}
    .muted, .empty {{ color: var(--muted); }}
    [hidden] {{ display: none !important; }}
    @media (max-width: 900px) {{
      .shell {{ padding: 18px; }}
      .topbar {{ grid-template-columns: 1fr; }}
      .controls {{ grid-template-columns: 1fr 1fr; }}
      .metrics {{ grid-template-columns: repeat(2, 1fr); }}
      .evidence-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <!-- generated-by: scripts/build_project_cockpit.py -->
  <main class="shell">
    <section class="topbar">
      <div>
        <p class="eyebrow">Trading Advisor 3000</p>
        <h1>Project Cockpit</h1>
        <p class="subtitle">Architecture and attention view generated from Obsidian project-map notes.</p>
      </div>
      <form class="controls" id="filters">
        <input id="search" type="search" placeholder="Search nodes, refs, items" aria-label="Search">
        <select id="state" aria-label="State filter">
          <option value="">All states</option>
          {state_select}
        </select>
        <select id="surface" aria-label="Surface filter">
          <option value="">All surfaces</option>
          {surface_select}
        </select>
        <select id="priority" aria-label="Priority filter">
          <option value="">All priorities</option>
          {priority_select}
        </select>
        <select id="kind" aria-label="Kind filter">
          <option value="">All kinds</option>
          <option value="node">Nodes</option>
          <option value="item">Items</option>
          <option value="evidence">Evidence</option>
        </select>
      </form>
    </section>

    <section class="metrics" aria-label="Project map summary">
      {_render_summary(nodes, items)}
    </section>

    <section class="section">
      <header>
        <div>
          <p class="eyebrow">Level 1</p>
          <h2>Architecture spine</h2>
        </div>
      </header>
      <div class="level-one-grid">
        {_render_level_one(repo_root, output, nodes, items_by_node)}
      </div>
    </section>

    <section class="section">
      <header>
        <div>
          <p class="eyebrow">Level 2</p>
          <h2>Capability lanes</h2>
        </div>
      </header>
      {_render_lanes(repo_root, output, nodes, items_by_node)}
    </section>

    <section class="section">
      <header>
        <div>
          <p class="eyebrow">Priority queue</p>
          <h2>Open questions, risks, and problems</h2>
        </div>
      </header>
      <div class="attention-grid">
        {_render_attention(repo_root, output, items, node_by_id)}
      </div>
    </section>

    <section class="section">
      <header>
        <div>
          <p class="eyebrow">Traceability</p>
          <h2>DFD, source, and proof refs</h2>
        </div>
      </header>
      <div class="evidence-list">
        {_render_evidence(repo_root, output, nodes)}
      </div>
    </section>
  </main>
  <script>
    const search = document.getElementById('search');
    const state = document.getElementById('state');
    const surface = document.getElementById('surface');
    const priority = document.getElementById('priority');
    const kind = document.getElementById('kind');
    const cards = Array.from(document.querySelectorAll('.filterable'));

    function applyFilters() {{
      const query = search.value.trim().toLowerCase();
      const stateValue = state.value;
      const surfaceValue = surface.value;
      const priorityValue = priority.value;
      const kindValue = kind.value;
      for (const card of cards) {{
        const matchesQuery = !query || card.dataset.search.includes(query);
        const matchesState = !stateValue || card.dataset.state === stateValue;
        const matchesSurface = !surfaceValue || card.dataset.surface === surfaceValue;
        const matchesPriority = !priorityValue || card.dataset.priority === priorityValue;
        const matchesKind = !kindValue || card.dataset.kind === kindValue;
        card.hidden = !(matchesQuery && matchesState && matchesSurface && matchesPriority && matchesKind);
      }}
    }}

    for (const element of [search, state, surface, priority, kind]) {{
      element.addEventListener('input', applyFilters);
      element.addEventListener('change', applyFilters);
    }}
  </script>
</body>
</html>
"""
    return "\n".join(line.rstrip() for line in html.splitlines()) + "\n"


def _check_output(path: Path, rendered: str) -> int:
    if not path.exists():
        print(f"[build_project_cockpit] Missing generated HTML: {path.as_posix()}")
        return 1
    current = path.read_text(encoding="utf-8")
    if current == rendered:
        print("[build_project_cockpit] Generated cockpit is current.")
        return 0
    diff = "\n".join(
        difflib.unified_diff(
            current.splitlines(),
            rendered.splitlines(),
            fromfile=path.as_posix(),
            tofile="rendered",
            lineterm="",
        )
    )
    print("[build_project_cockpit] Generated cockpit is out of date.")
    print(diff)
    return 1


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = args.output if args.output.is_absolute() else args.output
    output_path = repo_root / output
    rendered = render(repo_root, output=output)

    if args.check:
        return _check_output(output_path, rendered)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    print(f"[build_project_cockpit] Wrote {output.as_posix()}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
