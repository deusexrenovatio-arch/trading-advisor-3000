#!/usr/bin/env python3
"""Create a project-map attention item."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any
from unicodedata import normalize

import yaml


NODE_ROOT = Path("docs/project-map/state/nodes")
ITEM_ROOT = Path("docs/project-map/state/items")
ITEM_TYPES = ("problem", "question", "idea", "risk", "task")
STATUSES = ("inbox", "open", "watch", "done", "dropped")
PRIORITIES = ("p0", "p1", "p2", "p3")
SEVERITIES = ("low", "medium", "high")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add a project-map problem, question, idea, risk, or task.")
    parser.add_argument("title", help="Item title.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--node", required=True, help="Target project node_id.")
    parser.add_argument("--type", choices=ITEM_TYPES, default="question", dest="item_type")
    parser.add_argument("--status", choices=STATUSES, default="inbox")
    parser.add_argument("--priority", choices=PRIORITIES, default="p2")
    parser.add_argument("--severity", choices=SEVERITIES, default="medium")
    parser.add_argument("--owner", default="user-and-agent")
    parser.add_argument("--origin-kind", default="manual", help="Where this item came from.")
    parser.add_argument(
        "--origin-ref",
        action="append",
        default=[],
        help="Source path for this item. Can be provided more than once.",
    )
    parser.add_argument("--attention", action="store_true", help="Mark as needing user attention.")
    parser.add_argument("--body", default="", help="Optional note body.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing item file.")
    return parser.parse_args()


def _read_frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig")
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end == -1:
        return {}
    loaded = yaml.safe_load(text[3:end]) or {}
    return loaded if isinstance(loaded, dict) else {}


def _slug(value: str) -> str:
    ascii_value = normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value).strip("-").lower()
    if slug:
        return slug
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    return f"item-{digest}"


def _filename(value: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', " ", value).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or "Project Map Item"


def _node_ids(repo_root: Path) -> dict[str, str]:
    nodes: dict[str, str] = {}
    for path in sorted((repo_root / NODE_ROOT).glob("*.md")):
        fm = _read_frontmatter(path)
        node_id = fm.get("node_id")
        title = fm.get("title")
        if isinstance(node_id, str) and node_id.strip():
            nodes[node_id] = str(title or path.stem)
    return nodes


def render_item(
    *,
    title: str,
    item_type: str,
    linked_node: str,
    status: str,
    priority: str,
    severity: str,
    owner: str,
    origin_kind: str,
    origin_refs: list[str] | tuple[str, ...],
    needs_user_attention: bool,
    body: str,
    created: date | None = None,
) -> str:
    created = created or date.today()
    item_id = f"{item_type}-{_slug(title)}"
    body = body.strip() or "Describe the signal, problem, or question here."
    if not origin_refs:
        origin_refs = ("docs/project-map/state/Project Map Update Rules.md",)
    frontmatter = {
        "title": title,
        "type": "project-item",
        "item_id": item_id,
        "item_type": item_type,
        "linked_node": linked_node,
        "status": status,
        "aliases": [title],
        "severity": severity,
        "priority": priority,
        "needs_user_attention": needs_user_attention,
        "owner": owner,
        "created": created.isoformat(),
        "origin_kind": origin_kind,
        "origin_refs": list(origin_refs),
        "tags": [
            "ta3000/project-item",
            "ta3000/project-graph",
            f"item/{item_type}",
            f"status/{status}",
            f"priority/{priority}",
            f"severity/{severity}",
        ],
    }
    if origin_kind != "manual":
        frontmatter["tags"].append(f"origin/{origin_kind}")
    dumped = yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=True).strip()
    return f"""---
{dumped}
---

# {title}

{body}

## Graph Links

- Belongs to {linked_node}.
"""


def create_item(
    *,
    repo_root: Path,
    title: str,
    linked_node: str,
    item_type: str = "question",
    status: str = "inbox",
    priority: str = "p2",
    severity: str = "medium",
    owner: str = "user-and-agent",
    origin_kind: str = "manual",
    origin_refs: list[str] | tuple[str, ...] = (),
    needs_user_attention: bool = False,
    body: str = "",
    force: bool = False,
    created: date | None = None,
) -> Path:
    repo_root = repo_root.resolve()
    nodes = _node_ids(repo_root)
    if linked_node not in nodes:
        known = ", ".join(sorted(nodes))
        raise ValueError(f"unknown project node_id `{linked_node}`. Known node_ids: {known}")

    item_root = repo_root / ITEM_ROOT
    item_root.mkdir(parents=True, exist_ok=True)
    path = item_root / f"{_filename(title)}.md"
    if path.exists() and not force:
        raise FileExistsError(f"project-map item already exists: {path}")

    path.write_text(
        render_item(
            title=title,
            item_type=item_type,
            linked_node=linked_node,
            status=status,
            priority=priority,
            severity=severity,
            owner=owner,
            origin_kind=origin_kind,
            origin_refs=origin_refs,
            needs_user_attention=needs_user_attention,
            body=body,
            created=created,
        ),
        encoding="utf-8",
    )
    return path


def main() -> int:
    args = parse_args()
    try:
        path = create_item(
            repo_root=args.repo_root,
            title=args.title,
            linked_node=args.node,
            item_type=args.item_type,
            status=args.status,
            priority=args.priority,
            severity=args.severity,
            owner=args.owner,
            origin_kind=args.origin_kind,
            origin_refs=args.origin_ref,
            needs_user_attention=args.attention,
            body=args.body,
            force=args.force,
        )
    except (FileExistsError, ValueError) as exc:
        print(f"[add_project_map_item] {exc}", file=sys.stderr)
        return 1
    print(f"[add_project_map_item] Wrote {path.relative_to(args.repo_root.resolve()).as_posix()}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
