#!/usr/bin/env python3
"""Validate the Obsidian project map contract."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


MAP_ROOT = Path("docs/project-map/state")
NODE_ROOT = MAP_ROOT / "nodes"
ITEM_ROOT = MAP_ROOT / "items"
GRAPH_CONFIG = Path(".obsidian/graph.json")
OVERVIEW_DOC = MAP_ROOT / "Project Map Overview.md"
REQUIRED_NODE_FIELDS = (
    "title",
    "type",
    "node_id",
    "surface",
    "level",
    "state",
    "confidence",
    "last_verified",
    "update_rule",
    "state_source",
    "source_refs",
    "dfd_refs",
    "proof_refs",
    "tags",
)
REQUIRED_ITEM_FIELDS = (
    "title",
    "type",
    "item_id",
    "item_type",
    "linked_node",
    "status",
    "priority",
    "severity",
    "needs_user_attention",
    "owner",
    "created",
    "origin_kind",
    "origin_refs",
    "tags",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate docs/project-map Obsidian metadata.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root path.",
    )
    parser.add_argument(
        "--from-git",
        action="store_true",
        help="Also fail when changed source/DFD/proof refs are not paired with a changed node.",
    )
    return parser.parse_args()


def _rel(path: Path, repo_root: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def _read_frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig")
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end == -1:
        return {}
    loaded = yaml.safe_load(text[3:end]) or {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def _as_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item.strip()]


def _run_git(repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def _changed_files(repo_root: Path) -> set[str]:
    changed: set[str] = set()
    diff = _run_git(repo_root, ["diff", "--name-only", "--diff-filter=ACMRTUXB", "HEAD", "--"])
    if diff.returncode == 0 and diff.stdout:
        changed.update(line.strip().replace("\\", "/") for line in diff.stdout.splitlines() if line.strip())
    untracked = _run_git(repo_root, ["ls-files", "--others", "--exclude-standard"])
    if untracked.returncode == 0 and untracked.stdout:
        changed.update(line.strip().replace("\\", "/") for line in untracked.stdout.splitlines() if line.strip())
    return changed


def _validate_visual_entrypoints(repo_root: Path) -> list[str]:
    errors: list[str] = []
    graph_path = repo_root / GRAPH_CONFIG
    if graph_path.exists():
        try:
            json.loads(graph_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"invalid graph config JSON: {GRAPH_CONFIG.as_posix()}: {exc}")

    overview_path = repo_root / OVERVIEW_DOC
    if not overview_path.exists():
        errors.append(f"missing project-map overview: {OVERVIEW_DOC.as_posix()}")
    elif "```mermaid" not in overview_path.read_text(encoding="utf-8-sig"):
        errors.append(f"{OVERVIEW_DOC.as_posix()} must include a Mermaid overview")
    return errors


def _validate_ref_list(
    *,
    repo_root: Path,
    node_path: Path,
    field: str,
    refs: list[str],
    required_prefix: str | None = None,
) -> list[str]:
    rel_node = _rel(node_path, repo_root)
    if not refs:
        return [f"{rel_node}: `{field}` must be a non-empty list"]

    errors: list[str] = []
    for ref in refs:
        if required_prefix and not ref.startswith(required_prefix):
            errors.append(f"{rel_node}: `{field}` ref must start with `{required_prefix}`: {ref}")
            continue
        target = repo_root / ref
        if not target.exists():
            errors.append(f"{rel_node}: `{field}` ref does not exist: {ref}")
    return errors


def _validate_node(
    *,
    repo_root: Path,
    node_path: Path,
    frontmatter: dict[str, Any],
    node_ids: dict[str, Path],
) -> list[str]:
    rel_node = _rel(node_path, repo_root)
    errors: list[str] = []
    for field in REQUIRED_NODE_FIELDS:
        if field not in frontmatter:
            errors.append(f"{rel_node}: missing required field `{field}`")

    if frontmatter.get("type") != "project-node":
        errors.append(f"{rel_node}: `type` must be `project-node`")

    node_id = frontmatter.get("node_id")
    if not isinstance(node_id, str) or not node_id.strip():
        errors.append(f"{rel_node}: `node_id` must be a non-empty string")

    level = frontmatter.get("level")
    if not isinstance(level, int) or level < 1:
        errors.append(f"{rel_node}: `level` must be an integer >= 1")
    elif level > 1:
        parent = frontmatter.get("parent_node")
        if not isinstance(parent, str) or not parent.strip():
            errors.append(f"{rel_node}: level-{level} node must declare `parent_node`")
        elif parent not in node_ids:
            errors.append(f"{rel_node}: parent node does not exist: {parent}")

    tags = _as_list(frontmatter.get("tags"))
    surface = frontmatter.get("surface")
    state = frontmatter.get("state")
    expected_tags = {
        "ta3000/project-node",
        "ta3000/project-graph",
        f"surface/{surface}",
        f"level/{level}",
        f"state/{state}",
    }
    for tag in sorted(expected_tags):
        if tag not in tags:
            errors.append(f"{rel_node}: missing tag `{tag}`")

    errors.extend(
        _validate_ref_list(
            repo_root=repo_root,
            node_path=node_path,
            field="source_refs",
            refs=_as_list(frontmatter.get("source_refs")),
        )
    )
    errors.extend(
        _validate_ref_list(
            repo_root=repo_root,
            node_path=node_path,
            field="dfd_refs",
            refs=_as_list(frontmatter.get("dfd_refs")),
            required_prefix="docs/obsidian/dfd/",
        )
    )
    errors.extend(
        _validate_ref_list(
            repo_root=repo_root,
            node_path=node_path,
            field="proof_refs",
            refs=_as_list(frontmatter.get("proof_refs")),
        )
    )
    return errors


def _validate_item(
    *,
    repo_root: Path,
    item_path: Path,
    frontmatter: dict[str, Any],
    node_ids: dict[str, Path],
) -> list[str]:
    rel_item = _rel(item_path, repo_root)
    errors: list[str] = []
    for field in REQUIRED_ITEM_FIELDS:
        if field not in frontmatter:
            errors.append(f"{rel_item}: missing required field `{field}`")

    if frontmatter.get("type") != "project-item":
        errors.append(f"{rel_item}: `type` must be `project-item`")

    item_id = frontmatter.get("item_id")
    if not isinstance(item_id, str) or not item_id.strip():
        errors.append(f"{rel_item}: `item_id` must be a non-empty string")

    linked_node = frontmatter.get("linked_node")
    if not isinstance(linked_node, str) or not linked_node.strip():
        errors.append(f"{rel_item}: `linked_node` must be a non-empty string")
    elif linked_node not in node_ids:
        errors.append(f"{rel_item}: linked node does not exist: {linked_node}")

    item_type = frontmatter.get("item_type")
    status = frontmatter.get("status")
    priority = frontmatter.get("priority")
    severity = frontmatter.get("severity")
    origin_kind = frontmatter.get("origin_kind")
    tags = _as_list(frontmatter.get("tags"))
    expected_tags = {
        "ta3000/project-item",
        "ta3000/project-graph",
        f"item/{item_type}",
        f"status/{status}",
        f"priority/{priority}",
        f"severity/{severity}",
    }
    if isinstance(origin_kind, str) and origin_kind.strip() and origin_kind != "manual":
        expected_tags.add(f"origin/{origin_kind}")
    for tag in sorted(expected_tags):
        if tag not in tags:
            errors.append(f"{rel_item}: missing tag `{tag}`")

    origin_refs = _as_list(frontmatter.get("origin_refs"))
    if not origin_refs:
        errors.append(f"{rel_item}: `origin_refs` must be a non-empty list")
    for ref in origin_refs:
        if not (repo_root / ref).exists():
            errors.append(f"{rel_item}: `origin_refs` ref does not exist: {ref}")

    return errors


def run(
    repo_root: Path,
    *,
    from_git: bool = False,
    changed_files_override: set[str] | None = None,
) -> int:
    repo_root = repo_root.resolve()
    node_root = repo_root / NODE_ROOT
    errors: list[str] = []
    if not node_root.exists():
        errors.append(f"missing project-map node root: {NODE_ROOT.as_posix()}")
    if errors:
        return _finish(errors)

    node_frontmatters: dict[Path, dict[str, Any]] = {}
    node_ids: dict[str, Path] = {}
    for node_path in sorted(node_root.glob("*.md")):
        fm = _read_frontmatter(node_path)
        node_frontmatters[node_path] = fm
        node_id = fm.get("node_id")
        if isinstance(node_id, str) and node_id.strip():
            if node_id in node_ids:
                errors.append(f"duplicate node_id `{node_id}`: {node_path} and {node_ids[node_id]}")
            node_ids[node_id] = node_path

    for node_path, fm in node_frontmatters.items():
        errors.extend(
            _validate_node(
                repo_root=repo_root,
                node_path=node_path,
                frontmatter=fm,
                node_ids=node_ids,
            )
        )

    item_root = repo_root / ITEM_ROOT
    if item_root.exists():
        for item_path in sorted(item_root.glob("*.md")):
            errors.extend(
                _validate_item(
                    repo_root=repo_root,
                    item_path=item_path,
                    frontmatter=_read_frontmatter(item_path),
                    node_ids=node_ids,
                )
            )

    errors.extend(_validate_visual_entrypoints(repo_root))

    if from_git or changed_files_override is not None:
        changed = changed_files_override if changed_files_override is not None else _changed_files(repo_root)
        errors.extend(_validate_changed_ref_coupling(repo_root, node_frontmatters, changed))

    return _finish(errors)


def _validate_changed_ref_coupling(
    repo_root: Path,
    node_frontmatters: dict[Path, dict[str, Any]],
    changed_files: set[str],
) -> list[str]:
    errors: list[str] = []
    normalized_changed = {path.replace("\\", "/") for path in changed_files}
    changed_nodes = {
        _rel(node_path, repo_root)
        for node_path in node_frontmatters
        if _rel(node_path, repo_root) in normalized_changed
    }
    if not normalized_changed:
        return []

    for node_path, fm in node_frontmatters.items():
        node_rel = _rel(node_path, repo_root)
        refs = set(_as_list(fm.get("source_refs")))
        refs.update(_as_list(fm.get("dfd_refs")))
        refs.update(_as_list(fm.get("proof_refs")))
        touched = sorted(ref for ref in refs if ref in normalized_changed)
        if touched and node_rel not in changed_nodes:
            errors.append(
                f"{node_rel}: referenced source changed without project-map node update: {', '.join(touched)}"
            )
    return errors


def _finish(errors: list[str]) -> int:
    if errors:
        print("[validate_project_map] Project map validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1
    print("[validate_project_map] Project map contract OK.")
    return 0


def main() -> int:
    args = parse_args()
    return run(args.repo_root, from_git=args.from_git)


if __name__ == "__main__":
    raise SystemExit(main())
