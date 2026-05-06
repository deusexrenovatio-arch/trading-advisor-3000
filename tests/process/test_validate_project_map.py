from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from validate_project_map import run  # noqa: E402


def _write_graph_config(root: Path) -> None:
    graph = root / ".obsidian" / "graph.json"
    graph.parent.mkdir(parents=True)
    graph.write_text(
        """{
  "colorGroups": [
    {"query": "tag:#ta3000/project-node tag:#level/1", "color": {"a": 1, "rgb": 1}},
    {"query": "tag:#ta3000/project-node tag:#level/2", "color": {"a": 1, "rgb": 2}}
  ]
}
""",
        encoding="utf-8",
    )


def _write_overview(root: Path) -> None:
    overview = root / "docs" / "project-map" / "state" / "Project Map Overview.md"
    overview.parent.mkdir(parents=True, exist_ok=True)
    overview.write_text("# Project Map Overview\n\n```mermaid\nflowchart LR\n  A --> B\n```\n", encoding="utf-8")


def _write_ref(root: Path, rel: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("# ref\n", encoding="utf-8")


def _write_node(root: Path, name: str, body: str) -> None:
    node = root / "docs" / "project-map" / "state" / "nodes" / f"{name}.md"
    node.parent.mkdir(parents=True, exist_ok=True)
    node.write_text(body, encoding="utf-8")


def _write_item(root: Path, name: str, body: str) -> None:
    item = root / "docs" / "project-map" / "state" / "items" / f"{name}.md"
    item.parent.mkdir(parents=True, exist_ok=True)
    item.write_text(body, encoding="utf-8")


def _valid_node_frontmatter(*, title: str, node_id: str, level: int = 1, parent: str = "") -> str:
    parent_line = f"parent_node: {parent}" if parent else "parent_node:"
    return f"""---
title: {title}
type: project-node
node_id: {node_id}
surface: shell
level: {level}
{parent_line}
state: ok
confidence: medium
last_verified: 2026-05-05
update_rule: source-doc-backed
state_source: docs/source.md
source_refs:
  - docs/source.md
dfd_refs:
  - docs/obsidian/dfd/level-1-demo.md
proof_refs:
  - scripts/proof.py
tags:
  - ta3000/project-node
  - ta3000/project-graph
  - surface/shell
  - level/{level}
  - state/ok
---

# {title}
"""


def _valid_item_frontmatter(*, title: str, item_id: str, linked_node: str) -> str:
    return f"""---
title: {title}
type: project-item
item_id: {item_id}
item_type: question
linked_node: {linked_node}
status: open
aliases:
  - {title}
severity: medium
priority: p1
needs_user_attention: true
owner: user-and-agent
created: 2026-05-05
origin_kind: manual
origin_refs:
  - docs/source.md
tags:
  - ta3000/project-item
  - ta3000/project-graph
  - item/question
  - status/open
  - priority/p1
  - severity/medium
---

# {title}
"""


def test_project_map_validator_passes_minimal_valid_map(tmp_path: Path) -> None:
    _write_graph_config(tmp_path)
    _write_overview(tmp_path)
    _write_ref(tmp_path, "docs/source.md")
    _write_ref(tmp_path, "docs/obsidian/dfd/level-1-demo.md")
    _write_ref(tmp_path, "scripts/proof.py")
    _write_node(tmp_path, "Demo", _valid_node_frontmatter(title="Demo", node_id="demo"))

    assert run(tmp_path) == 0


def test_project_map_validator_requires_mermaid_overview(tmp_path: Path) -> None:
    _write_ref(tmp_path, "docs/source.md")
    _write_ref(tmp_path, "docs/obsidian/dfd/level-1-demo.md")
    _write_ref(tmp_path, "scripts/proof.py")
    _write_graph_config(tmp_path)
    _write_node(tmp_path, "Demo", _valid_node_frontmatter(title="Demo", node_id="demo"))

    assert run(tmp_path) == 1


def test_project_map_validator_requires_existing_parent_for_level_two(tmp_path: Path) -> None:
    _write_graph_config(tmp_path)
    _write_overview(tmp_path)
    _write_ref(tmp_path, "docs/source.md")
    _write_ref(tmp_path, "docs/obsidian/dfd/level-1-demo.md")
    _write_ref(tmp_path, "scripts/proof.py")
    _write_node(
        tmp_path,
        "Child",
        _valid_node_frontmatter(title="Child", node_id="child", level=2, parent="missing-parent"),
    )

    assert run(tmp_path) == 1


def test_project_map_validator_pairs_changed_source_with_node_update(tmp_path: Path) -> None:
    _write_graph_config(tmp_path)
    _write_overview(tmp_path)
    _write_ref(tmp_path, "docs/source.md")
    _write_ref(tmp_path, "docs/obsidian/dfd/level-1-demo.md")
    _write_ref(tmp_path, "scripts/proof.py")
    _write_node(tmp_path, "Demo", _valid_node_frontmatter(title="Demo", node_id="demo"))

    assert run(tmp_path, changed_files_override={"docs/source.md"}) == 1
    assert (
        run(
            tmp_path,
            changed_files_override={
                "docs/source.md",
                "docs/project-map/state/nodes/Demo.md",
            },
        )
        == 0
    )


def test_project_map_validator_requires_item_linked_node_to_exist(tmp_path: Path) -> None:
    _write_graph_config(tmp_path)
    _write_overview(tmp_path)
    _write_ref(tmp_path, "docs/source.md")
    _write_ref(tmp_path, "docs/obsidian/dfd/level-1-demo.md")
    _write_ref(tmp_path, "scripts/proof.py")
    _write_node(tmp_path, "Demo", _valid_node_frontmatter(title="Demo", node_id="demo"))
    _write_item(
        tmp_path,
        "Question",
        _valid_item_frontmatter(title="Question", item_id="question-demo", linked_node="missing-node"),
    )

    assert run(tmp_path) == 1
