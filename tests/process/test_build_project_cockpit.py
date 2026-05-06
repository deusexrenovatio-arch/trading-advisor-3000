from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from add_project_map_item import create_item  # noqa: E402
from build_project_cockpit import render  # noqa: E402


def _write_node(
    root: Path,
    *,
    title: str,
    node_id: str,
    level: int = 1,
    parent: str = "",
    state: str = "ok",
    surface: str = "product-plane",
) -> None:
    path = root / "docs" / "project-map" / "state" / "nodes" / f"{title}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    parent_value = parent if parent else "null"
    path.write_text(
        f"""---
title: {title}
type: project-node
node_id: {node_id}
surface: {surface}
level: {level}
parent_node: {parent_value}
state: {state}
confidence: medium
last_verified: 2026-05-05
update_rule: test-backed
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
  - surface/{surface}
  - level/{level}
  - state/{state}
---

# {title}

Short note body for {title}.
""",
        encoding="utf-8",
    )


def test_project_cockpit_renders_nodes_items_and_traceability(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Product Plane", node_id="product-plane", state="attention")
    _write_node(tmp_path, title="Research Data Prep", node_id="research-data-prep", level=2, parent="product-plane")
    item_path = create_item(
        repo_root=tmp_path,
        title="Open Research Question",
        linked_node="product-plane",
        item_type="question",
        priority="p1",
        severity="high",
        needs_user_attention=True,
        created=date(2026, 5, 5),
    )

    html = render(tmp_path)

    assert "Project Cockpit" in html
    assert "Product Plane" in html
    assert "Research Data Prep" in html
    assert "Open Research Question" in html
    assert "P1 decision" in html
    assert "Decision" in html
    assert "DFD, source, and proof refs" in html
    assert item_path.name.replace(" ", "%20") in html


def test_project_cockpit_priority_queue_contains_items_not_nodes(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Product Plane", node_id="product-plane", state="attention")
    create_item(
        repo_root=tmp_path,
        title="Open Research Question",
        linked_node="product-plane",
        item_type="question",
        priority="p2",
        severity="medium",
        created=date(2026, 5, 5),
    )

    html = render(tmp_path)
    priority_section = html.split('<p class="eyebrow">Priority queue</p>', 1)[1].split(
        '<p class="eyebrow">Traceability</p>',
        1,
    )[0]

    assert 'data-kind="item"' in priority_section
    assert 'data-kind="node"' not in priority_section
    assert "Open Research Question" in priority_section
    assert "Product Plane" in html


def test_project_cockpit_check_mode_detects_drift(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Product Plane", node_id="product-plane")
    output = tmp_path / "docs" / "project-map" / "project-cockpit.html"
    script = SCRIPTS / "build_project_cockpit.py"

    build = subprocess.run(
        [sys.executable, str(script), "--repo-root", str(tmp_path)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert build.returncode == 0, build.stderr

    check = subprocess.run(
        [sys.executable, str(script), "--repo-root", str(tmp_path), "--check"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert check.returncode == 0, check.stdout + check.stderr

    output.write_text("stale", encoding="utf-8")
    stale_check = subprocess.run(
        [sys.executable, str(script), "--repo-root", str(tmp_path), "--check"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert stale_check.returncode == 1
    assert "out of date" in stale_check.stdout


def test_add_project_map_item_rejects_unknown_node(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Product Plane", node_id="product-plane")

    with pytest.raises(ValueError, match="unknown project node_id"):
        create_item(
            repo_root=tmp_path,
            title="Unknown Node Question",
            linked_node="missing-node",
            priority="p2",
            created=date(2026, 5, 5),
        )
