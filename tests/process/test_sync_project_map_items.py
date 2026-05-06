from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from sync_project_map_items import collect_signals, run  # noqa: E402


def _write_node(root: Path, *, title: str, node_id: str, state: str = "ok", attention: bool = False) -> None:
    path = root / "docs" / "project-map" / "state" / "nodes" / f"{title}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    attention_value = "true" if attention else "false"
    path.write_text(
        f"""---
title: {title}
type: project-node
node_id: {node_id}
surface: product-plane
level: 1
parent_node: null
state: {state}
needs_user_attention: {attention_value}
confidence: low
last_verified: 2026-05-05
update_rule: test-backed
state_source: test source
source_refs:
  - docs/source.md
dfd_refs:
  - docs/obsidian/dfd/demo.md
proof_refs:
  - scripts/proof.py
tags:
  - ta3000/project-node
  - ta3000/project-graph
  - surface/product-plane
  - level/1
  - state/{state}
---

# {title}
""",
        encoding="utf-8",
    )


def _write_task(root: Path) -> Path:
    path = root / "docs" / "tasks" / "active" / "TASK-demo.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """# Task Note

## Goal
- Deliver: Fix runtime proof route

## Blockers
- Acceptance is currently blocked by missing runtime proof.
""",
        encoding="utf-8",
    )
    return path


def test_sync_project_map_items_collects_current_node_signals_by_default(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Runtime Plane", node_id="runtime-plane", state="unknown")
    _write_node(tmp_path, title="Delivery Shell", node_id="delivery-shell", state="ok")
    _write_task(tmp_path)

    signals = collect_signals(tmp_path)

    assert {signal.item_id for signal in signals} == {"sync-node-state-runtime-plane"}
    assert all(signal.origin_kind != "sync-task-blocker" for signal in signals)


def test_sync_project_map_items_can_include_legacy_task_notes_explicitly(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Runtime Plane", node_id="runtime-plane", state="unknown")
    _write_node(tmp_path, title="Delivery Shell", node_id="delivery-shell", state="ok")
    _write_task(tmp_path)

    signals = collect_signals(tmp_path, include_legacy_task_notes=True)

    assert {signal.item_id for signal in signals} == {
        "sync-node-state-runtime-plane",
        "sync-task-blocker-task-demo",
    }
    assert any(signal.linked_node == "runtime-plane" for signal in signals)


def test_sync_project_map_items_writes_and_check_passes(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Runtime Plane", node_id="runtime-plane", state="unknown")
    _write_node(tmp_path, title="Delivery Shell", node_id="delivery-shell", state="ok")
    _write_task(tmp_path)

    assert run(tmp_path) == 0
    assert run(tmp_path, check=True) == 0

    generated = tmp_path / "docs" / "project-map" / "state" / "items" / "Verify unknown node Runtime Plane.md"
    text = generated.read_text(encoding="utf-8")
    assert "origin_kind: sync-node-state" in text
    assert "sync_managed: true" in text

    stale_task = tmp_path / "docs" / "project-map" / "state" / "items" / "Task blocker Fix runtime proof route.md"
    assert not stale_task.exists()


def test_sync_project_map_items_drops_previous_legacy_task_signals_by_default(tmp_path: Path) -> None:
    _write_node(tmp_path, title="Delivery Shell", node_id="delivery-shell", state="ok")
    item_root = tmp_path / "docs" / "project-map" / "state" / "items"
    item_root.mkdir(parents=True, exist_ok=True)
    stale_task = item_root / "Task blocker Old Worktree Task.md"
    stale_task.write_text(
        """---
title: Task blocker: Old Worktree Task
type: project-item
item_id: sync-task-blocker-old-worktree-task
item_type: problem
linked_node: delivery-shell
status: open
priority: p1
severity: medium
needs_user_attention: true
owner: agent
created: 2026-05-05
last_seen: 2026-05-05
sync_managed: true
origin_kind: sync-task-blocker
origin_refs:
  - docs/tasks/active/TASK-old.md
tags:
  - ta3000/project-item
  - ta3000/project-graph
  - status/open
---

# Task blocker: Old Worktree Task

Legacy generated signal.
""",
        encoding="utf-8",
    )

    assert run(tmp_path) == 0

    text = stale_task.read_text(encoding="utf-8")
    assert "status: dropped" in text
    assert "status/dropped" in text
