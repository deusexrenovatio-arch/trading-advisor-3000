#!/usr/bin/env python3
"""
Reclassify MemPalace drawers into logical wings for this repository.

Goals:
- keep delivery governance memory separate from product memory;
- isolate strategy-verification artifacts;
- optionally split emotional chat memory from decision/problem/milestone.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import chromadb


NOISE_PREFIXES = (
    ".tmp/",
    ".tmp_openspace/",
    ".runlogs/",
    ".cursor/",
    ".pytest_cache/",
    ".codex/",
)

PRODUCT_PREFIXES = (
    "src/",
    "tests/",
    "product-plane/",
)

STRATEGY_PREFIXES = (
    "artifacts/",
)

SHELL_PREFIXES = (
    "shell/",
    "docs/",
    "scripts/",
    "registry/",
    "plans/",
    "memory/",
    "configs/",
    "deployment/",
    "codex_ai_delivery_shell_package/",
    ".github/",
    ".githooks/",
)

ROOT_SHELL_FILES = {
    "agents.md",
    "readme.md",
    "harness-guideline.md",
    "agent-runbook.md",
    "pyproject.toml",
    "skillkit.yaml",
    "codeowners",
    "entities.json",
    "mempalace.yaml",
}


def _normalize(path: str) -> str:
    return path.replace("\\", "/").strip().lower()


def _relative_to_project(source_file: str, project_root_norm: str) -> Optional[str]:
    if not source_file:
        return None
    s = _normalize(source_file)
    if s == project_root_norm:
        return ""
    prefix = project_root_norm + "/"
    if s.startswith(prefix):
        return s[len(prefix) :]
    return None


def _classify_repo_drawer(rel: str, drop_noise: bool) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Returns:
      ("drop", None, None) or ("keep", wing, room_or_none)
    """
    if drop_noise:
        if any(rel.startswith(prefix) for prefix in NOISE_PREFIXES):
            return "drop", None, None
        if "/__pycache__/" in rel or rel.endswith(".pyc"):
            return "drop", None, None

    if any(rel.startswith(prefix) for prefix in STRATEGY_PREFIXES):
        return "keep", "strategy_verification", None

    if any(rel.startswith(prefix) for prefix in PRODUCT_PREFIXES):
        return "keep", "product_plane", None

    if any(rel.startswith(prefix) for prefix in SHELL_PREFIXES):
        return "keep", "delivery_shell", None

    if "/" not in rel and rel in ROOT_SHELL_FILES:
        return "keep", "delivery_shell", None

    # Safe default for this repository: everything non-product/non-artifact is shell/process context.
    return "keep", "delivery_shell", None


def _batched(items, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def reclassify(
    palace_path: str,
    collection_name: str,
    project_root: str,
    drop_noise: bool,
    separate_chat_emotional: bool,
    dry_run: bool,
) -> dict:
    client = chromadb.PersistentClient(path=palace_path)
    col = client.get_collection(collection_name)

    project_root_norm = _normalize(str(Path(project_root).resolve()))
    ts = datetime.now(timezone.utc).isoformat()

    total = col.count()
    batch_size = 1000
    offset = 0

    to_upsert_ids = []
    to_upsert_docs = []
    to_upsert_metas = []
    to_delete_ids = []

    stats = Counter()
    by_wing_move = Counter()
    by_drop_reason = Counter()

    while offset < total:
        result = col.get(limit=batch_size, offset=offset, include=["documents", "metadatas"])
        ids = result.get("ids") or []
        docs = result.get("documents") or []
        metas = result.get("metadatas") or []
        if not ids:
            break

        offset += len(ids)
        stats["seen"] += len(ids)

        for drawer_id, doc, meta in zip(ids, docs, metas):
            old_wing = (meta or {}).get("wing", "")
            old_room = (meta or {}).get("room", "")
            source_file = (meta or {}).get("source_file", "")

            target_wing = old_wing
            target_room = old_room

            # 1) Optional chat split: keep decision/problem/milestone separate from emotional context.
            if separate_chat_emotional and old_wing == "codex_chats" and old_room == "emotional":
                target_wing = "codex_chats_emotional"

            # 2) Repository drawers: reclassify by path.
            rel = _relative_to_project(source_file, project_root_norm)
            if rel is not None:
                action, wing, room = _classify_repo_drawer(rel, drop_noise=drop_noise)
                if action == "drop":
                    to_delete_ids.append(drawer_id)
                    by_drop_reason["noise_or_cache"] += 1
                    stats["drop_marked"] += 1
                    continue
                target_wing = wing or target_wing
                if room is not None:
                    target_room = room

            if target_wing != old_wing or target_room != old_room:
                new_meta = dict(meta or {})
                new_meta["wing"] = target_wing
                new_meta["room"] = target_room
                new_meta["reclassified_at"] = ts
                new_meta["reclassify_version"] = "2026-04-09.v1"

                to_upsert_ids.append(drawer_id)
                to_upsert_docs.append(doc)
                to_upsert_metas.append(new_meta)
                stats["reclassified"] += 1
                by_wing_move[f"{old_wing}->{target_wing}"] += 1

    # Guard against repeated IDs in mutation lists.
    upsert_by_id = {}
    for drawer_id, doc, meta in zip(to_upsert_ids, to_upsert_docs, to_upsert_metas):
        upsert_by_id[drawer_id] = (doc, meta)

    unique_upsert_ids = list(upsert_by_id.keys())
    unique_upsert_docs = [upsert_by_id[i][0] for i in unique_upsert_ids]
    unique_upsert_metas = [upsert_by_id[i][1] for i in unique_upsert_ids]
    unique_delete_ids = list(dict.fromkeys(to_delete_ids))

    if not dry_run:
        for ids_chunk, docs_chunk, metas_chunk in zip(
            _batched(unique_upsert_ids, 500),
            _batched(unique_upsert_docs, 500),
            _batched(unique_upsert_metas, 500),
        ):
            col.upsert(ids=ids_chunk, documents=docs_chunk, metadatas=metas_chunk)
        for ids_chunk in _batched(unique_delete_ids, 500):
            col.delete(ids=ids_chunk)

    stats["to_upsert"] = len(unique_upsert_ids)
    stats["to_delete"] = len(unique_delete_ids)
    stats["dry_run"] = int(dry_run)

    return {
        "palace_path": palace_path,
        "collection": collection_name,
        "project_root": str(Path(project_root).resolve()),
        "drop_noise": drop_noise,
        "separate_chat_emotional": separate_chat_emotional,
        "stats": dict(stats),
        "wing_moves": dict(by_wing_move),
        "drop_reasons": dict(by_drop_reason),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reclassify MemPalace drawers into logical wings.")
    parser.add_argument(
        "--palace",
        default="D:/mempalace/palace",
        help="ChromaDB palace path (default: D:/mempalace/palace)",
    )
    parser.add_argument(
        "--collection",
        default="mempalace_drawers",
        help="Collection name (default: mempalace_drawers)",
    )
    parser.add_argument(
        "--project-root",
        default="D:/trading advisor 3000",
        help="Project root used for path-based classification",
    )
    parser.add_argument(
        "--drop-noise",
        action="store_true",
        help="Delete drawers from temp/cache/noise paths (recommended)",
    )
    parser.add_argument(
        "--separate-chat-emotional",
        action="store_true",
        help="Move codex_chats/emotional into codex_chats_emotional wing",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    args = parser.parse_args()

    summary = reclassify(
        palace_path=args.palace,
        collection_name=args.collection,
        project_root=args.project_root,
        drop_noise=args.drop_noise,
        separate_chat_emotional=args.separate_chat_emotional,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
