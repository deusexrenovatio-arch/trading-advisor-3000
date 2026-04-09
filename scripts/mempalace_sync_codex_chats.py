#!/usr/bin/env python3
"""
Incremental sync of Codex chat sessions into MemPalace.

This script tracks file mtime/size and ingests only changed session files.
For changed files, it upserts deterministic drawer IDs (stable updates + append growth).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

import chromadb
from mempalace.general_extractor import extract_memories
from mempalace.normalize import normalize


DEFAULT_PALACE = "D:/mempalace/palace"
DEFAULT_SESSIONS = str(Path.home() / ".codex" / "sessions")
DEFAULT_STATE = "D:/mempalace/hook_state/codex_chat_sync_state.json"
DEFAULT_COLLECTION = "mempalace_drawers"
DEFAULT_WING = "codex_chats"

CONVO_EXTENSIONS = {".jsonl"}
MIN_CONTENT_CHARS = 30


def _iter_convo_files(root: Path) -> Iterable[Path]:
    for current_root, dirnames, filenames in os.walk(root, followlinks=False):
        safe_dirs = []
        for d in dirnames:
            p = Path(current_root) / d
            try:
                if p.is_symlink():
                    continue
            except OSError:
                continue
            safe_dirs.append(d)
        dirnames[:] = safe_dirs
        for name in filenames:
            p = Path(current_root) / name
            if p.suffix.lower() in CONVO_EXTENSIONS:
                yield p


def _load_state(path: Path) -> Dict:
    if not path.exists():
        return {"files": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"files": {}}


def _save_state(path: Path, state: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _drawer_id(wing: str, room: str, source_file: str, chunk_index: int) -> str:
    digest = hashlib.md5((source_file + str(chunk_index)).encode(), usedforsecurity=False).hexdigest()[
        :16
    ]
    return f"drawer_{wing}_{room}_{digest}"


def _upsert_chunks(collection, wing: str, source_file: str, chunks: List[Dict], stat: os.stat_result) -> int:
    if not chunks:
        return 0
    ids = []
    docs = []
    metas = []
    filed_at = datetime.now(timezone.utc).isoformat()
    for c in chunks:
        room = c.get("memory_type", "general")
        idx = int(c.get("chunk_index", 0))
        ids.append(_drawer_id(wing, room, source_file, idx))
        docs.append(c.get("content", ""))
        metas.append(
            {
                "wing": wing,
                "room": room,
                "source_file": source_file,
                "chunk_index": idx,
                "added_by": "codex_hook_sync",
                "filed_at": filed_at,
                "ingest_mode": "convos",
                "extract_mode": "general",
                "source_mtime": float(stat.st_mtime),
                "source_size": int(stat.st_size),
            }
        )
    for i in range(0, len(ids), 500):
        collection.upsert(ids=ids[i : i + 500], documents=docs[i : i + 500], metadatas=metas[i : i + 500])
    return len(ids)


def sync(
    palace_path: Path,
    sessions_dir: Path,
    state_file: Path,
    collection_name: str,
    wing: str,
) -> Dict:
    state = _load_state(state_file)
    files_state = state.setdefault("files", {})

    files = sorted(_iter_convo_files(sessions_dir))
    seen = set()

    # First-run bootstrap: trust current files as baseline and only track future changes.
    if not files_state:
        now = datetime.now(timezone.utc).isoformat()
        bootstrapped = 0
        for f in files:
            try:
                st = f.stat()
            except OSError:
                continue
            files_state[str(f)] = {
                "mtime": float(st.st_mtime),
                "size": int(st.st_size),
                "synced_at": now,
                "chunks": -1,
            }
            bootstrapped += 1
        _save_state(state_file, state)
        return {
            "palace_path": str(palace_path),
            "sessions_dir": str(sessions_dir),
            "state_file": str(state_file),
            "wing": wing,
            "bootstrap": True,
            "stats": {
                "files_total": len(files),
                "files_bootstrapped": bootstrapped,
                "files_changed": 0,
                "files_synced": 0,
                "files_skipped_empty": 0,
                "drawers_deleted": 0,
                "drawers_upserted": 0,
            },
        }

    changed_files: List[Path] = []
    for f in files:
        try:
            st = f.stat()
        except OSError:
            continue
        key = str(f)
        seen.add(key)
        prev = files_state.get(key)
        if not prev:
            changed_files.append(f)
            continue
        if float(prev.get("mtime", -1)) != float(st.st_mtime) or int(prev.get("size", -1)) != int(st.st_size):
            changed_files.append(f)

    # Remove state entries for files no longer present.
    for key in list(files_state.keys()):
        if key not in seen:
            del files_state[key]

    stats = {
        "files_total": len(files),
        "files_changed": len(changed_files),
        "files_synced": 0,
        "files_skipped_empty": 0,
        "drawers_deleted": 0,
        "drawers_upserted": 0,
    }

    if not changed_files:
        _save_state(state_file, state)
        return {
            "palace_path": str(palace_path),
            "sessions_dir": str(sessions_dir),
            "state_file": str(state_file),
            "wing": wing,
            "bootstrap": False,
            "stats": stats,
        }

    client = chromadb.PersistentClient(path=str(palace_path))
    collection = client.get_collection(collection_name)

    for f in changed_files:
        source_file = str(f)
        try:
            st = f.stat()
            content = normalize(source_file)
        except Exception:
            continue

        if not content or len(content.strip()) < MIN_CONTENT_CHARS:
            stats["files_skipped_empty"] += 1
            files_state[source_file] = {
                "mtime": float(st.st_mtime),
                "size": int(st.st_size),
                "synced_at": datetime.now(timezone.utc).isoformat(),
                "chunks": 0,
            }
            continue

        chunks = extract_memories(content)
        upserted = _upsert_chunks(collection, wing=wing, source_file=source_file, chunks=chunks, stat=st)

        stats["files_synced"] += 1
        stats["drawers_upserted"] += upserted
        files_state[source_file] = {
            "mtime": float(st.st_mtime),
            "size": int(st.st_size),
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "chunks": upserted,
        }
        # Persist progress incrementally, so interruptions do not restart from zero.
        _save_state(state_file, state)

    _save_state(state_file, state)

    return {
        "palace_path": str(palace_path),
        "sessions_dir": str(sessions_dir),
        "state_file": str(state_file),
        "wing": wing,
        "bootstrap": False,
        "stats": stats,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental Codex chat sync into MemPalace.")
    parser.add_argument("--palace", default=DEFAULT_PALACE, help=f"Palace path (default: {DEFAULT_PALACE})")
    parser.add_argument(
        "--sessions",
        default=DEFAULT_SESSIONS,
        help=f"Codex sessions root (default: {DEFAULT_SESSIONS})",
    )
    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE,
        help=f"State file path (default: {DEFAULT_STATE})",
    )
    parser.add_argument("--collection", default=DEFAULT_COLLECTION, help="Collection name")
    parser.add_argument("--wing", default=DEFAULT_WING, help="Target wing for synced chat chunks")
    args = parser.parse_args()

    summary = sync(
        palace_path=Path(args.palace),
        sessions_dir=Path(args.sessions),
        state_file=Path(args.state_file),
        collection_name=args.collection,
        wing=args.wing,
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
