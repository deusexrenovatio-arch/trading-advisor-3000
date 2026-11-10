"""Shared immutable artifact pins and the existing OS-owned command lock."""

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def child(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or Path(relative).is_absolute():
        raise ValueError("observation artifact escapes its supplied root")
    return path


def pin(root: Path, path: Path) -> dict[str, str]:
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def pinned_bytes(root: Path, reference: dict) -> bytes:
    raw = child(root, reference["path"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
        raise ValueError("observation artifact hash mismatch: " + reference["path"])
    return raw


@contextmanager
def command_lock(path: Path):
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
