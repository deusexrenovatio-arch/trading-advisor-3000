"""Finite, reviewed proof migration for the two deployed pre/post-223 sources.

Raw runtime provenance remains the strict resume/publication contract. This
adapter admits an accepted snapshot, never arbitrary caller permission.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

_REVIEWED_CURRENT_SOURCE_SHA256 = "eefc0f7d0ec96fa404eadb699729bd56f341a6609861df0590e84205f57e02fb"
LEGACY_SOURCES = {
    # Exact deployed raw file hashes; both changes leave numerical formulas intact.
    "6c41c2dfc0dca91d67eeb63065ccb23e6368895e511aab4c816868bfc3c4acec": (
        "0d697e2ac44572fa2bb62bf9b153b515bc556a4bf822b4b4700bf991fd6521f9"
    ),
    "6c266cccbd56afd3682146aba0a98a9aa7be9385d1f24408724b0ffdce30b188": (
        "5a297d32aeadf2e80a989396dc42be83cbb27a6e67a3f39bf86c88eb63929e49"
    ),
}
CANONICAL_L2_IMPLEMENTATION_SHA256 = next(iter(LEGACY_SOURCES.values()))
SCOPE_PARAMETER_KEYS = frozenset(
    {
        "changed_windows",
        "refresh_scope_version",
        "force_full_refresh",
        "data_snapshot_manifest_path",
        "hash_compatibility",
    }
)
REQUIRED_QUALITY = frozenset(
    {
        "continuous_front_bars",
        "research_datasets",
        "research_indicator_frames",
        "research_derived_indicator_frames",
        "continuous_front_indicator_acceptance_report",
        "continuous_front_acceptance",
    }
)
_source: ContextVar[str | None] = ContextVar("reviewed_hash_migration", default=None)


def reviewed_source_digest(package: Path | None = None) -> str:
    package = package or Path(__file__).resolve().parents[2]
    anchor = b'_REVIEWED_CURRENT_SOURCE_SHA256 = "' + b"[0-9a-f]{64}" + b'"'
    digest = hashlib.sha256()
    found = 0
    for path in sorted(package.rglob("*.py")):
        content = path.read_bytes().replace(b"\r\n", b"\n")
        if path.relative_to(package).as_posix() == "product_plane/research/hash_compatibility.py":
            pattern = b"(?m)^" + anchor + b"$"
            found = len(re.findall(pattern, content))
            if found != 1:
                raise ValueError("ambiguous reviewed source anchor")
            content = re.sub(pattern, b'_REVIEWED_CURRENT_SOURCE_SHA256 = "REVIEWED"', content)
        digest.update(path.relative_to(package).as_posix().encode())
        digest.update(b"\0")
        digest.update(content)
    if found != 1:
        raise ValueError("missing reviewed source anchor")
    return digest.hexdigest()


def reviewed_current_source() -> bool:
    try:
        return reviewed_source_digest() == _REVIEWED_CURRENT_SOURCE_SHA256
    except (OSError, ValueError):
        return False


def compatible_provenance(old: dict, current: dict) -> bool:
    return (
        old.get("source_sha256") in LEGACY_SOURCES
        and {k: v for k, v in old.items() if k != "source_sha256"}
        == {k: v for k, v in current.items() if k != "source_sha256"}
        and reviewed_current_source()
    )


def migration_source() -> str | None:
    source = _source.get()
    if source and not reviewed_current_source():
        raise ValueError("reviewed migration source changed")
    return source


@contextmanager
def _verified_migration_scope(source: str):
    # Only the snapshot owner calls this after validating accepted state/QC.
    if source not in LEGACY_SOURCES or not reviewed_current_source():
        raise ValueError("unreviewed hash migration")
    token = _source.set(source)
    try:
        yield
        if not reviewed_current_source():
            raise ValueError("reviewed migration source changed")
    finally:
        _source.reset(token)
