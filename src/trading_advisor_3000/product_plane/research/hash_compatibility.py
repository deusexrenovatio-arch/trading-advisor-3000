"""Finite, reviewed proof migration for audited deployed sources.

Raw runtime provenance remains the strict resume/publication contract. This
adapter admits an accepted snapshot, never arbitrary caller permission.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

_REVIEWED_CURRENT_SOURCE_SHA256 = "048dfb4406d4fac2e504b0505dc7b8e5d74ad4b6a3f74728576a43f6c85939fa"
# Exact post-226 production79257fa3, reconstructed from its CRLF Git checkout.
NATIVE_LEGACY_SOURCE = "8a4432db9fc8e5da29cc3b6668b61972ded6bc270072252e000d4414a6827fbb"
LEGACY_NATIVE_L2_IMPLEMENTATION = (
    "3183b255524a646acbf791cad4a12b31bd5017d410a7c8ff444ef7c95a40fb15",
    "03e55fd245842af90f697ad9a30cdd5005e88038060bf56e44bf62685dcc3f9c",
    "78630475a3a79745d9116a6e00b888e1bdc258240a7f839fb280c495c29a0ee1",
    "b017e27e3ed51f21b8b0799bbfa06ebf252306c895707b07ed00c29205204796",
    "2f4efdbb0a62db05f963b92daad11bcc2d7a1e83ef847fb8537706280ecec376",
    "f003b68ccafe64d2bd063dd9ea94d9eebdf2b5462619b46d697f4c3301ba059f",
)
LEGACY_SOURCES = {
    # Exact deployed raw file hashes; both changes leave numerical formulas intact.
    "6c41c2dfc0dca91d67eeb63065ccb23e6368895e511aab4c816868bfc3c4acec": (
        "0d697e2ac44572fa2bb62bf9b153b515bc556a4bf822b4b4700bf991fd6521f9"
    ),
    "6c266cccbd56afd3682146aba0a98a9aa7be9385d1f24408724b0ffdce30b188": (
        "5a297d32aeadf2e80a989396dc42be83cbb27a6e67a3f39bf86c88eb63929e49"
    ),
    NATIVE_LEGACY_SOURCE: LEGACY_NATIVE_L2_IMPLEMENTATION[0],
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
