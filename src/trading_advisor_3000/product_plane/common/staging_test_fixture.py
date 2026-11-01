"""Immutable SYNTHETIC inputs for a bounded native Research branch exercise.

Only immutable input metadata is verified here. Delta owns their storage and the snapshot
owner owns their pins/publication. This is neither Research output nor market QC.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from . import data_snapshots as snapshots

LOGICAL_ROOT = "/staging-control/fixture"
FIXTURE_VERSION = "synthetic-native-v1"
INSTRUMENTS = ("BR", "Si")
TIMEFRAMES = ("15m", "1h", "4h", "1d")
BARS_PER_GROUP = 512
MAX_ROWS, MAX_BYTES, MAX_FILES = 32_768, 16 * 1024 * 1024, 256
FINAL_DAY = datetime(2026, 3, 16, tzinfo=UTC)
RAW_SESSIONS = 8
CANONICAL_TABLES = (
    "canonical_bars",
    "canonical_bar_provenance",
    "canonical_session_intervals",
    "canonical_session_calendar",
    "canonical_closed_session_dates",
    "canonical_roll_map",
    "canonical_contract_economics",
)


def fixture_time(value):
    return value.isoformat().replace("+00:00", "Z")


def _plain(path):
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
        raise ValueError("fixture links/reparse points are forbidden")
    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise ValueError("fixture requires regular files/directories")
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
        raise ValueError("fixture hard links are forbidden")
    return info


def fixture_root(path):
    path = Path(os.path.abspath(path))
    for part in (path, *path.parents):
        if part.exists() or part.is_symlink():
            _plain(part)
    return path


def fixture_logical_root(root):
    value = PurePosixPath(root)
    if not value.is_absolute() or str(value) != root or ".." in value.parts or "\\" in root:
        raise ValueError("fixture logical root must be a normalized absolute POSIX path")
    if root == "/":
        raise ValueError("fixture logical root cannot be filesystem root")
    return value


def _relative(value):
    if not isinstance(value, str):
        raise ValueError("fixture path must be a string")
    path = PurePosixPath(value)
    if path.is_absolute() or str(path) != value or any(p in {"", ".", ".."} for p in path.parts):
        raise ValueError("fixture relative path is unsafe")
    if not path.parts or "\\" in value or ":" in value:
        raise ValueError("fixture relative path is unsafe")
    return path


def fixture_inventory(root):
    files, directories, total = {}, set(), 0
    for directory, names, filenames in os.walk(root, followlinks=False):
        for name in sorted([*names, *filenames]):
            path = Path(directory) / name
            info = _plain(path)
            if stat.S_ISDIR(info.st_mode):
                directories.add(path.relative_to(root).as_posix())
                if len(directories) > MAX_FILES:
                    raise ValueError("fixture directory cap exceeded")
                continue
            total += info.st_size
            if total > MAX_BYTES or len(files) >= MAX_FILES:
                raise ValueError("fixture file/byte cap exceeded")
            relative = path.relative_to(root).as_posix()
            _relative(relative)
            with path.open("rb") as stream:
                sha = hashlib.file_digest(stream, "sha256").hexdigest()
            files[relative] = {"sha256": sha, "bytes": info.st_size}
    parents = {
        str(parent)
        for file in files
        for parent in PurePosixPath(file).parents
        if str(parent) != "."
    }
    if directories != parents:
        raise ValueError("fixture contains extra empty directories")
    return files


def raw_coverage():
    return {
        "start_utc": fixture_time(FINAL_DAY - timedelta(days=7) + timedelta(hours=6)),
        "end_utc": fixture_time(FINAL_DAY + timedelta(hours=21)),
        "session_open_utc": "06:00",
        "session_close_utc": "21:00",
        "session_count": RAW_SESSIONS,
        "full_history": False,
    }


def fixture_descriptor(manifest, sha):
    fields = (
        "instrument_ids",
        "timeframes",
        "warmup_bars",
        "start_utc",
        "end_utc",
        "data_manifest_path",
        "canonical_dir",
        "raw_dir",
        "dataset_version",
        "data_snapshot_id",
        "data_manifest_sha256",
        "raw_1m_coverage",
    )
    return {"manifest_sha256": sha, **{key: deepcopy(manifest[key]) for key in fields}}


def verify_fixture(path, expected_sha256):
    """Verify bytes first, then native IDs/versions/schema/counts; never repair."""
    if not isinstance(expected_sha256, str) or not re.fullmatch("[0-9a-f]{64}", expected_sha256):
        raise ValueError("fixture requires an exact SHA256")
    root = fixture_root(path)
    files = fixture_inventory(root)
    if files.get("manifest.json", {}).get("sha256") != expected_sha256:
        raise ValueError("fixture manifest hash mismatch")
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    logical = fixture_logical_root(manifest["logical_root"])
    expected = {
        "schema_version": "staging_test_fixture.v1",
        "fixture_version": FIXTURE_VERSION,
        "synthetic": True,
        "instrument_ids": list(INSTRUMENTS),
        "timeframes": list(TIMEFRAMES),
        "warmup_bars": BARS_PER_GROUP,
        "dataset_version": FIXTURE_VERSION,
        "start_utc": fixture_time(
            FINAL_DAY - timedelta(days=BARS_PER_GROUP - 1) + timedelta(hours=6)
        ),
        "end_utc": fixture_time(FINAL_DAY + timedelta(hours=21)),
        "raw_1m_coverage": raw_coverage(),
        "limits": {"rows": MAX_ROWS, "bytes": MAX_BYTES, "files": MAX_FILES},
    }
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ValueError("fixture fixed contract/caps changed")
    if {key: value for key, value in files.items() if key != "manifest.json"} != manifest["files"]:
        raise ValueError("fixture files changed or contain extras")

    def local(value):
        relative = PurePosixPath(value).relative_to(logical).as_posix()
        return root / str(_relative(relative))

    if manifest["canonical_dir"] != str(logical / "canonical") or manifest["raw_dir"] != str(
        logical / "raw"
    ):
        raise ValueError("fixture directories changed")
    data_path = local(manifest["data_manifest_path"])
    if (
        data_path
        != root / "canonical/.data_snapshots/manifests" / f"{manifest['data_snapshot_id']}.json"
    ):
        raise ValueError("fixture Data manifest path changed")
    native = json.loads(data_path.read_text(encoding="utf-8"))
    native_payload = {key: value for key, value in native.items() if key != "manifest_sha256"}
    if (
        snapshots._hash(native_payload) != manifest["data_manifest_sha256"]
        or native.get("manifest_sha256") != manifest["data_manifest_sha256"]
        or native.get("snapshot_id") != manifest["data_snapshot_id"]
        or native.get("tables") != manifest["tables"]
        or native.get("parameters", {}).get("synthetic") is not True
    ):
        raise ValueError("fixture Data manifest binding changed")
    if set(native["tables"]) != {*CANONICAL_TABLES, "raw_moex_history"}:
        raise ValueError("fixture input table set changed")
    translated = deepcopy(native)
    total = 0
    for name, reference in translated["tables"].items():
        expected_path = (
            logical / ("raw" if name == "raw_moex_history" else "canonical") / f"{name}.delta"
        )
        if reference["table_path"] != str(expected_path):
            raise ValueError("fixture table path changed")
        reference["table_path"] = local(reference["table_path"]).resolve().as_posix()
        snapshots.open_pinned_table(reference)
        if snapshots.capture_table(Path(reference["table_path"])) != reference:
            raise ValueError("fixture Delta head/pin changed")
        total += reference["row_count"]
    translated["manifest_sha256"] = snapshots._hash(
        {key: value for key, value in translated.items() if key != "manifest_sha256"}
    )
    snapshots._validate(translated)
    if total > MAX_ROWS:
        raise ValueError("fixture row cap exceeded")
    for name, count in (
        ("canonical_bars", 2 * 4 * BARS_PER_GROUP),
        ("canonical_bar_provenance", 2 * 4 * BARS_PER_GROUP),
        ("raw_moex_history", 2 * RAW_SESSIONS * 900),
    ):
        if native["tables"][name]["row_count"] != count:
            raise ValueError("fixture fixed input row count changed")
    pointer = json.loads(
        (data_path.parent.parent / "published_current.json").read_text(encoding="utf-8")
    )
    if pointer != {
        "schema_version": "data_snapshot_publication.v1",
        "snapshot_id": native["snapshot_id"],
        "manifest_sha256": native["manifest_sha256"],
    }:
        raise ValueError("fixture Data publication changed")
    return fixture_descriptor(manifest, expected_sha256)
