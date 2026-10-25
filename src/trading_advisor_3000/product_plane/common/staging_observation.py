"""Bounded native SQLite backups for a protected observer with a read-only HOME.

Each database backup is consistent, including committed WAL content. The caller
must separately bind successful runs and accepted publication before/after the
consumer read; this is not a cross-database transaction or route acceptance.
"""

from __future__ import annotations

import hashlib
import math
import sqlite3
import tempfile
import time
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

import yaml


class ReadonlyObservationError(ValueError):
    """A protected metadata snapshot could not be read without source writes."""


def _stamp(path: Path) -> dict | None:
    if not path.exists():
        return None
    value = path.stat()
    return {"size": value.st_size, "mtime_ns": value.st_mtime_ns, "inode": value.st_ino}


def _databases(root: Path, deadline=float("inf")) -> list[Path]:
    result = []
    for path in root.rglob("*"):
        if time.monotonic() >= deadline:
            raise ReadonlyObservationError("Metadata discovery deadline exceeded")
        if path.suffix not in {".db", ".sqlite", ".sqlite3"}:
            continue
        if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ReadonlyObservationError("Unsafe source metadata database path")
        if any(parent.is_symlink() for parent in path.parents if parent != root.parent):
            raise ReadonlyObservationError("Source HOME metadata cannot traverse symlinks")
        result.append(path)
    if not result or root / "history/runs.db" not in result:
        raise ReadonlyObservationError("Native source HOME run database is missing")
    return sorted(result)


def _families(root: Path, deadline) -> dict[str, Path]:
    files = {}
    for database in _databases(root, deadline):
        for path in (database, Path(str(database) + "-wal")):
            if not path.exists():
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                raise ReadonlyObservationError("Unsafe source database family path")
            files[path.relative_to(root).as_posix()] = path
        if Path(str(database) + "-journal").exists():
            raise ReadonlyObservationError("A live rollback journal requires explicit recovery")
    return dict(sorted(files.items()))


def _fingerprint(root, deadline, max_bytes):
    values, consumed = {}, 0
    for name, path in _families(root, deadline).items():
        before = _stamp(path)
        if before["size"] + consumed > max_bytes:
            raise ReadonlyObservationError("Metadata capture exceeded its byte bounds")
        checksum = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(512 * 1024):
                if time.monotonic() >= deadline:
                    raise ReadonlyObservationError("Metadata capture deadline exceeded")
                checksum.update(chunk)
        if before != _stamp(path):
            raise ReadonlyObservationError("Source database family changed while reading")
        consumed += before["size"]
        values[name] = {**before, "sha256": checksum.hexdigest()}
    return values


def _usage(root):
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _source_config(root: Path) -> str | None:
    path = root / "dagster.yaml"
    if not path.exists():
        return None
    if path.is_symlink() or path.stat().st_size > 1024 * 1024:
        raise ReadonlyObservationError("Unsafe source HOME configuration")
    content = path.read_bytes()
    try:
        value = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise ReadonlyObservationError("Invalid source HOME configuration") from exc
    if not isinstance(value, dict) or any(
        key in value for key in ("storage", "run_storage", "event_log_storage", "schedule_storage")
    ):
        raise ReadonlyObservationError(
            "Non-default source storage needs a protected explicit mapping"
        )
    # Do not execute source-defined run coordinator/launcher plugins in the
    # observer. Its fresh default instance reads the copied native SQLite layout.
    return hashlib.sha256(content).hexdigest()


def _run_identities(connection, identifiers):
    if not identifiers:
        return {}
    query = (
        "SELECT run_id,status,run_body FROM runs WHERE run_id IN ("
        + ",".join("?" for _ in identifiers)
        + ")"
    )
    values = {
        row[0]: {"status": row[1], "body_sha256": hashlib.sha256(row[2].encode()).hexdigest()}
        for row in connection.execute(query, identifiers).fetchall()
    }
    if set(values) != set(identifiers):
        raise ReadonlyObservationError("Requested source run identity is unavailable")
    return values


@contextmanager
def readonly_home_snapshot(
    source_home: Path,
    *,
    temporary_root: Path = Path("/tmp"),
    max_bytes: int = 64 * 1024 * 1024,
    max_seconds: float = 30,
    run_ids=(),
):
    """Stable RO byte capture, then native mode=ro backup inside observer tmpfs.

    This explicit two-phase method handles WAL databases whose missing auxiliary
    files prevent direct SQLite opening on a read-only mount. All main/WAL bytes
    must remain identical until context exit. Derived SHM is regenerated only in
    observer-owned temporary storage. Never use immutable=1 or source RW access.
    """
    source, temporary = source_home.resolve(), temporary_root.resolve()
    if (
        not source.is_dir()
        or not temporary.is_dir()
        or type(max_bytes) is not int
        or not 0 < max_bytes <= 256 * 1024 * 1024
        or not math.isfinite(max_seconds)
        or max_seconds <= 0
        or source.is_relative_to(temporary)
        or temporary.is_relative_to(source)
    ):
        raise ReadonlyObservationError(
            "Observer needs independent HOME/temp roots and finite bounds"
        )
    started = time.monotonic()
    deadline = started + max_seconds
    config_digest = _source_config(source)
    run_ids = tuple(dict.fromkeys(run_ids))
    if len(run_ids) > 32 or any(not isinstance(value, str) or not value for value in run_ids):
        raise ReadonlyObservationError("Readonly run identity request exceeds its bounds")
    before = _fingerprint(source, deadline, max_bytes)
    evidence = {
        "schema_version": "staging_home_observation.v1",
        "source_home": str(source),
        "started_at_utc": datetime.now(UTC).isoformat(),
        "capture_method": "stable-readonly-family-then-native-ro-backup",
        "source_open_mode": "rb",
        "sqlite_backup_open_mode": "ro",
        "ignored_derived_files": ["*-shm"],
        "immutable": False,
        "cross_database_atomic": False,
        "source_configuration_sha256": config_digest,
        "source_family_before": before,
        "observation_complete": False,
        "databases": [],
    }
    with tempfile.TemporaryDirectory(prefix="ta3000-observer-home-", dir=temporary) as name:
        work = Path(name)
        capture, target = work / "capture", work / "home"
        target.mkdir()
        try:
            copied = {}
            for relative, expected in before.items():
                destination = capture / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                checksum = hashlib.sha256()
                with (source / relative).open("rb") as incoming, destination.open("xb") as outgoing:
                    while chunk := incoming.read(512 * 1024):
                        if time.monotonic() >= deadline or _usage(work) + len(chunk) > max_bytes:
                            raise ReadonlyObservationError("Metadata capture exceeded its bounds")
                        outgoing.write(chunk)
                        checksum.update(chunk)
                copied[relative] = {
                    "size": destination.stat().st_size,
                    "sha256": checksum.hexdigest(),
                }
                if any(copied[relative][key] != expected[key] for key in ("size", "sha256")):
                    raise ReadonlyObservationError("Source database changed during capture")
            after_capture = _fingerprint(source, deadline, max_bytes)
            if before != after_capture or _source_config(source) != config_digest:
                raise ReadonlyObservationError("Source database family changed during capture")
            evidence.update(captured_family=copied, source_family_after_capture=after_capture)
            for path in _databases(capture, deadline):
                relative = path.relative_to(capture)
                destination = target / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                with closing(
                    sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)
                ) as incoming:
                    incoming.execute("PRAGMA query_only=ON")
                    page_size = incoming.execute("PRAGMA page_size").fetchone()[0]
                    if (
                        _usage(work)
                        + incoming.execute("PRAGMA page_count").fetchone()[0] * page_size
                        > max_bytes
                    ):
                        raise ReadonlyObservationError(
                            "Readonly metadata backup exceeded its bounds"
                        )
                    version_before = incoming.execute("PRAGMA data_version").fetchone()[0]
                    identities_before = (
                        _run_identities(incoming, run_ids)
                        if relative.as_posix() == "history/runs.db"
                        else {}
                    )
                    with closing(sqlite3.connect(destination)) as outgoing:

                        def progress(status, remaining, total):
                            if time.monotonic() >= deadline or _usage(work) > max_bytes:
                                raise ReadonlyObservationError(
                                    "Readonly metadata backup exceeded its bounds"
                                )

                        incoming.backup(outgoing, pages=128, progress=progress, sleep=0.01)
                        outgoing.set_progress_handler(
                            lambda: int(time.monotonic() >= deadline), 1000
                        )
                        if outgoing.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                            raise ReadonlyObservationError(
                                "Copied metadata database failed integrity check"
                            )
                        version_after = incoming.execute("PRAGMA data_version").fetchone()[0]
                    identities_after = (
                        _run_identities(incoming, run_ids)
                        if relative.as_posix() == "history/runs.db"
                        else {}
                    )
                    if identities_before != identities_after:
                        raise ReadonlyObservationError(
                            "Requested source run identities changed during backup"
                        )
                size = destination.stat().st_size
                if _usage(work) > max_bytes:
                    raise ReadonlyObservationError("Observer metadata copy exceeded its byte bound")
                evidence["databases"].append(
                    {
                        "relative_path": relative.as_posix(),
                        "bytes": size,
                        "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                        "data_version_before": version_before,
                        "data_version_after": version_after,
                        "run_identities_before": identities_before,
                        "run_identities_after": identities_after,
                    }
                )
            evidence.update(
                observer_home=str(target),
                total_bytes=_usage(work),
                backup_completed_at_utc=datetime.now(UTC).isoformat(),
                elapsed_seconds=time.monotonic() - started,
            )
            yield target, evidence
            final = _fingerprint(source, deadline, max_bytes)
            if before != final or _source_config(source) != config_digest:
                raise ReadonlyObservationError("Source metadata changed during observation")
            if _usage(work) > max_bytes:
                raise ReadonlyObservationError("Observer metadata exceeded its total byte bounds")
            evidence.update(
                source_family_after_observation=final,
                observation_complete=True,
                completed_at_utc=datetime.now(UTC).isoformat(),
            )
        except (sqlite3.Error, OSError) as exc:
            raise ReadonlyObservationError("Readonly source metadata backup failed") from exc
