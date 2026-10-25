"""Readonly WAL backup preserves metadata without source write or unsafe fallback."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from trading_advisor_3000.product_plane.common.staging_observation import (
    ReadonlyObservationError,
    readonly_home_snapshot,
)


def fixture_home(tmp_path):
    home, temporary = tmp_path / "home", tmp_path / "observer"
    (home / "history").mkdir(parents=True)
    temporary.mkdir()
    connection = sqlite3.connect(home / "history/runs.db")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute("CREATE TABLE probe (value TEXT)")
    connection.execute("INSERT INTO probe VALUES ('committed-in-live-WAL')")
    connection.commit()
    return home, temporary, connection


def test_live_wal_is_in_backup_and_source_is_opened_readonly(tmp_path, monkeypatch):
    home, temporary, writer = fixture_home(tmp_path)
    original, incoming = sqlite3.connect, []

    def record(database, *args, **kwargs):
        if isinstance(database, str) and database.startswith("file:"):
            incoming.append((database, kwargs))
        return original(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", record)
    try:
        with readonly_home_snapshot(home, temporary_root=temporary) as (copy, proof):
            with closing(original(copy / "history/runs.db")) as reader:
                assert reader.execute("SELECT value FROM probe").fetchall() == [
                    ("committed-in-live-WAL",)
                ]
            assert proof["source_open_mode"] == "rb" and proof["sqlite_backup_open_mode"] == "ro"
            assert proof["immutable"] is False
            assert incoming and all(
                "mode=ro" in uri and options["uri"] for uri, options in incoming
            )
            assert writer.execute("SELECT count(*) FROM probe").fetchone()[0] == 1
        assert not list(temporary.iterdir())
        assert proof["observation_complete"] is True
        for name, copied in proof["captured_family"].items():
            assert copied["sha256"] == proof["source_family_before"][name]["sha256"]
            assert copied["sha256"] == proof["source_family_after_observation"][name]["sha256"]
    finally:
        writer.close()


def test_bound_failure_never_falls_back_to_source_write(tmp_path):
    home, temporary, writer = fixture_home(tmp_path)
    try:
        with pytest.raises(ReadonlyObservationError, match="bounds"):
            with readonly_home_snapshot(home, temporary_root=temporary, max_bytes=1):
                pytest.fail("oversized backup was accepted")
        assert writer.execute("SELECT count(*) FROM probe").fetchone()[0] == 1
        assert not list(temporary.iterdir())
    finally:
        writer.close()


def test_source_storage_override_is_rejected_without_loading_plugin(tmp_path):
    home, temporary, writer = fixture_home(tmp_path)
    (home / "dagster.yaml").write_text(
        "storage:\n  sqlite:\n    base_dir: /outside\n", encoding="utf-8"
    )
    try:
        with pytest.raises(ReadonlyObservationError, match="explicit mapping"):
            with readonly_home_snapshot(home, temporary_root=temporary):
                pytest.fail("unmapped storage was accepted")
    finally:
        writer.close()


def test_source_change_during_observation_is_never_accepted(tmp_path):
    home, temporary, writer = fixture_home(tmp_path)
    try:
        with pytest.raises(ReadonlyObservationError, match="changed during observation"):
            with readonly_home_snapshot(home, temporary_root=temporary):
                writer.execute("INSERT INTO probe VALUES ('concurrent source change')")
                writer.commit()
        assert not list(temporary.iterdir())
    finally:
        writer.close()


def test_infinite_deadline_is_rejected(tmp_path):
    home, temporary, writer = fixture_home(tmp_path)
    try:
        with pytest.raises(ReadonlyObservationError, match="finite bounds"):
            with readonly_home_snapshot(home, temporary_root=temporary, max_seconds=float("inf")):
                pytest.fail("unbounded snapshot was accepted")
    finally:
        writer.close()
