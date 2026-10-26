"""Host disk safeguards charge only the isolated data root's file allocation."""

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import run_staging_control as control  # noqa: E402


def prepared(root, monkeypatch, free):
    monkeypatch.setattr(control.shutil, "disk_usage", lambda _: SimpleNamespace(free=free))
    return {
        "deadline": datetime.now(UTC) + timedelta(minutes=1),
        "paths": {"DATA_ROOT": root},
        "values": {
            control.PREFIX + "MIN_FREE_BYTES": "5000",
            control.PREFIX + "MAX_GROWTH_BYTES": "1024",
        },
        "free": 10000,
        "data_bytes": 0,
    }


def test_production_disk_growth_does_not_consume_own_cap(tmp_path, monkeypatch):
    # Shared-volume free space fell by 4000; this data root has no new file bytes.
    control.watch_limits(prepared(tmp_path, monkeypatch, free=6000))


def test_own_growth_blocks_even_when_external_cleanup_offsets_it(tmp_path, monkeypatch):
    (tmp_path / "candidate-output").write_bytes(b"x" * 2048)
    with pytest.raises(ValueError, match="growth"):
        control.watch_limits(prepared(tmp_path, monkeypatch, free=11000))


def test_global_minimum_free_space_still_blocks(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="reserve"):
        control.watch_limits(prepared(tmp_path, monkeypatch, free=4999))


def test_inventory_is_bounded_and_refuses_reparse_points(tmp_path, monkeypatch):
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "file").write_bytes(b"payload")
    assert control.data_root_file_bytes(tmp_path) == 7
    with pytest.raises(ValueError, match="count/time"):
        control.data_root_file_bytes(tmp_path, max_entries=1)
    with pytest.raises(ValueError, match="time"):
        control.data_root_file_bytes(tmp_path, max_seconds=0)
    monkeypatch.setattr(
        Path, "stat", lambda *a, **kw: SimpleNamespace(st_mode=0, st_file_attributes=0x400)
    )
    monkeypatch.setattr(control.os, "scandir", lambda *a: pytest.fail("Followed reparse point"))
    with pytest.raises(ValueError, match="reparse"):
        control.data_root_file_bytes(tmp_path)


def test_last_file_metadata_cannot_return_after_inventory_deadline(tmp_path, monkeypatch):
    output = tmp_path / "output"
    output.write_bytes(b"payload")
    clock, original_stat = [0.0], Path.stat

    def slow_final_stat(path, **kwargs):
        result = original_stat(path, **kwargs)
        if path == output:
            clock[0] = 11.0
        return result

    monkeypatch.setattr(control.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(Path, "stat", slow_final_stat)
    with pytest.raises(ValueError, match="time"):
        control.data_root_file_bytes(tmp_path, max_seconds=10)
