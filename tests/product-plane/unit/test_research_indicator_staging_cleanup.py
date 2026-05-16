from __future__ import annotations

from pathlib import Path

from scripts.cleanup_research_indicator_staging import (
    build_cleanup_inventory,
    delete_cleanup_inventory,
)


def test_research_indicator_staging_cleanup_is_allowlisted_and_protects_current(
    tmp_path: Path,
) -> None:
    staging_root = tmp_path / "research" / "gold" / "staging" / "phase4"
    current_root = tmp_path / "research" / "gold" / "current" / "phase4"
    allowed_stale_delta = staging_root / "research_indicator_frames.delta"
    allowed_status = staging_root / "indicator_refresh.status.json"
    protected_current_delta = current_root / "research_indicator_frames.delta"
    protected_raw = staging_root / "raw_baseline.delta"

    for path in (allowed_stale_delta, protected_current_delta, protected_raw):
        (path / "_delta_log").mkdir(parents=True)
    allowed_status.parent.mkdir(parents=True, exist_ok=True)
    allowed_status.write_text("stale", encoding="utf-8")

    inventory = build_cleanup_inventory(staging_root)
    relative_paths = {item["relative_path"] for item in inventory}

    assert "research_indicator_frames.delta" in relative_paths
    assert "indicator_refresh.status.json" in relative_paths
    assert "raw_baseline.delta" not in relative_paths
    assert not any("current" in item["path"] for item in inventory)

    deleted = delete_cleanup_inventory(inventory)

    assert deleted == len(inventory)
    assert not allowed_stale_delta.exists()
    assert not allowed_status.exists()
    assert protected_current_delta.exists()
    assert protected_raw.exists()
