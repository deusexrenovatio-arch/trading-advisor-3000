from __future__ import annotations

import json
from pathlib import Path

import pytest

from trading_advisor_3000.dagster_defs import build_moex_data_rebuild_run_config
from trading_advisor_3000.product_plane.data_plane.moex.data_rebuild_profiles import (
    FORBIDDEN_REBUILD_STAGE_NAMES,
    MOEX_DATA_REBUILD_PROFILE_NAMES,
    build_moex_data_rebuild_manifest,
    dependent_stale_targets_for_stages,
    resolve_moex_data_layer_stages,
    resolve_moex_data_rebuild_profile,
    write_moex_data_rebuild_manifest,
)


def test_moex_data_rebuild_profile_registry_rejects_unknown_profile() -> None:
    with pytest.raises(ValueError, match="unknown MOEX data rebuild profile"):
        resolve_moex_data_rebuild_profile("strategy_rebuild")


def test_moex_data_rebuild_profiles_are_whitelisted_data_layer_only() -> None:
    assert set(MOEX_DATA_REBUILD_PROFILE_NAMES) == {
        "full_raw_to_canonical",
        "canonical_from_existing_raw",
        "cf_rebuild",
        "research_bar_rebuild",
        "indicator_rebuild",
        "derived_rebuild",
        "data_layer_rebuild",
        "invalidate_downstream_only",
    }

    full = resolve_moex_data_rebuild_profile("full_raw_to_canonical")
    assert full.stage_names == ("raw", "sessions", "canonical")
    assert full.source_mode == "full_raw_ingest"
    assert full.requires_raw_ingest is True

    canonical = resolve_moex_data_rebuild_profile("canonical_from_existing_raw")
    assert canonical.stage_names == ("sessions", "canonical")
    assert canonical.source_mode == "existing_raw_delta"
    assert canonical.requires_raw_ingest is False

    data_layer = resolve_moex_data_rebuild_profile("data_layer_rebuild")
    assert data_layer.stage_names == (
        "continuous_front",
        "research_bar",
        "indicator",
        "derived",
    )

    for profile_name in MOEX_DATA_REBUILD_PROFILE_NAMES:
        profile = resolve_moex_data_rebuild_profile(profile_name)
        assert not set(profile.stage_names).intersection(FORBIDDEN_REBUILD_STAGE_NAMES)


def test_moex_data_rebuild_stage_resolver_rejects_out_of_scope_layers() -> None:
    assert resolve_moex_data_layer_stages(["indicator", "derived"]) == (
        "indicator",
        "derived",
    )

    with pytest.raises(ValueError, match="outside the MOEX data-layer rebuild scope"):
        resolve_moex_data_layer_stages(["strategy"])

    with pytest.raises(ValueError, match="outside the MOEX data-layer rebuild scope"):
        resolve_moex_data_layer_stages(["backtest", "projection"])


def test_moex_data_rebuild_manifest_records_stage_first_publish_and_invalidation(
    tmp_path: Path,
) -> None:
    profile = resolve_moex_data_rebuild_profile("indicator_rebuild")
    staged_output = tmp_path / "staging" / "run-1" / "research_indicator_frames.delta"
    promoted_output = tmp_path / "current" / "research_indicator_frames.delta"

    manifest = build_moex_data_rebuild_manifest(
        profile=profile,
        run_id="run-1",
        publish_mode="promote",
        downstream_mode="invalidate",
        input_roots={"canonical": tmp_path / "canonical"},
        staged_outputs={"research_indicator_frames": staged_output},
        promoted_outputs={"research_indicator_frames": promoted_output},
        row_counts={"research_indicator_frames": 12},
    )

    assert manifest["profile_name"] == "indicator_rebuild"
    assert manifest["stage_names"] == ["indicator"]
    assert manifest["staged_outputs"]["research_indicator_frames"].endswith(
        "staging/run-1/research_indicator_frames.delta"
    )
    assert manifest["promoted_outputs"]["research_indicator_frames"].endswith(
        "current/research_indicator_frames.delta"
    )
    assert manifest["row_counts"] == {"research_indicator_frames": 12}
    assert manifest["invalidated_outputs"] == [
        "derived",
        "strategy",
        "backtest",
        "projection",
        "execution",
    ]

    manifest_path = write_moex_data_rebuild_manifest(tmp_path / "manifest.json", manifest)
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest


def test_moex_data_rebuild_invalidation_policy_is_upstream_ordered() -> None:
    assert dependent_stale_targets_for_stages(("raw", "canonical")) == (
        "continuous_front",
        "research_bar",
        "indicator",
        "derived",
        "strategy",
        "backtest",
        "projection",
        "execution",
    )
    assert dependent_stale_targets_for_stages(("continuous_front",)) == (
        "research_bar",
        "indicator",
        "derived",
        "strategy",
        "backtest",
        "projection",
        "execution",
    )
    assert dependent_stale_targets_for_stages(("research_bar",)) == (
        "indicator",
        "derived",
        "strategy",
        "backtest",
        "projection",
        "execution",
    )
    assert dependent_stale_targets_for_stages(("derived",)) == (
        "strategy",
        "backtest",
        "projection",
        "execution",
    )


def test_moex_data_rebuild_run_config_distinguishes_raw_source_modes(tmp_path: Path) -> None:
    existing_raw_config = build_moex_data_rebuild_run_config(
        profile_name="canonical_from_existing_raw",
        raw_table_path=tmp_path / "raw_moex_history.delta",
        raw_ingest_report_path=tmp_path / "raw-ingest-report.json",
        canonical_output_dir=tmp_path / "canonical",
        canonical_run_id="run-existing-raw",
    )
    existing_raw_op_config = existing_raw_config["ops"]["moex_raw_ingest"]["config"]
    assert existing_raw_op_config["source_mode"] == "existing_raw_delta"
    assert existing_raw_op_config["raw_table_path"].endswith("raw_moex_history.delta")
    assert existing_raw_op_config["raw_ingest_report_path"].endswith("raw-ingest-report.json")

    full_raw_config = build_moex_data_rebuild_run_config(
        profile_name="full_raw_to_canonical",
        canonical_output_dir=tmp_path / "canonical",
        canonical_run_id="run-full-raw",
        raw_root=tmp_path / "raw",
        canonical_root=tmp_path / "canonical-root",
        session_root=tmp_path / "sessions",
        research_root=tmp_path / "research",
    )
    full_raw_op_config = full_raw_config["ops"]["moex_raw_ingest"]["config"]
    assert full_raw_op_config["source_mode"] == "full_raw_ingest"
    assert "raw_table_path" not in full_raw_op_config
    assert "raw_ingest_report_path" not in full_raw_op_config
    assert full_raw_op_config["raw_root"].endswith("raw")
