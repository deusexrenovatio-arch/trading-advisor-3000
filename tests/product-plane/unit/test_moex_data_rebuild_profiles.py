from __future__ import annotations

import json
from pathlib import Path

import pytest
from dagster import DagsterInstance

import trading_advisor_3000.dagster_defs.moex_historical_assets as moex_historical_assets
import trading_advisor_3000.dagster_defs.research_assets as research_assets
from trading_advisor_3000.dagster_defs import (
    MOEX_DATA_REBUILD_JOB_NAME,
    build_moex_data_rebuild_run_config,
    build_moex_historical_definitions,
)
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
        canonical_output_dir=tmp_path / "canonical-staging",
        canonical_target_output_dir=tmp_path / "canonical-current",
        canonical_run_id="run-existing-raw",
    )
    existing_raw_op_config = existing_raw_config["ops"]["moex_data_rebuild"]["config"]
    assert existing_raw_op_config["source_mode"] == "existing_raw_delta"
    assert existing_raw_op_config["raw_table_path"].endswith("raw_moex_history.delta")
    assert existing_raw_op_config["raw_ingest_report_path"].endswith("raw-ingest-report.json")
    assert existing_raw_op_config["canonical_target_output_dir"].endswith("canonical-current")

    full_raw_config = build_moex_data_rebuild_run_config(
        profile_name="full_raw_to_canonical",
        canonical_output_dir=tmp_path / "canonical-staging",
        canonical_run_id="run-full-raw",
        publish_mode="staging_only",
        raw_root=tmp_path / "raw",
        canonical_root=tmp_path / "canonical-root",
        session_root=tmp_path / "sessions",
        research_root=tmp_path / "research",
    )
    full_raw_op_config = full_raw_config["ops"]["moex_data_rebuild"]["config"]
    assert full_raw_op_config["source_mode"] == "full_raw_ingest"
    assert "raw_table_path" not in full_raw_op_config
    assert "raw_ingest_report_path" not in full_raw_op_config
    assert full_raw_op_config["raw_root"].endswith("raw")


def test_moex_canonical_rebuild_promote_requires_explicit_target_contract(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="requires canonical_target_output_dir or explicit target"):
        build_moex_data_rebuild_run_config(
            profile_name="canonical_from_existing_raw",
            raw_table_path=tmp_path / "raw_moex_history.delta",
            raw_ingest_report_path=tmp_path / "raw-ingest-report.json",
            canonical_output_dir=tmp_path / "canonical-staging",
            canonical_run_id="run-missing-target",
            publish_mode="promote",
        )


def test_moex_canonical_rebuild_publish_mode_controls_target_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[dict[str, object]] = []

    def _fake_materialize(**kwargs: object) -> dict[str, object]:
        captured.append(dict(kwargs))
        return {
            "success": True,
            "selected_assets": ["moex_raw_ingest", "moex_canonical_refresh"],
            "materialized_assets": ["moex_raw_ingest", "moex_canonical_refresh"],
            "output_paths": {
                "canonical_bars": Path(str(kwargs["canonical_bars_path"])).as_posix(),
                "canonical_bar_provenance": Path(
                    str(kwargs["canonical_provenance_path"])
                ).as_posix(),
                "canonical_session_intervals": Path(
                    str(kwargs["canonical_session_intervals_path"])
                ).as_posix(),
                "canonical_session_calendar": Path(
                    str(kwargs["canonical_session_calendar_path"])
                ).as_posix(),
                "canonical_roll_map": Path(str(kwargs["canonical_roll_map_path"])).as_posix(),
            },
            "rows_by_table": {"canonical_bars": 2},
        }

    monkeypatch.setattr(
        moex_historical_assets,
        "materialize_moex_historical_assets",
        _fake_materialize,
    )

    staging_root = tmp_path / "canonical-staging"
    target_root = tmp_path / "canonical-current"
    staging_report = moex_historical_assets.run_moex_data_rebuild_profile(
        {
            "profile_name": "canonical_from_existing_raw",
            "raw_table_path": (tmp_path / "raw_moex_history.delta").as_posix(),
            "raw_ingest_report_path": (tmp_path / "raw-ingest-report.json").as_posix(),
            "canonical_output_dir": staging_root.as_posix(),
            "canonical_run_id": "run-staging-only",
            "publish_mode": "staging_only",
        }
    )
    staging_call = captured[-1]
    assert (
        Path(str(staging_call["canonical_bars_path"]))
        == (staging_root / "delta" / "canonical_bars.delta").resolve()
    )
    assert staging_report["manifest"]["promoted_outputs"] == {}
    assert staging_report["manifest"]["staged_outputs"]["canonical_bars"].endswith(
        "canonical-staging/delta/canonical_bars.delta"
    )

    promote_report = moex_historical_assets.run_moex_data_rebuild_profile(
        {
            "profile_name": "canonical_from_existing_raw",
            "raw_table_path": (tmp_path / "raw_moex_history.delta").as_posix(),
            "raw_ingest_report_path": (tmp_path / "raw-ingest-report.json").as_posix(),
            "canonical_output_dir": staging_root.as_posix(),
            "canonical_target_output_dir": target_root.as_posix(),
            "canonical_run_id": "run-promote",
            "publish_mode": "promote",
        }
    )
    promote_call = captured[-1]
    assert (
        Path(str(promote_call["canonical_bars_path"]))
        == (target_root / "canonical_bars.delta").resolve()
    )
    assert (
        Path(str(promote_call["canonical_session_intervals_path"]))
        == (target_root / "canonical_session_intervals.delta").resolve()
    )
    assert promote_report["manifest"]["staged_outputs"]["canonical_bars"].endswith(
        "canonical-staging/delta/canonical_bars.delta"
    )
    assert promote_report["manifest"]["promoted_outputs"]["canonical_bars"].endswith(
        "canonical-current/canonical_bars.delta"
    )
    assert promote_report["manifest"]["promoted_outputs"]["canonical_session_intervals"].endswith(
        "canonical-current/canonical_session_intervals.delta"
    )


def test_moex_data_rebuild_job_dispatches_data_layer_profile_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def _fake_layer(layer_name: str, table_name: str):
        def _run(**kwargs: object) -> dict[str, object]:
            calls.append(layer_name)
            assert Path(str(kwargs["canonical_output_dir"])) == (tmp_path / "canonical").resolve()
            assert Path(str(kwargs["research_output_dir"])) == (tmp_path / "research").resolve()
            assert kwargs["dataset_version"] == "run-data-layer"
            return {
                "success": True,
                "materialized_assets": [table_name],
                "output_paths": {
                    table_name: (tmp_path / "research" / f"{table_name}.delta").as_posix()
                },
                "rows_by_table": {table_name: 1},
            }

        return _run

    monkeypatch.setattr(
        research_assets,
        "materialize_moex_cf_rebuild_assets",
        _fake_layer("continuous_front", "continuous_front_bars"),
        raising=False,
    )
    monkeypatch.setattr(
        research_assets,
        "materialize_moex_research_bar_rebuild_assets",
        _fake_layer("research_bar", "research_bar_views"),
        raising=False,
    )
    monkeypatch.setattr(
        research_assets,
        "materialize_moex_indicator_rebuild_assets",
        _fake_layer("indicator", "research_indicator_frames"),
        raising=False,
    )
    monkeypatch.setattr(
        research_assets,
        "materialize_moex_derived_indicator_rebuild_assets",
        _fake_layer("derived", "research_derived_indicator_frames"),
        raising=False,
    )

    definitions = build_moex_historical_definitions()
    job = definitions.get_repository_def().get_job(MOEX_DATA_REBUILD_JOB_NAME)
    result = job.execute_in_process(
        run_config=build_moex_data_rebuild_run_config(
            profile_name="data_layer_rebuild",
            canonical_output_dir=tmp_path / "canonical",
            canonical_run_id="run-data-layer",
            research_root=tmp_path / "research",
        ),
        instance=DagsterInstance.ephemeral(),
        raise_on_error=True,
    )

    assert result.success
    assert calls == ["continuous_front", "research_bar", "indicator", "derived"]
    report = result.output_for_node("moex_data_rebuild")
    assert report["profile_name"] == "data_layer_rebuild"
    assert Path(str(report["manifest_path"])).exists()


def test_moex_layer_materialization_filters_run_config_to_selected_assets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class _Result:
        success = False

    def _fake_materialize(**kwargs: object) -> _Result:
        captured.update(kwargs)
        return _Result()

    monkeypatch.setattr(research_assets, "assert_research_definitions_executable", lambda: None)
    monkeypatch.setattr(research_assets, "materialize", _fake_materialize)

    report = research_assets.materialize_moex_cf_rebuild_assets(
        canonical_output_dir=tmp_path / "canonical",
        research_output_dir=tmp_path / "research",
        dataset_version="run-data-layer",
        timeframes=("15m",),
        raise_on_error=False,
    )

    assert report["success"] is False
    assert captured["selection"] == list(research_assets.MOEX_CF_REBUILD_ASSETS)
    assert set(captured["run_config"]["ops"]) == {"continuous_front_bars"}
