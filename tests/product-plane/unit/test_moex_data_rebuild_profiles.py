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
from trading_advisor_3000.product_plane.research.derived_indicators import (
    materialize as derived_materialize,
)
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    DerivedIndicatorFramePartitionKey,
)
from trading_advisor_3000.product_plane.research.indicators import (
    materialize as indicator_materialize,
)
from trading_advisor_3000.product_plane.research.indicators.store import (
    IndicatorFramePartitionKey,
)


def test_moex_data_rebuild_profile_registry_rejects_unknown_profile() -> None:
    with pytest.raises(ValueError, match="unknown MOEX data rebuild profile"):
        resolve_moex_data_rebuild_profile("strategy_rebuild")


def test_moex_data_rebuild_profiles_are_whitelisted_data_layer_only() -> None:
    assert set(MOEX_DATA_REBUILD_PROFILE_NAMES) == {
        "full_raw_to_canonical",
        "money_math_bootstrap",
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

    money_math = resolve_moex_data_rebuild_profile("money_math_bootstrap")
    assert money_math.stage_names == (
        "economics_raw",
        "economics_canonical",
        "continuous_front",
        "research_bar",
    )
    assert money_math.source_mode == "existing_raw_delta"
    assert money_math.requires_raw_ingest is False

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
        "indicator_sidecar",
    )

    for profile_name in MOEX_DATA_REBUILD_PROFILE_NAMES:
        profile = resolve_moex_data_rebuild_profile(profile_name)
        assert not set(profile.stage_names).intersection(FORBIDDEN_REBUILD_STAGE_NAMES)


def test_moex_data_rebuild_stage_resolver_rejects_out_of_scope_layers() -> None:
    assert resolve_moex_data_layer_stages(["indicator", "derived", "economics_raw"]) == (
        "economics_raw",
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
        "indicator_sidecar",
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
        "indicator_sidecar",
        "strategy",
        "backtest",
        "projection",
        "execution",
    )
    assert dependent_stale_targets_for_stages(("continuous_front",)) == (
        "research_bar",
        "indicator",
        "derived",
        "indicator_sidecar",
        "strategy",
        "backtest",
        "projection",
        "execution",
    )
    assert dependent_stale_targets_for_stages(("research_bar",)) == (
        "indicator",
        "derived",
        "indicator_sidecar",
        "strategy",
        "backtest",
        "projection",
        "execution",
    )
    assert dependent_stale_targets_for_stages(("derived",)) == (
        "indicator_sidecar",
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


def test_moex_data_rebuild_run_config_exposes_research_scope_and_spark_master(
    tmp_path: Path,
) -> None:
    run_config = build_moex_data_rebuild_run_config(
        profile_name="data_layer_rebuild",
        canonical_output_dir=tmp_path / "canonical",
        canonical_run_id="run-scoped",
        research_root=tmp_path / "research",
        dataset_version="research-scope-v2",
        timeframes=("15m", "1h"),
        dataset_contract_ids=("BRM6",),
        dataset_instrument_ids=("FUT_BR", "FUT_RTS"),
        spark_master="local[4]",
    )

    op_config = run_config["ops"][moex_historical_assets.MOEX_DATA_REBUILD_OP_NAME]["config"]
    assert op_config["dataset_version"] == "research-scope-v2"
    assert op_config["timeframes"] == "15m,1h"
    assert op_config["dataset_contract_ids"] == "BRM6"
    assert op_config["dataset_instrument_ids"] == "FUT_BR,FUT_RTS"
    assert op_config["spark_master"] == "local[4]"


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
            if layer_name in {"continuous_front", "research_bar", "indicator_sidecar"}:
                assert kwargs["start_ts"] == "2021-04-01T00:00:00Z"
                assert kwargs["end_ts"] == ""
                assert kwargs["warmup_bars"] == 300
                assert kwargs["split_method"] == "holdout"
                assert kwargs["dataset_instrument_ids"] == ("FUT_BR", "FUT_RTS")
                assert kwargs["spark_master"] == "local[4]"
            else:
                assert "start_ts" not in kwargs
                assert "end_ts" not in kwargs
                assert "warmup_bars" not in kwargs
                assert "split_method" not in kwargs
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
    monkeypatch.setattr(
        research_assets,
        "materialize_moex_indicator_sidecar_assets",
        _fake_layer("indicator_sidecar", "continuous_front_indicator_acceptance_report"),
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
            start_ts="2021-04-01T00:00:00Z",
            end_ts="",
            warmup_bars=300,
            split_method="holdout",
            dataset_instrument_ids=("FUT_BR", "FUT_RTS"),
            spark_master="local[4]",
        ),
        instance=DagsterInstance.ephemeral(),
        raise_on_error=True,
    )

    assert result.success
    assert calls == [
        "continuous_front",
        "research_bar",
        "indicator",
        "derived",
        "indicator_sidecar",
    ]
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
        dataset_instrument_ids=("FUT_BR",),
        spark_master="local[4]",
        raise_on_error=False,
    )

    assert report["success"] is False
    assert captured["selection"] == list(research_assets.MOEX_CF_REBUILD_ASSETS)
    assert set(captured["run_config"]["ops"]) == {"continuous_front_bars"}
    cf_config = captured["run_config"]["ops"]["continuous_front_bars"]["config"]
    assert cf_config["dataset_instrument_ids"] == ["FUT_BR"]
    assert cf_config["spark_master"] == "local[4]"

    captured.clear()
    report = research_assets.materialize_moex_research_bar_rebuild_assets(
        canonical_output_dir=tmp_path / "canonical",
        research_output_dir=tmp_path / "research",
        dataset_version="run-data-layer",
        timeframes=("15m",),
        dataset_instrument_ids=("FUT_BR",),
        spark_master="local[4]",
        raise_on_error=False,
    )

    assert report["success"] is False
    assert captured["selection"] == list(research_assets.MOEX_RESEARCH_BAR_REBUILD_ASSETS)
    assert set(captured["run_config"]["ops"]) == {"research_datasets"}
    research_bar_config = captured["run_config"]["ops"]["research_datasets"]["config"]
    assert research_bar_config["dataset_instrument_ids"] == ["FUT_BR"]
    assert research_bar_config["spark_master"] == "local[4]"

    captured.clear()
    report = research_assets.materialize_moex_indicator_sidecar_assets(
        canonical_output_dir=tmp_path / "canonical",
        research_output_dir=tmp_path / "research",
        dataset_version="run-data-layer",
        timeframes=("15m",),
        dataset_instrument_ids=("FUT_BR",),
        spark_master="local[4]",
        raise_on_error=False,
    )

    assert report["success"] is False
    assert captured["selection"] == list(research_assets.MOEX_RESEARCH_INDICATOR_SIDECAR_ASSETS)
    assert set(captured["run_config"]["ops"]) == {"continuous_front_indicator_acceptance_report"}
    sidecar_config = captured["run_config"]["ops"]["continuous_front_indicator_acceptance_report"][
        "config"
    ]
    assert sidecar_config["dataset_instrument_ids"] == ["FUT_BR"]
    assert sidecar_config["spark_master"] == "local[4]"


@pytest.mark.parametrize(
    "runner",
    (
        research_assets.materialize_moex_cf_rebuild_assets,
        research_assets.materialize_moex_research_bar_rebuild_assets,
        research_assets.materialize_moex_indicator_rebuild_assets,
        research_assets.materialize_moex_derived_indicator_rebuild_assets,
        research_assets.materialize_moex_indicator_sidecar_assets,
    ),
)
def test_moex_continuous_front_rebuild_rejects_contract_scope(tmp_path: Path, runner) -> None:
    with pytest.raises(ValueError, match="dataset_contract_ids is not supported"):
        runner(
            canonical_output_dir=tmp_path / "canonical",
            research_output_dir=tmp_path / "research",
            dataset_version="run-data-layer",
            timeframes=("15m",),
            series_mode="continuous_front",
            dataset_contract_ids=("BRM6",),
        )


@pytest.mark.parametrize(
    "runner",
    (
        research_assets.materialize_moex_indicator_rebuild_assets,
        research_assets.materialize_moex_derived_indicator_rebuild_assets,
    ),
)
def test_moex_indicator_rebuild_wrappers_reject_unsupported_scope_knobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner
) -> None:
    monkeypatch.setattr(
        research_assets,
        "_materialize_moex_layer_rebuild_assets",
        lambda **_: {"success": True, "materialized_assets": [], "output_paths": {}},
    )

    with pytest.raises(ValueError, match="does not support scoped rebuild parameters"):
        runner(
            canonical_output_dir=tmp_path / "canonical",
            research_output_dir=tmp_path / "research",
            dataset_version="run-data-layer",
            timeframes=("15m",),
            start_ts="2021-04-01T00:00:00Z",
            end_ts="2021-04-02T00:00:00Z",
            warmup_bars=300,
            split_method="holdout",
        )


@pytest.mark.parametrize(
    "runner",
    (
        research_assets.materialize_moex_cf_rebuild_assets,
        research_assets.materialize_moex_research_bar_rebuild_assets,
        research_assets.materialize_moex_indicator_sidecar_assets,
    ),
)
def test_moex_layer_rebuild_wrappers_forward_rebuild_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runner
) -> None:
    captured: dict[str, object] = {}

    def _fake_materialize(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"success": True, "materialized_assets": [], "output_paths": {}, "rows_by_table": {}}

    monkeypatch.setattr(
        research_assets,
        "_materialize_moex_layer_rebuild_assets",
        _fake_materialize,
    )

    runner(
        canonical_output_dir=tmp_path / "canonical",
        dataset_version="run-data-layer",
        timeframes=("15m",),
        start_ts="2021-04-01T00:00:00Z",
        end_ts="",
        warmup_bars=300,
        split_method="holdout",
        dataset_instrument_ids=("FUT_BR",),
        spark_master="local[4]",
    )

    assert captured["start_ts"] == "2021-04-01T00:00:00Z"
    assert captured["end_ts"] == ""
    assert captured["warmup_bars"] == 300
    assert captured["split_method"] == "holdout"
    assert captured["dataset_instrument_ids"] == ("FUT_BR",)
    assert captured["spark_master"] == "local[4]"


def test_scoped_indicator_rebuild_preserves_unrequested_partitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_partition = IndicatorFramePartitionKey(
        dataset_version="run-data-layer",
        indicator_set_version="indicators-v1",
        timeframe="15m",
        instrument_id="FUT_BR",
        contour_id="pit_active_front",
        series_mode="continuous_front",
        series_id="FUT_BR",
    )
    unrelated_partition = IndicatorFramePartitionKey(
        dataset_version="run-data-layer",
        indicator_set_version="indicators-v1",
        timeframe="1h",
        instrument_id="FUT_RTS",
        contour_id="pit_active_front",
        series_mode="continuous_front",
        series_id="FUT_RTS",
    )
    (tmp_path / "indicators" / "research_indicator_frames.delta" / "_delta_log").mkdir(parents=True)

    class _Profile:
        version = "core_v1"

        def expected_output_columns(self) -> tuple[str, ...]:
            return ("rsi_14",)

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        indicator_materialize,
        "_load_dataset_manifest",
        lambda **_: {
            "series_mode": "continuous_front",
            "bars_hash": "bars-v2",
            "created_at": "2026-06-10T00:00:00Z",
        },
    )
    monkeypatch.setattr(indicator_materialize, "_load_adjustment_ladder_rows", lambda **_: ())

    def _fake_partition_counts(**kwargs: object) -> dict[IndicatorFramePartitionKey, int]:
        assert kwargs["timeframes"] == ("15m",)
        assert kwargs["dataset_instrument_ids"] == ("FUT_BR",)
        return {target_partition: 1}

    monkeypatch.setattr(indicator_materialize, "_load_bar_partition_counts", _fake_partition_counts)
    monkeypatch.setattr(
        indicator_materialize, "_latest_delta_commit_timestamp", lambda *_, **__: None
    )
    monkeypatch.setattr(indicator_materialize, "load_indicator_partition_metadata", lambda **_: [])
    monkeypatch.setattr(
        indicator_materialize,
        "_group_existing_partition_metadata",
        lambda **_: {unrelated_partition: {"row_count": 1, "source_bars_hash": "old"}},
    )
    monkeypatch.setattr(
        indicator_materialize, "existing_indicator_value_columns", lambda **_: {"rsi_14"}
    )
    monkeypatch.setattr(indicator_materialize, "indicator_output_columns_hash", lambda _: "cols-v1")
    monkeypatch.setattr(indicator_materialize, "_load_bar_partition_rows", lambda **_: [object()])
    monkeypatch.setattr(indicator_materialize, "_bars_hash", lambda *_, **__: "bars-v2")
    monkeypatch.setattr(indicator_materialize, "_ladder_rows_for_series", lambda *_, **__: ())
    monkeypatch.setattr(indicator_materialize, "_profile_requires_volume_profile", lambda _: False)
    monkeypatch.setattr(indicator_materialize, "_build_partition_rows", lambda **_: [])
    monkeypatch.setattr(
        indicator_materialize,
        "indicator_store_contract",
        lambda **_: {"research_indicator_frames": {"columns": []}},
    )

    def _fake_write(**kwargs: object) -> tuple[dict[str, str], int, int]:
        captured["delete_partitions"] = tuple(kwargs["delete_partitions"])
        return {"research_indicator_frames": "memory://research_indicator_frames"}, 0, 0

    monkeypatch.setattr(
        indicator_materialize,
        "write_indicator_frame_partition_batches",
        _fake_write,
    )

    report = indicator_materialize.materialize_indicator_frames(
        dataset_output_dir=tmp_path / "datasets",
        indicator_output_dir=tmp_path / "indicators",
        dataset_version="run-data-layer",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
        profile=_Profile(),
        timeframes=("15m",),
        dataset_instrument_ids=("FUT_BR",),
    )

    assert report["deleted_partition_count"] == 0
    assert captured.get("delete_partitions", ()) == ()
    assert unrelated_partition.instrument_id == "FUT_RTS"


def test_scoped_derived_rebuild_preserves_unrequested_partitions_and_passes_spark_master(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    unrelated_partition = DerivedIndicatorFramePartitionKey(
        dataset_version="run-data-layer",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        timeframe="1h",
        instrument_id="FUT_RTS",
        contour_id="pit_active_front",
        series_mode="continuous_front",
        series_id="FUT_RTS",
    )
    (tmp_path / "indicators" / "research_indicator_frames.delta" / "_delta_log").mkdir(parents=True)
    (tmp_path / "derived" / "research_derived_indicator_frames.delta" / "_delta_log").mkdir(
        parents=True
    )

    class _Profile:
        version = "core_v1"
        output_columns = ("derived_signal",)

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        derived_materialize,
        "_load_dataset_manifest",
        lambda **_: {"series_mode": "continuous_front", "bars_hash": "bars-v2"},
    )
    monkeypatch.setattr(
        derived_materialize.cf_input_projection,
        "load_adjustment_ladder_rows",
        lambda **_: (),
    )
    monkeypatch.setattr(
        derived_materialize, "_source_indicator_columns_for_profile", lambda _: ("rsi_14",)
    )
    monkeypatch.setattr(
        derived_materialize, "existing_indicator_value_columns", lambda **_: {"rsi_14"}
    )
    monkeypatch.setattr(
        derived_materialize, "_latest_delta_commit_timestamp", lambda *_, **__: None
    )

    def _fake_source_job(**kwargs: object) -> dict[str, object]:
        captured["source_job_kwargs"] = dict(kwargs)
        return {
            "success": True,
            "rows_by_table": {"research_derived_source_frames": 0},
        }

    monkeypatch.setattr(
        derived_materialize,
        "run_research_derived_source_frames_spark_job",
        _fake_source_job,
    )
    monkeypatch.setattr(
        derived_materialize,
        "load_derived_source_frame_partition_metadata",
        lambda **_: [],
    )
    monkeypatch.setattr(
        derived_materialize, "load_derived_indicator_partition_metadata", lambda **_: []
    )
    monkeypatch.setattr(
        derived_materialize,
        "_group_existing_partition_metadata",
        lambda **_: {unrelated_partition: [{"row_count": 1}]},
    )
    monkeypatch.setattr(
        derived_materialize, "existing_derived_indicator_value_columns", lambda **_: set()
    )
    monkeypatch.setattr(
        derived_materialize, "derived_indicator_output_columns_hash", lambda _: "cols-v1"
    )
    monkeypatch.setattr(
        derived_materialize,
        "research_derived_source_frame_store_contract",
        lambda **_: {"research_derived_source_frames": {"columns": []}},
    )
    monkeypatch.setattr(
        derived_materialize,
        "research_derived_indicator_store_contract",
        lambda **_: {"research_derived_indicator_frames": {"columns": []}},
    )

    def _fake_write(**kwargs: object) -> tuple[dict[str, str], int, int]:
        captured["replace_partitions"] = tuple(kwargs["replace_partitions"])
        return {"research_derived_indicator_frames": "memory://derived"}, 0, 0

    monkeypatch.setattr(
        derived_materialize,
        "write_derived_indicator_frame_batches",
        _fake_write,
    )

    report = derived_materialize.materialize_derived_indicator_frames(
        dataset_output_dir=tmp_path / "datasets",
        indicator_output_dir=tmp_path / "indicators",
        derived_indicator_output_dir=tmp_path / "derived",
        dataset_version="run-data-layer",
        indicator_set_version="indicators-v1",
        derived_indicator_set_version="derived-v1",
        contour_id="pit_active_front",
        profile=_Profile(),
        spark_master="local[4]",
        timeframes=("15m",),
        dataset_instrument_ids=("FUT_BR",),
    )

    assert captured["source_job_kwargs"]["spark_master"] == "local[4]"
    assert captured["source_job_kwargs"]["timeframes"] == ("15m",)
    assert captured["source_job_kwargs"]["dataset_instrument_ids"] == ("FUT_BR",)
    assert report["deleted_partition_count"] == 0
    assert captured.get("replace_partitions", ()) == ()
