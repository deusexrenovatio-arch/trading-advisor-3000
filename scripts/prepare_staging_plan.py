"""Native staging recipe adapter. Preparation only; never starts a producer."""

from __future__ import annotations

from pathlib import Path


def native_research_run_config(assets, fixture, root, run_id):
    """Capture the existing Research recipe; admission reads fixture metadata only."""
    from trading_advisor_3000.product_plane.research import admission

    windows = [
        dict(
            instrument_id=instrument,
            timeframe=timeframe,
            start_ts=fixture["start_utc"],
            end_ts=fixture["end_utc"],
        )
        for instrument in fixture["instrument_ids"]
        for timeframe in fixture["timeframes"]
    ]
    config = assets._build_moex_cf_catch_up_run_config(
        _admission_probe=True,
        canonical_output_dir=Path(fixture["canonical_dir"]),
        dataset_version=fixture["dataset_version"],
        campaign_run_id=run_id,
        data_snapshot_manifest_path=fixture["data_manifest_path"],
        data_snapshot_raw_output_dir=fixture["raw_dir"],
        windows=windows,
    )["ops"]["continuous_front_bars"]["config"]
    materialized, results = assets._resolve_research_output_dirs(
        materialized_output_dir=root / "research/gold/current",
        results_output_dir=root / "research/runs/data-prep",
    )
    config.update(
        registry_root=assets.research_registry_root(materialized_root=materialized).as_posix(),
        materialized_output_dir=materialized.as_posix(),
        results_output_dir=results.as_posix(),
        canonical_contract_economics_path=(
            Path(fixture["canonical_dir"]) / "canonical_contract_economics.delta"
        ).as_posix(),
        volume_profile_raw_1m_table_path=(
            Path(fixture["raw_dir"]) / "raw_moex_history.delta"
        ).as_posix(),
        # The native builder derives the explicit fixture domain from these windows;
        # a cold rebuild admits that domain instead of daily refresh windows.
        changed_windows=[],
    )
    config = admission.configure_admission(config, operation="rebuild")
    return assets._research_data_prep_run_config_from_research_config(config)


def prepare(plan):
    import shutil
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from trading_advisor_3000.dagster_defs import moex_historical_assets as data
    from trading_advisor_3000.dagster_defs import research_assets as research
    from trading_advisor_3000.product_plane.common.data_snapshots import current_snapshot
    from trading_advisor_3000.product_plane.common.staging_test_fixture import verify_fixture
    from trading_advisor_3000.product_plane.research.data_snapshots import (
        _registry,
        snapshot_registry,
    )
    from trading_advisor_3000.product_plane.research.io import ResearchSliceRequest

    root = Path("/ta3000-data/moex-historical")
    if plan["stage"] == "branch":
        fixture = verify_fixture(Path("/staging-control/fixture"), plan["fixture_sha256"])
        plan["input_pins"] = {
            "fixture": plan["fixture_sha256"],
            "data": fixture["data_snapshot_id"],
        }
        if plan["target"] == "research":
            plan["run_config"] = native_research_run_config(
                research, fixture, root, plan["root_run_id"]
            )
            plan["jobs"] = ["research_data_prep_job"]
        else:
            if not Path("/staging-control/fixture/raw-ingest-report.json").is_file():
                raise ValueError("Miniature raw ingest input report missing; prepare fixture first")
            output = root / "canonical"
            paths = data.moex_historical_output_paths(output)
            for name in ("canonical_session_intervals", "canonical_closed_session_dates"):
                target = Path(paths.get(name, output / "delta" / f"{name}.delta"))
                shutil.copytree(Path(fixture["canonical_dir"]) / f"{name}.delta", target)
            plan["run_config"] = data.build_moex_data_rebuild_run_config(
                profile_name="canonical_from_existing_raw",
                canonical_run_id=plan["root_run_id"],
                canonical_output_dir=output,
                raw_table_path=Path(fixture["raw_dir"]) / "raw_moex_history.delta",
                raw_ingest_report_path=Path("/staging-control/fixture/raw-ingest-report.json"),
                publish_mode="staging_only",
                downstream_mode="invalidate",
            )
            config = plan["run_config"]["ops"]["moex_data_rebuild"]["config"]
            config["canonical_closed_session_dates_path"] = str(
                output / "delta/canonical_closed_session_dates.delta"
            )
            plan["jobs"] = ["moex_data_rebuild_job"]
            plan["data_table"] = paths["canonical_bars"]
            plan["data_manifest"] = str(
                data._default_data_rebuild_manifest_path(config, plan["root_run_id"])
            )
    else:
        plan["run_config"] = data._build_baseline_daily_schedule_run_config(
            SimpleNamespace(scheduled_execution_time=datetime.now(UTC))
        )
        plan["run_config"]["ops"]["moex_baseline_update"]["config"]["run_id"] = plan["root_run_id"]
        before = current_snapshot(root / "canonical/moex/baseline-4y-current/.data_snapshots")
        if before is None:
            raise ValueError("Working staging requires an accepted Data checkpoint")
        plan["input_pins"] = {"data": before["snapshot_id"], "tables": before["tables"]}
        plan["jobs"] = [
            "moex_baseline_update_job",
            "moex_cf_catch_up_job",
            "research_data_prep_job",
        ]
    if plan["target"] != "data":
        request = ResearchSliceRequest(**plan["consumer"])
        if (
            not request.instrument_ids
            or not request.price_columns
            or not request.indicator_columns
            or not request.derived_columns
        ):
            raise ValueError("Explicit price/L1/L2 consumer coverage required")
        registry = snapshot_registry(
            root / "research/gold/current", request.dataset_version, request.contour_id
        )
        plan["research_registry"] = str(registry)
        if plan["stage"] == "branch":
            config = next(iter(plan["run_config"]["ops"].values()))["config"]
            if (
                _registry(config) != registry
                or set(request.instrument_ids) != set(fixture["instrument_ids"])
                or request.timeframe not in fixture["timeframes"]
                or getattr(request, "warmup_bars", 0) > 512
                or request.indicator_set_version != config["indicator_set_version"]
                or request.derived_indicator_set_version != config["derived_indicator_set_version"]
            ):
                raise ValueError("Consumer does not match bounded native fixture recipe")
    return plan


if __name__ == "__main__":
    import json
    import sys

    print(json.dumps(prepare(json.loads(Path(sys.argv[1]).read_text()))))
