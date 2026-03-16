from __future__ import annotations

from trading_advisor_3000.app.data_plane.schemas import phase2a_delta_schema_manifest
from trading_advisor_3000.dagster_defs.phase2a_assets import phase2a_asset_specs
from trading_advisor_3000.spark_jobs.canonical_bars_job import build_sql_plan


def test_delta_schema_manifest_contains_required_tables() -> None:
    manifest = phase2a_delta_schema_manifest()
    assert {"raw_market_backfill", "canonical_bars"} <= set(manifest)
    assert manifest["canonical_bars"]["format"] == "delta"


def test_dagster_asset_specs_declared() -> None:
    keys = {spec.key for spec in phase2a_asset_specs()}
    assert keys == {"raw_market_backfill", "canonical_bars"}


def test_spark_sql_plan_contains_dedup_window() -> None:
    sql = build_sql_plan()
    assert "ROW_NUMBER() OVER" in sql
    assert "WHERE rn = 1" in sql
