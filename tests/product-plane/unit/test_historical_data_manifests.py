from __future__ import annotations

from trading_advisor_3000.product_plane.data_plane.schemas import historical_data_delta_schema_manifest
from trading_advisor_3000.dagster_defs.historical_data_proof_assets import (
    HISTORICAL_DATA_PROOF_ASSETS,
    build_historical_data_proof_definitions,
    historical_data_proof_asset_specs,
)
from trading_advisor_3000.spark_jobs.canonical_bars_job import (
    build_contracts_sql_plan,
    build_instruments_sql_plan,
    build_roll_map_sql_plan,
    build_session_calendar_sql_plan,
    build_sql_plan,
)


def test_delta_schema_manifest_contains_required_tables() -> None:
    manifest = historical_data_delta_schema_manifest()
    assert {
        "raw_market_backfill",
        "canonical_bars",
        "canonical_instruments",
        "canonical_contracts",
        "canonical_session_calendar",
        "canonical_roll_map",
    } <= set(manifest)
    assert manifest["canonical_bars"]["format"] == "delta"
    canonical_columns = manifest["canonical_bars"]["columns"]
    assert {"contract_id", "instrument_id", "timeframe", "ts", "open_interest"} <= set(canonical_columns)
    assert "ts_open" not in canonical_columns
    assert "ts_close" not in canonical_columns


def test_dagster_asset_specs_declared() -> None:
    keys = {spec.key for spec in historical_data_proof_asset_specs()}
    assert keys == {
        "raw_market_backfill",
        "canonical_bars",
        "canonical_instruments",
        "canonical_contracts",
        "canonical_session_calendar",
        "canonical_roll_map",
    }


def test_historical_data_dagster_definitions_load_with_materialization_job() -> None:
    defs = build_historical_data_proof_definitions()
    repository = defs.get_repository_def()
    job = repository.get_job("historical_data_proof_materialization_job")
    assert job.name == "historical_data_proof_materialization_job"
    assert {asset.key.path[-1] for asset in HISTORICAL_DATA_PROOF_ASSETS} == {
        "raw_market_backfill",
        "canonical_bars",
        "canonical_instruments",
        "canonical_contracts",
        "canonical_session_calendar",
        "canonical_roll_map",
    }


def test_spark_sql_plan_contains_dedup_window() -> None:
    sql = build_sql_plan()
    assert "ts_open AS ts" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "ORDER BY ts_close DESC" in sql
    assert "WHERE rn = 1" in sql

    instruments_sql = build_instruments_sql_plan()
    assert "SELECT DISTINCT instrument_id" in instruments_sql

    contracts_sql = build_contracts_sql_plan()
    assert "GROUP BY contract_id" in contracts_sql

    calendar_sql = build_session_calendar_sql_plan()
    assert "to_date(ts_open) AS session_date" in calendar_sql

    roll_sql = build_roll_map_sql_plan()
    assert "ORDER BY open_interest DESC, ts_close DESC" in roll_sql
