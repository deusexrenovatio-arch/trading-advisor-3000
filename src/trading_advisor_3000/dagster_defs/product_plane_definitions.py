from __future__ import annotations

from dagster import Definitions

from .moex_historical_assets import (
    MOEX_BASELINE_DAILY_SCHEDULE_NAME,
    MOEX_BASELINE_UPDATE_JOB_NAME,
    MOEX_DATA_REBUILD_JOB_NAME,
    MOEX_HISTORICAL_ASSETS,
    assert_moex_historical_definitions_executable,
    moex_baseline_daily_update_schedule,
    moex_baseline_update_job,
    moex_data_rebuild_job,
)
from .research_assets import (
    MOEX_CF_REBUILD_JOB_NAME,
    MOEX_DERIVED_INDICATOR_REBUILD_JOB_NAME,
    MOEX_INDICATOR_REBUILD_JOB_NAME,
    MOEX_RESEARCH_BAR_REBUILD_JOB_NAME,
    RESEARCH_ASSETS,
    RESEARCH_BACKTEST_JOB_NAME,
    RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME,
    RESEARCH_PROJECTION_JOB_NAME,
    STRATEGY_REGISTRY_REFRESH_JOB_NAME,
    assert_research_definitions_executable,
    moex_cf_rebuild_job,
    moex_derived_indicator_rebuild_job,
    moex_indicator_rebuild_job,
    moex_research_bar_rebuild_job,
    research_backtest_job,
    research_projection_job,
    strategy_registry_refresh_job,
)

PRODUCT_PLANE_NIGHTLY_JOB_NAMES = (
    MOEX_BASELINE_UPDATE_JOB_NAME,
    MOEX_DATA_REBUILD_JOB_NAME,
    MOEX_CF_REBUILD_JOB_NAME,
    MOEX_RESEARCH_BAR_REBUILD_JOB_NAME,
    MOEX_INDICATOR_REBUILD_JOB_NAME,
    MOEX_DERIVED_INDICATOR_REBUILD_JOB_NAME,
    STRATEGY_REGISTRY_REFRESH_JOB_NAME,
    RESEARCH_BACKTEST_JOB_NAME,
    RESEARCH_PROJECTION_JOB_NAME,
)


product_plane_definitions = Definitions(
    assets=[
        *MOEX_HISTORICAL_ASSETS,
        *RESEARCH_ASSETS,
    ],
    jobs=[
        moex_baseline_update_job,
        moex_data_rebuild_job,
        moex_cf_rebuild_job,
        moex_research_bar_rebuild_job,
        moex_indicator_rebuild_job,
        moex_derived_indicator_rebuild_job,
        strategy_registry_refresh_job,
        research_backtest_job,
        research_projection_job,
    ],
    schedules=[moex_baseline_daily_update_schedule],
    sensors=[],
)


def assert_product_plane_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or product_plane_definitions
    assert_moex_historical_definitions_executable(defs)
    assert_research_definitions_executable(defs)

    repository = defs.get_repository_def()
    for job_name in PRODUCT_PLANE_NIGHTLY_JOB_NAMES:
        repository.get_job(job_name)

    schedule_names = {schedule.name for schedule in repository.schedule_defs}
    if MOEX_BASELINE_DAILY_SCHEDULE_NAME not in schedule_names:
        raise RuntimeError(
            "product-plane Dagster definitions are missing the MOEX baseline daily schedule"
        )

    sensor_names = {sensor.name for sensor in repository.sensor_defs}
    if RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME in sensor_names:
        raise RuntimeError(
            "product-plane Dagster definitions must not auto-trigger research data-prep "
            "after MOEX baseline update"
        )


def build_product_plane_definitions() -> Definitions:
    assert_product_plane_definitions_executable(product_plane_definitions)
    return product_plane_definitions
